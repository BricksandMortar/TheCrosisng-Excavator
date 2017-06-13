// <copyright>
// Copyright 2013 by the Spark Development Network
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>
//

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Data.Entity;
using System.Linq;
using Excavator.Utility;
using OrcaMDF.Core.Engine;
using OrcaMDF.Core.MetaData;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using Database = OrcaMDF.Core.Engine.Database;

namespace Excavator.F1
{
    /// <summary>
    /// This extends the base Excavator class to consume FellowshipOne's database model.
    /// Data models and mapping methods are in the Models and Maps directories, respectively.
    /// </summary>
    [Export( typeof( ExcavatorComponent ) )]
    public partial class F1Component : ExcavatorComponent
    {
        #region Fields

        /// <summary>
        /// Gets the full name of the excavator type.
        /// </summary>
        /// <value>
        /// The full name.
        /// </value>
        public override string FullName
        {
            get { return "FellowshipOne"; }
        }

        /// <summary>
        /// Gets the supported file extension type(s).
        /// </summary>
        /// <value>
        /// The supported extension type(s).
        /// </value>
        public override string ExtensionType
        {
            get { return ".mdf"; }
        }

        public const string BATCH_TABLE_NAME = "Batch";
        public const string USERS_TABLE_NAME = "Users";
        public const string COMMUNICATION_TABLE_NAME = "Communication";
        public const string ACCOUNT_TABLE_NAME = "Account";
        public const string COMPANY_TABLE_NAME = "Company";
        public const string CONTRIBUTION_TABLE_NAME = "Contribution";
        public const string ADDRESS_TABLE_NAME = "Household_Address";
        public const string INDIVIDUAL_HOUSEHOLD_TABLE_NAME = "Individual_Household";
        public const string NOTES_TABLE = "Notes";
        public const string PLEDGE_TABLE_NAME = "Pledge";
        public const string ATTRIBUTE_TABLE_NAME = "Attribute";
        public const string REQUIREMENTS_TABLE_NAME = "Requirement";
        public const string CONTACT_FORM_DATA_TABLE_NAME = "ContactFormData";

        /// <summary>
        /// The local database
        /// </summary>
        protected Database Database;

        /// <summary>
        /// The person assigned to do the import
        /// </summary>
        protected static int? ImportPersonAliasId;

        /// <summary>
        /// All the people who've been imported
        /// </summary>
        protected static List<PersonKeys> ImportedPeople;

        /// <summary>
        /// All imported batches. Used in Batches & Contributions
        /// </summary>
        protected static Dictionary<int, int?> ImportedBatches;

        /// <summary>
        /// All campuses
        /// </summary>
        protected static List<CampusCache> CampusList;

        // Existing entity types

        protected static int TextFieldTypeId;
        protected static int IntegerFieldTypeId;
        protected static int PersonEntityTypeId;
        protected static int GroupEntityTypeId;
        protected static int? AuthProviderEntityTypeId;

        // Custom attribute types

        protected static AttributeCache IndividualIdAttribute;
        protected static AttributeCache HouseholdIdAttribute;
        protected static AttributeCache InFellowshipLoginAttribute;
        protected static AttributeCache SecondaryEmailAttribute;
        protected static AttributeCache HouseholdPositionAttribute;

        #endregion Fields

        #region Methods

        /// <summary>
        /// Loads the database for this instance.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public override bool LoadSchema( string fileName )
        {
            Database = new Database( fileName );
            DataNodes = new List<DataNode>();
            var scanner = new DataScanner( Database );
            var tables = Database.Dmvs.Tables;

            foreach ( var table in tables.Where( t => !t.IsMSShipped ).OrderBy( t => t.Name ) )
            {
                var rows = scanner.ScanTable( table.Name );
                var tableItem = new DataNode();
                tableItem.Name = table.Name;

                var rowData = rows.FirstOrDefault();
                if ( rowData != null )
                {
                    foreach ( var column in rowData.Columns )
                    {
                        var childItem = new DataNode();
                        childItem.Name = column.Name;
                        childItem.NodeType = Extensions.GetSQLType( column.Type );
                        childItem.Value = rowData[column] ?? DBNull.Value;
                        childItem.Parent.Add( tableItem );
                        tableItem.Children.Add( childItem );
                    }
                }

                DataNodes.Add( tableItem );
            }

            return DataNodes.Count > 0 ? true : false;
        }

        /// <summary>
        /// Transforms the data from the dataset.
        /// </summary>
        /// <returns></returns>
        public override int TransformData( Dictionary<string, string> settings )
        {
            var importUser = settings["ImportUser"];

            ReportProgress( 0, "Starting health checks..." );
            var rockContext = new RockContext();
            var personService = new PersonService( rockContext );
            var importPerson = personService.GetByFullName( importUser, allowFirstNameOnly: true ).FirstOrDefault();

            if ( importPerson == null )
            {
                importPerson = personService.Queryable().AsNoTracking().FirstOrDefault();
            }

            ImportPersonAliasId = importPerson.PrimaryAliasId;
            var tableList = DataNodes.Where( n => n.Checked != false ).ToList();

            ReportProgress( 0, "Checking for existing attributes..." );
            LoadExistingRockData();

            ReportProgress( 0, "Checking for existing people..." );
            bool isValidImport = ImportedPeople.Any() || tableList.Any( n => n.Name.Equals( INDIVIDUAL_HOUSEHOLD_TABLE_NAME ) );

            var tableDependencies = new List<string>();
            tableDependencies.Add( BATCH_TABLE_NAME );                // needed to attribute contributions properly
            tableDependencies.Add( USERS_TABLE_NAME );                // needed for notes, user logins
            tableDependencies.Add( COMPANY_TABLE_NAME );              // needed to attribute any business items
            tableDependencies.Add( INDIVIDUAL_HOUSEHOLD_TABLE_NAME ); // needed for just about everything

            if ( isValidImport )
            {
                ReportProgress( 0, "Checking for table dependencies..." );
                // Order tables so non-dependents are imported first
                if ( tableList.Any( n => tableDependencies.Contains( n.Name ) ) )
                {
                    tableList = tableList.OrderByDescending( n => tableDependencies.IndexOf( n.Name ) ).ToList();
                }

                ReportProgress( 0, "Starting data import..." );
                var scanner = new DataScanner( Database );
                foreach ( var table in tableList )
                {
                    switch ( table.Name )
                    {
                        case ACCOUNT_TABLE_NAME:
                            MapBankAccount( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;

                        case BATCH_TABLE_NAME:
                            MapBatch( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;

                        case COMMUNICATION_TABLE_NAME:
                            MapCommunication( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;

                        case COMPANY_TABLE_NAME:
                            MapCompany( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;

                        case CONTRIBUTION_TABLE_NAME:
                            MapContribution( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;

                        case ADDRESS_TABLE_NAME:
                            MapFamilyAddress( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;

                        case INDIVIDUAL_HOUSEHOLD_TABLE_NAME:
                            MapPerson( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;

                        case NOTES_TABLE:
                            MapNotes( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;

                        case PLEDGE_TABLE_NAME:
                            MapPledge( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;

                        case USERS_TABLE_NAME:
                            MapUsers( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;

                        case ATTRIBUTE_TABLE_NAME:
                            MapAttributes( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;

                        case REQUIREMENTS_TABLE_NAME:
                            MapRequirements( scanner.ScanTable( table.Name ).AsQueryable() );
                            break;
                        case CONTACT_FORM_DATA_TABLE_NAME:
                            MapContactFormData(scanner.ScanTable(table.Name).AsQueryable());
                            break;
                    }
                }

                ReportProgress( 100, "Import completed.  " );
            }
            else
            {
                ReportProgress( 0, "No imported people exist. Please include the Individual_Household table during the import." );
            }

            return 100; // return total number of rows imported?
        }

        /// <summary>
        /// Loads Rock data that's used globally by the transform
        /// </summary>
        private void LoadExistingRockData()
        {
            var lookupContext = new RockContext();
            var attributeValueService = new AttributeValueService( lookupContext );
            var attributeService = new AttributeService( lookupContext );

            IntegerFieldTypeId = FieldTypeCache.Read( new Guid( Rock.SystemGuid.FieldType.INTEGER ) ).Id;
            TextFieldTypeId = FieldTypeCache.Read( new Guid( Rock.SystemGuid.FieldType.TEXT ) ).Id;
            PersonEntityTypeId = EntityTypeCache.Read( "Rock.Model.Person" ).Id;
            GroupEntityTypeId = EntityTypeCache.Read( "Rock.Model.Group" ).Id;
            CampusList = CampusCache.All();

            int attributeEntityTypeId = EntityTypeCache.Read( "Rock.Model.Attribute" ).Id;
            int batchEntityTypeId = EntityTypeCache.Read( "Rock.Model.FinancialBatch" ).Id;
            int userLoginTypeId = EntityTypeCache.Read( "Rock.Model.UserLogin" ).Id;

            int visitInfoCategoryId = new CategoryService( lookupContext ).GetByEntityTypeId( attributeEntityTypeId )
                .Where( c => c.Name == "Visit Information" ).Select( c => c.Id ).FirstOrDefault();

            // Look up and create attributes for F1 unique identifiers if they don't exist
            var personAttributes = attributeService.GetByEntityTypeId( PersonEntityTypeId ).AsNoTracking().ToList();

            var householdPosition =
                personAttributes.FirstOrDefault(
                    a => a.Key.Equals( "HouseHoldPosition", StringComparison.InvariantCultureIgnoreCase ) );
            if( householdPosition == null )
            {
                householdPosition = new Rock.Model.Attribute();
                householdPosition.Key = "HouseHoldPosition";
                householdPosition.Name = "F1 HouseHoldPosition";
                householdPosition.FieldTypeId = TextFieldTypeId;
                householdPosition.EntityTypeId = PersonEntityTypeId;
                householdPosition.EntityTypeQualifierValue = string.Empty;
                householdPosition.EntityTypeQualifierColumn = string.Empty;
                householdPosition.Description = "The person's household position in F1";
                householdPosition.DefaultValue = string.Empty;
                householdPosition.IsMultiValue = false;
                householdPosition.IsRequired = false;
                householdPosition.Order = 0;

                lookupContext.Attributes.Add( householdPosition );
                lookupContext.SaveChanges( DisableAuditing );
                personAttributes.Add( householdPosition );
            }

            var householdAttribute = personAttributes.FirstOrDefault( a => a.Key.Equals( "F1HouseholdId", StringComparison.InvariantCultureIgnoreCase ) );
            

            if ( householdAttribute == null )
            {
                householdAttribute = new Rock.Model.Attribute();
                householdAttribute.Key = "F1HouseholdId";
                householdAttribute.Name = "F1 Household Id";
                householdAttribute.FieldTypeId = IntegerFieldTypeId;
                householdAttribute.EntityTypeId = PersonEntityTypeId;
                householdAttribute.EntityTypeQualifierValue = string.Empty;
                householdAttribute.EntityTypeQualifierColumn = string.Empty;
                householdAttribute.Description = "The FellowshipOne household identifier for the person that was imported";
                householdAttribute.DefaultValue = string.Empty;
                householdAttribute.IsMultiValue = false;
                householdAttribute.IsRequired = false;
                householdAttribute.Order = 0;

                lookupContext.Attributes.Add( householdAttribute );
                lookupContext.SaveChanges( DisableAuditing );
                personAttributes.Add( householdAttribute );
            }

            var individualAttribute = personAttributes.FirstOrDefault( a => a.Key.Equals( "F1IndividualId", StringComparison.InvariantCultureIgnoreCase ) );
            if ( individualAttribute == null )
            {
                individualAttribute = new Rock.Model.Attribute();
                individualAttribute.Key = "F1IndividualId";
                individualAttribute.Name = "F1 Individual Id";
                individualAttribute.FieldTypeId = IntegerFieldTypeId;
                individualAttribute.EntityTypeId = PersonEntityTypeId;
                individualAttribute.EntityTypeQualifierValue = string.Empty;
                individualAttribute.EntityTypeQualifierColumn = string.Empty;
                individualAttribute.Description = "The FellowshipOne individual identifier for the person that was imported";
                individualAttribute.DefaultValue = string.Empty;
                individualAttribute.IsMultiValue = false;
                individualAttribute.IsRequired = false;
                individualAttribute.Order = 0;

                lookupContext.Attributes.Add( individualAttribute );
                lookupContext.SaveChanges( DisableAuditing );
                personAttributes.Add( individualAttribute );
            }

            var secondaryEmailAttribute = personAttributes.FirstOrDefault( a => a.Key.Equals( "SecondaryEmail", StringComparison.InvariantCultureIgnoreCase ) );
            if ( secondaryEmailAttribute == null )
            {
                secondaryEmailAttribute = new Rock.Model.Attribute();
                secondaryEmailAttribute.Key = "SecondaryEmail";
                secondaryEmailAttribute.Name = "Secondary Email";
                secondaryEmailAttribute.FieldTypeId = TextFieldTypeId;
                secondaryEmailAttribute.EntityTypeId = PersonEntityTypeId;
                secondaryEmailAttribute.EntityTypeQualifierValue = string.Empty;
                secondaryEmailAttribute.EntityTypeQualifierColumn = string.Empty;
                secondaryEmailAttribute.Description = "The secondary email for this person";
                secondaryEmailAttribute.DefaultValue = string.Empty;
                secondaryEmailAttribute.IsMultiValue = false;
                secondaryEmailAttribute.IsRequired = false;
                secondaryEmailAttribute.Order = 0;

                lookupContext.Attributes.Add( secondaryEmailAttribute );
                var visitInfoCategory = new CategoryService( lookupContext ).Get( visitInfoCategoryId );
                secondaryEmailAttribute.Categories.Add( visitInfoCategory );
                lookupContext.SaveChanges( DisableAuditing );
            }

            var infellowshipLoginAttribute = personAttributes.FirstOrDefault( a => a.Key.Equals( "InFellowshipLogin", StringComparison.InvariantCultureIgnoreCase ) );
            if ( infellowshipLoginAttribute == null )
            {
                infellowshipLoginAttribute = new Rock.Model.Attribute();
                infellowshipLoginAttribute.Key = "InFellowshipLogin";
                infellowshipLoginAttribute.Name = "InFellowship Login";
                infellowshipLoginAttribute.FieldTypeId = TextFieldTypeId;
                infellowshipLoginAttribute.EntityTypeId = PersonEntityTypeId;
                infellowshipLoginAttribute.EntityTypeQualifierValue = string.Empty;
                infellowshipLoginAttribute.EntityTypeQualifierColumn = string.Empty;
                infellowshipLoginAttribute.Description = "The InFellowship login for this person";
                infellowshipLoginAttribute.DefaultValue = string.Empty;
                infellowshipLoginAttribute.IsMultiValue = false;
                infellowshipLoginAttribute.IsRequired = false;
                infellowshipLoginAttribute.Order = 0;

                // don't add a category as this attribute is only used via the API
                lookupContext.Attributes.Add( infellowshipLoginAttribute );
                lookupContext.SaveChanges( DisableAuditing );
            }

            HouseholdPositionAttribute = AttributeCache.Read(householdPosition.Id);
            IndividualIdAttribute = AttributeCache.Read( individualAttribute.Id );
            HouseholdIdAttribute = AttributeCache.Read( householdAttribute.Id );
            InFellowshipLoginAttribute = AttributeCache.Read( infellowshipLoginAttribute.Id );
            SecondaryEmailAttribute = AttributeCache.Read( secondaryEmailAttribute.Id );

            // Set AuthProviderEntityTypeId if Apollos/Infellowship provider exists
            var f1AuthProvider = "cc.newspring.F1.Security.Authentication.F1Migrator";
            var cache = EntityTypeCache.Read( f1AuthProvider );
            AuthProviderEntityTypeId = cache == null ? (int?)null : cache.Id;

            var aliasIdList = new PersonAliasService( lookupContext ).Queryable().AsNoTracking()
                .Select( pa => new
                {
                    PersonAliasId = pa.Id,
                    PersonId = pa.PersonId,
                    IndividualId = pa.ForeignId,
                    FamilyRole = pa.Person.ReviewReasonNote
                } ).ToList();
            var householdIdList = attributeValueService.GetByAttributeId( householdAttribute.Id ).AsNoTracking()
                .Select( av => new
                {
                    PersonId = (int)av.EntityId,
                    HouseholdId = av.Value
                } ).ToList();
            var householdPositionList = attributeValueService.GetByAttributeId( householdPosition.Id ).AsNoTracking()
                .Select( av => new
                {
                    PersonId = ( int ) av.EntityId,
                    Position = av.Value
                } ).ToList();

            ImportedPeople = householdIdList
                .Join(householdPositionList, householdIds => householdIds.PersonId, householdPositions => householdPositions.PersonId, (houseHoldIds, householdPositions) => new {houseHold = houseHoldIds, householdPositions})
                .GroupJoin(aliasIdList, hhp => hhp.houseHold.PersonId, aliadIds => aliadIds.PersonId, (householdsWithPositions, aliasIds) => new {HouseHoldWithPositions = householdsWithPositions, aliases = aliasIds})
                .Select(a => new PersonKeys
                {
                    PersonAliasId = a.aliases.Select( x => x.PersonAliasId ).FirstOrDefault(),
                    PersonId = a.HouseHoldWithPositions.houseHold.PersonId,
                    IndividualId = a.aliases.Select( x => x.IndividualId ).FirstOrDefault(),
                    HouseholdId = a.HouseHoldWithPositions.houseHold.HouseholdId.AsType<int?>(),
                    FamilyRoleId = a.aliases.Select( x => x.FamilyRole.ConvertToEnum<FamilyRole>( 0 ) ).FirstOrDefault(),
                    HouseholdPosition = a.HouseHoldWithPositions.householdPositions.Position
                } )
                .ToList();
         
            ImportedBatches = new FinancialBatchService( lookupContext ).Queryable().AsNoTracking()
                .Where( b => b.ForeignId != null )
                .ToDictionary( t => (int)t.ForeignId, t => (int?)t.Id );
        }

        /// <summary>
        /// Gets the person keys.
        /// </summary>
        /// <param name="individualId">The individual identifier.</param>
        /// <param name="householdId">The household identifier.</param>
        /// <param name="includeVisitors">if set to <c>true</c> [include visitors].</param>
        /// <returns></returns>
        protected static PersonKeys GetPersonKeys( int? individualId = null, int? householdId = null, bool includeVisitors = true )
        {
            //If given an individual ID grab by that always
            if ( individualId != null )
            {
                return ImportedPeople.FirstOrDefault( p => p.IndividualId == individualId );
            }

            // Else handle a household id being provided
            if ( householdId != null )
            {
                var orderedFamily = ImportedPeople.Where( p => p.HouseholdId == householdId && ( includeVisitors || p.FamilyRoleId != FamilyRole.Visitor ) )
                                     .OrderByDescending( p => p.HouseholdPosition != null && p.HouseholdPosition.ToLower() == "head")
                                     .ThenByDescending( p => p.HouseholdPosition != null && p.HouseholdPosition.ToLower() == "spouse");
                return orderedFamily.FirstOrDefault();
            }
            return null;
        }

        /// <summary>
        /// Gets the family by household identifier.
        /// </summary>
        /// <param name="householdId">The household identifier.</param>
        /// <param name="includeVisitors">if set to <c>true</c> [include visitors].</param>
        /// <returns></returns>
        protected static List<PersonKeys> GetFamilyByHouseholdId( int? householdId, bool includeVisitors = true )
        {
            return ImportedPeople.Where( p => p.HouseholdId == householdId && ( includeVisitors || p.FamilyRoleId != FamilyRole.Visitor ) ).ToList();
        }

        #endregion Methods
    }

    #region Helper Classes

    /// <summary>
    /// Generic map interface
    /// </summary>
    public interface IFellowshipOne
    {
        void Map( IQueryable<Row> tableData );
    }

    /// <summary>
    /// Adapter helper method to call the right object Map()
    /// </summary>
    public static class IMapAdapterFactory
    {
        public static IFellowshipOne GetAdapter( string fileName )
        {
            IFellowshipOne adapter = null;

            //var configFileTypes = ConfigurationManager.GetSection( "binaryFileTypes" ) as NameValueCollection;

            // by default will assume a ministry document
            //var iBinaryFileType = typeof( IBinaryFile );
            //var mappedFileTypes = iBinaryFileType.Assembly.ExportedTypes
            //    .Where( p => iBinaryFileType.IsAssignableFrom( p ) && !p.IsInterface );
            //var selectedType = mappedFileTypes.FirstOrDefault( t => fileName.StartsWith( t.Name.RemoveWhitespace() ) );
            //if ( selectedType != null )
            //{
            //    adapter = (IBinaryFile)Activator.CreateInstance( selectedType );
            //}
            //else
            //{
            //    adapter = new MinistryDocument();
            //}

            return adapter;
        }
    }

    #endregion
}