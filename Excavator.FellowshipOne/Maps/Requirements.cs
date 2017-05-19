using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using OrcaMDF.Core.MetaData;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;

namespace Excavator.F1
{
    public partial class F1Component
    {
        private const string APPLICATION_ON_FILE_REQUIREMENT_NAME = "Application on File";
        private const string DRIVING_RECORD_CLEARANCE_REQUIREMENT_NAME = "Driving Record Clearance";
        private const string MEGANS_LAW_CLEARANCE_REQUIREMENT_NAME = "Megan's Law Clearance";
        private const string CIA_CLEARANCE_REQUIREMENT_NAME = "CIA Clearance";
        private const string F1_CONFIDENTIALITY_STAREMENT_REQUIREMENT_NAME = "F1 Confidentiality Statement";
        private const string COPY_OF_ID_ON_FILE_REQUIREMENT_NAME = "Copy of ID on file";
        private const string COPY_OF_DL_ON_FILE_REQUIREMENT_NAME = "Copy of DL on File";

        private void MapRequirements( IQueryable<Row> tableData )
        {
            var lookupContext = new RockContext();
            var personService = new PersonService( lookupContext );
            var attributeService = new AttributeService( lookupContext );

            var personAttributes = attributeService.GetByEntityTypeId( PersonEntityTypeId ).ToList();

            var applicationOnFileStatusAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "ApplicationOnFileStatus", StringComparison.InvariantCultureIgnoreCase ) ) );
            var applicationOnFileDateAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "ApplicationOnFileDate", StringComparison.InvariantCultureIgnoreCase ) ) );
            var applicationOnFileFileAttribte = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "ApplicationOnFileFile", StringComparison.InvariantCultureIgnoreCase ) ) );

            var idDocumentDateAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "IdDocumentAddedDate", StringComparison.InvariantCultureIgnoreCase ) ) );
            var idDocumentFile = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "IdDocumentFile", StringComparison.InvariantCultureIgnoreCase ) ) );

            var f1ConfidentialityDateAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "F1ConfidentialityDocumentDate", StringComparison.InvariantCultureIgnoreCase ) ) );
            var f1ConfidentintialityDocumentFile = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "F1ConfidentialityDocumentFile", StringComparison.InvariantCultureIgnoreCase ) ) );

            var drivingClearedDateAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "DrivingClearedDate", StringComparison.InvariantCultureIgnoreCase ) ) );

            var backgroundCheckDateAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "BackgroundCheckDate", StringComparison.InvariantCultureIgnoreCase ) ) );
            var backgroundCheckDocumentAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "BackgroundCheckDocument", StringComparison.InvariantCultureIgnoreCase ) ) );
            var backgroundCheckedAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "BackgroundChecked", StringComparison.InvariantCultureIgnoreCase ) ) );
            var backgroundCheckResultAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "BackgroundCheckResult", StringComparison.InvariantCultureIgnoreCase ) ) );

            var newPeopleAttributes = new Dictionary<int, Person>();


            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying requirement value import ({0:N0} found to import in total.", totalRows ) );

            //TODO Look at the line before this
            foreach ( var groupedRows in tableData.GroupBy( r => r[RequirementColumnNames.IndividualId] as int? ) )
            {
                foreach ( var row in groupedRows.Where( r => r != null ) )
                {

                    var individualId = row[RequirementColumnNames.IndividualId] as int?;

                    if ( !individualId.HasValue )
                    {
                        continue;
                    }
                    var matchingPerson = GetPersonKeys( individualId, null, false );

                    Person person;
                    if ( matchingPerson != null )
                    {
                        if ( !newPeopleAttributes.ContainsKey( matchingPerson.PersonId ) )
                        {
                            // not in dictionary, get person from database
                            person = personService.Queryable( true ).FirstOrDefault( p => p.Id == matchingPerson.PersonId );
                        }
                        else
                        {
                            // reuse person from dictionary
                            person = newPeopleAttributes[matchingPerson.PersonId];
                        }
                    }
                    else
                    {
                        person = personService.Queryable( true ).FirstOrDefault( p => p.ForeignId == individualId.Value );
                    }

                    if ( matchingPerson != null )
                    {
                        var requirementDate = row[RequirementColumnNames.RequirementDate].ToString().AsDateTime();
                        if (requirementDate == null)
                        {
                            throw new Exception("Date is null!!!!");
                        }
                        string requirementName = row[RequirementColumnNames.RequuirementName].ToString();
                        string requirementStatus = row[RequirementColumnNames.RequirementStatusName].ToString();

                        if ( person != null )
                        {
                            person.LoadAttributes();

                            //
                            //Requirements handling here
                            //
                            switch ( requirementName )
                            {
                                case APPLICATION_ON_FILE_REQUIREMENT_NAME:
                                    if ( !person.Attributes.ContainsKey( applicationOnFileDateAttribute.Key ) || !person.Attributes.ContainsKey( applicationOnFileDateAttribute.Key ) )
                                    {
                                        AddOrUpdatePersonAttribute( applicationOnFileStatusAttribute, person,
                                            GetNewStatus( requirementStatus ) );
                                        AddOrUpdatePersonAttribute( applicationOnFileDateAttribute, person,
                                            requirementDate.ToString() );
                                    }
                                    else if ( IsDateMoreRecent( applicationOnFileDateAttribute, person, requirementDate ) )
                                    {
                                        AddOrUpdatePersonAttribute(applicationOnFileStatusAttribute, person,
                                            GetNewStatus( requirementStatus ));
                                        AddOrUpdatePersonAttribute(applicationOnFileDateAttribute, person,
                                            requirementDate.ToString());
                                    }
                                    break;
                                case DRIVING_RECORD_CLEARANCE_REQUIREMENT_NAME:
                                    if ( !person.Attributes.ContainsKey( drivingClearedDateAttribute.Key ) )
                                    {
                                        AddOrUpdatePersonAttribute( drivingClearedDateAttribute, person,
                                            requirementDate.ToString() );
                                    }
                                    else if ( IsDateMoreRecent( drivingClearedDateAttribute, person, requirementDate ) )
                                    {
                                        AddOrUpdatePersonAttribute(drivingClearedDateAttribute, person,
                                            requirementDate.ToString());
                                    }
                                    break;
                                case CIA_CLEARANCE_REQUIREMENT_NAME:
                                case MEGANS_LAW_CLEARANCE_REQUIREMENT_NAME:
                                    if ( !person.Attributes.ContainsKey( backgroundCheckDateAttribute.Key ) || !person.Attributes.ContainsKey( backgroundCheckResultAttribute.Key ) )
                                    {
                                        AddOrUpdatePersonAttribute( backgroundCheckResultAttribute, person,
                                            GetBackgroundStatus( requirementStatus ) );
                                        AddOrUpdatePersonAttribute( backgroundCheckDateAttribute, person,
                                            requirementDate.ToString() );
                                        AddOrUpdatePersonAttribute( backgroundCheckedAttribute, person, "True" );
                                    }
                                    else if ( IsDateMoreRecent( backgroundCheckDateAttribute, person, requirementDate ) )
                                    {
                                        AddOrUpdatePersonAttribute(backgroundCheckDateAttribute, person,
                                            requirementDate.ToString());
                                        AddOrUpdatePersonAttribute(backgroundCheckResultAttribute, person,
                                            GetBackgroundStatus( requirementStatus ));
                                    }
                                    break;
                                case F1_CONFIDENTIALITY_STAREMENT_REQUIREMENT_NAME:
                                    if ( !person.Attributes.ContainsKey( f1ConfidentialityDateAttribute.Key ) )
                                    {
                                        AddOrUpdatePersonAttribute( f1ConfidentialityDateAttribute, person,
                                            requirementDate.ToString() );
                                    }
                                    else if ( IsDateMoreRecent( f1ConfidentialityDateAttribute, person,
                                        requirementDate ) )
                                    {
                                        AddOrUpdatePersonAttribute( f1ConfidentialityDateAttribute, person,
                                            requirementDate.ToString() );
                                    }
                                    break;
                                case COPY_OF_DL_ON_FILE_REQUIREMENT_NAME:
                                case COPY_OF_ID_ON_FILE_REQUIREMENT_NAME:
                                    if ( !person.Attributes.ContainsKey( idDocumentDateAttribute.Key ) )
                                    {
                                        AddOrUpdatePersonAttribute( idDocumentDateAttribute, person,
                                            requirementDate.ToString() );
                                    }
                                    else if ( IsDateMoreRecent( idDocumentDateAttribute, person,
                                        requirementDate ) )
                                    {
                                        AddOrUpdatePersonAttribute( idDocumentDateAttribute, person,
                                            requirementDate.ToString() );
                                    }
                                    break;
                            }

                            if ( !newPeopleAttributes.ContainsKey( matchingPerson.PersonId ) )
                            {
                                newPeopleAttributes.Add( matchingPerson.PersonId, person );
                            }
                            else
                            {
                                newPeopleAttributes[matchingPerson.PersonId] = person;
                            }
                        }

                        completed++;

                        if ( completed % percentage < 1 )
                        {
                            int percentComplete = completed / percentage;
                            ReportProgress( percentComplete,
                                string.Format( "{0:N0} records imported ({1}% complete).", completed, percentComplete ) );
                        }
                        else if ( completed % ReportingNumber < 1 )
                        {
                            if ( newPeopleAttributes.Any() )
                            {
                                SaveAttributes( newPeopleAttributes );
                            }

                            // reset so context doesn't bloat
                            lookupContext = new RockContext();
                            personService = new PersonService( lookupContext );
                            newPeopleAttributes.Clear();
                            ReportPartialProgress();
                        }
                    }
                }
            }

            ReportProgress( 100, string.Format( "Finished attribute value import: {0:N0} records imported.", completed ) );
        }

        private static string GetNewStatus( string requirementStatus )
        {
            if ( requirementStatus == "Approved" )
            {
                return "Completed";
            }
            return requirementStatus;
        }

        private static string GetBackgroundStatus( string requirementStatus )
        {
            if ( requirementStatus == "Approved" || requirementStatus == "Completed" )
            {
                return "Pass";
            }
            return "Fail";
        }

        private static bool IsDateMoreRecent( AttributeCache dateAttribute, Person person, DateTime? date )
        {
            return person.GetAttributeValue( dateAttribute.Key ).AsDateTime() == null ||
                   ( date != null && person.GetAttributeValue( dateAttribute.Key ).AsDateTime() != null && person.GetAttributeValue( dateAttribute.Key ).AsDateTime() < date );
        }

        protected static void AddOrUpdatePersonAttribute( AttributeCache attribute, Person person, string value )
        {
            if ( attribute == null || person == null || string.IsNullOrWhiteSpace( value ) )
            {
                return;
            }
            if ( person.Attributes.ContainsKey( attribute.Key ) )
            {
                UpdatePersonAttribute( attribute, person, value );
            }
            else
            {
                AddPersonAttribute( attribute, person, value );
            }
        }

        protected static void UpdatePersonAttribute( AttributeCache attribute, Person person, string value )
        {
            if ( attribute != null && !string.IsNullOrWhiteSpace( value ) )
            {
                person.AttributeValues[attribute.Key].Value = value;
                person.SaveAttributeValues();
            }
        }


    }

    internal class RequirementColumnNames
    {
        public const string IndividualId = "Individual_ID";
        public const string RequuirementName = "Requirement_Name";
        public const string RequirementDate = "Requirement_Date";
        public const string RequirementStatusName = "Requirement_Status_Name";
    }

}
