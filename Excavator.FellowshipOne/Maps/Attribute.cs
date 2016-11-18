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
        private void MapAttributes( IQueryable<Row> tableData )
        {
            var lookupContext = new RockContext();
            var personService = new PersonService( lookupContext );
            var attributeService = new AttributeService( lookupContext );

            var personAttributes = attributeService.GetByEntityTypeId( PersonEntityTypeId ).ToList();

            var childSponsorshipLocationAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "ChildSponsorshipLocation", StringComparison.InvariantCultureIgnoreCase ) ) );
            var childSponsorshipStartDateAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "ChildSponsorshipStartDate", StringComparison.InvariantCultureIgnoreCase ) ) );
            var childSponsorshipNameAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "ChildSponsorshipName", StringComparison.InvariantCultureIgnoreCase ) ) );
            var leadershipDevelopmentAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "LeadershipDevelopment", StringComparison.InvariantCultureIgnoreCase ) ) );
            var paidChildcareWorkerAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "PaidChildcareWorker", StringComparison.InvariantCultureIgnoreCase ) ) );
            var hireDateAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "HireDate", StringComparison.InvariantCultureIgnoreCase ) ) );
            var w9Attribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "W9", StringComparison.InvariantCultureIgnoreCase ) ) );
            var childDedicationDateAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "ChildDedicationDate", StringComparison.InvariantCultureIgnoreCase ) ) );
            var photoConsentAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "PhotoConsent", StringComparison.InvariantCultureIgnoreCase ) ) );
            var baptismDateAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "BaptismDate", StringComparison.InvariantCultureIgnoreCase ) ) );


            var newPeopleAttributes = new Dictionary<int, Person>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying attribute value import ({0:N0} found to import in total.", totalRows ) );

            foreach ( var groupedRows in tableData.GroupBy<Row, int?>( r => r["Individual_Id"] as int? ) )
            {
                foreach ( var row in groupedRows.Where( r => r != null ) )
                {

                    int? individualId = row["Individual_Id"] as int?;

                    if ( individualId != null )
                    {
                        var matchingPerson = GetPersonKeys( individualId, null, includeVisitors: false );
                        
                        if ( matchingPerson != null )
                        {

                            DateTime? startDate = row["Start_Date"] as DateTime?;
                            DateTime? endDate = row["End_Date"] as DateTime?;
                            string comment = row["Comment"] as string;
                            int? staffIndividualId = row["Staff_Individual_ID"] as int?;
                            string attributeName = row["Attribute_Name"] as string;
                            
                            {
                                Person person = null;
                                
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

                                if ( person != null )
                                {
                                    if ( person.Attributes == null || person.AttributeValues == null )
                                    {
                                        // make sure we have valid objects to assign to
                                        person.Attributes = new Dictionary<string, AttributeCache>();
                                        person.AttributeValues = new Dictionary<string, AttributeValueCache>();
                                    }


                                    //Attribute case handling here

                                    //Child Sponsorship
                                    if ( attributeName.Contains( "Child Sponsorship" ) )
                                    {
                                        //Child Sponsorship Location
                                        if ( !person.Attributes.ContainsKey( childSponsorshipLocationAttribute.Key ) )
                                        {
                                            switch ( attributeName )
                                            {
                                                case "Child Sponsorship-El Salvador":
                                                    AddPersonAttribute( childSponsorshipLocationAttribute, person, "El Salvador" );
                                                    break;
                                                case "Child Sponsorship-India":
                                                    AddPersonAttribute( childSponsorshipLocationAttribute, person, "India" );
                                                    break;

                                                case "Child Sponsorship-India ServLife":
                                                    AddPersonAttribute( childSponsorshipLocationAttribute, person, "India ServLife" );
                                                    break;

                                                case "Child Sponsorship-Mexico":
                                                    AddPersonAttribute( childSponsorshipLocationAttribute, person, "Mexico" );
                                                    break;

                                                case "Child Sponsorship-Mexico (El Nino)":
                                                    AddPersonAttribute( childSponsorshipLocationAttribute, person, "Mexico (El Nino)" );
                                                    break;

                                                case "Child Sponsorship-Uganda":
                                                    AddPersonAttribute( childSponsorshipLocationAttribute, person, "Uganda" );
                                                    break;

                                                case "Child Sponsorship-Uganda-CSP":
                                                    AddPersonAttribute( childSponsorshipLocationAttribute, person, "Uganda-CSP" );
                                                    break;

                                                case "Child Sponsorship-Uganda-Mosaic":
                                                    AddPersonAttribute( childSponsorshipLocationAttribute, person, "Uganda-Mosaic" );
                                                    break;

                                                case "Child Sponsorship-Vietnam":
                                                    AddPersonAttribute( childSponsorshipLocationAttribute, person, "Vietnam" );
                                                    break;

                                                case "Child Sponsorship-Vietnam-Father's House":
                                                    AddPersonAttribute( childSponsorshipLocationAttribute, person, "Vietnam-Father's House" );
                                                    break;
                                            }
                                        }

                                        //Child Sponsorship Name
                                        if ( !person.Attributes.ContainsKey( childSponsorshipNameAttribute.Key ) && !string.IsNullOrWhiteSpace(comment) )
                                        {
                                            AddPersonAttribute( childSponsorshipNameAttribute, person, comment );
                                        }

                                        if ( !person.Attributes.ContainsKey( childSponsorshipStartDateAttribute.Key ) && startDate.HasValue )
                                        {
                                            AddPersonAttribute( childSponsorshipStartDateAttribute, person, startDate.Value.ToString( "o") );
                                        }
                                    }
                                    else if ( attributeName.Contains( "Leadership Development" ) && !person.Attributes.ContainsKey( leadershipDevelopmentAttribute.Key ) )
                                    {
                                        var matchingStaffPersonKeys = GetPersonKeys( individualId, null, includeVisitors: false );
                                        if (matchingStaffPersonKeys != null )
                                        {
                                            var matchingStaffPerson = personService.Get( matchingStaffPersonKeys.PersonId );
                                            if ( matchingStaffPerson != null )
                                            {
                                                AddPersonAttribute( leadershipDevelopmentAttribute, person, matchingStaffPerson.PrimaryAlias.Guid.ToString() );
                                            }
                                        }
                                    }
                                    else if (attributeName == "Paid Childcare Worker" && !person.Attributes.ContainsKey( paidChildcareWorkerAttribute.Key ) )
                                    {
                                        AddPersonAttribute( paidChildcareWorkerAttribute, person, "True" );
                                    }
                                    else if ( attributeName == "Photo Consent" && !person.Attributes.ContainsKey( photoConsentAttribute.Key ) )
                                    {
                                        AddPersonAttribute( photoConsentAttribute, person, "True" );
                                    }
                                    else if (attributeName.Contains( "W-9 on file (W-9)" ) && !person.Attributes.ContainsKey( w9Attribute.Key ) )
                                    {
                                        AddPersonAttribute( w9Attribute, person, "True" );
                                    }
                                    else if (attributeName.Contains( "Baptismal Date") && !person.Attributes.ContainsKey( baptismDateAttribute.Key ) && startDate.HasValue )
                                    {
                                        AddPersonAttribute( baptismDateAttribute, person, startDate.Value.ToString( "o" ));
                                    }
                                    else if ( attributeName.Contains( "Child Dedication Dat" ) && !person.Attributes.ContainsKey( childDedicationDateAttribute.Key ) )
                                    {
                                        AddPersonAttribute( childDedicationDateAttribute, person, startDate.Value.ToString( "o" ) );
                                    }
                                    else if ( attributeName.Contains( "Hire Date" ) && !person.Attributes.ContainsKey( hireDateAttribute.Key ))
                                    {
                                        AddPersonAttribute( hireDateAttribute, person, startDate.Value.ToString( "o" ) );
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
                            }

                            if ( completed % percentage < 1 )
                            {
                                int percentComplete = completed / percentage;
                                ReportProgress( percentComplete, string.Format( "{0:N0} records imported ({1}% complete).", completed, percentComplete ) );
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
            }

            ReportProgress( 100, string.Format( "Finished attribute value import: {0:N0} records imported.", completed ) );
        }

        private static void SaveAttributes( Dictionary<int, Person> updatedPersonList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;


                if ( updatedPersonList.Any() )
                {
                    foreach ( var person in updatedPersonList.Values.Where( p => p.Attributes.Any() ) )
                    {
                        // don't call LoadAttributes, it only rewrites existing cache objects
                        // person.LoadAttributes( rockContext );

                        foreach ( var attributeCache in person.Attributes.Select( a => a.Value ) )
                        {
                            var existingValue = rockContext.AttributeValues.FirstOrDefault( v => v.Attribute.Key == attributeCache.Key && v.EntityId == person.Id );
                            var newAttributeValue = person.AttributeValues[attributeCache.Key];

                            // set the new value and add it to the database
                            if ( existingValue == null )
                            {
                                existingValue = new AttributeValue();
                                existingValue.AttributeId = newAttributeValue.AttributeId;
                                existingValue.EntityId = person.Id;
                                existingValue.Value = newAttributeValue.Value;

                                rockContext.AttributeValues.Add( existingValue );
                            }
                            else if (existingValue.Value != newAttributeValue.Value)
                            {
                                existingValue.Value = newAttributeValue.Value;
                                rockContext.Entry( existingValue ).State = EntityState.Modified;
                            }
                        }
                    }
                }

                rockContext.ChangeTracker.DetectChanges();
                rockContext.SaveChanges( DisableAuditing );
            } );
        }

    }
    
}
