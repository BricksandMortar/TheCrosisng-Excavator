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

            var twitterAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "Twitter", StringComparison.InvariantCultureIgnoreCase ) ) );
            var facebookAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "Facebook", StringComparison.InvariantCultureIgnoreCase ) ) );
            var instagramAttribute = AttributeCache.Read( personAttributes.FirstOrDefault( a => a.Key.Equals( "Instagram", StringComparison.InvariantCultureIgnoreCase ) ) );

            var existingPersonAttributeValues = new AttributeValueService( lookupContext ).Queryable().Where( a => personAttributes.Select( pa => pa.Id ).Any( pa => pa == a.AttributeId ) ).ToList();
            var newPeopleAttributes = new Dictionary<int, Person>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying attribute value import ({0:N0} found, {1:N0} already exist).", totalRows, existingPersonAttributeValues.Count ) );

            //TODO Look at the line before this
            foreach ( var groupedRows in tableData.GroupBy<Row, int?>( r => r["Individual_Id"] as int? ) )
            {
                foreach ( var row in groupedRows.Where( r => r != null ) )
                {

                    int? individualId = row["Individual_ID"] as int?;

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
                                    person = personService.Queryable( includeDeceased: true ).FirstOrDefault( p => p.Id == matchingPerson.PersonId );
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

                                    if ( attributeName.Contains( "Twitter" ) && !person.Attributes.ContainsKey( twitterAttribute.Key ) )
                                    {
                                        AddPersonAttribute( twitterAttribute, person, "BLAH" );
                                    }
                                    else if ( attributeName.Contains( "Facebook" ) && !person.Attributes.ContainsKey( facebookAttribute.Key ) )
                                    {
                                       //
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
                            else
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
