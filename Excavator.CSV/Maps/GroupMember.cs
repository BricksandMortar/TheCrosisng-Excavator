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
using System.Data.Entity;
using System.Globalization;
using System.Linq;
using Excavator.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using Attribute = Rock.Model.Attribute;

namespace Excavator.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the People import methods
    /// </summary>
    partial class CSVComponent
    {
        #region Main Methods

        /// <summary>
        /// Loads the individual data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadGroupMapping( CSVInstance csvData )
        {
            var rockContext = new RockContext();

            // Set the supported date formats
            var dateFormats = new[] { "yyyy-MM-dd", "M/dd/yyyy", "MM/dd/yyyy" };

            int completed = 0;
            ReportProgress( 0, "Starting Group Member import " );

            var personService = new PersonService( rockContext );
            var groupService = new GroupService( rockContext );
            var groupTypeRoleService = new GroupTypeRoleService( rockContext );
            var attributeService = new AttributeService( rockContext );
            var groupMemberService = new GroupMemberService(rockContext);


            var newHistory = new List<History>();
            var newGroupMembers = new List<GroupMember>();
            int personEntityTypeId = EntityTypeCache.Read( typeof( Rock.Model.Person ) ).Id;
            int groupMemberEntityTypeId = EntityTypeCache.Read( typeof( Rock.Model.GroupMember ) ).Id;
            int groupEntityTypeId = EntityTypeCache.Read( typeof( Rock.Model.Group ) ).Id;
            int addToGroupCategoryId = new CategoryService( rockContext ).Queryable().FirstOrDefault( c => c.Name == "Group Membership" ).Id;

            string[] row;
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var groupId = row[15].AsIntegerOrNull();
                var individualId = row[2].AsIntegerOrNull();
                if ( !groupId.HasValue || !individualId.HasValue )
                {
                    continue;
                }
                var group = groupService.Get( groupId.Value );
                var person = personService.Queryable().FirstOrDefault( p => p.ForeignId == individualId );

                if ( person == null || group == null )
                {
                    continue;
                }

                string memberStatus = string.IsNullOrWhiteSpace( row[19] ) ? "Inactive" : row[19];

                if ( memberStatus == "Inactive" )
                {
                    var addedHistory = new History
                    {
                        EntityTypeId = personEntityTypeId,
                        EntityId = person.Id,
                        RelatedEntityTypeId = groupEntityTypeId,
                        RelatedEntityId = groupId,
                        CategoryId = addToGroupCategoryId,
                        CreatedDateTime = DateTime.ParseExact( row[20], dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None ),
                        Summary = "Added to group.",
                        Caption = groupService.Get( groupId.Value ).Name
                    };

                    var removedHistory = new History
                    {
                        EntityTypeId = personEntityTypeId,
                        EntityId = person.Id,
                        RelatedEntityTypeId = groupEntityTypeId,
                        RelatedEntityId = groupId,
                        CategoryId = addToGroupCategoryId,
                        CreatedDateTime = DateTime.ParseExact( row[21], dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None ),
                        Summary = "Removed from group.",
                        Caption = groupService.Get( groupId.Value ).Name
                    };

                    newHistory.Add( addedHistory );
                    newHistory.Add( removedHistory );
                }
                else
                {
                    var groupTypeId = row[14].AsIntegerOrNull();
                    string grouproleName = row[16];

                    if ( !groupTypeId.HasValue || string.IsNullOrWhiteSpace( grouproleName ) )
                    {
                        continue;
                    }

                    var attributes = attributeService.GetByEntityTypeId( groupMemberEntityTypeId ).Where( a => a.EntityTypeQualifierValue == groupTypeId.ToString() ).ToList();

                    var groupRole = groupTypeRoleService.GetByGroupTypeId( groupTypeId.Value )
                                                        .FirstOrDefault( r => r.Name == grouproleName );

                    var groupMember = groupMemberService.GetByGroupIdAndPersonId(groupId.Value, person.Id).FirstOrDefault();
                    if (groupMember == null)
                    {
                        groupMember = new GroupMember
                        {
                            GroupId = groupId.Value,
                            PersonId = person.Id,
                            GroupRoleId = groupRole.Id,
                            DateTimeAdded =
                                DateTime.ParseExact(row[20], dateFormats, CultureInfo.InvariantCulture,
                                    DateTimeStyles.None),
                            GroupMemberStatus = GroupMemberStatus.Active
                        };
                        groupMember.Attributes = new Dictionary<string, AttributeCache>();
                        groupMember.AttributeValues = new Dictionary<string, AttributeValueCache>();

                        SetGroupMemberAttributeValues(row, groupMember, attributes);
                        newGroupMembers.Add(groupMember);
                    }
                    else
                    {
                        SetGroupMemberAttributeValues( row, groupMember, attributes );
                        newGroupMembers.Add( groupMember );
                    }
                    
                }

                completed++;
                if ( completed % ( ReportingNumber * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} group members imported.", completed ) );
                }
                else if ( completed % ReportingNumber < 1 )
                {
                    SaveChanges( newHistory, newGroupMembers, rockContext );
                    ReportPartialProgress();

                    // Reset lookup context
                    rockContext.SaveChanges( DisableAuditing );
                    rockContext = new RockContext();
                    newHistory.Clear();
                    newGroupMembers.Clear();
                    
                    personService = new PersonService( rockContext );
                    groupService = new GroupService( rockContext );
                    groupTypeRoleService = new GroupTypeRoleService( rockContext );
                }
            }

            SaveChanges( newHistory, newGroupMembers, rockContext );

            ReportProgress( 0, string.Format( "Finished group member import: {0:N0} rows processed", completed ) );
            return completed;
        }

        private static void SetGroupMemberAttributeValues( string[] row, GroupMember groupMember, List<Attribute> attributes )
        {
            if ( !string.IsNullOrWhiteSpace( row[17] ) )
            {
                UpdateGroupMemberAttribute( "AssignedServices", groupMember, row[17], attributes );
            }
            if ( !string.IsNullOrWhiteSpace( row[18] ) )
            {
                AddGroupMemberAttribute( "AssignedTeam", groupMember, row[18], attributes );
            }

            if ( !string.IsNullOrWhiteSpace( row[22] ) )
            {
                AddGroupMemberAttribute( "Job", groupMember, row[22], attributes );
            }
        }

        /// <summary>
        /// Saves the individuals.
        /// </summary>
        /// <param name="newFamilyList">The family list.</param>
        /// <param name="visitorList">The optional visitor list.</param>
        private void SaveChanges( List<History> newHistory, List<GroupMember> newGroupMembers, RockContext rockContext )
        {
            if ( newHistory.Any() )
            {
                rockContext.WrapTransaction( () =>
                {
                    rockContext.Histories.AddRange( newHistory );
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }

            if ( newGroupMembers.Any() )
            {
                rockContext.WrapTransaction( () =>
                {
                    rockContext.GroupMembers.AddRange( newGroupMembers );
                    rockContext.SaveChanges( DisableAuditing );

//                     new group members
                    foreach ( var groupMember in newGroupMembers )
                    {
                        foreach ( var attributeCache in groupMember.Attributes.Select( a => a.Value ) )
                        {
                            var existingValue = rockContext.AttributeValues.FirstOrDefault( v => v.Attribute.Key == attributeCache.Key && v.EntityId == groupMember.Id );
                            var newAttributeValue = groupMember.AttributeValues[attributeCache.Key];

                            // set the new value and add it to the database
                            if ( existingValue == null )
                            {
                                existingValue = new AttributeValue();
                                existingValue.AttributeId = newAttributeValue.AttributeId;
                                existingValue.EntityId = groupMember.Id;
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
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }

        }

        private static void AddGroupMemberAttribute( string attributeKey, GroupMember groupMember, string attributeValue, List<Attribute> attributes )
        {
            var attributeModel = attributes.FirstOrDefault( a => a.Key == attributeKey );
            if ( attributeModel == null )
            {
                string message = "Expected " + attributeKey +
                " attribute to exist for " + groupMember.Group.GroupType.Name;
                throw new Exception( message );
            }
            var attributeCache = AttributeCache.Read( attributeModel );
            if ( attributeCache != null && !string.IsNullOrWhiteSpace( attributeValue ) )
            {
                if ( groupMember.Attributes.ContainsKey( attributeCache.Key ) )
                {
                    groupMember.AttributeValues[attributeCache.Key] = new AttributeValueCache()
                    {
                        AttributeId = attributeCache.Id,
                        Value = attributeValue
                    };
                }
                else
                {
                    groupMember.Attributes.Add( attributeCache.Key, attributeCache );
                    groupMember.AttributeValues.Add( attributeCache.Key, new AttributeValueCache()
                    {
                        AttributeId = attributeCache.Id,
                        Value = attributeValue
                    } );
                }

            }
        }

        private static void UpdateGroupMemberAttribute( string attributeKey, GroupMember groupMember, string attributeValue, List<Attribute> attributes )
        {
            var attributeModel = attributes.FirstOrDefault( a => a.Key == attributeKey );
            if ( attributeModel == null )
            {
                string message = "Expected " + attributeKey +
                " attribute to exist for " + groupMember.Group.GroupType.Name;
                throw new Exception( message );
            }
            var attributeCache = AttributeCache.Read( attributeModel );
            if ( attributeCache != null && !string.IsNullOrWhiteSpace( attributeValue ) )
            {
                if ( groupMember.Attributes.ContainsKey( attributeCache.Key ) )
                {
                    groupMember.AttributeValues[attributeCache.Key] = new AttributeValueCache()
                    {
                        AttributeId = attributeCache.Id,
                        Value = groupMember.AttributeValues[attributeCache.Key].Value + "," + attributeValue
                    };
                }
                else
                {
                    groupMember.Attributes.Add( attributeCache.Key, attributeCache );
                    groupMember.AttributeValues.Add( attributeCache.Key, new AttributeValueCache()
                    {
                        AttributeId = attributeCache.Id,
                        Value = attributeValue
                    } );
                }

            }
        }

        #endregion Main Methods
    }
}