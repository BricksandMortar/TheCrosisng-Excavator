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

using com.bricksandmortarstudio.TheCrossing.Model;
using com.bricksandmortarstudio.TheCrossing.Data;

namespace Excavator.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the People import methods
    /// </summary>
    partial class CSVComponent
    {
        #region Main Methods

        private const int GROUP_MEMBER_MEMBER_STATUS_COLUMN_NUMBER = 21;
        private const int GROUP_MEMBER_INDIVIDUAL_ID_COLUMN_NUMBER = 2;
        private const int GROUP_MEMBER_GROUP_ID_COLUMN_NUMBER = 16;
        private const int GROUP_MEMBER_GROUP_TYPE_ID_COLUMN_NUMBER = 15;
        private const int GROUP_MEMBER_JOIN_DATE_COLUMN_NUMBER = 22;
        private const int GROUP_MEMBER_INACTIVATED_DATE_COLUMN_NUMBER = 23;
        private const int GROUP_MEMBER_JOB_COLUMN_NUMBER = 24;
        private const int GROUP_MEMBER_MINISTRY_COLUMN_NUMBER = 7;
        private const int GROUP_MEMBER_ACTIVITY_COLUMN_NUMBER = 8;
        private const int GROUP_MEMBER_SCHEDULE_COLUMN_NUMBER = 10;
        private const int GROUP_MEMBER_ROLE_COLUMN_NUMBER = 18;
        private const int GROUP_MEMBER_TEAM_COLUMN_NUMBER = 19;
        private const int GROUP_MEMBER_SERVICE_COLUMN_NUMBER = 20;

        /// <summary>
        /// Loads the individual data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadGroupMapping( CSVInstance csvData )
        {
            var rockContext = new RockContext();
            var volunteerContext = new VolunteerTrackingContext();

            // Set the supported date formats
            var dateFormats = new[] { "yyyy-MM-dd", "M/dd/yyyy", "M/d/yyyy", "M/d/yy", "M/dd/yy" };

            int completed = 0;
            ReportProgress( 0, "Starting Group Member import " );

            var personService = new PersonService( rockContext );
            var groupService = new GroupService( rockContext );
            var groupTypeRoleService = new GroupTypeRoleService( rockContext );
            var attributeService = new AttributeService( rockContext );
            var groupMemberService = new GroupMemberService( rockContext );

            var newHistory = new List<History>();
            var newGroupMembers = new List<GroupMember>();
            var memberships = new List<VolunteerMembership>();
            int personEntityTypeId = EntityTypeCache.Read( typeof( Person ) ).Id;
            int groupMemberEntityTypeId = EntityTypeCache.Read( typeof( GroupMember ) ).Id;
            int groupEntityTypeId = EntityTypeCache.Read( typeof( Group ) ).Id;
            int addToGroupCategoryId = new CategoryService( rockContext ).Queryable().FirstOrDefault( c => c.Name == "Group Membership" ).Id;

            string[] row;
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var groupId = row[GROUP_MEMBER_GROUP_ID_COLUMN_NUMBER].AsIntegerOrNull();
                var individualId = row[GROUP_MEMBER_INDIVIDUAL_ID_COLUMN_NUMBER].AsIntegerOrNull();
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

                string memberStatus = string.IsNullOrWhiteSpace( row[GROUP_MEMBER_MEMBER_STATUS_COLUMN_NUMBER] ) ? "Inactive" : row[GROUP_MEMBER_MEMBER_STATUS_COLUMN_NUMBER];
                DateTime createdDateTime;
                try
                {
                    createdDateTime = DateTime.ParseExact( row[GROUP_MEMBER_JOIN_DATE_COLUMN_NUMBER], dateFormats,
                        new CultureInfo( "en-US" ), DateTimeStyles.None );
                }
                catch ( Exception exception )
                {
                    throw new Exception( "Failed to parse created date time for person with individua id " + individualId + exception.Message );
                }

                string grouproleName = row[GROUP_MEMBER_ROLE_COLUMN_NUMBER];

                var groupTypeId = row[GROUP_MEMBER_GROUP_TYPE_ID_COLUMN_NUMBER].AsIntegerOrNull();

                if ( !groupTypeId.HasValue || string.IsNullOrWhiteSpace( grouproleName ) )
                {
                    continue;
                }

                var groupRole = groupTypeRoleService.GetByGroupTypeId( groupTypeId.Value )
                                                   .FirstOrDefault( r => r.Name == grouproleName ) ?? group.GroupType.DefaultGroupRole;

                if ( memberStatus == "Inactive" )
                {
                    var addedHistory = new History
                    {
                        EntityTypeId = personEntityTypeId,
                        EntityId = person.Id,
                        RelatedEntityTypeId = groupEntityTypeId,
                        RelatedEntityId = groupId,
                        CategoryId = addToGroupCategoryId,
                        CreatedDateTime = createdDateTime,
                        Summary = string.Format( "Added to group (team: {0}, service: {1}, role: {2}, job: {3})", row[GROUP_MEMBER_TEAM_COLUMN_NUMBER],
                                    row[GROUP_MEMBER_SERVICE_COLUMN_NUMBER], row[GROUP_MEMBER_ROLE_COLUMN_NUMBER], row[GROUP_MEMBER_JOB_COLUMN_NUMBER] ),
                        Caption = groupService.Get( groupId.Value ).Name
                    };

                    DateTime inactivatedDateTime;
                    try
                    {
                        inactivatedDateTime = DateTime.ParseExact( row[GROUP_MEMBER_INACTIVATED_DATE_COLUMN_NUMBER], dateFormats,
                            new CultureInfo( "en-US" ), DateTimeStyles.None );
                    }
                    catch (Exception)
                    {
                        continue;
                    }

                    var removedHistory = new History
                    {
                        EntityTypeId = personEntityTypeId,
                        EntityId = person.Id,
                        RelatedEntityTypeId = groupEntityTypeId,
                        RelatedEntityId = groupId,
                        CategoryId = addToGroupCategoryId,
                        CreatedDateTime = inactivatedDateTime,
                        Summary = string.Format( "Removed from group (team: {0}, service: {1}, role: {2}, job: {3})", row[GROUP_MEMBER_TEAM_COLUMN_NUMBER],
                                    row[GROUP_MEMBER_SERVICE_COLUMN_NUMBER], row[GROUP_MEMBER_ROLE_COLUMN_NUMBER], row[GROUP_MEMBER_JOB_COLUMN_NUMBER] ),
                        Caption = groupService.Get( groupId.Value ).Name
                    };

                    newHistory.Add( addedHistory );
                    newHistory.Add( removedHistory );

                    var membership = new VolunteerMembership
                    {
                        GroupId = group.Id,
                        PersonId = person.Id,
                        GroupRoleId = groupRole.Id,
                        JoinedGroupDateTime = createdDateTime,
                        LeftGroupDateTime = inactivatedDateTime
                    };
                    memberships.Add(membership);
                }
                else
                {
                    

                    var attributes = attributeService.GetByEntityTypeId( groupMemberEntityTypeId ).Where( a => a.EntityTypeQualifierValue == groupTypeId.ToString() ).ToList();

                    // Get the attributes for this specific group's group members
                    int entityTypeId = EntityTypeCache.Read( typeof( GroupMember ) ).Id;
                    string qualifierColumn = "GroupId";
                    string qualifierValue = group.Id.ToString();
                    var groupMemberAttributes = attributeService.Get( entityTypeId, qualifierColumn, qualifierValue );
                    attributes.AddRange(groupMemberAttributes);

                   

                    bool loadedFromMemory = true;
                    var groupMember = newGroupMembers.FirstOrDefault( gm => gm.PersonId == person.Id );
                    if ( groupMember == null )
                    {
                        loadedFromMemory = false;
                        groupMember = groupMemberService.GetByGroupIdAndPersonId( groupId.Value, person.Id ).FirstOrDefault();
                    }
                    if ( groupMember == null )
                    {
                        var dateTimeAdded = DateTime.ParseExact( row[GROUP_MEMBER_JOIN_DATE_COLUMN_NUMBER], dateFormats, new CultureInfo( "en-US" ),
                            DateTimeStyles.None );
                        groupMember = new GroupMember
                        {
                            GroupId = groupId.Value,
                            PersonId = person.Id,
                            GroupRoleId = groupRole.Id,
                            DateTimeAdded = dateTimeAdded,
                            GroupMemberStatus = GroupMemberStatus.Active
                        };
                        groupMember.Attributes = new Dictionary<string, AttributeCache>();
                        groupMember.AttributeValues = new Dictionary<string, AttributeValueCache>();

                        SetGroupMemberAttributeValues( row, groupMember, attributes );
                        newGroupMembers.Add( groupMember );
                    }
                    else
                    {
                        if ( !loadedFromMemory )
                        {
                            groupMember.LoadAttributes( rockContext );
                        }
                        SetGroupMemberAttributeValues( row, groupMember, attributes );
                    }

                    var membership = new VolunteerMembership
                    {
                        GroupId = group.Id,
                        PersonId = person.Id,
                        GroupRoleId = groupRole.Id,
                        JoinedGroupDateTime = createdDateTime,
                        LeftGroupDateTime = null
                    };
                    memberships.Add( membership );
                }

                completed++;
                if ( completed % ( ReportingNumber * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} group members imported.", completed ) );
                }
                else if ( completed % ReportingNumber < 1 )
                {
                    SaveChanges( newHistory, newGroupMembers, memberships, rockContext, volunteerContext );

                    ReportPartialProgress();

                    rockContext.SaveChanges( DisableAuditing );
                    // Reset lookup context
                    rockContext = new RockContext();
                    newHistory.Clear();
                    newGroupMembers.Clear();
                    memberships.Clear();


                    groupMemberService = new GroupMemberService( rockContext );
                    personService = new PersonService( rockContext );
                    groupService = new GroupService( rockContext );
                    groupTypeRoleService = new GroupTypeRoleService( rockContext );
                }
            }

            SaveChanges( newHistory, newGroupMembers, memberships, rockContext, volunteerContext );


            ReportProgress( 0, string.Format( "Finished group member import: {0:N0} rows processed", completed ) );
            return completed;
        }

        private void SetGroupMemberAttributeValues( string[] row, GroupMember groupMember, List<Attribute> attributes )
        {
            if ( !string.IsNullOrWhiteSpace( row[GROUP_MEMBER_SERVICE_COLUMN_NUMBER] ) )
            {
                UpdateGroupMemberAttribute( "AssignedServices", groupMember, row[GROUP_MEMBER_SERVICE_COLUMN_NUMBER].Trim(), attributes );
            }
            if ( !string.IsNullOrWhiteSpace( row[GROUP_MEMBER_TEAM_COLUMN_NUMBER] ) )
            {
                AddGroupMemberAttribute( "AssignedTeam", groupMember, row[GROUP_MEMBER_TEAM_COLUMN_NUMBER].Replace( ", ", "," ), attributes );
            }

            if ( !string.IsNullOrWhiteSpace( row[GROUP_MEMBER_JOB_COLUMN_NUMBER] ) )
            {
                AddGroupMemberAttribute( "Job", groupMember, row[GROUP_MEMBER_JOB_COLUMN_NUMBER], attributes );
            }
        }

        /// <summary>
        /// Saves the individuals.
        /// </summary>
        /// <param name="newFamilyList">The family list.</param>
        /// <param name="visitorList">The optional visitor list.</param>
        private void SaveChanges( List<History> newHistory, List<GroupMember> newGroupMembers, List<VolunteerMembership> memberships, RockContext rockContext, VolunteerTrackingContext volunteerContext )
        {
            volunteerContext.WrapTransaction(() =>
            {
                volunteerContext.VolunteerMemberships.AddRange( memberships );
            });
            volunteerContext.SaveChanges(DisableAuditing);

            rockContext.WrapTransaction( () =>
            {
                rockContext.Histories.AddRange( newHistory );
                rockContext.GroupMembers.AddRange( newGroupMembers );
                rockContext.SaveChanges( DisableAuditing );
                // new group members
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

        private void AddGroupMemberAttribute( string attributeKey, GroupMember groupMember, string attributeValue, List<Attribute> attributes )
        {
            // get attribute
            var attributeModel = attributes.FirstOrDefault( a => a.Key == attributeKey );
            if ( attributeModel == null )
            {
                string message = "Expected " + attributeKey +
                                 " attribute to exist for " +
                                 ( groupMember.Group == null
                                     ? groupMember.GroupId.ToString()
                                     : groupMember.Group?.GroupType?.Name );
                ReportProgress( 0, message );
            }
            else
            {
                var attribute = AttributeCache.Read( attributeModel );

                if ( attribute != null && !string.IsNullOrWhiteSpace( attributeValue ) )
                {
                    Console.WriteLine( "Added attribute" );
                    if ( groupMember.Attributes.ContainsKey( attribute.Key ) )
                    {
                        groupMember.AttributeValues[attribute.Key] = new AttributeValueCache()
                        {
                            AttributeId = attribute.Id,
                            Value = attributeValue
                        };
                    }
                    else
                    {
                        groupMember.Attributes.Add( attribute.Key, attribute );
                        groupMember.AttributeValues.Add( attribute.Key, new AttributeValueCache()
                        {
                            AttributeId = attribute.Id,
                            Value = attributeValue
                        } );
                    }
                }
            }
            
        }

        private void UpdateGroupMemberAttribute( string attributeKey, GroupMember groupMember, string attributeValue, List<Attribute> attributes )
        {
            var attributeModel = attributes.FirstOrDefault( a => a.Key == attributeKey );
            if (attributeModel == null)
            {
                string message = "Expected " + attributeKey +
                                  " attribute to exist for " +
                                  ( groupMember.Group == null
                                      ? groupMember.GroupId.ToString()
                                      : groupMember.Group?.GroupType?.Name );
                ReportProgress( 0, message );
            }
            else
            {
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
            
        }

        #endregion Main Methods
    }
}