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
            var dateFormats = new[] { "yyyy-MM-dd", "MM/dd/yyyy", "MM/dd/yy" };

            int completed = 0;
            ReportProgress( 0, string.Format( "Starting Individual import ({0:N0} already exist).", ImportedPeopleKeys.Count() ) );

            var personService = new PersonService( rockContext );
            var groupService = new GroupService( rockContext );
            var groupTypeRoleService = new GroupTypeRoleService( rockContext );

            var newHistory = new List<History>();
            var newGroupMembers = new List<GroupMember>();
            int personEntityTypeId = EntityTypeCache.Read( typeof( Rock.Model.Person ) ).Id;
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
                        CreatedDateTime = DateTime.Parse( row[20] ),
                        Caption = "Added to group."
                    };

                    var removedHistory = new History
                    {
                        EntityTypeId = personEntityTypeId,
                        EntityId = person.Id,
                        RelatedEntityTypeId = groupEntityTypeId,
                        RelatedEntityId = groupId,
                        CategoryId = addToGroupCategoryId,
                        CreatedDateTime = DateTime.Parse( row[21] ),
                        Caption = "Removed from group."
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

                    var groupMember = new GroupMember
                    {
                        GroupId = groupId.Value,
                        PersonId = person.Id,
                        GroupRole = groupTypeRoleService.GetByGroupTypeId( groupEntityTypeId )
                                                        .FirstOrDefault( r => r.Name == grouproleName ),
                        DateTimeAdded = DateTime.Parse( row[20] ),
                        GroupMemberStatus = GroupMemberStatus.Active
                    };

                    if ( !string.IsNullOrWhiteSpace( row[17] ) )
                    {
                        groupMember.SetAttributeValue( "AssignedServices", row[17] );
                    }
                    if ( !string.IsNullOrWhiteSpace( row[18] ) )
                    {
                        groupMember.SetAttributeValue( "AssignedTeam", row[18] );
                    }

                    if ( !string.IsNullOrWhiteSpace( row[22] ) )
                    {
                        groupMember.SetAttributeValue( "Job", row[22] );
                    }
                }

                completed++;
                if ( completed % ( ReportingNumber * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} families imported.", completed ) );
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

        /// <summary>
        /// Saves the individuals.
        /// </summary>
        /// <param name="newFamilyList">The family list.</param>
        /// <param name="visitorList">The optional visitor list.</param>
        private void SaveChanges( List<History> newHistory, List<GroupMember> groupMembers, RockContext rockContext )
        {
            if ( newHistory.Any() )
            {
                rockContext.WrapTransaction( () =>
                {
                    rockContext.Histories.AddRange( newHistory );
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }

            if ( groupMembers.Any() )
            {
                rockContext.WrapTransaction( () =>
                {
                    rockContext.GroupMembers.AddRange( groupMembers );
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }
        }

        #endregion Main Methods
    }
}