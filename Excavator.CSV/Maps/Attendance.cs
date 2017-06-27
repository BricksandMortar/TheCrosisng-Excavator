using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using NodaTime.Text;
using Rock;
using Rock.Data;
using Rock.Model;

namespace Excavator.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the family import methods
    /// </summary>
    partial class CSVComponent
    {
        #region Main Methods

        private const int INDIVIDUAL_ID_STATUS_COLUMN_NUMBER = 0;
        private const int MINISTRY_STATUS_COLUMN_NUMBER = 1;
        private const int ACTIVITY_STATUS_COLUMN_NUMBER = 2;
        private const int ROSTER_STATUS_COLUMN_NUMBER = 3;
        private const int JOB_STATUS_COLUMN_NUMBER = 4;
        private const int DATE_STATUS_COLUMN_NUMBER = 5;
        private const int TIME_STATUS_COLUMN_NUMBER = 6;
        private const int INDIVIDUAL_TYPE_STATUS_COLUMN_NUMBER = 7;
        private const int GROUP_ID_STATUS_COLUMN_NUMBER = 8;

        /// <summary>
        /// Loads the individual data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int MapAttendance( CSVInstance csvData )
        {
            var rockContext = new RockContext();

            // Set the supported date formats
            var dateFormats = new[] { "yyyy-MM-dd", "MM/dd/yyyy", "MM/dd/yy", "M/dd/yyyy", "M/d/yyyy" };
            var attendances = new List<Attendance>();
            var groupService = new GroupService( rockContext );
            var personService = new PersonService( rockContext );

            int completed = 0;
            ReportProgress( 0, "Starting Attendance import " );

            string[] row;
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                // TODO ROWs need to change
                var groupId = row[GROUP_ID_STATUS_COLUMN_NUMBER].AsIntegerOrNull();
                var individualId = row[INDIVIDUAL_ID_STATUS_COLUMN_NUMBER].AsIntegerOrNull();
                string startDateUnparsed = row[DATE_STATUS_COLUMN_NUMBER];
                var startDate = DateTime.ParseExact( startDateUnparsed, dateFormats, new CultureInfo( "en-US" ), DateTimeStyles.None );
                string startTimeUnparsed = row[TIME_STATUS_COLUMN_NUMBER];
                var localTime = LocalTimePattern.CreateWithInvariantCulture( "h:mm tt" ).Parse( startTimeUnparsed );

                if ( localTime == null )
                {
                    throw new Exception( "Time could not be parsed" );
                }

                var startDateTime = startDate.AddHours( localTime.Value.Hour ).AddMinutes( localTime.Value.Minute );
                if ( !groupId.HasValue || !individualId.HasValue || groupId == 0 )
                {
                    continue;
                }

                var group = groupService.Get( groupId.Value );
                var person = personService.Queryable().FirstOrDefault( p => p.ForeignId == individualId );

                if ( group == null || person == null )
                {
                    continue;
                }


                var groupLocation = group.GroupLocations.FirstOrDefault( gl => gl.Schedules.Count > 0 );
                var schedule = groupLocation?.Schedules.FirstOrDefault( s => s.WasCheckInActive(startDateTime) && !s.Name.ToLower().Contains("test")) ?? groupLocation?.Schedules.FirstOrDefault();

                if (schedule == null)
                {
                    ReportProgress(0, string.Format("{0} ({1}) attended Group {2} ({3}) on {4}", person.FullName, person.Id, group, group.Id, startDateTime));
                }

                var attendance = new Attendance();
                attendance.PersonAliasId = person.PrimaryAliasId;
                attendance.GroupId = groupId;
                attendance.LocationId = groupLocation?.LocationId;
                attendance.ScheduleId = schedule?.Id;
                attendance.DidAttend = true;
                attendance.StartDateTime = startDateTime;
                attendance.RSVP = RSVP.Yes;
                if (group.CampusId.HasValue)
                {
                    attendance.CampusId = group.CampusId;
                }
                else
                {
                    attendance.CampusId = group.Name.Contains("TCE") ? 2 : 1;
                }
                attendances.Add(attendance);

                completed++;
                if ( completed % ( ReportingNumber * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} attendance imported.", completed ) );
                }
                else if ( completed % ReportingNumber < 1 )
                {
                    SaveChanges( attendances, rockContext );

                    ReportPartialProgress();
                    attendances.Clear();

                    rockContext.SaveChanges( DisableAuditing );
                    // Reset lookup context
                    rockContext = new RockContext();

                    groupService = new GroupService( rockContext );
                    personService = new PersonService( rockContext );
                }
            }

            SaveChanges( attendances, rockContext );


            ReportProgress( 0, string.Format( "Finished attendance import: {0:N0} rows processed", completed ) );
            return completed;
        }

        private static void SaveChanges( List<Attendance> attendances, RockContext rockContext )
        {
            rockContext.WrapTransaction(() =>
            {
                rockContext.Attendances.AddRange(attendances);
                rockContext.SaveChanges(DisableAuditing);
            });
        }
        #endregion Main Methods
    }
}