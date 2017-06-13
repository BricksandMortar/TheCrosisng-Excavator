using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using OrcaMDF.Core.MetaData;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;

namespace Excavator.F1
{
    public partial class F1Component
    {
        private void MapContactFormData( IQueryable<Row> tableData )
        {
            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Counting Contact Form Data import ({0:N0} found to import in total.", totalRows ) );
            var seenContactFormDataIds = new HashSet<int>();
            var rockContext = new RockContext();
            var noteTypeService = new NoteTypeService(rockContext);
            var f1ContactNoteType = noteTypeService.Queryable().FirstOrDefault(nt => nt.Name == "F1 Contact");
            var noteList = new List<Note>();

            if ( f1ContactNoteType == null)
            {
                var newNoteType = new NoteType();
                newNoteType.EntityTypeId = EntityTypeCache.Read(typeof(Person)).Id;
                newNoteType.EntityTypeQualifierColumn = string.Empty;
                newNoteType.EntityTypeQualifierValue = string.Empty;
                newNoteType.UserSelectable = true;
                newNoteType.IsSystem = false;
                newNoteType.Name = "F1 Contact";
                newNoteType.Order = 0;

                rockContext.NoteTypes.Add( newNoteType );
                rockContext.SaveChanges( DisableAuditing );

                f1ContactNoteType = newNoteType;
            }


            foreach ( var groupedRows in tableData.GroupBy<Row, int?>( r => r["ContactInstItemID"] as int? ) )
            {
                foreach ( var row in groupedRows.Where( r => r != null ) )
                {
                    string contactFormName = row["ContactIndividualID"] as string;
                    var individualId = row["ContactIndividualID"] as int?;
                    var householdId = row["HouseholdID"] as int?;
                    string type = row["ContactFormName"] as string;
                    var startDateTime = row["ContactDatetime"] as DateTime?;
                    var endDateTime = row["ContactItemLastUpdatedDate"] as DateTime?;
                    var instanceId = row["ContactInstanceID"] as int?;
                    string originalContactNote = row["ContactNote"] as string;
                    string disposition = row["ContactDispositionName"] as string;

                    // Ignore emails
                    if ( contactFormName == "Email" || !instanceId.HasValue || seenContactFormDataIds.Contains(instanceId.Value))
                    {
                        continue;
                    }

                    var personKeys = GetPersonKeys(individualId, householdId, false);
                    if (personKeys == null)
                    {
                        continue;
                    }

                    seenContactFormDataIds.Add( instanceId.Value );
                    var sb = new StringBuilder();
                    sb.AppendFormat("Contact Form (id: {3}) {0} submitted with original contents of {1} on {2}", type,
                        originalContactNote, startDateTime, instanceId);
                    if (endDateTime != null)
                    {
                        sb.AppendFormat(" closed on {0}", endDateTime);
                    }
                    sb.AppendFormat(" with disposition {0}", disposition);

                    string text = sb.ToString();
                    text = Regex.Replace( text, @"\t|\&nbsp;", " " );
                    text = text.Replace( "&#45;", "-" );
                    text = text.Replace( "&lt;", "<" );
                    text = text.Replace( "&gt;", ">" );
                    text = text.Replace( "&amp;", "&" );
                    text = text.Replace( "&quot;", @"""" );
                    text = text.Replace( "&#x0D", string.Empty );

                    var note = new Note();
                    note.CreatedDateTime = startDateTime;
                    note.EntityId = personKeys.PersonId;
                    note.Text = text.Trim();
                    note.NoteTypeId = f1ContactNoteType.Id;
                    noteList.Add(note);

                    completed++;

                    if ( completed % percentage < 1 )
                    {
                        int percentComplete = completed / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} contact form data imported ({1}% complete).", completed, percentComplete ) );
                    }
                    else if ( completed % ReportingNumber < 1 )
                    {
                        SaveNotes( noteList );
                        ReportPartialProgress();
                        noteList.Clear();
                    }
                }
            }

            ReportProgress( 100, string.Format( "Finished contact form data import: {0:N0} records imported.", completed ) );
        }

    }

}
