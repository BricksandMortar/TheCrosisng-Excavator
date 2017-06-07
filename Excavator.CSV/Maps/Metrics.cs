using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Globalization;
using System.Linq;
using NodaTime.Text;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;

namespace Excavator.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the family import methods
    /// </summary>
    partial class CSVComponent
    {
        private const int METRIC_ACTIVITY_COLUMN_NUMBER = 0;
        private const int METRIC_ROSTER_FOLDER_COLUMN_NUMBER = 1;
        private const int METRIC_ROSTER_COLUMN_NUMBER = 2;
        private const int METRIC_DATE_COLUMN_NUMBER = 3;
        private const int METRIC_TIME_COLUMN_NUMBER = 4;
        private const int METRIC_COUNT_COLUMN_NUMBER = 5;

        /// <summary>
        /// Loads the family data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadMetrics( CSVInstance csvData )
        {
            // Required variables

            var dateFormats = new[] { "yyyy-MM-dd", "MM/dd/yyyy", "MM/dd/yy", "M/dd/yyyy", "M/d/yyyy" };
            var lookupContext = new RockContext();
            var metricService = new MetricService( lookupContext );
            var categoryService = new CategoryService( lookupContext );
            var metricSourceTypes = DefinedTypeCache.Read( new Guid( Rock.SystemGuid.DefinedType.METRIC_SOURCE_TYPE ) ).DefinedValues;
            var metricManualSource = metricSourceTypes.FirstOrDefault( m => m.Guid == new Guid( Rock.SystemGuid.DefinedValue.METRIC_SOURCE_VALUE_TYPE_MANUAL ) );

            var metricEntityTypeId = EntityTypeCache.Read<MetricCategory>( false, lookupContext ).Id;
            var scheduleEntityTypeId = EntityTypeCache.Read<Schedule>( false, lookupContext ).Id;

            var schedules = new ScheduleService(lookupContext).Queryable().AsNoTracking().ToList();
            var allMetrics = metricService.Queryable().AsNoTracking().ToList();
            var metricCategories = categoryService.Queryable().AsNoTracking()
                .Where( c => c.EntityType.Guid == new Guid( Rock.SystemGuid.EntityType.METRICCATEGORY ) ).ToList();
            
            var serviceTimeCategory = categoryService.Queryable()
               .AsNoTracking().FirstOrDefault( c => c.EntityTypeId == scheduleEntityTypeId  && c.Name == "Service Times" );

            if (serviceTimeCategory == null)
            {
                throw new Exception("Service Time Category is null");
            }

            var defaultMetricCategory = metricCategories.FirstOrDefault( c => c.Name == "Metrics" );

            if ( defaultMetricCategory == null )
            {
                defaultMetricCategory = new Category();
                defaultMetricCategory.Name = "Metrics";
                defaultMetricCategory.IsSystem = false;
                defaultMetricCategory.EntityTypeId = metricEntityTypeId;
                defaultMetricCategory.EntityTypeQualifierColumn = string.Empty;
                defaultMetricCategory.EntityTypeQualifierValue = string.Empty;

                lookupContext.Categories.Add( defaultMetricCategory );
                lookupContext.SaveChanges();

                metricCategories.Add( defaultMetricCategory );
            }

            var metricValues = new List<MetricValue>();

            Metric currentMetric = null;
            int completed = 0;

            ReportProgress( 0, string.Format( "Starting metrics import ({0:N0} already exist).", 0 ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( (row = csvData.Database.FirstOrDefault()) != null )
            {
                string metricName = row[METRIC_ACTIVITY_COLUMN_NUMBER];
                if ( !string.IsNullOrEmpty( metricName ) )
                {
                    decimal? value = row[METRIC_COUNT_COLUMN_NUMBER].AsDecimalOrNull();

                    
                    string startDateUnparsed = row[METRIC_DATE_COLUMN_NUMBER];
                    var startDate = DateTime.ParseExact( startDateUnparsed, dateFormats, new CultureInfo( "en-US" ), DateTimeStyles.None );
                    string startTimeUnparsed = row[METRIC_TIME_COLUMN_NUMBER];
                    var localTime = LocalTimePattern.CreateWithInvariantCulture( "h:mmtt" ).Parse( startTimeUnparsed );

                    if ( localTime == null )
                    {
                        throw new Exception( "Time could not be parsed" );
                    }

                    var valueDateTime = startDate.AddHours( localTime.Value.Hour ).AddMinutes( localTime.Value.Minute );
                    var metricCategoryId = defaultMetricCategory.Id;

                    // create metric if it doesn't exist
                    currentMetric = allMetrics.FirstOrDefault( m => m.Title == metricName && m.MetricCategories.Any( c => c.CategoryId == metricCategoryId ) );
                    if ( currentMetric == null )
                    {
                        currentMetric = new Metric();
                        currentMetric.Title = metricName;
                        currentMetric.IsSystem = false;
                        currentMetric.IsCumulative = false;
                        currentMetric.SourceSql = string.Empty;
                        currentMetric.Subtitle = string.Empty;
                        currentMetric.Description = string.Empty;
                        currentMetric.IconCssClass = string.Empty;
                        currentMetric.EntityTypeId = scheduleEntityTypeId;
                        currentMetric.SourceValueTypeId = metricManualSource.Id;
                        currentMetric.CreatedByPersonAliasId = ImportPersonAliasId;
                        currentMetric.CreatedDateTime = ImportDateTime;
                        currentMetric.MetricCategories.Add( new MetricCategory { CategoryId = metricCategoryId } );

                        lookupContext.Metrics.Add( currentMetric );
                        lookupContext.SaveChanges();

                        allMetrics.Add( currentMetric );
                    }

                    var scheduleId = schedules.Where( s => s.CategoryId == serviceTimeCategory.Id  && s.WasCheckInActive( valueDateTime ) )
                        .Select( s => ( int? )s.Id ).FirstOrDefault();

                    // create values for this metric
                    var metricValue = new MetricValue();
                    metricValue.MetricValueType = MetricValueType.Measure;
                    metricValue.CreatedByPersonAliasId = ImportPersonAliasId;
                    metricValue.CreatedDateTime = ImportDateTime;
                    metricValue.MetricValueDateTime = valueDateTime;
                    metricValue.MetricId = currentMetric.Id;
                    metricValue.EntityId = scheduleId;
                    metricValue.Note = string.Empty;
                    metricValue.XValue = string.Empty;
                    metricValue.YValue = value;
                    metricValues.Add( metricValue );

                    completed++;
                    if ( completed % (ReportingNumber * 10) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} metrics imported.", completed ) );
                    }
                    else if ( completed % ReportingNumber < 1 )
                    {
                        SaveMetrics( metricValues );
                        ReportPartialProgress();

                        // Reset lookup context
                        lookupContext = new RockContext();
                        metricValues.Clear();
                    }
                }
            }

            // Check to see if any rows didn't get saved to the database
            if ( metricValues.Any() )
            {
                SaveMetrics( metricValues );
            }

            ReportProgress( 0, string.Format( "Finished metrics import: {0:N0} metrics added or updated.", completed ) );
            return completed;
        }

        /// <summary>
        /// Saves all the metric values.
        /// </summary>
        private void SaveMetrics( List<MetricValue> metricValues )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( ( ) =>
            {
                rockContext.MetricValues.AddRange( metricValues );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }
    }
}