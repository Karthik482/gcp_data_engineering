import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime, timedelta

class ExtractData(beam.DoFn):
    def process(self, element):
        data = json.loads(element)
        artist = data['artist_name']
        album = data['album']
        views = data['views']
        date = datetime.strptime(data['date'], '%Y-%m-%d').date()
        week = (date - timedelta(days=date.weekday())).strftime('%Y-%W')
        return [(artist, album, views, week, date)]

class GroupByWeek(beam.DoFn):
    def process(self, element):
        artist, album, views, week, date = element
        week_start = date - timedelta(days=date.weekday())
        week_end = week_start + timedelta(days=6)
        return [(artist, album, week_start, week_end, views)]

class FormatData(beam.DoFn):
    def process(self, element):
        artist, album, week_start, week_end, views = element
        return {
            'artist_name': artist,
            'album': album,
            'week_start': week_start.strftime('%Y-%m-%d'),
            'week_end': week_end.strftime('%Y-%m-%d'),
            'views': views
        }

def run(argv=None):
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(SetupOptions).temp_location = "gs://<BUCKET_NAME>/temp"
    p = beam.Pipeline(options=options)

    # Read from Pub/Sub topic
    data = p | beam.io.ReadFromPubSub(subscription='projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>')

    # Extract, group by week and format the data
    extracted_data = data | beam.ParDo(ExtractData())
    grouped_data = extracted_data | beam.ParDo(GroupByWeek())
    formatted_data = grouped_data | beam.ParDo(FormatData())

    # Calculate the view count for each week
    view_count_per_week = formatted_data | beam.GroupByKey() | beam.Map(lambda x: (x[0], sum(x[1])))

    # Define the BigQuery schema
    schema = 'artist_name:STRING,album:STRING,week_start:DATE,week_end:DATE,views:INTEGER'
    
    formated_data | beam.io.WriteToBigQuery(
    table_spec,
    schema=table_schema,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)'
    
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()

    
