import os
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection.
# change all null values
class changeNullsFn(beam.DoFn):
    def process(self, element):
    record = element
    vaers_id = record.get('vaers_id')
    state = record.get('state')
    if IFMISSING(state):
        state = "U"
    hospitalization = record.get('hospitalization')
    if IFMISSING(hospitalization):
        hospitalization = "false"
    disabled = record.get('disabled')
    if IFMISSIING(disabled):
        disabled = "false"
    age = record.get('age')
    sex = record.get('sex')
    died = record.get('died')
    if IFMISSING(died):
        died = "false"
    recovered = record.get('recovered')
    if IFMISSING(recovered):
        recovered = "U"
    year = record.get('year')

    record = {'vaers_id': vaers_id, 'state':state, 'hospitalization':hospitalization,
              'disabled':disabled, 'age':age, 'sex':sex, 'died':died,
              'recovered':recovered, 'year':year}

    return [record]

class MakeRecordFn(beam.DoFn):
  def process(self, element):
    vaers_id, state, hospitalization, disabled, age, sex, died, recovered, year = element
    record = {'vaers_id': vaers_id, 'state':state, 'hospitalization':hospitalization,
              'disabled':disabled, 'age':age, 'sex':sex, 'died':died,
              'recovered':recovered, 'year':year}
    return [record] 
        
PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH_IN = BUCKET + '/input/' 
DIR_PATH_OUT = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'nomination-count-8',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-1', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset2.personalinfo LIMIT 100'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('input.txt')

    # apply a ParDo to the PCollection 
    changeNulls_in_pcoll = query_results | 'change Null values' >> beam.ParDo(changeNullsFn())

    # write PCollection to log file
    changeNulls_in_pcoll | 'Write to log 2' >> WriteToText('output.txt')
    
    # make BQ records, when writing from Beam to BQ, BQ expects a dictionary
  
    changeNulls_out_pcoll = changeNulls_in_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':dataset2.personalinfo_2'
    table_schema = 'vax_id:INTEGER, vaers_id:INTEGER, vax_name:STRING, vax_type:STRING, vax_manu:STRING, vax_route:STRING, year:STRING'
    
    changeNulls_out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
