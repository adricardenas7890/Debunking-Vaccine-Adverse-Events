import os
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection.
# Changes null to values
class changeNullsFn(beam.DoFn):
    def process(self, element):
        record = element
        vaers_id = record.get('vaers_id')
        vaers_id = str(vaers_id)
        state = record.get('state')
        if state == None:
            state = str("U")
        else:
            state = str(state)
        hospitalization = record.get('hospitalization')
        if hospitalization == None:
            hospitalization = str("false")
        else:
            hospitalization = str(hospitalization)
        disabled = record.get('disabled')
        if disabled == None:
            disabled = str("false")
        else:
            disabled = str(disabled)
        age = record.get('age')
        age = str(age)
        sex = record.get('sex')
        died = record.get('died')
        if died == None:
            died = str("false")
        else:
            died = str(died)
        recovered = record.get('recovered')
        if recovered == None:
            recovered = str("U")
        else:
            recovered = str(recovered)
        year = record.get('year')
        year = str(year)

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
    'job_name': 'primaryinfo_3',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-1', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:

# Create a Pipeline using a local runner for execution.
#with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset2.primaryinfo'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('input.txt')

    # apply a ParDo to the PCollection 
    changeNulls_in_pcoll = query_results | 'change Null values' >> beam.ParDo(changeNullsFn())

    # write PCollection to log file
    changeNulls_in_pcoll | 'Write to log 2' >> WriteToText('output.txt')
    
    # make BQ records, when writing from Beam to BQ, BQ expects a dictionary
  
    changeNulls_out_pcoll = changeNulls_in_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':dataset2.primaryinfo_3'
    table_schema = 'vaers_id:STRING,state:STRING,hospitalization:STRING,disabled:STRING,age:STRING,sex:STRING,died:STRING,recovered:STRING,year:STRING'
    
    changeNulls_out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
