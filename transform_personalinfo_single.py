import os
import apache_beam as beam
#from apache_beam.io import 
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection.
# Adds primary key to table
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

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

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
