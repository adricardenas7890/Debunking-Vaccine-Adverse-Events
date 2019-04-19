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

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset2.primaryinfo LIMIT 100'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('input.txt')

    # apply a ParDo to the PCollection 
    changeNulls_in_pcoll = query_results | 'change Null values' >> beam.ParDo(changeNullsFn())

    # write PCollection to log file
    changeNulls_in_pcoll | 'Write to log 2' >> WriteToText('output.txt')
    
    # make BQ records, when writing from Beam to BQ, BQ expects a dictionary
  
    # changeNulls_out_pcoll = changeNulls_in_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':dataset2.primaryinfo_2'
    table_schema = 'vaers_id:STRING,state:STRING,hospitalization:STRING,disabled:STRING,age:STRING,sex:STRING,died:STRING,recovered:STRING,year:STRING'
    
    changeNulls_in_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
