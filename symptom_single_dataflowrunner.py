import os
import apache_beam as beam
import random
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


# DoFn performs on each element in the input PCollection.
# Adds primary key to table
class SypmtomCountFn(beam.DoFn):
  def process(self, element):
    record = element
    symptom_id = round(random.randint(1,1000000))
    vaers_id = record.get('vaers_id')
    symptom1 = record.get('symptom1')
    symptom2 = record.get('symptom2')
    year = record.get('year')

    record = {'symptom_id': symptom_id, 'vaers_id': vaers_id, 'symptom1': symptom1,
              'symptom2': symptom2, 'year': year}
    return [record]
    

class MakeRecordFn(beam.DoFn):
  def process(self, element):
    symptom_id, vaers_id, symptom1, symptom2, year = element
    record = {'symptom_id': symptom_id, 'vaers_id': vaers_id, 'symptom1': symptom1,
              'symptom2': symptom2, 'year': year}
    return [record] 

         
PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DataflowRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset1clean.symptom LIMIT 100'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('input.txt')

    # apply a ParDo to the PCollection 
    vaxid_in_pcoll = query_results | 'Make SYMPTOM ID' >> beam.ParDo(SymptomCountFn())

    # write PCollection to log file
    vaxid_in_pcoll | 'Write to log 2' >> WriteToText('output.txt')
    
    # make BQ records, when writing from Beam to BQ, BQ expects a dictionary
  
    vaxid_out_pcoll = vaxid_in_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':dataset1clean.symptom_PK'
    table_schema = 'symptom_id:INTEGER, vaers_id:INTEGER, symptom1:STRING, symptom2:STRING, year:STRING'
    
    vaxid_out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
