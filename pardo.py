import os
import apache_beam as beam
import random
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


# DoFn performs on each element in the input PCollection.
# Adds primary key to table
class VaccineCountFn(beam.DoFn):
  def process(self, element):
    record = element
    vax_id = round(random.random() * 10000000)
    vaers_id= record.get('vaers_id')
    vax_name = record.get('vax_name')
    vax_type = record.get('vax_type')
    vax_manu = record.get('vax_manu')
    vax_route = record.get('vax_route')
    year = record.get('year')

    record = {'vax_id': vax_id, 'vaers_id': vaers_id, 'vax_name': vax_name,
              'vax_type': vax_type, 'vax_manu': vax_manu, 'vax_route': vax_route,
              'year': year}
    return [record]
    

class MakeRecordFn(beam.DoFn):
  def process(self, element):
    vax_id, vaers_id, vax_name, vax_type, vax_manu, vax_route, year = element
    record = {'vax_id': vax_id, 'vaers_id': vaers_id, 'vax_name': vax_name,
              'vax_type': vax_type, 'vax_manu': vax_manu, 'vax_route': vax_route,
              'year': year}
    return [record] 

         
PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset1clean.vaccine LIMIT 100'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('input.txt')

    # apply a ParDo to the PCollection 
    vaxid_in_pcoll = query_results | 'Make Vax ID' >> beam.ParDo(VaccineCountFn())

    # write PCollection to log file
    vaxid_in_pcoll | 'Write to log 2' >> WriteToText('output.txt')
    
    # make BQ records, when writing from Beam to BQ, BQ expects a dictionary
  
    vaxid_out_pcoll = vaxid_in_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':dataset1clean.vaccine_PK'
    table_schema = 'vax_id:INTEGER, vaers_id:INTEGER, vax_name:STRING, vax_type:STRING, vax_manu:STRING, vax_route:STRING, year:INTEGER'
    
    vaxid_out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
