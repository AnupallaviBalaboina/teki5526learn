import csv
import datetime
import logging
import importlib
import re
from google.cloud import bigquery
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.pipeline_options import SetupOptions
from dependencies import parse_schema
from dependencies import common_template_functions
from dependencies import beam_tables

def _get_table_info(dataset_id, table_name):
    """
    Parse the schema of given table

    Parameters
    ----------
    dataset_id: str, dataset_id
    table_name: str, table name

    Returns
    ----------
    column_names: list, list of column names
    parsed_schema: dictionary, parsed schema of the given table
    """

    schema_lib = importlib.import_module('dependencies.schemas_dir.{d}.{t}'.format(
        d = dataset_id,
        t = table_name))
    schema = schema_lib.SCHEMA
    parsed_schema = parse_schema.fn_parse_schema(schema)

    column_names = []
    for i in range(len(schema)):
        column_names.append(schema[i].name)

    return column_names, parsed_schema

class ParseFile(beam.DoFn):
    """
    Parses the raw CSV row into a list of strings and convert into json format
    """
    def __init__(self, dataset_id, table_name, filter_records):
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.filter_records = filter_records
        self.column_names = _get_table_info(self.dataset_id, self.table_name)[0]

    def process(self, element):
        if self.filter_records and re.search(r'{pattern}'.format(pattern=self.filter_records), element):
            return

        import csv
        # Parses the raw CSV row into a list of strings.
        reader = csv.reader([element])

        # Since csv.reader will return a list of rows and in our case,
        # it's just one row, we can use next() to get the first (and only) row.
        row = next(reader)

        # get list of column names from schema
        # column_names = _get_table_info(dataset_id, table_name)[0]
        dict_data = {}

        for i in range(len(self.column_names)):
            data = row[i]
            dict_data[self.column_names[i]] = data
        return [dict_data]

class GetFilesOptions(PipelineOptions):
    """
    Runtime value providers
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--files',
            required=False,
            help='gcs files')

class copy_column(beam.DoFn):
    """
    Function to copy the value of one column into another
    
    Parameters
    ----------
        source_column: str. examples: execution_date, segments.date, etc.
        target_column: str. examples: partition_column, etc.
    """
    def __init__(self, source_column, target_column):
        self.source_column = source_column
        self.target_column = target_column
    
    def process(self, element):
        if '.' in self.source_column:
            nested_element = element

            for key in self.source_column.split('.'):
                nested_element = nested_element.get(key, {})

            element[self.target_column] = nested_element
        else:
            element[self.target_column] = element[self.source_column]
        
        yield element

def run(project_id, known_args, temp_location, template_location):
    pipeline_options = PipelineOptions(
        region= 'us-central1',
        project= project_id,
        temp_location = temp_location ,
        template_location= template_location,
        runner= known_args.runner,
        streaming=False,
    )

    get_files_options = pipeline_options.view_as(GetFilesOptions)

    table_name_obj = beam_tables.get_beam_table_class(
            table_name = known_args.table_name,
            dataset_name = known_args.dataset_id,
            project = project_id,
            date_part = known_args.date_part)

    with beam.Pipeline(options=pipeline_options) as p:
        if known_args.zipped_file:
            data = p | 'Read Gzipped File' >> beam.io.ReadFromText(get_files_options.files,
                                                                       compression_type=fileio.CompressionTypes.GZIP,
                                                                       skip_header_lines=known_args.skip_header_lines
                                                                       )
        else:
            data = p | 'Read gcs files' >> beam.io.ReadFromText(get_files_options.files,
                                                                   skip_header_lines=known_args.skip_header_lines
                                                                   )

        if known_args.file_format.lower() == 'csv':
            # Transform the CSV lines to dictionary objects
            parsed_data = data | 'Parse CSV file' >> beam.ParDo(ParseFile(dataset_id=known_args.dataset_id,
                                                                            table_name=known_args.table_name,
                                                                            filter_records=known_args.filter_records
                                                                            )
                                                                    )
        elif known_args.file_format.lower() == 'json':
            # Transform the JSON lines to dictionary objects
            parsed_data = data | 'Parse JSON file' >> beam.Map(lambda x: eval(x))
        
        if known_args.copy_column and ':' in known_args.copy_column:
            source_column, target_column = known_args.copy_column.split(':')
            parsed_data = parsed_data | beam.ParDo(copy_column(source_column=source_column, target_column=target_column))

        # Load parsed data into BQ table
        parsed_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                    table=table_name_obj.fn_table_name,
                    schema=_get_table_info(dataset_id=known_args.dataset_id, table_name=known_args.table_name)[1],
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    # input parameters
    known_args, pipeline_args = common_template_functions.get_args()
    temp_location = common_template_functions.get_temp_location(known_args)
    project_id = common_template_functions.get_project(known_args)
    template_location = common_template_functions.get_template_location(known_args)

    # load csv data into BQ table
    run(project_id=project_id,
        known_args=known_args,
        temp_location=temp_location,
        template_location=template_location
        )
