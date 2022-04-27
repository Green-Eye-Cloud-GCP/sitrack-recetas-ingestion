import logging
import json
import requests

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class GetApiData(beam.DoFn):

    def __init__(self, auth: str, year: int, month: int):
        self.auth = auth
        self.year = year
        self.month = month
        logging.debug('Fetching api data')

    def process(self, element):

        api_url = 'https://api.sitrack.io/event/flow/trigger?wait=true'

        response = requests.post(
            api_url,
            json={
                'processId': '43effed1-314b-4a8e-8e65-1115dc518c7d',
                'year': self.year,
                'month': [self.month]
            },
            headers={
                'Authorization': self.auth
            }
        )

        json_response = json.loads(response.text)
        return json_response['recipeHistory']


class TemplateOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--auth',
            dest='auth',
            required=True,
            type=str,
            help='Sitrack API key'
        )
        parser.add_argument(
            '--dataset',
            dest='dataset',
            required=True,
            type=str,
            help='BigQuery dataset'
        )
        parser.add_argument(
            '--table',
            dest='table',
            required=True,
            type=str,
            help='BigQuery table'
        )
        parser.add_argument(
            '--year',
            dest='year',
            required=True,
            type=int,
            help='Year to query'
        )
        parser.add_argument(
            '--month',
            dest='month',
            required=True,
            type=int,
            help='Month to query'
        )


def run():

    pipeline_options = PipelineOptions(flags=None, save_main_session=True)
    pipeline_options.view_as(TemplateOptions)

    options = pipeline_options.get_all_options()

    with beam.Pipeline(options=pipeline_options) as p:

        lines = (
            p
            | 'Create' >> beam.Create(['Start'])
            | 'Fetch API Data' >> beam.ParDo(GetApiData(auth=options['auth'], year=options['year'], month=options['month']))
        )

        counts = (
            lines
            | 'Count Fields' >> beam.Map(lambda x: (x['landName'], len(x['lotName'].split(', '))))
            | 'Group and Sum' >> beam.CombinePerKey(sum)
            | 'Convert to Dict' >> beam.Map(lambda x: dict(Year=options['year'], Month=options['month'], Campo=x[0], Recetas=x[1]))
            # | beam.Map(print)
        )

        counts | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            '{}:{}.{}'.format(options['project'],
                              options['dataset'],
                              options['table']),
            schema='Year:INTEGER,Month:INTEGER,Campo:STRING,Recetas:INTEGER',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
