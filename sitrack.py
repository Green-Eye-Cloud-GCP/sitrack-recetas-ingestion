import argparse
import logging
import json
import requests
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from google.cloud.bigquery import Client


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


def delete_records(table: str, year: int, month: int):

    client = Client()

    sql = """
    DELETE 
    FROM 
        {}
    WHERE
        Year = {} AND
        Month = {}
    """.format(table, year, month)

    job = client.query(sql)
    job.result()

    logging.debug('Records deleted')


def run(argv=None, save_main_session=True):

    today = datetime.today()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--auth',
        dest='auth',
        required=True,
        help='Sitrack API key'
    )
    parser.add_argument(
        '--project',
        dest='project',
        required=True,
        help='GCP project'
    )
    parser.add_argument(
        '--dataset',
        dest='dataset',
        required=True,
        help='BigQuery dataset'
    )
    parser.add_argument(
        '--table',
        dest='table',
        required=True,
        help='BigQuery table'
    )
    parser.add_argument(
        '--year',
        dest='year',
        default=today.year,
        type=int,
        help='Year to query'
    )
    parser.add_argument(
        '--month',
        dest='month',
        default=today.month,
        type=int,
        help='Month to query'
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    delete_records(
        '{}.{}.{}'.format(known_args.project,
                          known_args.dataset,
                          known_args.table),
        known_args.year,
        known_args.month
    )

    pipeline_options = PipelineOptions(
        pipeline_args,
        project=known_args.project
    )
    pipeline_options.view_as(
        SetupOptions
    ).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        lines = (
            p
            | 'Create' >> beam.Create(['Start'])
            | 'Fetch API Data' >> beam.ParDo(GetApiData(auth=known_args.auth, year=known_args.year, month=known_args.month))
        )

        counts = (
            lines
            | 'Count Fields' >> beam.Map(lambda x: (x['landName'], len(x['lotName'].split(', '))))
            | 'Group and Sum' >> beam.CombinePerKey(sum)
            | 'Convert to Dict' >> beam.Map(lambda x: dict(Year=known_args.year, Month=known_args.month, Campo=x[0], Recetas=x[1]))
            # | beam.Map(print)
        )

        counts | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            '{}:{}.{}'.format(known_args.project,
                              known_args.dataset,
                              known_args.table),
            schema='Year:INTEGER,Month:INTEGER,Campo:STRING,Recetas:INTEGER',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
