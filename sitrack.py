import argparse
import logging
import json
import requests

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class GetApiData(beam.DoFn):

    def __init__(self, auth):
        self.auth = auth
        logging.debug('Fetching api data')

    def process(self, element):

        api_url = 'https://api.sitrack.io/event/flow/trigger?wait=true'

        response = requests.post(
            api_url,
            json={
                "processId": "43effed1-314b-4a8e-8e65-1115dc518c7d",
                "year": 2022,
                "month": [1, 2, 3, 4]
            },
            headers={
                'Authorization': self.auth
            }
        )

        json_response = json.loads(response.text)
        return json_response['recipeHistory']


def run(argv=None, save_main_session=True):

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--auth',
        dest='auth',
        required=True,
        help='Sitrack API key'
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions
    ).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        lines = (
            p
            | 'Create' >> beam.Create(['Start'])
            | 'Fetch API Data' >> beam.ParDo(GetApiData(auth=known_args.auth))
        )

        counts = (
            lines
            | 'Count Fields' >> beam.Map(lambda x: (x['landName'], len(x['lotName'].split(', '))))
            | 'Group and Sum' >> beam.CombinePerKey(sum)
            | 'Convert to Dict' >> beam.Map(lambda x: dict(Campo=x[0], Recetas=x[1]))
            #| beam.Map(print)
        )

        counts | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            'green-eye-cloud:warehouse.test',
            schema='Campo:STRING,Recetas:INTEGER',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
