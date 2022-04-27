from datetime import datetime
import os

from dotenv import load_dotenv

from googleapiclient.discovery import build
from google.cloud.bigquery import Client

today = datetime.today()
load_dotenv()


def bigquery_delete():

    client = Client()

    query = """
        DELETE 
        FROM 
            green-eye-cloud.warehouse.sitrack_recetas
        WHERE
            Year = {} AND
            Month = {}
    """.format(today.year, today.month)

    job = client.query(query)
    job.result()


def sitrack_recetas():

    service = build('dataflow', 'v1b3', cache_discovery=False)

    request = service.projects().templates().launch(
        projectId='green-eye-cloud',
        location='us-central1',
        gcsPath='gs://green-eye-cloud-dataflow/templates/sitrack_template',
        body={
            'jobName': 'sitrack_recetas',
            'parameters': {
                'auth': os.getenv('SITRACK_API_KEY'),
                'dataset': 'warehouse',
                'table': 'sitrack_recetas',
                'project': 'green-eye-cloud',
                'year': str(today.year),
                'month': str(today.month)
            },
            'environment': {
                'tempLocation': 'gs://cloud-temp/dataflow/',
                'zone': 'us-central1-a'
            }
        }
    )
    response = request.execute()

    return response


def main(event, context):

    bigquery_delete
    response = sitrack_recetas()

    print('response', response)
