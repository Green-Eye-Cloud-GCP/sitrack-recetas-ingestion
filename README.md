# sitrack-recetas-ingestion
Pipeline that runs in Dataflow to load into BigQuery the sum of recipes that we have made on Sitrack during the current month. The pipeline is fired by Cloud Functions after it deletes old BigQuery records.

![Demo](https://github.com/Green-Eye-Cloud-GCP/sitrack-recetas-ingestion/blob/master/diagram.jpg)
