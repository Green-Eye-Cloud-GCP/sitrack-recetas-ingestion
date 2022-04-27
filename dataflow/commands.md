# RUN LOCALLY
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS='E:\HUB\dataflow\service_account.json'
python -m sitrack `
    --auth <# 'SITRACK_API_KEY' #> `
    --job_name 'sitrack-recetas' `
    --project 'green-eye-cloud' `
    --temp_location 'gs://cloud-temp/dataflow/' `
    --table 'sitrack_recetas' `
    --region 'us-central1' `
    --dataset 'warehouse' `
    --year '2022' `
    --month '1'
```

# CREATE TEMPLATE
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS='E:\HUB\dataflow\service_account.json'
python -m sitrack_template `
    --runner 'DataflowRunner' `
    --project 'green-eye-cloud' `
    --staging_location 'gs://green-eye-cloud-dataflow/staging' `
    --temp_location 'gs://cloud-temp/dataflow' `
    --template_location 'gs://green-eye-cloud-dataflow/templates/sitrack_template' `
    --region 'us-central1' `
    --auth <# 'SITRACK_API_KEY' #> `
    --dataset 'warehouse' `
    --table 'sitrack_recetas' `
    --service_account_email 'dataflow-templates@green-eye-cloud.iam.gserviceaccount.com'
```