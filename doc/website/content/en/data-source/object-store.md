---
title: Object Stores
weight: 100
---

{{< toc >}}

## Supported Object Stores

* Amazon S3
* Google Cloud Storage (Experimental Support)
* Microsoft Azure (Planned)
* WebDAV (Planned)

## Preparing Object Store

### Amazon S3

Parameters can be defined in a configuration file or as environment variables. If you’re writing it in the config.toml file, you would add an entry like this.

```toml
[[storages]]
type = "aws"
access_key_id = "AKIA..."
secret_access_key = "SECRET"
region = "us-east-1"
bucket = "my-bucket"
```

If you’re using multiple buckets, you would write multiple entries.

If you’re specifying values through environment variables, set the values to the following keys. The definitions specified in environment variables and those defined in config.toml will be merged.

* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `AWS_DEFAULT_REGION`
* `AWS_BUCKET`

Parameters defined in environment variables are effective even in Docker.

```shell
docker run -d --rm \
    -p 4000:4000 \
    -v ./data:/var/datafusion-server/data \
    -e AWS_ACCESS_KEY_ID="AKIA..." \
    -e AWS_SECRET_ACCESS_KEY="SECRET" \
    -e AWS_DEFAULT_REGION="us-west-2" \
    -e AWS_BUCKET="my-bucket" \
    --name datafusion-server \
    datafusion-server:x.y.z
```

### Google Cloud Storage

GCS, like S3, also specifies parameters in the environment configuration file or environment variables.

```toml
[[storages]]
type = "gcs"
service_account_key = "SERVICE_ACCOUNT_KEY"
bucket = "my-bucket"
```

The `service_account_key` should be set to the JSON serialized credentials.

Likewise for environment variables,

* `GOOGLE_SERVICE_ACCOUNT_KEY`
* `GOOGLE_BUCKET`

## Data Source Definition

Please refer to the details of the data source definition [here]({{< ref "/data-source/definition-basics" >}}). The only thing that changes when dealing with a data source from Object Store is the location key.

```json
[
  {
    "format": "csv",
    "name": "example",
    "location": "s3://my-bucket/example.csv",
    "options": {
      "hasHeader": true
    }
  }
]
```

In this example, a data source is defined to read “example.csv” from an S3 bucket. Similarly, if reading Parquet from GCS, it would look like this:

```json
[
  {
    "format": "parquet",
    "name": "example",
    "location": "gs://my-bucket/path/to/example.parquet"
  }
]
```



