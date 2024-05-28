---
title: Object Stores
weight: 100
---

{{< toc >}}

## Supported Object Stores

* [Amazon S3](https://aws.amazon.com/s3/) {{< icon "external-link" >}}
* [Google Cloud Storage](https://cloud.google.com/storage) {{< icon "external-link" >}} (Experimental Support)
* [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) {{< icon "
  external-link" >}} (Experimental Support)
* [WebDAV](https://datatracker.ietf.org/doc/html/rfc2518) {{< icon "external-link" >}}

## Preparing Object Store

### Amazon S3

Parameters can be defined in a configuration file or as environment variables. If you’re writing it in the config.toml
file, you would add an entry like this.

```toml
[[storages]]
type = "aws"
access_key_id = "AKIA..."
secret_access_key = "SECRET"
region = "us-east-1"
bucket = "my-bucket"
```

If you’re using multiple buckets, you would write multiple entries.

If you’re specifying values through environment variables, set the values to the following keys. The definitions
specified in environment variables and those defined in config.toml will be merged.

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
type = "gcp"
service_account_key = "SERVICE_ACCOUNT_KEY"
bucket = "my-bucket"
```

The `service_account_key` should be set to the JSON serialized credentials.

Likewise for environment variables,

* `GOOGLE_SERVICE_ACCOUNT_KEY`
* `GOOGLE_BUCKET`

### Microsoft Azure Blob Storage

Specify parameters from the environment configuration file or environment variables.

```toml
[[storages]]
type = "azure"
account_name = "AZURE_STORAGE_ACCOUNT_NAME"
access_key = "AZURE_STORAGE_ACCESS_KEY"
container = "my-container"
```

Likewise for environment variables,

* `AZURE_STORAGE_ACCOUNT_NAME`
* `AZURE_STORAGE_ACCESS_KEY`
* `AZURE_CONTAINER`

### WebDAV

Specify the scheme and authority part in the URL (for example, `https://server.com`). DataFusion Server treats the
location scheme as either http or https, and if the authority matches the server defined here, it handles it as an
extension of WebDAV for HTTP.

```toml
[[storages]]
type = "webdav"
url = "https://server.com"
user = "USER"
password = "PASSWORD"
```

Likewise for environment variables,

* `HTTP_URL`
* `HTTP_USER`
* `HTTP_PASSWORD`

## Data Source Definition

Please refer to the details of the data source definition [here]({{< ref "/data-source/definition-basics" >}}). The only
thing that changes when dealing with a data source from Object Store is the location key.

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

In this example, a data source is defined to read / write “example.csv” from / to an S3 bucket. Similarly, if reading
/writing Parquet from / to Google Cloud Storage, it would look like this:

```json
[
  {
    "format": "parquet",
    "name": "example",
    "location": "gs://my-bucket/path/to/example.parquet"
  }
]
```

For Microsoft Azure Blob Storage, the same applies,

```json
[
  {
    "format": "ndJson",
    "name": "example",
    "location": "az://my-container/path/to/example.json"
  }
]
```

The scheme can be specified using commonly used schemes such as `adl`, `abfs`, and `abfss`, in addition to `az`.

WebDAV might need a bit of explanation. Just by looking at the location, it’s not clear whether it’s for regular http(s)
access or for accessing WebDAV, which is an extension of HTTP.

```json
[
  {
    "format": "avro",
    "name": "example",
    "location": "https://server.com/path/to/example.avro"
  }
]
```

If DataFusion Server has the following entry defined in the configuration file or environment variables, it treats
access to server.com via http(s) as WebDAV. This includes adding methods like `PROPFIND` and adding basic
authentication.

```toml
[[storages]]
type = "webdav"
url = "https://server.com"
user = "USER"
password = "PASSWORD"
```

The `url` defined in the configuration includes only the scheme and authority. Any path or query parameters are
completely ignored.
