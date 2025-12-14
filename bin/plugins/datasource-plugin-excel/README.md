# Microsoft Excel data source connector plugin for datafusion-server

* Supports Excel 2003+ (.xls and .xlsx) files
    + .xlsm and other format is **not tested**

## Specifications

### Location URL Scheme

| Scheme     | Authority                         | Path       |
|------------|-----------------------------------|------------|
| `excel://` | Excel workbook file path and name | Sheet name |

### Plugin Options

| Key         |      Type       | Example | Description                                                         |
|-------------|:---------------:|:-------:|---------------------------------------------------------------------|
| `skipRows`  | Integer or None |   `0`   | Skips heading rows in worksheet                                     |
| `nRows`     | Integer or None |  `500`  | Number of rows to read (default none to read try all existing rows) |
| `hasHeader` | Boolean or None | `true`  | Exists header row in worksheet                                      |
| `headerRow` | Integer or None |   `0`   | Header row index (effects only `hasHeader` to `true`)               |

* `row` is zero based index (line 1 equally row 0)

### Request example

```sh
$ curl -X "POST" "http://localhost:4000/dataframe/query" \
     -H 'Content-Type: application/json' \
     -d $'
{
  "dataSources": [
    {
      "format": "arrow",
      "name": "example",
      "location": "excel://example-workbook.xlsx/Sheet1",
      "options": {
        "skipRows": 2
      }
    }
  ],
  "query": {
    "sql": "SELECT * FROM example"
  },
  "response": {
    "format": "json"
  }
}'
```

## Development

### Pre-requirement

#### Python runtime

Required Python interpreter v3.7+

* Linux (Ubuntu, Debian)

```sh
$ sudo apt install python3-dev
```

* Windows
    - https://www.python.org/downloads/windows/

### Debugging

#### Prepare Virtual Environment

##### Create venv

```sh
$ python3 -m venv .venv
```

##### Activate venv

* Linux, macOS, Unix

```sh
$ source venv/bin/activate
```

* Windows

```
> .\venv\Scripts\activate
```

##### Install required packages for venv

```sh
$ pip install -r requirements.txt
```

##### Deactivate venv

```sh
$ deactivate
```
