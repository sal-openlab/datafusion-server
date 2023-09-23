# Pivot table processor plugin for datafusion-server

## Usage

### Example for executes the plugin

```sh
$ curl -X "POST" "http://localhost:4000/dataframe/query" \
     -H 'Content-Type: application/json' \
     -d $'
{
  "dataSources": [
    {
      "format": "csv",
      "name": "superstore",
      "location": "file:///superstore.csv",
      "options": {
        "hasHeader": true
      }
    }
  ],
  "query": {
    "sql": "SELECT * FROM superstore",
    "postProcessors": [
      {
        "module": "pivot-table",
        "pluginOptions": {
          "values": "Sales",
          "index": ["State", "City"],
          "columns": "Department"
        }
      }
    ]
  },
  "response": {
    "format": "json"
  }
}'
```

* `superstore` table like this.

```
                            Category       Department          City  Sales Shipping Cost ...
0                              Paper  Office Supplies       Lombard     53             5 ...
1                              Paper  Office Supplies       Lombard     76             1 ...
2                Pens & Art Supplies        Furniture     Southbury     16             2 ...
3     Binders and Binder Accessories  Office Supplies     Coachella     65             8 ...
4                       Rubber Bands  Office Supplies     Coachella     19             1 ...
...                              ...              ...           ...    ...           ...
9421   Scissors, Rulers and Trimmers  Office Supplies        Dublin     38             2 ...
9422             Pens & Art Supplies        Furniture Cottage Grove     53             2 ...
9423                      Appliances       Technology       Sanford    825             4 ...
9424                           Paper  Office Supplies      Paterson    163            17 ...
9425            Computer Peripherals       Technology        Taylor    242            20 ...
```

* Processed by pivot-table processor plugin results like this.

```
State   City          Furniture  Office Supplies  Technology
Alabama Auburn              NaN      3119.000000      1200.0
        Bessemer          362.0        66.200000         NaN
        Birmingham       2491.0      1054.000000       187.0
        Decatur             NaN        76.000000      1355.5
        Enterprise       1595.5       227.000000       222.0
...                         ...              ...         ...
Wyoming Casper            595.5         9.000000      2310.0
        Cheyenne          507.0       121.500000         NaN
        Gillette          386.0       368.000000      2701.0
        Laramie          1935.0        41.000000      1085.0
        Rock Springs      657.5        92.666667         NaN
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
$ python3 -m venv venv
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
