# arrowcat - Shows apache arrow format

## Build

```sh
cargo build --release
```

## Usage

```
arrowcat [OPTIONS] <FILE>

Arguments:
<FILE>  filename or '-' (stdin)

Options:
-b, --base64   decodes base64
-h, --help     Print help
-V, --version  Print version
```

## Detail

Specify a file containing either Arrow format or Arrow format encoded in Base64.

For Arrow binary files,

```sh
arrowcat example.arrow
```

Or if it’s a file encoded in Base64,

```sh
arrowcat --base64 example-arrow-base64.txt
```

Specify standard input (stdin) by using `-` in the file name. This would be useful for displaying the response from
datafusion-server.

```sh
curl -X "POST" "http://127.0.0.1:4000/dataframe/query" \
     -H 'Content-Type: application/json' \
     -H 'Accept: application/vnd.apache.arrow.stream' \
     -d $'
{
  "dataSources": [
    {
      "name": "hello",
      "location": "example://hello.world",
      "format": "arrow"
    }
  ],
  "query": {
    "sql": "SELECT * FROM hello"
  }
}' | arrowcat -
```

The display would look like this.

```
+---------------+-------+------------------+---------------------------+
| foo           | bar   | col_list         | col_struct                |
+---------------+-------+------------------+---------------------------+
| hello - arrow | 12345 | [1, 2, 3, 4, 5]  | {key1: value1, key2: 1.1} |
| world         | 67890 | [6, 7, 8, 9, 10] | {key1: value2, key2: 2.2} |
+---------------+-------+------------------+---------------------------+
```
