# DataFusion Server - flight test client

## Usage

```
Usage: flight-client [OPTIONS] --ticket <TICKET or PATH>

Options:
  -m, --method <METHOD>          Flight method [default: do-get] [possible values: get-flight-info, get-schema, do-get, do-put]
  -t, --ticket <TICKET or PATH>  Ticket or path - session_id/table_name
  -a, --host <HOST>              Target host [default: 127.0.0.1]
  -p, --port <port>              target port [default: 50051]
  -h, --help                     Print help
  -V, --version                  Print version
```

## Example

### Preparation on the DataFusion server side

Creates a new session context with named session identifier.

```shell
curl http://127.0.0.1:4000/session/create?id=test1
```

### Uploading test data to session context by do-put

```shell
flight-client --method=do-put --ticket=test1/test_table
```

The response looks like this.

```
>>> do_put(): FlightDescriptor { r#type: Path, cmd: b"", path: ["test-id2/test"] }
>>> Response: Response { metadata: MetadataMap { headers: {"content-type": "application/grpc", "date": "Tue, 25 Jun 2024 06:52:59 GMT"} }, message: Streaming, extensions: Extensions }
```

### Inspecting uploaded schema

```shell
flight-client --method=get-schema --ticket=test1/test_table
```

This kind of response will likely be output.

```
>>> get_schema()
>>> schema result: Schema { fields: [Field { name: "field1", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }], metadata: {} }
```

### Getting data from session context by do-get

```shell
flight-client --method=do-get --ticket=test1/test_table
```

The response looks like this.

```
>>> do_get(): Request { metadata: MetadataMap { headers: {} }, message: Ticket { ticket: b"test-id2/test" }, extensions: Extensions }
>>> schema from flight_data
Schema { fields: [Field { name: "field1", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }], metadata: {} }
>>> record_batch(es) from flight_data, number of batch(es)=1
+--------+
| field1 |
+--------+
| 1      |
| 2      |
| 3      |
| 4      |
| 5      |
+--------+
```
