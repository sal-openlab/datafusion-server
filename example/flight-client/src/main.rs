// main.rs: DataFusion Server Flight gRPC Client

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int32Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
    util::pretty,
};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{
    flight_descriptor, flight_service_client::FlightServiceClient,
    utils::flight_data_to_arrow_batch, FlightDescriptor, Ticket,
};
use clap::{Parser, ValueEnum};
use futures::stream::StreamExt;

#[derive(Debug, Clone, PartialEq, ValueEnum)]
enum Method {
    GetFlightInfo,
    GetSchema,
    DoGet,
    DoPut,
}

#[derive(Parser)]
#[clap(author, version, about = "Arrow and other large datasets web server", long_about = None)]
struct Args {
    #[clap(
        long,
        value_enum,
        short = 'm',
        value_name = "METHOD",
        help = "Flight method",
        default_value_t = Method::DoGet
    )]
    method: Method,

    #[clap(
        long,
        value_parser,
        short = 't',
        value_name = "TICKET or PATH",
        help = "Ticket or path - session_id/table_name"
    )]
    ticket: String,

    #[clap(
        long,
        value_parser,
        short = 'a',
        value_name = "HOST",
        help = "Target host",
        default_value = "127.0.0.1"
    )]
    host: String,

    #[clap(
        long,
        value_parser,
        short = 'p',
        value_name = "port",
        help = "target port",
        default_value = "50051"
    )]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arg = Args::parse();

    println!(
        "Connect to: '{}:{}' using ticket: '{}'",
        arg.host, arg.port, arg.ticket
    );

    let mut client =
        FlightServiceClient::connect(format!("grpc://{}:{}", arg.host, arg.port)).await?;

    match arg.method {
        Method::GetFlightInfo => {
            let request = tonic::Request::new(FlightDescriptor {
                r#type: flight_descriptor::DescriptorType::Path as i32,
                cmd: Default::default(),
                path: vec![format!("{}", arg.ticket)],
            });

            println!(">>> get_flight_info()");
            let flight_info = client.get_flight_info(request).await?.into_inner();
            println!(">>> flight info result: {flight_info:?}");
        }
        Method::GetSchema => {
            let request = tonic::Request::new(FlightDescriptor {
                r#type: flight_descriptor::DescriptorType::Path as i32,
                cmd: Default::default(),
                path: vec![format!("{}", arg.ticket)],
            });

            println!(">>> get_schema()");
            let schema_result = client.get_schema(request).await?.into_inner();
            let schema = Schema::try_from(&schema_result)?;
            println!(">>> schema result: {schema:?}");
        }
        Method::DoGet => {
            let request = tonic::Request::new(Ticket {
                ticket: arg.ticket.into(),
            });

            println!(">>> do_get(): {:?}", &request);
            let mut stream = client.do_get(request).await?.into_inner();

            // the schema should be the first message returned, else client should error
            let flight_data = stream.message().await?.unwrap();
            let schema = Arc::new(Schema::try_from(&flight_data)?);
            println!(">>> schema from flight_data\n{schema:?}");

            let mut results = vec![];
            let dictionaries_by_field = HashMap::new();

            while let Some(flight_data) = stream.message().await? {
                let record_batch = flight_data_to_arrow_batch(
                    &flight_data,
                    schema.clone(),
                    &dictionaries_by_field,
                )?;
                results.push(record_batch);
            }

            println!(
                ">>> record_batch(es) from flight_data, number of batch(es)={}",
                results.len()
            );
            pretty::print_batches(&results)?;
        }
        Method::DoPut => {
            let schema = Schema::new(vec![Field::new("field1", DataType::Int32, false)]);
            let data = Int32Array::from(vec![1, 2, 3, 4, 5]);
            let record_batch =
                RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(data) as ArrayRef])?;

            let descriptor = FlightDescriptor::new_path(vec![arg.ticket]);
            println!(">>> do_put(): {:?}", descriptor);

            let flight_data = FlightDataEncoderBuilder::new()
                .with_flight_descriptor(Some(descriptor))
                .build(futures::stream::iter(vec![Ok(record_batch)]))
                .map(|result| result.unwrap());

            let request = tonic::Request::new(flight_data);

            let response = client.do_put(request).await?;
            println!(">>> Response: {:?}", response);
        }
    }

    Ok(())
}
