// flight_stream - Arrow Flight gRPC Stream to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> March 24, 2024
//

use arrow_flight::{
    flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch, FlightData,
    FlightDescriptor, Ticket,
};
use datafusion::arrow::{datatypes::Schema, record_batch::RecordBatch};
use std::sync::Arc;
use tonic::transport::Channel;

use crate::data_source::location;
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use crate::server::flight::split_descriptor_value;

pub async fn do_get(
    uri: &str,
    #[allow(unused_variables)] options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let uri_parts =
        location::uri::to_parts(uri).map_err(|e| ResponseError::unsupported_type(e.to_string()))?;
    let uri_scheme = &uri_parts.scheme.as_ref().unwrap().to_string();
    let authority = &uri_parts.authority.as_ref().unwrap().to_string();
    let ticket = if let Some(pq) = &uri_parts.path_and_query {
        split_descriptor_value(Some(pq.path().trim_start_matches('/')))
            .map_err(|e| ResponseError::request_validation(format!("Unrecognized ticket: {e:?}")))?
    } else {
        return Err(ResponseError::request_validation(
            "Not found ticket in location URI",
        ));
    };

    let endpoint = format!("{uri_scheme}://{authority}");

    let channel = Channel::from_shared(endpoint)
        .map_err(|e| {
            ResponseError::connection_by_peer(format!("Invalid gRPC endpoint URI: {e:?}"))
        })?
        .connect()
        .await
        .map_err(|e| {
            ResponseError::connection_by_peer(format!("Can not establish gRPC channel: {e:?}"))
        })?;

    let mut grpc_client = FlightServiceClient::new(channel);

    let request = tonic::Request::new(Ticket {
        ticket: format!("{}/{}", ticket.0, ticket.1).into(),
    });

    let mut stream = grpc_client
        .do_get(request)
        .await
        .map_err(|e| from_tonic_err(&e))?
        .into_inner();

    let (record_batches, _) = to_record_batches(&mut stream).await?;

    Ok(record_batches)
}

pub async fn to_record_batches(
    stream: &mut tonic::Streaming<FlightData>,
) -> Result<(Vec<RecordBatch>, Option<FlightDescriptor>), ResponseError> {
    let flight_data = stream
        .message()
        .await
        .map_err(|e| from_tonic_err(&e))?
        .unwrap();

    let schema = Arc::new(Schema::try_from(&flight_data)?);
    let descriptor = flight_data.flight_descriptor.clone();

    let mut record_batches: Vec<RecordBatch> = vec![];
    let map_by_field = std::collections::HashMap::new();

    while let Some(flight_data) = stream.message().await.map_err(|e| from_tonic_err(&e))? {
        let record_batch = flight_data_to_arrow_batch(&flight_data, schema.clone(), &map_by_field)?;
        record_batches.push(record_batch);
    }

    Ok((record_batches, descriptor))
}

fn from_tonic_err(e: &tonic::Status) -> ResponseError {
    ResponseError::connection_by_peer(format!("gRPC communication error: {e:?}"))
}
