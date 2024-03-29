// flight_stream - Arrow Flight gRPC Stream to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> March 24, 2024
//

use std::sync::Arc;

use crate::data_source::location_uri;
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use crate::server::flight::extract_ticket;
use arrow_flight::{
    flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch, Ticket,
};
use datafusion::arrow::{datatypes::Schema, record_batch::RecordBatch};

pub async fn to_record_batch(
    uri: &str,
    _options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let uri_parts =
        location_uri::to_parts(uri).map_err(|e| ResponseError::unsupported_type(e.to_string()))?;
    let uri_scheme = &uri_parts.scheme.as_ref().unwrap().to_string();
    let authority = &uri_parts.authority.as_ref().unwrap().to_string();
    let ticket = if let Some(pq) = &uri_parts.path_and_query {
        extract_ticket(Some(pq.path().trim_start_matches('/')))
            .map_err(|e| ResponseError::request_validation(format!("Unrecognized ticket: {e:?}")))?
    } else {
        return Err(ResponseError::request_validation(
            "Not found ticket in location URI",
        ));
    };

    let mut grpc_client = FlightServiceClient::connect(format!("{uri_scheme}://{authority}"))
        .await
        .map_err(|e| {
            ResponseError::connection_by_peer(format!(
                "Can not established with peer gRPC server: {e:?}"
            ))
        })?;

    let request = tonic::Request::new(Ticket {
        ticket: format!("{}/{}", ticket.0, ticket.1).into(),
    });

    let mut stream = grpc_client
        .do_get(request)
        .await
        .map_err(|e| from_tonic_err(&e))?
        .into_inner();

    let flight_data = stream
        .message()
        .await
        .map_err(|e| from_tonic_err(&e))?
        .unwrap();
    let schema = Arc::new(Schema::try_from(&flight_data)?);

    let mut record_batches: Vec<RecordBatch> = vec![];
    let map_by_field = std::collections::HashMap::new();

    while let Some(flight_data) = stream.message().await.map_err(|e| from_tonic_err(&e))? {
        let record_batch = flight_data_to_arrow_batch(&flight_data, schema.clone(), &map_by_field)?;
        record_batches.push(record_batch);
    }

    Ok(record_batches)
}

fn from_tonic_err(e: &tonic::Status) -> ResponseError {
    ResponseError::connection_by_peer(format!("gRPC communication error: {e:?}"))
}
