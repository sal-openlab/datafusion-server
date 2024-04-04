// server/flight.rs: Flight Implementation (Only for enables `flight` feature)

use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{
    flight_descriptor::DescriptorType, flight_service_server::FlightService, Action, ActionType,
    Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use datafusion::arrow::{
    error::ArrowError,
    ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
};
use futures::stream::BoxStream;
use tonic::{Request, Response, Status, Streaming};

use crate::context::session_manager::SessionManager;
use crate::settings::Settings;

#[derive(Clone)]
pub struct DataFusionServerFlightService {
    session_mgr: Arc<tokio::sync::Mutex<dyn SessionManager>>,
}

impl DataFusionServerFlightService {
    fn new(session_mgr: Arc<tokio::sync::Mutex<dyn SessionManager>>) -> Self {
        Self { session_mgr }
    }

    async fn ipc_schema_result(
        &self,
        session_id: &str,
        table_name: &str,
        options: IpcWriteOptions,
    ) -> Result<SchemaResult, Status> {
        if let Some(schema) = self
            .session_mgr
            .lock()
            .await
            .schema_ref(session_id, table_name)
            .await
        {
            let schema_result = SchemaAsIpc::new(schema.as_ref(), &options)
                .try_into()
                .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

            Ok(schema_result)
        } else {
            Err(Status::not_found(format!(
                "Not found table '{table_name}' in session '{session_id}'"
            )))
        }
    }
}

type BoxedStream<T> = BoxStream<'static, Result<T, Status>>;

#[tonic::async_trait]
impl FlightService for DataFusionServerFlightService {
    type HandshakeStream = BoxedStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type ListFlightsStream = BoxedStream<FlightInfo>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();

        if let Ok(desc_type) = DescriptorType::try_from(descriptor.r#type) {
            match desc_type {
                DescriptorType::Path => {
                    let (session_id, table_name) =
                        extract_ticket(descriptor.path.first().map(String::as_str))?;
                    let schema_result = Self::ipc_schema_result(
                        self,
                        &session_id,
                        &table_name,
                        IpcWriteOptions::default(),
                    )
                    .await?;
                    Ok(Response::new(schema_result))
                }
                DescriptorType::Cmd => Err(Status::invalid_argument(
                    "Currently unsupported Flight with command, like a SQL",
                )),
                DescriptorType::Unknown => Err(Status::invalid_argument("Invalid descriptor type")),
            }
        } else {
            Err(Status::invalid_argument("Invalid descriptor type"))
        }
    }

    type DoGetStream = BoxedStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        if let Ok(ticket) = std::str::from_utf8(&ticket.ticket) {
            log::info!("Call do_get: {ticket}");

            let (session_id, table_name) = extract_ticket(Some(ticket))?;

            let batches = self
                .session_mgr
                .lock()
                .await
                .execute_sql(&session_id, &format!("SELECT * FROM {table_name}"))
                .await
                .map_err(from_http_response_err)?;

            let mut flights = vec![FlightData::from(SchemaAsIpc::new(
                &batches[0].schema().clone(),
                &IpcWriteOptions::default(),
            ))];

            let encoder = IpcDataGenerator::default();
            let mut tracker = DictionaryTracker::new(false);

            for batch in batches {
                let (flight_dictionaries, flight_batch) = encoder
                    .encoded_batch(&batch, &mut tracker, &IpcWriteOptions::default())
                    .map_err(|e| from_arrow_err(&e))?;
                flights.extend(flight_dictionaries.into_iter().map(Into::into));
                flights.push(flight_batch.into());
            }

            let output = futures::stream::iter(flights.into_iter().map(Ok));
            Ok(Response::new(Box::pin(output) as Self::DoGetStream))
        } else {
            Err(Status::invalid_argument(format!(
                "Invalid ticket {ticket:?}"
            )))
        }
    }

    type DoPutStream = BoxedStream<PutResult>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoExchangeStream = BoxedStream<FlightData>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoActionStream = BoxedStream<arrow_flight::Result>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type ListActionsStream = BoxedStream<ActionType>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

pub fn extract_ticket(path: Option<&str>) -> Result<(String, String), Status> {
    if path.is_none() {
        return Err(Status::invalid_argument(
            "Invalid path, descriptor not found",
        ));
    }

    let parts: Vec<&str> = path.unwrap().splitn(2, '/').collect();

    if parts.len() < 2 {
        return Err(Status::invalid_argument(
            "Invalid path, must be included 'id/value'",
        ));
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

fn from_arrow_err(e: &ArrowError) -> Status {
    Status::internal(format!("{e:?}"))
}

fn from_http_response_err(e: crate::response::http_error::ResponseError) -> Status {
    match e.code {
        axum::http::StatusCode::BAD_REQUEST => Status::invalid_argument(e.message),
        _ => Status::internal(e.message),
    }
}

pub fn create_server<S: SessionManager>(
    session_mgr: &Arc<tokio::sync::Mutex<S>>,
) -> Result<
    (
        FlightServiceServer<DataFusionServerFlightService>,
        SocketAddr,
    ),
    anyhow::Error,
> {
    let sock_addr = format!(
        "{}:{}",
        Settings::global().server.address,
        Settings::global().server.flight_grpc_port,
    )
    .parse::<SocketAddr>()?;

    Ok((
        FlightServiceServer::new(DataFusionServerFlightService::new(session_mgr.clone())),
        sock_addr,
    ))
}
