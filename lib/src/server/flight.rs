// server/flight.rs: Flight Implementation (Only for enables `flight` feature)

use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::{
    flight_descriptor::DescriptorType, flight_service_server::FlightService,
    flight_service_server::FlightServiceServer, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo,
    PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use datafusion::{
    arrow::{
        datatypes::Schema,
        error::ArrowError,
        ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
    },
    physical_plan::SendableRecordBatchStream,
};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use tonic::{
    codegen::tokio_stream::wrappers::ReceiverStream, Request, Response, Status, Streaming,
};

use crate::context::session_manager::SessionManager;
use crate::data_source::flight_stream;
use crate::request::body::DataSourceFormat;
use crate::response::receiver_stream;
use crate::server::metrics;
use crate::settings::Settings;

macro_rules! process_descriptor {
    ($descriptor:expr, $process_path:expr, $process_cmd:expr) => {
        if let Ok(desc_type) = DescriptorType::try_from($descriptor.r#type) {
            match desc_type {
                DescriptorType::Path => $process_path,
                DescriptorType::Cmd => $process_cmd,
                DescriptorType::Unknown => Err(Status::invalid_argument("Invalid descriptor type")),
            }
        } else {
            Err(Status::invalid_argument("Invalid descriptor type"))
        }
    };
}

#[derive(Clone)]
pub struct DataFusionServerFlightService {
    session_mgr: Arc<tokio::sync::Mutex<dyn SessionManager>>,
}

impl DataFusionServerFlightService {
    fn new(session_mgr: Arc<tokio::sync::Mutex<dyn SessionManager>>) -> Self {
        Self { session_mgr }
    }

    fn resolve_descriptor(descriptor: &FlightDescriptor) -> Result<(String, String), Box<Status>> {
        Ok(process_descriptor!(
            descriptor,
            {
                let (session_id, table_name) = split_descriptor_path(descriptor)?;
                let sql = format!("SELECT * FROM {table_name}");
                Ok((session_id, sql))
            },
            {
                let (session_id, cmd) = split_descriptor_cmd(descriptor)?;
                Ok((session_id, cmd))
            }
        )?)
    }

    async fn ipc_schema_result(&self, session_id: &str, sql: &str) -> Result<SchemaResult, Status> {
        let schema = Self::schema_from_logical_plan(self, session_id, sql).await?;
        let schema_result = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

        Ok(schema_result)
    }

    async fn schema_from_logical_plan(
        &self,
        session_id: &str,
        sql: &str,
    ) -> Result<Schema, Status> {
        Ok(Schema::from(
            self.session_mgr
                .lock()
                .await
                .execute_logical_plan(session_id, sql)
                .await
                .map_err(from_http_response_err)?
                .schema(),
        ))
    }

    async fn send_record_batch_stream(
        mut batch_stream: SendableRecordBatchStream,
        tx: tokio::sync::mpsc::Sender<Result<FlightData, Status>>,
    ) -> Result<(), Status> {
        let options = IpcWriteOptions::default();
        let generator = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);

        let flight_data_schema = FlightData::new().with_data_header(bytes::Bytes::from(
            generator
                .schema_to_bytes_with_dictionary_tracker(
                    batch_stream.schema().as_ref(),
                    &mut dictionary_tracker,
                    &options,
                )
                .ipc_message,
        ));

        tx.send(Ok(flight_data_schema))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        while let Some(batch_result) = batch_stream.next().await {
            log::trace!("batch_stream.next(): {batch_result:#?}");

            match batch_result {
                Ok(batch) => {
                    let (encoded_dictionaries, encoded_message) = generator
                        .encoded_batch(&batch, &mut dictionary_tracker, &options)
                        .map_err(|e| Status::internal(e.to_string()))?;

                    for dict in encoded_dictionaries {
                        tx.send(Ok(dict.into()))
                            .await
                            .map_err(|e| Status::internal(e.to_string()))?;
                    }

                    tx.send(Ok(encoded_message.into()))
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                }
                Err(e) => {
                    return Err(Status::internal(e.to_string()));
                }
            }
        }

        Ok(())
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
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        metrics::track_flight("get_flight_info", request, |request| async move {
            let descriptor = request.into_inner();
            let (session_id, sql) = Self::resolve_descriptor(&descriptor).map_err(|e| *e)?;
            let schema = Self::schema_from_logical_plan(self, &session_id, &sql).await?;

            Ok(Response::new(
                FlightInfo::new()
                    .try_with_schema(&schema)
                    .map_err(|e| Status::internal(e.to_string()))?
                    .with_endpoint(
                        FlightEndpoint::new()
                            .with_ticket(Ticket::new(format!("{session_id}/{sql}"))),
                    )
                    .with_descriptor(descriptor),
            ))
        })
        .await
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
        metrics::track_flight("get_schema", request, |request| async move {
            let descriptor = request.into_inner();
            let (session_id, sql) = Self::resolve_descriptor(&descriptor).map_err(|e| *e)?;

            Ok(Response::new(
                Self::ipc_schema_result(self, &session_id, &sql).await?,
            ))
        })
        .await
    }

    type DoGetStream = BoxedStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        metrics::track_flight("do_get", request, |request| async move {
            let ticket = request.into_inner();

            if let Ok(ticket_str) = std::str::from_utf8(&ticket.ticket) {
                log::info!("Call do_get: {ticket_str}");

                let (session_id, ticket_value) =
                    split_descriptor_value(Some(ticket_str)).map_err(|e| *e)?;
                let sql = if ticket_value.chars().any(char::is_whitespace) {
                    ticket_value // Maybe SQL statement
                } else {
                    format!("SELECT * FROM {ticket_value}")
                };

                let batch_stream = self
                    .session_mgr
                    .lock()
                    .await
                    .execute_sql_stream(&session_id, &sql)
                    .await
                    .map_err(from_http_response_err)?;

                let (tx, rx) = tokio::sync::mpsc::channel(32);

                tokio::spawn(async move {
                    if let Err(e) = Self::send_record_batch_stream(batch_stream, tx).await {
                        log::error!("Error converting and sending batches: {e}");
                    }
                });

                let flight_data_stream = receiver_stream::Receive::new(rx)
                    .map_err(|_| Status::internal("Channel receive error"));

                Ok(Response::new(
                    Box::pin(flight_data_stream) as Self::DoGetStream
                ))
            } else {
                Err(Status::invalid_argument("Invalid ticket"))
            }
        })
        .await
    }

    type DoPutStream = BoxedStream<PutResult>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        metrics::track_flight("do_put", request, |request| async move {
            let mut stream = request.into_inner();

            let (record_batches, descriptor) = flight_stream::to_record_batches(&mut stream)
                .await
                .map_err(from_http_response_err)?;

            let (session_id, table_name) = if let Some(descriptor) = &descriptor {
                process_descriptor!(
                    descriptor,
                    {
                        let (session_id, table_name) =
                            split_descriptor_path(descriptor).map_err(|e| *e)?;
                        Ok((session_id, table_name))
                    },
                    { Err(Status::invalid_argument("Invalid descriptor type 'cmd'")) }
                )?
            } else {
                return Err(Status::invalid_argument(
                    "No descriptor found in FlightData",
                ));
            };

            self.session_mgr
                .lock()
                .await
                .append_record_batch(
                    &session_id,
                    DataSourceFormat::Flight,
                    &table_name,
                    &record_batches,
                )
                .await
                .map_err(from_http_response_err)?;

            // Send PutResult messages back to the client
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let result_stream = ReceiverStream::new(rx);
            tx.send(Ok(PutResult::default())).await.unwrap();

            Ok(Response::new(Box::pin(result_stream) as Self::DoPutStream))
        })
        .await
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

fn split_descriptor_path(descriptor: &FlightDescriptor) -> Result<(String, String), Box<Status>> {
    split_descriptor_value(descriptor.path.first().map(String::as_str))
}

fn split_descriptor_cmd(descriptor: &FlightDescriptor) -> Result<(String, String), Box<Status>> {
    split_descriptor_value(Some(std::str::from_utf8(descriptor.cmd.as_ref()).map_err(
        |e| Status::invalid_argument(format!("Descriptor `cmd` is not utf-8 encoded string: {e}",)),
    )?))
}

pub fn split_descriptor_value(value: Option<&str>) -> Result<(String, String), Box<Status>> {
    if value.is_none() {
        return Err(Box::new(Status::invalid_argument(
            "Invalid path, descriptor not found",
        )));
    }

    let parts: Vec<&str> = value.unwrap().splitn(2, '/').collect();

    if parts.len() < 2 {
        return Err(Box::new(Status::invalid_argument(
            "Invalid descriptor format, must be included 'id/value'",
        )));
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
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
        Settings::global().server.flight_address,
        Settings::global().server.flight_grpc_port,
    )
    .parse::<SocketAddr>()?;

    Ok((
        FlightServiceServer::new(DataFusionServerFlightService::new(session_mgr.clone())),
        sock_addr,
    ))
}
