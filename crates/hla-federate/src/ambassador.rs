//! `RtiAmbassador` — federate-side typed API + connection lifecycle.
//!
//! Owns the FedPro session (TCP, framing, sequence numbers). Splits I/O into
//! a writer task (drains an mpsc) and a pump task (reads frames, demuxes
//! responses vs callbacks, dispatches callbacks to the user-supplied
//! [`FederateAmbassador`]).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicI32;

use hla_core::{
    AttributeHandle, AttributeHandleSet, AttributeHandleValueMap, FederateHandle,
    InteractionClassHandle, ObjectClassHandle, ObjectInstanceHandle, ParameterHandleValueMap,
    ResignAction,
};
use hla_fedpro_proto::fedpro::{
    self, call_request::CallRequest as Req, call_response::CallResponse as Resp,
};
use hla_wire::{
    AsyncReadSource, AsyncWriteSink, Frame, FrameSink, FrameSource, HlaCallResponsePayload,
    MessageHeader, MessageType, NO_SEQUENCE_NUMBER, client_open_session,
    client_open_session_frames,
};
use parking_lot::Mutex;
use prost::Message;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::callbacks::FederateAmbassador;
use crate::handles::{
    decode_attribute_value_map, decode_federate, decode_interaction_class,
    decode_object_class, decode_object_instance, decode_parameter_value_map,
    encode_attribute, encode_interaction_class, encode_object_class, encode_object_instance,
    encode_parameter,
};

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("invalid RTI URL: {0}")]
    InvalidUrl(String),
    #[error("TCP connect failed: {0}")]
    Connect(std::io::Error),
    #[error("session handshake failed: {0}")]
    Session(#[from] hla_wire::SessionError),
}

#[derive(Debug, Error)]
pub enum CallError {
    #[error("RTI exception {name}: {details}")]
    RtiException { name: String, details: String },
    #[error("unexpected response variant")]
    UnexpectedResponse,
    #[error("decode error: {0}")]
    Decode(String),
    #[error("connection closed")]
    Closed,
    #[error(transparent)]
    Codec(#[from] hla_wire::CodecError),
}

struct PendingMap {
    inner: Mutex<HashMap<i32, oneshot::Sender<Vec<u8>>>>,
}

impl PendingMap {
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    fn insert(&self, seq: i32, tx: oneshot::Sender<Vec<u8>>) {
        self.inner.lock().insert(seq, tx);
    }

    fn remove(&self, seq: i32) -> Option<oneshot::Sender<Vec<u8>>> {
        self.inner.lock().remove(&seq)
    }

    fn drain(&self) -> Vec<oneshot::Sender<Vec<u8>>> {
        let mut g = self.inner.lock();
        g.drain().map(|(_, tx)| tx).collect()
    }
}

/// Federate-side ambassador handle. Cheap to `Arc`-clone — actually all
/// shared state lives behind the `Arc<Inner>` internally.
#[derive(Clone)]
pub struct RtiAmbassador {
    inner: Arc<Inner>,
}

struct Inner {
    session_id: u64,
    frame_tx: mpsc::Sender<Frame>,
    next_outbound_seq: AtomicI32,
    pending: Arc<PendingMap>,
    pump: Mutex<Option<JoinHandle<()>>>,
    /// Writer task handle. Held so it stays alive as long as the
    /// ambassador exists; aborted on Drop. Never read directly.
    #[allow(dead_code)]
    writer: Mutex<Option<JoinHandle<()>>>,
}

impl RtiAmbassador {
    /// Connect to an RTI URL of the form `rti://host:port` (or `host:port`
    /// for short). Plain TCP — for TLS use [`Self::connect_tls`].
    pub async fn connect<A: FederateAmbassador>(
        rti_url: &str,
        callbacks: A,
    ) -> Result<Self, ConnectError> {
        let addr = parse_rti_url(rti_url)?;
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(ConnectError::Connect)?;
        Self::connect_with_stream(stream, callbacks).await
    }

    /// Connect via TLS. Caller provides the rustls `ClientConfig` (root
    /// trust store, ALPN, etc.) and the `ServerName` expected on the
    /// server's certificate.
    pub async fn connect_tls<A: FederateAmbassador>(
        rti_url: &str,
        tls_config: Arc<rustls::ClientConfig>,
        server_name: rustls::pki_types::ServerName<'static>,
        callbacks: A,
    ) -> Result<Self, ConnectError> {
        let addr = parse_rti_url(rti_url)?;
        let tcp = TcpStream::connect(&addr)
            .await
            .map_err(ConnectError::Connect)?;
        let connector = tokio_rustls::TlsConnector::from(tls_config);
        let stream = connector
            .connect(server_name, tcp)
            .await
            .map_err(ConnectError::Connect)?;
        Self::connect_with_stream(stream, callbacks).await
    }

    /// Generic core: takes any already-connected `AsyncRead + AsyncWrite`
    /// stream, performs the FedPro handshake via byte stream, and spins up
    /// the pump + writer tasks.
    pub async fn connect_with_stream<S, A>(mut stream: S, callbacks: A) -> Result<Self, ConnectError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        A: FederateAmbassador,
    {
        let ack = client_open_session(&mut stream).await?;
        let (read_half, write_half) = tokio::io::split(stream);
        let source: Box<dyn FrameSource + Send + 'static> =
            Box::new(AsyncReadSource(read_half));
        let sink: Box<dyn FrameSink + Send + 'static> =
            Box::new(AsyncWriteSink(write_half));
        Self::spin_up_with_transport(source, sink, ack.session_id, callbacks).await
    }

    /// Connect via WebSocket. The URL form is `ws://host:port` for now —
    /// secure WebSocket (`wss://`) can be layered on top of `connect_with_transport`.
    pub async fn connect_ws<A: FederateAmbassador>(
        rti_url: &str,
        callbacks: A,
    ) -> Result<Self, ConnectError> {
        let ws_url = if rti_url.starts_with("ws://") || rti_url.starts_with("wss://") {
            rti_url.to_string()
        } else if let Some(rest) = rti_url.strip_prefix("rti://") {
            format!("ws://{rest}")
        } else {
            format!("ws://{rti_url}")
        };
        let (ws, _) = tokio_tungstenite::connect_async(&ws_url)
            .await
            .map_err(|e| ConnectError::Connect(std::io::Error::other(format!("ws: {e}"))))?;
        let (mut source, mut sink) = hla_wire::split_ws(ws);
        let ack = client_open_session_frames(&mut source, &mut sink).await?;
        let source_boxed: Box<dyn FrameSource + Send + 'static> = Box::new(source);
        let sink_boxed: Box<dyn FrameSink + Send + 'static> = Box::new(sink);
        Self::spin_up_with_transport(source_boxed, sink_boxed, ack.session_id, callbacks).await
    }

    /// Internal: given a handshake-completed `FrameSource`/`FrameSink` pair
    /// and the assigned `session_id`, spawn the writer + pump tasks.
    async fn spin_up_with_transport<A>(
        mut source: Box<dyn FrameSource + Send + 'static>,
        mut sink: Box<dyn FrameSink + Send + 'static>,
        session_id: u64,
        callbacks: A,
    ) -> Result<Self, ConnectError>
    where
        A: FederateAmbassador,
    {
        let _ = &mut source; // explicit ownership transfer
        let _ = &mut sink;
        let (frame_tx, mut frame_rx) = mpsc::channel::<Frame>(128);
        let writer = tokio::spawn(async move {
            while let Some(frame) = frame_rx.recv().await {
                if sink.send_frame(&frame).await.is_err() {
                    break;
                }
            }
        });

        let pending = Arc::new(PendingMap::new());
        let inner = Arc::new(Inner {
            session_id,
            frame_tx: frame_tx.clone(),
            next_outbound_seq: AtomicI32::new(1),
            pending: Arc::clone(&pending),
            pump: Mutex::new(None),
            writer: Mutex::new(Some(writer)),
        });

        let pump_pending = Arc::clone(&pending);
        let pump_tx = frame_tx.clone();
        let pump_session = session_id;
        let cb_arc: Arc<A> = Arc::new(callbacks);
        let pump = tokio::spawn(pump_loop(
            source,
            pump_pending,
            pump_tx,
            pump_session,
            cb_arc,
        ));
        *inner.pump.lock() = Some(pump);

        Ok(Self { inner })
    }

    /// Gracefully terminate the FedPro session and shut down the I/O tasks.
    pub async fn disconnect(&self) -> Result<(), CallError> {
        // Send CTRL_TERMINATE_SESSION. Don't bother awaiting CTRL_SESSION_TERMINATED;
        // the pump will see EOF and exit.
        let header = MessageHeader::with_payload_size(
            0,
            hla_wire::claim_next_outbound_seq(&self.inner.next_outbound_seq),
            self.inner.session_id,
            NO_SEQUENCE_NUMBER,
            MessageType::CtrlTerminateSession,
        );
        let _ = self
            .inner
            .frame_tx
            .send(Frame::new(header, Vec::new()))
            .await;
        // Wake every pending caller with `Closed`.
        for tx in self.inner.pending.drain() {
            let _ = tx.send(Vec::new());
        }
        if let Some(pump) = self.inner.pump.lock().take() {
            pump.abort();
        }
        Ok(())
    }

    pub fn session_id(&self) -> u64 {
        self.inner.session_id
    }

    // -------------------------------------------------------------------------
    // Core call helper — sends a CallRequest variant, awaits the response.
    // -------------------------------------------------------------------------

    /// Escape hatch for tests: send an arbitrary `CallRequest` variant and
    /// return the decoded `CallResponse`. Only compiled in tests.
    #[doc(hidden)]
    pub async fn raw_call_for_test(&self, request: Req) -> Result<Resp, CallError> {
        self.call(request).await
    }

    async fn call(&self, request: Req) -> Result<Resp, CallError> {
        let envelope = fedpro::CallRequest {
            call_request: Some(request),
        };
        let body = envelope.encode_to_vec();
        let seq = hla_wire::claim_next_outbound_seq(&self.inner.next_outbound_seq);
        let (tx, rx) = oneshot::channel();
        self.inner.pending.insert(seq, tx);

        let frame =
            hla_wire::hla_call_request_frame(seq, self.inner.session_id, 0, body);
        self.inner
            .frame_tx
            .send(frame)
            .await
            .map_err(|_| CallError::Closed)?;

        let body = rx.await.map_err(|_| CallError::Closed)?;
        if body.is_empty() {
            return Err(CallError::Closed);
        }
        let envelope = fedpro::CallResponse::decode(&body[..])
            .map_err(|e| CallError::Decode(e.to_string()))?;
        envelope.call_response.ok_or(CallError::UnexpectedResponse)
    }

    fn check_exception(resp: Resp) -> Result<Resp, CallError> {
        match resp {
            Resp::ExceptionData(e) => Err(CallError::RtiException {
                name: e.exception_name,
                details: e.details,
            }),
            other => Ok(other),
        }
    }

    // -------------------------------------------------------------------------
    // Federation Management
    // -------------------------------------------------------------------------

    pub async fn create_federation_execution(&self, name: &str) -> Result<(), CallError> {
        let r = self
            .call(Req::CreateFederationExecutionRequest(
                fedpro::CreateFederationExecutionRequest {
                    federation_name: name.into(),
                    fom_module: None,
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::CreateFederationExecutionResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    /// Create a federation with one or more inline FOM modules. Each
    /// `(name, xml_bytes)` pair becomes a `FileFomModule`. The RTI parses
    /// and merges them.
    pub async fn create_federation_execution_with_modules(
        &self,
        name: &str,
        modules: Vec<(String, Vec<u8>)>,
    ) -> Result<(), CallError> {
        let fom_modules: Vec<fedpro::FomModule> = modules
            .into_iter()
            .map(|(name, content)| fedpro::FomModule {
                fom_module: Some(fedpro::fom_module::FomModule::File(fedpro::FileFomModule {
                    name,
                    content,
                })),
            })
            .collect();
        let r = self
            .call(Req::CreateFederationExecutionWithModulesRequest(
                fedpro::CreateFederationExecutionWithModulesRequest {
                    federation_name: name.into(),
                    fom_modules: Some(fedpro::FomModuleSet {
                        fom_module: fom_modules,
                    }),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::CreateFederationExecutionWithModulesResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    /// Request the list of currently-active federation executions. The list
    /// itself arrives via the [`FederateAmbassador::report_federation_executions`]
    /// callback per IEEE 1516.1.
    pub async fn list_federation_executions(&self) -> Result<(), CallError> {
        let r = self
            .call(Req::ListFederationExecutionsRequest(
                fedpro::ListFederationExecutionsRequest {},
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::ListFederationExecutionsResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    /// Request the list of federates currently joined to `federation_name`.
    /// Result arrives via [`FederateAmbassador::report_federation_execution_members`]
    /// or [`FederateAmbassador::report_federation_execution_does_not_exist`].
    pub async fn list_federation_execution_members(
        &self,
        federation_name: &str,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::ListFederationExecutionMembersRequest(
                fedpro::ListFederationExecutionMembersRequest {
                    federation_name: federation_name.into(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::ListFederationExecutionMembersResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    /// Federate-level Connect (HLA service, not the transport-level connect).
    /// For our impl this is a no-op — the FedPro session opening *is* the
    /// connect — but we expose it for spec completeness.
    pub async fn connect_federation(&self) -> Result<(), CallError> {
        let r = self.call(Req::ConnectRequest(fedpro::ConnectRequest {})).await?;
        match Self::check_exception(r)? {
            Resp::ConnectResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn disconnect_federation(&self) -> Result<(), CallError> {
        let r = self.call(Req::DisconnectRequest(fedpro::DisconnectRequest {})).await?;
        match Self::check_exception(r)? {
            Resp::DisconnectResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn get_federate_handle(&self, name: &str) -> Result<FederateHandle, CallError> {
        let r = self
            .call(Req::GetFederateHandleRequest(
                fedpro::GetFederateHandleRequest { federate_name: name.into() },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::GetFederateHandleResponse(g) => g
                .result
                .as_ref()
                .and_then(crate::handles::decode_federate)
                .ok_or(CallError::UnexpectedResponse),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn get_federate_name(&self, handle: FederateHandle) -> Result<String, CallError> {
        let r = self
            .call(Req::GetFederateNameRequest(
                fedpro::GetFederateNameRequest {
                    federate: Some(fedpro::FederateHandle {
                        data: handle.raw().to_be_bytes().to_vec(),
                    }),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::GetFederateNameResponse(g) => Ok(g.result),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn destroy_federation_execution(&self, name: &str) -> Result<(), CallError> {
        let r = self
            .call(Req::DestroyFederationExecutionRequest(
                fedpro::DestroyFederationExecutionRequest {
                    federation_name: name.into(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::DestroyFederationExecutionResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn join_federation_execution_with_name(
        &self,
        federate_name: &str,
        federate_type: &str,
        federation: &str,
    ) -> Result<FederateHandle, CallError> {
        let r = self
            .call(Req::JoinFederationExecutionWithNameRequest(
                fedpro::JoinFederationExecutionWithNameRequest {
                    federate_name: federate_name.into(),
                    federate_type: federate_type.into(),
                    federation_name: federation.into(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::JoinFederationExecutionWithNameResponse(j) => {
                let result = j.result.ok_or(CallError::UnexpectedResponse)?;
                let h = result
                    .federate_handle
                    .as_ref()
                    .and_then(decode_federate)
                    .ok_or(CallError::UnexpectedResponse)?;
                Ok(h)
            }
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn join_federation_execution(
        &self,
        federate_type: &str,
        federation: &str,
    ) -> Result<FederateHandle, CallError> {
        let r = self
            .call(Req::JoinFederationExecutionRequest(
                fedpro::JoinFederationExecutionRequest {
                    federate_type: federate_type.into(),
                    federation_name: federation.into(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::JoinFederationExecutionResponse(j) => {
                let result = j.result.ok_or(CallError::UnexpectedResponse)?;
                let h = result
                    .federate_handle
                    .as_ref()
                    .and_then(decode_federate)
                    .ok_or(CallError::UnexpectedResponse)?;
                Ok(h)
            }
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn resign_federation_execution(
        &self,
        action: ResignAction,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::ResignFederationExecutionRequest(
                fedpro::ResignFederationExecutionRequest {
                    resign_action: encode_resign_action(action),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::ResignFederationExecutionResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    // -------------------------------------------------------------------------
    // Handle lookup
    // -------------------------------------------------------------------------

    pub async fn get_object_class_handle(
        &self,
        name: &str,
    ) -> Result<ObjectClassHandle, CallError> {
        let r = self
            .call(Req::GetObjectClassHandleRequest(
                fedpro::GetObjectClassHandleRequest {
                    object_class_name: name.into(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::GetObjectClassHandleResponse(g) => g
                .result
                .as_ref()
                .and_then(decode_object_class)
                .ok_or(CallError::UnexpectedResponse),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn get_attribute_handle(
        &self,
        class: ObjectClassHandle,
        name: &str,
    ) -> Result<AttributeHandle, CallError> {
        let r = self
            .call(Req::GetAttributeHandleRequest(
                fedpro::GetAttributeHandleRequest {
                    object_class: Some(encode_object_class(class)),
                    attribute_name: name.into(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::GetAttributeHandleResponse(g) => g
                .result
                .as_ref()
                .and_then(|h| crate::handles::decode_attribute(h))
                .ok_or(CallError::UnexpectedResponse),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn get_interaction_class_handle(
        &self,
        name: &str,
    ) -> Result<InteractionClassHandle, CallError> {
        let r = self
            .call(Req::GetInteractionClassHandleRequest(
                fedpro::GetInteractionClassHandleRequest {
                    interaction_class_name: name.into(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::GetInteractionClassHandleResponse(g) => g
                .result
                .as_ref()
                .and_then(decode_interaction_class)
                .ok_or(CallError::UnexpectedResponse),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn get_parameter_handle(
        &self,
        class: InteractionClassHandle,
        name: &str,
    ) -> Result<hla_core::ParameterHandle, CallError> {
        let r = self
            .call(Req::GetParameterHandleRequest(
                fedpro::GetParameterHandleRequest {
                    interaction_class: Some(encode_interaction_class(class)),
                    parameter_name: name.into(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::GetParameterHandleResponse(g) => g
                .result
                .as_ref()
                .and_then(crate::handles::decode_parameter)
                .ok_or(CallError::UnexpectedResponse),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    // -------------------------------------------------------------------------
    // Declaration Management
    // -------------------------------------------------------------------------

    pub async fn publish_object_class_attributes(
        &self,
        class: ObjectClassHandle,
        attrs: AttributeHandleSet,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::PublishObjectClassAttributesRequest(
                fedpro::PublishObjectClassAttributesRequest {
                    object_class: Some(encode_object_class(class)),
                    attributes: Some(encode_attr_set(&attrs)),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::PublishObjectClassAttributesResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn subscribe_object_class_attributes(
        &self,
        class: ObjectClassHandle,
        attrs: AttributeHandleSet,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::SubscribeObjectClassAttributesRequest(
                fedpro::SubscribeObjectClassAttributesRequest {
                    object_class: Some(encode_object_class(class)),
                    attributes: Some(encode_attr_set(&attrs)),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::SubscribeObjectClassAttributesResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn unsubscribe_object_class_attributes(
        &self,
        class: ObjectClassHandle,
        attrs: AttributeHandleSet,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::UnsubscribeObjectClassAttributesRequest(
                fedpro::UnsubscribeObjectClassAttributesRequest {
                    object_class: Some(encode_object_class(class)),
                    attributes: Some(encode_attr_set(&attrs)),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::UnsubscribeObjectClassAttributesResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn unsubscribe_interaction_class(
        &self,
        class: InteractionClassHandle,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::UnsubscribeInteractionClassRequest(
                fedpro::UnsubscribeInteractionClassRequest {
                    interaction_class: Some(encode_interaction_class(class)),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::UnsubscribeInteractionClassResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn unpublish_object_class_attributes(
        &self,
        class: ObjectClassHandle,
        attrs: AttributeHandleSet,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::UnpublishObjectClassAttributesRequest(
                fedpro::UnpublishObjectClassAttributesRequest {
                    object_class: Some(encode_object_class(class)),
                    attributes: Some(encode_attr_set(&attrs)),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::UnpublishObjectClassAttributesResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn unpublish_interaction_class(
        &self,
        class: InteractionClassHandle,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::UnpublishInteractionClassRequest(
                fedpro::UnpublishInteractionClassRequest {
                    interaction_class: Some(encode_interaction_class(class)),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::UnpublishInteractionClassResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn publish_interaction_class(
        &self,
        class: InteractionClassHandle,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::PublishInteractionClassRequest(
                fedpro::PublishInteractionClassRequest {
                    interaction_class: Some(encode_interaction_class(class)),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::PublishInteractionClassResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn subscribe_interaction_class(
        &self,
        class: InteractionClassHandle,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::SubscribeInteractionClassRequest(
                fedpro::SubscribeInteractionClassRequest {
                    interaction_class: Some(encode_interaction_class(class)),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::SubscribeInteractionClassResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    // -------------------------------------------------------------------------
    // Object Management
    // -------------------------------------------------------------------------

    pub async fn register_object_instance(
        &self,
        class: ObjectClassHandle,
    ) -> Result<ObjectInstanceHandle, CallError> {
        let r = self
            .call(Req::RegisterObjectInstanceRequest(
                fedpro::RegisterObjectInstanceRequest {
                    object_class: Some(encode_object_class(class)),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::RegisterObjectInstanceResponse(g) => g
                .result
                .as_ref()
                .and_then(decode_object_instance)
                .ok_or(CallError::UnexpectedResponse),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn update_attribute_values(
        &self,
        instance: ObjectInstanceHandle,
        values: AttributeHandleValueMap,
        tag: &[u8],
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::UpdateAttributeValuesRequest(
                fedpro::UpdateAttributeValuesRequest {
                    object_instance: Some(encode_object_instance(instance)),
                    attribute_values: Some(encode_attr_value_map(&values)),
                    user_supplied_tag: tag.to_vec(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::UpdateAttributeValuesResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn send_interaction(
        &self,
        class: InteractionClassHandle,
        params: ParameterHandleValueMap,
        tag: &[u8],
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::SendInteractionRequest(fedpro::SendInteractionRequest {
                interaction_class: Some(encode_interaction_class(class)),
                parameter_values: Some(encode_param_value_map(&params)),
                user_supplied_tag: tag.to_vec(),
            }))
            .await?;
        match Self::check_exception(r)? {
            Resp::SendInteractionResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    // -------------------------------------------------------------------------
    // Ownership Management
    // -------------------------------------------------------------------------

    pub async fn is_attribute_owned_by_federate(
        &self,
        instance: ObjectInstanceHandle,
        attribute: AttributeHandle,
    ) -> Result<bool, CallError> {
        let r = self
            .call(Req::IsAttributeOwnedByFederateRequest(
                fedpro::IsAttributeOwnedByFederateRequest {
                    object_instance: Some(encode_object_instance(instance)),
                    attribute: Some(encode_attribute(attribute)),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::IsAttributeOwnedByFederateResponse(b) => Ok(b.result),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn query_attribute_ownership(
        &self,
        instance: ObjectInstanceHandle,
        attributes: AttributeHandleSet,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::QueryAttributeOwnershipRequest(
                fedpro::QueryAttributeOwnershipRequest {
                    object_instance: Some(encode_object_instance(instance)),
                    attributes: Some(encode_attr_set(&attributes)),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::QueryAttributeOwnershipResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn attribute_ownership_acquisition_if_available(
        &self,
        instance: ObjectInstanceHandle,
        desired: AttributeHandleSet,
        tag: &[u8],
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::AttributeOwnershipAcquisitionIfAvailableRequest(
                fedpro::AttributeOwnershipAcquisitionIfAvailableRequest {
                    object_instance: Some(encode_object_instance(instance)),
                    desired_attributes: Some(encode_attr_set(&desired)),
                    user_supplied_tag: tag.to_vec(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::AttributeOwnershipAcquisitionIfAvailableResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    // -------------------------------------------------------------------------
    // DDM region lifecycle
    // -------------------------------------------------------------------------

    pub async fn create_region(
        &self,
        dimensions: &[hla_core::DimensionHandle],
    ) -> Result<hla_core::RegionHandle, CallError> {
        let r = self
            .call(Req::CreateRegionRequest(fedpro::CreateRegionRequest {
                dimensions: Some(fedpro::DimensionHandleSet {
                    dimension_handle: dimensions
                        .iter()
                        .map(|d| fedpro::DimensionHandle {
                            data: d.raw().to_be_bytes().to_vec(),
                        })
                        .collect(),
                }),
            }))
            .await?;
        match Self::check_exception(r)? {
            Resp::CreateRegionResponse(g) => {
                let h = g.result.ok_or(CallError::UnexpectedResponse)?;
                if h.data.len() == 8 {
                    Ok(hla_core::RegionHandle::new(u64::from_be_bytes(
                        h.data[..].try_into().unwrap(),
                    )))
                } else {
                    Err(CallError::UnexpectedResponse)
                }
            }
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn delete_region(&self, region: hla_core::RegionHandle) -> Result<(), CallError> {
        let r = self
            .call(Req::DeleteRegionRequest(fedpro::DeleteRegionRequest {
                region: Some(fedpro::RegionHandle {
                    data: region.raw().to_be_bytes().to_vec(),
                }),
            }))
            .await?;
        match Self::check_exception(r)? {
            Resp::DeleteRegionResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn set_range_bounds(
        &self,
        region: hla_core::RegionHandle,
        dimension: hla_core::DimensionHandle,
        lower: u32,
        upper: u32,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::SetRangeBoundsRequest(fedpro::SetRangeBoundsRequest {
                region: Some(fedpro::RegionHandle {
                    data: region.raw().to_be_bytes().to_vec(),
                }),
                dimension: Some(fedpro::DimensionHandle {
                    data: dimension.raw().to_be_bytes().to_vec(),
                }),
                range_bounds: Some(fedpro::RangeBounds { lower, upper }),
            }))
            .await?;
        match Self::check_exception(r)? {
            Resp::SetRangeBoundsResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn get_range_bounds(
        &self,
        region: hla_core::RegionHandle,
        dimension: hla_core::DimensionHandle,
    ) -> Result<(u32, u32), CallError> {
        let r = self
            .call(Req::GetRangeBoundsRequest(fedpro::GetRangeBoundsRequest {
                region: Some(fedpro::RegionHandle {
                    data: region.raw().to_be_bytes().to_vec(),
                }),
                dimension: Some(fedpro::DimensionHandle {
                    data: dimension.raw().to_be_bytes().to_vec(),
                }),
            }))
            .await?;
        match Self::check_exception(r)? {
            Resp::GetRangeBoundsResponse(g) => {
                let b = g.result.ok_or(CallError::UnexpectedResponse)?;
                Ok((b.lower, b.upper))
            }
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn commit_region_modifications(
        &self,
        regions: &[hla_core::RegionHandle],
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::CommitRegionModificationsRequest(
                fedpro::CommitRegionModificationsRequest {
                    regions: Some(fedpro::RegionHandleSet {
                        region_handle: regions
                            .iter()
                            .map(|h| fedpro::RegionHandle {
                                data: h.raw().to_be_bytes().to_vec(),
                            })
                            .collect(),
                    }),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::CommitRegionModificationsResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn unconditional_attribute_ownership_divestiture(
        &self,
        instance: ObjectInstanceHandle,
        attributes: AttributeHandleSet,
        tag: &[u8],
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::UnconditionalAttributeOwnershipDivestitureRequest(
                fedpro::UnconditionalAttributeOwnershipDivestitureRequest {
                    object_instance: Some(encode_object_instance(instance)),
                    attributes: Some(encode_attr_set(&attributes)),
                    user_supplied_tag: tag.to_vec(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::UnconditionalAttributeOwnershipDivestitureResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn delete_object_instance(
        &self,
        instance: ObjectInstanceHandle,
        tag: &[u8],
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::DeleteObjectInstanceRequest(
                fedpro::DeleteObjectInstanceRequest {
                    object_instance: Some(encode_object_instance(instance)),
                    user_supplied_tag: tag.to_vec(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::DeleteObjectInstanceResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    // -------------------------------------------------------------------------
    // Time Management
    // -------------------------------------------------------------------------

    pub async fn enable_time_regulation(&self, lookahead: f64) -> Result<(), CallError> {
        let r = self
            .call(Req::EnableTimeRegulationRequest(
                fedpro::EnableTimeRegulationRequest {
                    lookahead: Some(fedpro::LogicalTimeInterval {
                        data: lookahead.to_be_bytes().to_vec(),
                    }),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::EnableTimeRegulationResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn enable_time_constrained(&self) -> Result<(), CallError> {
        let r = self
            .call(Req::EnableTimeConstrainedRequest(
                fedpro::EnableTimeConstrainedRequest {},
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::EnableTimeConstrainedResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    // -------------------------------------------------------------------------
    // Federation Save (MVP: orchestration only)
    // -------------------------------------------------------------------------

    pub async fn request_federation_save(&self, label: &str) -> Result<(), CallError> {
        let r = self
            .call(Req::RequestFederationSaveRequest(
                fedpro::RequestFederationSaveRequest { label: label.into() },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::RequestFederationSaveResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn federate_save_begun(&self) -> Result<(), CallError> {
        let r = self
            .call(Req::FederateSaveBegunRequest(
                fedpro::FederateSaveBegunRequest {},
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::FederateSaveBegunResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn federate_save_complete(&self) -> Result<(), CallError> {
        let r = self
            .call(Req::FederateSaveCompleteRequest(
                fedpro::FederateSaveCompleteRequest {},
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::FederateSaveCompleteResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn request_federation_restore(&self, label: &str) -> Result<(), CallError> {
        let r = self
            .call(Req::RequestFederationRestoreRequest(
                fedpro::RequestFederationRestoreRequest { label: label.into() },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::RequestFederationRestoreResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn federate_restore_complete(&self) -> Result<(), CallError> {
        let r = self
            .call(Req::FederateRestoreCompleteRequest(
                fedpro::FederateRestoreCompleteRequest {},
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::FederateRestoreCompleteResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn federate_restore_not_complete(&self) -> Result<(), CallError> {
        let r = self
            .call(Req::FederateRestoreNotCompleteRequest(
                fedpro::FederateRestoreNotCompleteRequest {},
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::FederateRestoreNotCompleteResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn federate_save_not_complete(&self) -> Result<(), CallError> {
        let r = self
            .call(Req::FederateSaveNotCompleteRequest(
                fedpro::FederateSaveNotCompleteRequest {},
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::FederateSaveNotCompleteResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    // -------------------------------------------------------------------------
    // Synchronization Points
    // -------------------------------------------------------------------------

    pub async fn register_federation_synchronization_point(
        &self,
        label: &str,
        tag: &[u8],
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::RegisterFederationSynchronizationPointRequest(
                fedpro::RegisterFederationSynchronizationPointRequest {
                    synchronization_point_label: label.into(),
                    user_supplied_tag: tag.to_vec(),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::RegisterFederationSynchronizationPointResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn synchronization_point_achieved(
        &self,
        label: &str,
        successfully: bool,
    ) -> Result<(), CallError> {
        let r = self
            .call(Req::SynchronizationPointAchievedRequest(
                fedpro::SynchronizationPointAchievedRequest {
                    synchronization_point_label: label.into(),
                    successfully,
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::SynchronizationPointAchievedResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }

    pub async fn time_advance_request(&self, time: f64) -> Result<(), CallError> {
        let r = self
            .call(Req::TimeAdvanceRequestRequest(
                fedpro::TimeAdvanceRequestRequest {
                    time: Some(fedpro::LogicalTime {
                        data: time.to_be_bytes().to_vec(),
                    }),
                },
            ))
            .await?;
        match Self::check_exception(r)? {
            Resp::TimeAdvanceRequestResponse(_) => Ok(()),
            _ => Err(CallError::UnexpectedResponse),
        }
    }
}

// -----------------------------------------------------------------------------
// Pump task
// -----------------------------------------------------------------------------

async fn pump_loop<A>(
    mut source: Box<dyn FrameSource + Send + 'static>,
    pending: Arc<PendingMap>,
    frame_tx: mpsc::Sender<Frame>,
    session_id: u64,
    callbacks: Arc<A>,
) where
    A: FederateAmbassador,
{
    loop {
        let frame = match source.recv_frame().await {
            Ok(f) => f,
            Err(_) => {
                tracing::debug!(session_id, "federate pump exiting on read failure");
                for tx in pending.drain() {
                    let _ = tx.send(Vec::new());
                }
                return;
            }
        };
        match frame.header.message_type {
            MessageType::HlaCallResponse => {
                let payload = match HlaCallResponsePayload::decode(&frame.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(error = %e, "bad HLA_CALL_RESPONSE");
                        continue;
                    }
                };
                if let Some(tx) = pending.remove(payload.response_to_sequence_number) {
                    let _ = tx.send(payload.body);
                } else {
                    tracing::warn!(
                        seq = payload.response_to_sequence_number,
                        "no pending waiter for response"
                    );
                }
            }
            MessageType::HlaCallbackRequest => {
                let cb = match fedpro::CallbackRequest::decode(&frame.payload[..]) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(error = %e, "bad CallbackRequest");
                        continue;
                    }
                };
                dispatch_callback(&callbacks, cb).await;
            }
            MessageType::CtrlHeartbeat => {
                // Respond.
                let header = MessageHeader::with_payload_size(
                    0,
                    NO_SEQUENCE_NUMBER,
                    session_id,
                    frame.header.sequence_number,
                    MessageType::CtrlHeartbeatResponse,
                );
                let _ = frame_tx.send(Frame::new(header, Vec::new())).await;
            }
            MessageType::CtrlHeartbeatResponse | MessageType::CtrlSessionTerminated => {
                // Acks; ignore (or close on terminate)
            }
            other => {
                tracing::trace!(?other, "federate pump: unexpected frame type");
            }
        }
    }
}

async fn dispatch_callback<A: FederateAmbassador>(
    callbacks: &Arc<A>,
    request: fedpro::CallbackRequest,
) {
    use fedpro::callback_request::CallbackRequest as CB;
    let Some(variant) = request.callback_request else {
        return;
    };
    match variant {
        CB::DiscoverObjectInstance(d) => {
            let Some(instance) = d.object_instance.as_ref().and_then(decode_object_instance) else {
                return;
            };
            let Some(class) = d.object_class.as_ref().and_then(decode_object_class) else {
                return;
            };
            let producer = d.producing_federate.as_ref().and_then(decode_federate);
            callbacks
                .discover_object_instance(instance, class, d.object_instance_name, producer)
                .await;
        }
        CB::ReflectAttributeValues(r) => {
            let Some(instance) = r.object_instance.as_ref().and_then(decode_object_instance) else {
                return;
            };
            let values = r
                .attribute_values
                .as_ref()
                .map(decode_attribute_value_map)
                .unwrap_or_default();
            let producer = r.producing_federate.as_ref().and_then(decode_federate);
            callbacks
                .reflect_attribute_values(instance, values, r.user_supplied_tag, producer)
                .await;
        }
        CB::ReceiveInteraction(i) => {
            let Some(class) = i.interaction_class.as_ref().and_then(decode_interaction_class)
            else {
                return;
            };
            let params = i
                .parameter_values
                .as_ref()
                .map(decode_parameter_value_map)
                .unwrap_or_default();
            let producer = i.producing_federate.as_ref().and_then(decode_federate);
            callbacks
                .receive_interaction(class, params, i.user_supplied_tag, producer)
                .await;
        }
        CB::RemoveObjectInstance(r) => {
            let Some(instance) = r.object_instance.as_ref().and_then(decode_object_instance) else {
                return;
            };
            let producer = r.producing_federate.as_ref().and_then(decode_federate);
            callbacks
                .remove_object_instance(instance, r.user_supplied_tag, producer)
                .await;
        }
        CB::TimeRegulationEnabled(t) => {
            if let Some(lt) = t.time.as_ref()
                && lt.data.len() == 8
            {
                callbacks
                    .time_regulation_enabled(f64::from_be_bytes(lt.data[..].try_into().unwrap()))
                    .await;
            }
        }
        CB::TimeConstrainedEnabled(t) => {
            if let Some(lt) = t.time.as_ref()
                && lt.data.len() == 8
            {
                callbacks
                    .time_constrained_enabled(f64::from_be_bytes(lt.data[..].try_into().unwrap()))
                    .await;
            }
        }
        CB::TimeAdvanceGrant(t) => {
            if let Some(lt) = t.time.as_ref()
                && lt.data.len() == 8
            {
                callbacks
                    .time_advance_grant(f64::from_be_bytes(lt.data[..].try_into().unwrap()))
                    .await;
            }
        }
        CB::SynchronizationPointRegistrationSucceeded(s) => {
            callbacks
                .synchronization_point_registration_succeeded(s.synchronization_point_label)
                .await;
        }
        CB::SynchronizationPointRegistrationFailed(f) => {
            callbacks
                .synchronization_point_registration_failed(
                    f.synchronization_point_label,
                    f.reason,
                )
                .await;
        }
        CB::AnnounceSynchronizationPoint(a) => {
            callbacks
                .announce_synchronization_point(
                    a.synchronization_point_label,
                    a.user_supplied_tag,
                )
                .await;
        }
        CB::ReportFederationExecutionMembers(r) => {
            let members: Vec<(String, String)> = r
                .report
                .map(|s| {
                    s.federation_execution_member_information
                        .into_iter()
                        .map(|m| (m.federate_name, m.federate_type))
                        .collect()
                })
                .unwrap_or_default();
            callbacks
                .report_federation_execution_members(r.federation_name, members)
                .await;
        }
        CB::ReportFederationExecutionDoesNotExist(r) => {
            callbacks
                .report_federation_execution_does_not_exist(r.federation_name)
                .await;
        }
        CB::ReportFederationExecutions(r) => {
            let names: Vec<String> = r
                .report
                .map(|s| {
                    s.federation_execution_information
                        .into_iter()
                        .map(|f| f.federation_execution_name)
                        .collect()
                })
                .unwrap_or_default();
            callbacks.report_federation_executions(names).await;
        }
        CB::RequestFederationRestoreSucceeded(s) => {
            callbacks.request_federation_restore_succeeded(s.label).await;
        }
        CB::RequestFederationRestoreFailed(f) => {
            callbacks.request_federation_restore_failed(f.label).await;
        }
        CB::FederationRestoreBegun(_) => {
            callbacks.federation_restore_begun().await;
        }
        CB::InitiateFederateRestore(i) => {
            let post = i
                .post_restore_federate_handle
                .as_ref()
                .and_then(decode_federate)
                .unwrap_or(FederateHandle::new(0));
            callbacks
                .initiate_federate_restore(i.label, i.federate_name, post)
                .await;
        }
        CB::FederationRestored(_) => {
            callbacks.federation_restored().await;
        }
        CB::FederationNotRestored(f) => {
            callbacks.federation_not_restored(f.reason).await;
        }
        CB::InitiateFederateSave(s) => {
            callbacks.initiate_federate_save(s.label).await;
        }
        CB::FederationSaved(_) => {
            callbacks.federation_saved().await;
        }
        CB::FederationNotSaved(f) => {
            callbacks.federation_not_saved(f.reason).await;
        }
        CB::InformAttributeOwnership(i) => {
            let Some(instance) = i.object_instance.as_ref().and_then(decode_object_instance) else {
                return;
            };
            let attrs: Vec<_> = i
                .attributes
                .as_ref()
                .map(|s| {
                    s.attribute_handle
                        .iter()
                        .filter_map(crate::handles::decode_attribute)
                        .collect()
                })
                .unwrap_or_default();
            let Some(owner) = i.federate.as_ref().and_then(decode_federate) else {
                return;
            };
            callbacks.inform_attribute_ownership(instance, attrs, owner).await;
        }
        CB::AttributeOwnershipAcquisitionNotification(n) => {
            let Some(instance) = n.object_instance.as_ref().and_then(decode_object_instance) else {
                return;
            };
            let attrs: Vec<_> = n
                .secured_attributes
                .as_ref()
                .map(|s| {
                    s.attribute_handle
                        .iter()
                        .filter_map(crate::handles::decode_attribute)
                        .collect()
                })
                .unwrap_or_default();
            callbacks
                .attribute_ownership_acquisition_notification(instance, attrs, n.user_supplied_tag)
                .await;
        }
        CB::AttributeOwnershipUnavailable(u) => {
            let Some(instance) = u.object_instance.as_ref().and_then(decode_object_instance) else {
                return;
            };
            let attrs: Vec<_> = u
                .attributes
                .as_ref()
                .map(|s| {
                    s.attribute_handle
                        .iter()
                        .filter_map(crate::handles::decode_attribute)
                        .collect()
                })
                .unwrap_or_default();
            callbacks
                .attribute_ownership_unavailable(instance, attrs, u.user_supplied_tag)
                .await;
        }
        CB::AttributeIsNotOwned(i) => {
            let Some(instance) = i.object_instance.as_ref().and_then(decode_object_instance) else {
                return;
            };
            let attrs: Vec<_> = i
                .attributes
                .as_ref()
                .map(|s| {
                    s.attribute_handle
                        .iter()
                        .filter_map(crate::handles::decode_attribute)
                        .collect()
                })
                .unwrap_or_default();
            callbacks.attribute_is_not_owned(instance, attrs).await;
        }
        CB::FederationSynchronized(s) => {
            let failed: std::collections::HashSet<FederateHandle> = s
                .failed_to_sync_set
                .map(|set| {
                    set.federate_handle
                        .iter()
                        .filter_map(decode_federate)
                        .collect()
                })
                .unwrap_or_default();
            callbacks
                .federation_synchronized(s.synchronization_point_label, failed)
                .await;
        }
        // Time-stamped data flow callbacks.
        CB::ReflectAttributeValuesWithTime(r) => {
            let Some(instance) = r.object_instance.as_ref().and_then(decode_object_instance) else {
                return;
            };
            let values = r.attribute_values.as_ref().map(decode_attribute_value_map).unwrap_or_default();
            let producer = r.producing_federate.as_ref().and_then(decode_federate);
            let time = r.time.as_ref().and_then(|lt| {
                if lt.data.len() == 8 {
                    Some(f64::from_be_bytes(lt.data[..].try_into().unwrap()))
                } else {
                    None
                }
            }).unwrap_or(0.0);
            callbacks
                .reflect_attribute_values_with_time(instance, values, r.user_supplied_tag, producer, time)
                .await;
        }
        CB::ReceiveInteractionWithTime(i) => {
            let Some(class) = i.interaction_class.as_ref().and_then(decode_interaction_class) else { return };
            let params = i.parameter_values.as_ref().map(decode_parameter_value_map).unwrap_or_default();
            let producer = i.producing_federate.as_ref().and_then(decode_federate);
            let time = i.time.as_ref().and_then(|lt| {
                if lt.data.len() == 8 { Some(f64::from_be_bytes(lt.data[..].try_into().unwrap())) } else { None }
            }).unwrap_or(0.0);
            callbacks.receive_interaction_with_time(class, params, i.user_supplied_tag, producer, time).await;
        }
        CB::RemoveObjectInstanceWithTime(r) => {
            let Some(instance) = r.object_instance.as_ref().and_then(decode_object_instance) else { return };
            let producer = r.producing_federate.as_ref().and_then(decode_federate);
            let time = r.time.as_ref().and_then(|lt| {
                if lt.data.len() == 8 { Some(f64::from_be_bytes(lt.data[..].try_into().unwrap())) } else { None }
            }).unwrap_or(0.0);
            callbacks.remove_object_instance_with_time(instance, r.user_supplied_tag, producer, time).await;
        }
        CB::ObjectInstanceNameReservationFailed(f) => {
            callbacks.object_instance_name_reservation_failed(f.object_instance_name).await;
        }
        CB::MultipleObjectInstanceNameReservationSucceeded(s) => {
            callbacks
                .multiple_object_instance_name_reservation_succeeded(s.object_instance_names.into_iter().collect())
                .await;
        }
        CB::MultipleObjectInstanceNameReservationFailed(f) => {
            callbacks
                .multiple_object_instance_name_reservation_failed(f.object_instance_names.into_iter().collect())
                .await;
        }
        CB::StartRegistrationForObjectClass(s) => {
            if let Some(class) = s.object_class.as_ref().and_then(decode_object_class) {
                callbacks.start_registration_for_object_class(class).await;
            }
        }
        CB::StopRegistrationForObjectClass(s) => {
            if let Some(class) = s.object_class.as_ref().and_then(decode_object_class) {
                callbacks.stop_registration_for_object_class(class).await;
            }
        }
        CB::TurnInteractionsOn(t) => {
            if let Some(class) = t.interaction_class.as_ref().and_then(decode_interaction_class) {
                callbacks.turn_interactions_on(class).await;
            }
        }
        CB::TurnInteractionsOff(t) => {
            if let Some(class) = t.interaction_class.as_ref().and_then(decode_interaction_class) {
                callbacks.turn_interactions_off(class).await;
            }
        }
        CB::TurnUpdatesOnForObjectInstance(t) => {
            let Some(instance) = t.object_instance.as_ref().and_then(decode_object_instance) else { return };
            let attrs: Vec<_> = t.attributes.as_ref().map(|s| s.attribute_handle.iter().filter_map(crate::handles::decode_attribute).collect()).unwrap_or_default();
            callbacks.turn_updates_on_for_object_instance(instance, attrs).await;
        }
        CB::TurnUpdatesOffForObjectInstance(t) => {
            let Some(instance) = t.object_instance.as_ref().and_then(decode_object_instance) else { return };
            let attrs: Vec<_> = t.attributes.as_ref().map(|s| s.attribute_handle.iter().filter_map(crate::handles::decode_attribute).collect()).unwrap_or_default();
            callbacks.turn_updates_off_for_object_instance(instance, attrs).await;
        }
        CB::AttributesInScope(a) => {
            let Some(instance) = a.object_instance.as_ref().and_then(decode_object_instance) else { return };
            let attrs: Vec<_> = a.attributes.as_ref().map(|s| s.attribute_handle.iter().filter_map(crate::handles::decode_attribute).collect()).unwrap_or_default();
            callbacks.attributes_in_scope(instance, attrs).await;
        }
        CB::AttributesOutOfScope(a) => {
            let Some(instance) = a.object_instance.as_ref().and_then(decode_object_instance) else { return };
            let attrs: Vec<_> = a.attributes.as_ref().map(|s| s.attribute_handle.iter().filter_map(crate::handles::decode_attribute).collect()).unwrap_or_default();
            callbacks.attributes_out_of_scope(instance, attrs).await;
        }
        CB::ProvideAttributeValueUpdate(p) => {
            let Some(instance) = p.object_instance.as_ref().and_then(decode_object_instance) else { return };
            let attrs: Vec<_> = p.attributes.as_ref().map(|s| s.attribute_handle.iter().filter_map(crate::handles::decode_attribute).collect()).unwrap_or_default();
            callbacks.provide_attribute_value_update(instance, attrs, p.user_supplied_tag).await;
        }
        CB::RequestAttributeOwnershipAssumption(r) => {
            let Some(instance) = r.object_instance.as_ref().and_then(decode_object_instance) else { return };
            let attrs: Vec<_> = r.offered_attributes.as_ref().map(|s| s.attribute_handle.iter().filter_map(crate::handles::decode_attribute).collect()).unwrap_or_default();
            callbacks.request_attribute_ownership_assumption(instance, attrs, r.user_supplied_tag).await;
        }
        CB::RequestAttributeOwnershipRelease(r) => {
            let Some(instance) = r.object_instance.as_ref().and_then(decode_object_instance) else { return };
            let attrs: Vec<_> = r.candidate_attributes.as_ref().map(|s| s.attribute_handle.iter().filter_map(crate::handles::decode_attribute).collect()).unwrap_or_default();
            callbacks.request_attribute_ownership_release(instance, attrs, r.user_supplied_tag).await;
        }
        CB::RequestDivestitureConfirmation(r) => {
            let Some(instance) = r.object_instance.as_ref().and_then(decode_object_instance) else { return };
            let attrs: Vec<_> = r.released_attributes.as_ref().map(|s| s.attribute_handle.iter().filter_map(crate::handles::decode_attribute).collect()).unwrap_or_default();
            callbacks.request_divestiture_confirmation(instance, attrs, r.user_supplied_tag).await;
        }
        CB::AttributeIsOwnedByRti(a) => {
            let Some(instance) = a.object_instance.as_ref().and_then(decode_object_instance) else { return };
            let attrs: Vec<_> = a.attributes.as_ref().map(|s| s.attribute_handle.iter().filter_map(crate::handles::decode_attribute).collect()).unwrap_or_default();
            callbacks.attribute_is_owned_by_rti(instance, attrs).await;
        }
        CB::ConfirmAttributeOwnershipAcquisitionCancellation(c) => {
            let Some(instance) = c.object_instance.as_ref().and_then(decode_object_instance) else { return };
            let attrs: Vec<_> = c.attributes.as_ref().map(|s| s.attribute_handle.iter().filter_map(crate::handles::decode_attribute).collect()).unwrap_or_default();
            callbacks.confirm_attribute_ownership_acquisition_cancellation(instance, attrs).await;
        }
        CB::RequestRetraction(_r) => {
            callbacks.request_retraction(Vec::new()).await;
        }
        CB::FlushQueueGrant(g) => {
            let time = g.time.as_ref().and_then(|lt| {
                if lt.data.len() == 8 { Some(f64::from_be_bytes(lt.data[..].try_into().unwrap())) } else { None }
            }).unwrap_or(0.0);
            callbacks.flush_queue_grant(time, None).await;
        }
        CB::InitiateFederateSaveWithTime(i) => {
            let time = i.time.as_ref().and_then(|lt| {
                if lt.data.len() == 8 { Some(f64::from_be_bytes(lt.data[..].try_into().unwrap())) } else { None }
            }).unwrap_or(0.0);
            callbacks.initiate_federate_save_with_time(i.label, time).await;
        }
        CB::FederationSaveStatusResponse(_) => {
            callbacks.federation_save_status_response().await;
        }
        CB::FederationRestoreStatusResponse(_) => {
            callbacks.federation_restore_status_response().await;
        }
        CB::ConnectionLost(c) => {
            callbacks.connection_lost(c.fault_description).await;
        }
        CB::FederateResigned(f) => {
            callbacks.federate_resigned(f.reason_for_resign_description).await;
        }
        CB::ReceiveDirectedInteraction(_) | CB::ReceiveDirectedInteractionWithTime(_) => {
            // Directed-interaction routing not yet implemented end-to-end.
            callbacks.raw_callback("DirectedInteraction").await;
        }
        CB::ReportAttributeTransportationType(_) | CB::ReportInteractionTransportationType(_) => {
            callbacks.raw_callback("ReportTransportationType").await;
        }
        _ => {
            callbacks.raw_callback("<unhandled>").await;
        }
    }
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

fn parse_rti_url(url: &str) -> Result<String, ConnectError> {
    if let Some(rest) = url.strip_prefix("rti://") {
        Ok(rest.to_string())
    } else {
        Ok(url.to_string())
    }
}

fn encode_resign_action(a: ResignAction) -> i32 {
    match a {
        ResignAction::UnconditionallyDivestAttributes => 0,
        ResignAction::DeleteObjects => 1,
        ResignAction::CancelPendingOwnershipAcquisitions => 2,
        ResignAction::DeleteObjectsThenDivest => 3,
        ResignAction::CancelThenDeleteThenDivest => 4,
        ResignAction::NoAction => 5,
    }
}

fn encode_attr_set(set: &AttributeHandleSet) -> fedpro::AttributeHandleSet {
    fedpro::AttributeHandleSet {
        attribute_handle: set.iter().copied().map(encode_attribute).collect(),
    }
}

fn encode_attr_value_map(map: &AttributeHandleValueMap) -> fedpro::AttributeHandleValueMap {
    fedpro::AttributeHandleValueMap {
        attribute_handle_value: map
            .iter()
            .map(|(h, v)| fedpro::AttributeHandleValue {
                attribute_handle: Some(encode_attribute(*h)),
                value: v.clone(),
            })
            .collect(),
    }
}

fn encode_param_value_map(map: &ParameterHandleValueMap) -> fedpro::ParameterHandleValueMap {
    fedpro::ParameterHandleValueMap {
        parameter_handle_value: map
            .iter()
            .map(|(h, v)| fedpro::ParameterHandleValue {
                parameter_handle: Some(encode_parameter(*h)),
                value: v.clone(),
            })
            .collect(),
    }
}
