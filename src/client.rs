use itertools::Itertools;
use prost_types::FileDescriptorProto;
use quote::quote;

use proc_macro2::{Ident, Literal, TokenStream};

use crate::{FileProtoInfo, MethodProtoInfo, ServiceProtoInfo};

pub fn generate_grpc_client_impl(files: Vec<FileDescriptorProto>) -> String {
    let use_statements = generate_use_statements();
    let static_value_struct = generate_static_value_struct();
    let grpc_client_handler = generate_grpc_client_handler();
    let client_handler_methods = generate_grpc_client_handler_methods(files.clone());
    let tremor_grpc_client = generate_tremor_grpc_client();
    let tremor_grpc_impl = generate_tremor_grpc_client_impls(files);
    let insert_request_headers = generate_insert_request_headers();
    let insert_headers_in_meta = generate_insert_headers_in_meta();
    let code = quote! {
        #use_statements
        #insert_request_headers
        #insert_headers_in_meta
        #static_value_struct
        #grpc_client_handler
        #client_handler_methods
        #tremor_grpc_client
        #tremor_grpc_impl
    };

    let formatted_code = format!("{}", code);
    formatted_code
}

// Generat all use statements.
fn generate_use_statements() -> TokenStream {
    quote! {
        use async_std::prelude::StreamExt;
        use log::error;
        use value_trait::{ValueAccess, Mutable};
        use tremor_value::value::Value;
        use tonic::metadata::{KeyAndValueRef, AsciiMetadataValue, MetadataKey};
    }
}

fn generate_static_value_struct() -> TokenStream {
    // StaticValue is a wrapper around tremor_value::Value to get around async_trait lifetime caveats.
    // See https://github.com/dtolnay/async-trait/issues/177.
    quote! {
        struct StaticValue(tremor_value::Value<'static>);
    }
}

fn generate_grpc_client_handler() -> TokenStream {
    // GrpcClientHandler is what application code should work with and execute buisness logic.
    quote! {
        #[derive(Debug)]
        pub struct GrpcClientHandler {
            clients: hashbrown::HashMap<String, Box<dyn TremorGrpcClient>>,
            senders: hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>>,
            methods: hashbrown::HashMap<String, prost_types::MethodDescriptorProto>,
            reply_tx: async_std::channel::Sender<tremor_script::EventPayload>
        }
    }
}

fn generate_grpc_client_handler_methods(files: Vec<FileDescriptorProto>) -> TokenStream {
    let connect_code = generate_grpc_client_handler_connect(files);
    let send_request_code = generate_client_handler_send_request();
    quote! {
        impl GrpcClientHandler {
            #connect_code
            #send_request_code
        }
    }
}

fn generate_grpc_client_handler_connect(files: Vec<FileDescriptorProto>) -> TokenStream {
    let mut clients = vec![];
    let mut methods = vec![];
    for file in files {
        let mod_ident = file.mod_ident();
        let package = file.package();
        for service in &file.service {
            let client_mod_ident = service.client_mod_ident();
            let client_ident = service.client_ident();
            let client_identifier =
                quote::format_ident!("{}_{}", package.split('.').join("_"), client_mod_ident);
            let client_key_token = Literal::string(&format!("{}.{}", package, service.name()));
            clients.push(quote! {
                let #client_identifier = #mod_ident::#client_mod_ident::#client_ident::connect(addr.clone()).await?;
                client_map.insert(String::from(#client_key_token), Box::new(#client_identifier));
            });
            for method in &service.method {
                let method_key_token =
                    Literal::string(&format!("{}.{}/{}", package, service.name(), method.name()));
                let method_name = Literal::string(method.name());
                let method_name_expr = quote! {Some(String::from(#method_name))};
                let input_type_name = Literal::string(method.input_type());
                let input_type_expr = quote! {Some(String::from(#input_type_name))};
                let output_type_name = Literal::string(method.output_type());
                let output_type_expr = quote! {Some(String::from(#output_type_name))};
                let client_streaming = quote::format_ident!("{}", method.client_streaming());
                let client_streaming_expr = quote! {Some(#client_streaming)};
                let server_streaming = quote::format_ident!("{}", method.server_streaming());
                let server_streaming_expr = quote! {Some(#server_streaming)};
                methods.push(quote! {
                    methods_map.insert(String::from(#method_key_token), prost_types::MethodDescriptorProto {
                        name: #method_name_expr,
                        input_type: #input_type_expr,
                        output_type: #output_type_expr,
                        client_streaming: #client_streaming_expr,
                        server_streaming: #server_streaming_expr,
                        options: None
                    });
                });
            }
        }
    }
    let mut clients_code = TokenStream::default();
    clients_code.extend(clients);
    let mut methods_code = TokenStream::default();
    methods_code.extend(methods);
    // connect() constructs all required tonic clients, stores them for futures use and initialises
    // a GrpcClientHandler.
    quote! {
        pub async fn connect(addr: String, reply_tx: async_std::channel::Sender<tremor_script::EventPayload>) -> crate::errors::Result<Self> {
            let mut client_map: hashbrown::HashMap<String, Box<dyn TremorGrpcClient>> = hashbrown::HashMap::new();
            let mut methods_map: hashbrown::HashMap<String, prost_types::MethodDescriptorProto> = hashbrown::HashMap::new();
            let senders_map: hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>> = hashbrown::HashMap::new();

            #clients_code
            #methods_code

            Ok(GrpcClientHandler {
                clients: client_map,
                methods: methods_map,
                senders: senders_map,
                reply_tx
            })
        }
    }
}

fn generate_client_handler_send_request() -> TokenStream {
    // send_request() accepts an event to send to a grpc server and a sender to reply with any
    // potential errors.
    quote! {
        pub async fn send_request(
            &mut self,
            event: tremor_pipeline::Event,
            error_tx: async_std::channel::Sender<crate::errors::Error>,
            ctx: &crate::connectors::sink::SinkContext
        ) -> crate::errors::Result<()> {
            let request_path = event.value_meta_iter().next().and_then(|(_, meta)| meta.get_str("request_path"));
            if let Some(path) = request_path {
                let client_key = path.split("/").collect::<Vec<&str>>()[0];
                if let Some((method, client)) = self.methods.get(path).zip(self.clients.get_mut(client_key)) {
                    if !method.client_streaming() && !method.server_streaming() {
                        client.send_unary_request(event, self.reply_tx.clone(), ctx).await?;
                    }
                    else if method.client_streaming() && !method.server_streaming() {
                        client.send_client_stream_request(event, self.reply_tx.clone(), error_tx, &mut self.senders, ctx).await?;
                    } else if !method.client_streaming() && method.server_streaming() {
                        client.send_server_stream_request(event, self.reply_tx.clone(), ctx).await?;
                    } else {
                        client.send_binary_stream_request(event, self.reply_tx.clone(), error_tx, &mut self.senders, ctx).await?;
                    }
                }
            }
            Ok(())
        }
    }
}

fn generate_tremor_grpc_client() -> TokenStream {
    // TremorGrpcClient defines methods that tonic clients need to implement in order to send
    // tremor specific requests.
    quote! {
        #[async_trait::async_trait]
        trait TremorGrpcClient: Send  + std::fmt::Debug {
            async fn send_unary_request(
                &mut self,
                _event: tremor_pipeline::Event,
                _reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
                _ctx: &crate::connectors::sink::SinkContext
            ) -> crate::errors::Result<()> {
                Ok(())
            }
            async fn send_client_stream_request(
                &mut self,
                _event: tremor_pipeline::Event,
                _reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
                _error_tx: async_std::channel::Sender<crate::errors::Error>,
                _senders: &mut hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>>,
                _ctx: &crate::connectors::sink::SinkContext
            ) -> crate::errors::Result<()> {
                Ok(())
            }
            async fn send_server_stream_request(
                &mut self,
                _event: tremor_pipeline::Event,
                _reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
                _ctx: &crate::connectors::sink::SinkContext
            ) -> crate::errors::Result<()> {
                Ok(())
            }
            async fn send_binary_stream_request(
                &mut self,
                _event: tremor_pipeline::Event,
                _reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
                _error_tx: async_std::channel::Sender<crate::errors::Error>,
                _senders: &mut hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>>,
                _ctx: &crate::connectors::sink::SinkContext
            ) -> crate::errors::Result<()> {
                Ok(())
            }
        }
    }
}

fn generate_tremor_grpc_client_impls(files: Vec<FileDescriptorProto>) -> TokenStream {
    let mut trait_impls = vec![];
    for file in files {
        let mod_ident = file.mod_ident();
        for service in &file.service {
            let client_mod_ident = service.client_mod_ident();
            let client_ident = service.client_ident();
            let mut send_unary_request_code = TokenStream::default();
            let mut send_client_stream_request_code = TokenStream::default();
            let mut send_server_stream_request_code = TokenStream::default();
            let mut send_binary_stream_request_code = TokenStream::default();
            for method in &service.method {
                let request_message_ident = method.request_message_ident(file.package());
                let method_ident = method.name_ident();
                if !method.server_streaming() && !method.client_streaming() {
                    send_unary_request_code = generate_send_unary_request(
                        method_ident.clone(),
                        request_message_ident.clone(),
                    );
                }
                if !method.server_streaming() && method.client_streaming() {
                    send_client_stream_request_code = generate_send_client_stream_request(
                        method_ident.clone(),
                        request_message_ident.clone(),
                    );
                }
                if method.server_streaming() && !method.client_streaming() {
                    send_server_stream_request_code = generate_send_server_stream_request(
                        method_ident.clone(),
                        request_message_ident.clone(),
                    );
                }
                if method.server_streaming() && method.client_streaming() {
                    send_binary_stream_request_code = generate_send_binary_stream_request(
                        method_ident.clone(),
                        request_message_ident.clone(),
                    );
                }
            }
            trait_impls.push(quote! {
                #[async_trait::async_trait]
                impl TremorGrpcClient for #mod_ident::#client_mod_ident::#client_ident<tonic::transport::Channel> {
                    #send_unary_request_code

                    #send_client_stream_request_code

                    #send_server_stream_request_code

                    #send_binary_stream_request_code
                }
            });
        }
    }
    let mut code = TokenStream::default();
    code.extend(trait_impls);
    code
}

fn generate_insert_request_headers() -> TokenStream {
    quote! {
        fn insert_request_headers(key: beef::Cow<'static, str>, meta: &Value, metadata: &mut tonic::metadata::MetadataMap) -> crate::errors::Result<()> {
            if let Some(sink_meta) = meta.get(&key) {
                if let Some(headers) = sink_meta.get_object("headers") {
                    for (key, val) in headers.iter() {
                        if let Some(val_str) = val.as_str() {
                            if let Some(metadata_value) = val_str.parse::<AsciiMetadataValue>().ok() {
                                metadata.insert(MetadataKey::from_bytes(key.as_bytes())?, metadata_value);
                            }
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

fn generate_send_unary_request(
    method_ident: Ident,
    request_message_ident: TokenStream,
) -> TokenStream {
    // send_unary_request() sends a event as a grpc request and sends a grpc response (as an event)
    // back to the source.
    quote! {
        async fn send_unary_request(
            &mut self,
            event: tremor_pipeline::Event,
            reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
            ctx: &crate::connectors::sink::SinkContext
        ) -> crate::errors::Result<()> {
            if !event.is_batch {
                for (value, meta) in event.value_meta_iter() {
                    let meta = meta.clone_static();
                    dbg!("args: {:?} {:?}", &value, &meta);
                    let body: #request_message_ident = tremor_value::structurize(value.clone_static())?;
                    let mut request = tonic::Request::new(body.clone());
                    let metadata = request.metadata_mut();
                    let connector_key: beef::Cow<'static, str> = ctx.connector_type.to_string().into();
                    insert_request_headers(connector_key.clone(), &meta, metadata)?;
                    dbg!("request: {:?}", &request);

                    let resp = self.#method_ident(request).await?;
                    dbg!("tonic resp: {:?}", &resp);

                    let mut grpc_client_meta = tremor_value::value::Object::with_capacity(1);
                    let response_meta = insert_headers_in_meta(resp.metadata().clone())?;
                    grpc_client_meta.insert(connector_key, Value::from(response_meta));

                    let message = tremor_value::to_value(resp.into_inner())?;
                    dbg!("serialized resp: {:?}", &message);
                    let event: tremor_script::EventPayload = (message, grpc_client_meta).into();
                    reply_tx.send(event).await?;
                }
            }
            Ok(())
        }
    }
}

fn generate_insert_headers_in_meta() -> TokenStream {
    quote! {
        fn insert_headers_in_meta(metadata: tonic::metadata::MetadataMap) -> crate::errors::Result<tremor_value::value::Object<'static>> {
            let mut response_headers = tremor_value::value::Object::with_capacity(metadata.len());
            let mut response_meta = tremor_value::value::Object::with_capacity(1);
            for key_and_val in metadata.iter() {
                match key_and_val {
                    KeyAndValueRef::Ascii(key, value) => {
                        dbg!("response header: {:?}, {:?}", &key, &value);
                        if let Some(values) = response_headers.get_mut(key.as_str()) {
                            if let Some(vals) = values.as_array_mut() {
                                vals.push(Value::from(value.to_str()?.to_string()));
                            }
                        } else {
                            response_headers.insert(key.to_string().into(), Value::from(vec![Value::from(value.to_str()?.to_string())]));
                        }
                    }
                    _ => continue
                }
            }
            response_meta.insert("headers".into(), Value::from(response_headers));
            Ok(response_meta)
        }
    }
}

fn generate_send_client_stream_request(
    method_ident: Ident,
    request_message_ident: TokenStream,
) -> TokenStream {
    // send_client_stream_request() forms a stream and sends event into the stream according to the
    // stream_id present in the event's meta info. it sends a request stream's response back to the source after
    // it's done sending all events into a stream
    quote! {
        async fn send_client_stream_request(
            &mut self,
            event: tremor_pipeline::Event,
            reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
            error_tx: async_std::channel::Sender<crate::errors::Error>,
            senders: &mut hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>>,
            ctx: &crate::connectors::sink::SinkContext
        ) -> crate::errors::Result<()> {
            if !event.is_batch {
                for (value, meta) in event.value_meta_iter() {
                    let error_tx = error_tx.clone();
                    let err_tx = error_tx.clone();
                    let mut client = self.clone();
                    let reply_tx = reply_tx.clone();
                    let value = value.clone_static();
                    let meta = meta.clone_static();
                    dbg!("value: {:?}, meta: {:?}", &value, &meta);

                    if let Some(stream_id) = meta.get_u64("stream_id") {
                        dbg!("stream_id: {:?}", &stream_id);
                        if let Some(tx) = senders.get(&stream_id) {
                            tx.send(StaticValue(value)).await?;
                            if meta.contains_key("flag") {
                                dbg!("found a flag, closing stream: {:?}", &stream_id);
                                tx.close();
                            }
                        } else {
                            let (tx, rx) = async_std::channel::unbounded::<StaticValue>();
                            senders.insert(stream_id.clone(), tx.clone());

                            let ctx_url_1 = ctx.url.clone();
                            let ctx_url_2 = ctx.url.clone();
                            let ctx_url_3 = ctx.url.clone();

                            let rx = rx.filter_map(move |val| {
                                match tremor_value::structurize::<#request_message_ident>(val.0.clone()) {
                                    Ok(structured_value) => Some(structured_value),
                                    Err(error) => {
                                        error!("[Sink::{}] Could not deserialize {} to a valid {}: {}", ctx_url_1, val.0.clone(), stringify!(#request_message_ident), error);
                                        None
                                    }
                                }
                            });

                            let mut request = tonic::Request::new(rx);
                            let metadata = request.metadata_mut();
                            let connector_key: beef::Cow<'static, str> = ctx.connector_type.to_string().into();
                            insert_request_headers(connector_key.clone(), &meta, metadata)?;
                            async_std::task::spawn(async move {
                                if let Some(err) = tx.send(StaticValue(value)).await.err() {
                                    if let Some(error) = err_tx.send(err.into()).await.err() {
                                        error!("[Sink::{}] Could not notify system about a failed attempt to send an event into a gRPC stream request: {}", ctx_url_2, error);
                                    }
                                }
                            });

                            async_std::task::spawn(async move {
                                let mut error: Option<crate::errors::Error> = None;
                                match client.#method_ident(request).await {
                                    Ok(resp) => {
                                        dbg!("tonic resp: {:?}", &resp);
                                        let mut grpc_client_meta = tremor_value::value::Object::with_capacity(1);
                                        let response_meta = insert_headers_in_meta(resp.metadata().clone());
                                        if let Ok(response_meta) = response_meta {
                                            grpc_client_meta.insert(connector_key, Value::from(response_meta));
                                        } else if let Err(err) = response_meta {
                                            error = Some(err.into());
                                        }
                                        match tremor_value::to_value(resp.into_inner()) {
                                            Ok(message) => {
                                                let response_event: tremor_script::EventPayload = (message, grpc_client_meta).into();
                                                if let Err(err) = reply_tx.send(response_event).await {
                                                    error = Some(err.into());
                                                }
                                            }
                                            Err(err) => {
                                                error = Some(err.into());
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        error = Some(err.into());
                                    }
                                }
                            if let Some(error) = error {
                                if let Some(err) = error_tx.send(error).await.err() {
                                    error!("[Sink::{}] Could not notify system about a failed attempt to send an event into a gRPC stream request: {}", ctx_url_3, err);
                                }
                            }
                            });
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

fn generate_send_server_stream_request(
    method_ident: Ident,
    request_message_ident: TokenStream,
) -> TokenStream {
    quote! {
        async fn send_server_stream_request(
            &mut self,
            event: tremor_pipeline::Event,
            reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
            ctx: &crate::connectors::sink::SinkContext
        ) -> crate::errors::Result<()> {
            for (value, meta) in event.value_meta_iter() {
                let meta = meta.clone_static();
                dbg!("args: {:?} {:?}", &value, &meta);
                let body: #request_message_ident = tremor_value::structurize(value.clone_static())?;
                let mut request = tonic::Request::new(body.clone());
                let metadata = request.metadata_mut();
                let connector_key: beef::Cow<'static, str> = ctx.connector_type.to_string().into();
                insert_request_headers(connector_key.clone(), &meta, metadata)?;
                dbg!("request: {:?}", &request);
                let resp = self.#method_ident(request).await?;
                dbg!("resp: {:?}", &resp);
                let mut grpc_client_meta = tremor_value::value::Object::with_capacity(1);
                let response_meta = insert_headers_in_meta(resp.metadata().clone())?;
                grpc_client_meta.insert(connector_key, Value::from(response_meta));
                let mut stream = resp.into_inner();
                while let Some(item) = stream.message().await? {
                    dbg!("item: {:?}", &item);
                    let message = tremor_value::to_value(item)?;
                    dbg!("response serialized: {:?}", &message);
                    let event: tremor_script::EventPayload = (message, grpc_client_meta.clone()).into();
                    reply_tx.send(event).await?;
                }
            }
            Ok(())
        }
    }
}

fn generate_send_binary_stream_request(
    method_ident: Ident,
    request_message_ident: TokenStream,
) -> TokenStream {
    quote! {
        async fn send_binary_stream_request(
            &mut self,
            event: tremor_pipeline::Event,
            reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
            error_tx: async_std::channel::Sender<crate::errors::Error>,
            senders: &mut hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>>,
            ctx: &crate::connectors::sink::SinkContext
        ) -> crate::errors::Result<()> {
            for (value, meta) in event.value_meta_iter() {
                let error_tx = error_tx.clone();
                let err_tx = error_tx.clone();
                let mut client = self.clone();
                let reply_tx = reply_tx.clone();
                let value = value.clone_static();
                let meta = meta.clone_static();
                dbg!("value: {:?}, meta: {:?} \n", &value, &meta);
                if let Some(stream_id) = meta.get_u64("stream_id") {
                    dbg!("stream_id: {:?}", &stream_id);
                    if let Some(tx) = senders.get(&stream_id) {
                        tx.send(StaticValue(value)).await?;
                        if meta.contains_key("flag") {
                            dbg!("found a flag, closing stream: {:?}", &stream_id);
                            tx.close();
                        }
                    } else {
                        let (tx, rx) = async_std::channel::unbounded::<StaticValue>();
                        senders.insert(stream_id.clone(), tx.clone());
                        let ctx_url_1 = ctx.url.clone();
                        let ctx_url_2 = ctx.url.clone();
                        let ctx_url_3 = ctx.url.clone();
                        let rx = rx.filter_map(move |val| {
                            match tremor_value::structurize::<#request_message_ident>(val.0.clone()) {
                                Ok(structured_value) => Some(structured_value),
                                Err(error) => {
                                    error!("[Sink::{}] Could not deserialize {} to a valid {}: {}", ctx_url_1, val.0.clone(), stringify!(#request_message_ident), error);
                                    None
                                }
                            }
                        });

                        let mut request = tonic::Request::new(rx);
                        let metadata = request.metadata_mut();
                        let connector_key: beef::Cow<'static, str> = ctx.connector_type.to_string().into();
                        insert_request_headers(connector_key.clone(), &meta, metadata)?;

                        async_std::task::spawn(async move {
                            if let Some(err) = tx.send(StaticValue(value)).await.err() {
                                if let Some(error) = err_tx.send(err.into()).await.err() {
                                    error!("[Sink::{}] Could not notify system about a failed attempt to send an event into a gRPC stream request: {}", ctx_url_2, error);
                                }
                            }
                        });

                        async_std::task::spawn(async move {
                            let mut error: Option<crate::errors::Error> = None;
                            match client.#method_ident(request).await {
                                Ok(resp) => {
                                    let mut grpc_client_meta = tremor_value::value::Object::with_capacity(1);
                                    match insert_headers_in_meta(resp.metadata().clone()) {
                                        Ok(response_meta) => {
                                            grpc_client_meta.insert(connector_key, Value::from(response_meta));
                                        }
                                        Err(err) => {
                                            error = Some(err.into());
                                        }
                                    }

                                    let mut stream = resp.into_inner();
                                    loop {
                                        match stream.message().await {
                                            Ok(Some(item)) => {
                                                dbg!("item: {:?}", &item);
                                                match tremor_value::to_value(item) {
                                                    Ok(message) => {
                                                        let response_event: tremor_script::EventPayload = (message, grpc_client_meta.clone()).into();
                                                        if let Err(err) = reply_tx.send(response_event).await {
                                                            if let Some(notif_err) = error_tx.send(err.into()).await.err() {
                                                                error!(
                                                                    "[Sink::{}] Could not notify system about a failed attempt to send server response back: {}",
                                                                    ctx_url_3,
                                                                    notif_err
                                                                );
                                                            }
                                                        }
                                                    }
                                                    Err(err) => {
                                                        if let Some(notif_err) = error_tx.send(err.into()).await.err() {
                                                            error!(
                                                                "[Sink::{}] Could not notify system about a failed attempt to fetch gRPC stream message: {}",
                                                                ctx_url_3,
                                                                notif_err
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Ok(None) => {
                                                continue
                                            }
                                            Err(err) => {
                                                if let Some(notif_err) = error_tx.send(err.into()).await.err() {
                                                    error!(
                                                        "[Sink::{}] Could not notify system about a failed attempt to deserialize gRPC message: {}",
                                                        ctx_url_3,
                                                        notif_err
                                                    );
                                                }
                                                break
                                            }
                                        }
                                    }
                                }
                                Err(err) => {
                                    error = Some(err.into());
                                }
                            }
                            println!("{:?}", error);
                            if let Some(error) = error {
                                if let Some(err) = error_tx.send(error).await.err() {
                                    error!("[Sink::{}] Could not notify system about a failed attempt to send an event into a gRPC stream request: {}", ctx_url_3, err);
                                }
                            } else {
                                println!("{:?}", error);
                            }
                        });
                    }
                }
            }
            Ok(())
        }
    }
}
