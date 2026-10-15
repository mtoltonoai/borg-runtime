//! Private loopback HTTP adapter for the trusted-host profile.
//! Host/SSH access is a deployment precondition; loopback is not hostile-user isolation.
use crate::{
    controller::Controller,
    product::{self, UiRequest},
    protocol::{Operation, Outcome, Request, WIRE_REVISION},
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Request as HttpRequest, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use std::{convert::Infallible, net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, sync::Semaphore};

fn response(status: u16, content_type: &str, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(body.into()));
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let h = r.headers_mut();
    h.insert(
        "content-type",
        content_type
            .parse()
            .unwrap_or_else(|_| hyper::header::HeaderValue::from_static("text/plain")),
    );
    h.insert(
        "cache-control",
        hyper::header::HeaderValue::from_static("no-store"),
    );
    h.insert("content-security-policy",hyper::header::HeaderValue::from_static("default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'"));
    h.insert(
        "x-content-type-options",
        hyper::header::HeaderValue::from_static("nosniff"),
    );
    h.insert(
        "referrer-policy",
        hyper::header::HeaderValue::from_static("same-origin"),
    );
    r
}

async fn handle(
    controller: Controller,
    origin: String,
    request: HttpRequest<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = request.method().as_str().to_string();
    let path = request.uri().path().to_string();
    let product_path = request
        .uri()
        .path_and_query()
        .map(|x| x.as_str())
        .unwrap_or(&path)
        .to_string();
    let request_origin = request
        .headers()
        .get("origin")
        .and_then(|x| x.to_str().ok())
        .map(str::to_owned);
    let expected = origin.strip_prefix("http://").unwrap_or("");
    if request.headers().get("host").and_then(|x| x.to_str().ok()) != Some(expected) {
        return Ok(response(400, "text/plain", "Unexpected host"));
    }
    if method == "GET" && path == "/health" {
        let probe = controller
            .handle(Request {
                protocol_revision: WIRE_REVISION,
                request_id: "health".into(),
                command: Operation::ListTasks {
                    after: None,
                    limit: 1,
                },
            })
            .await;
        return Ok(if matches!(probe.outcome, Outcome::Ok { .. }) {
            response(200, "application/json", r#"{"status":"ready"}"#)
        } else {
            response(503, "application/json", r#"{"status":"unavailable"}"#)
        });
    }
    let mcp_caller = request
        .headers()
        .get(crate::mcp::CALLER_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let mcp_version = request
        .headers()
        .get("mcp-protocol-version")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let body = match tokio::time::timeout(
        std::time::Duration::from_millis(controller.limits().transport.http_body_timeout_ms),
        Limited::new(
            request.into_body(),
            controller.limits().transport.body_bytes,
        )
        .collect(),
    )
    .await
    {
        Ok(Ok(x)) => x.to_bytes(),
        Ok(Err(error)) if error.is::<http_body_util::LengthLimitError>() => {
            return Ok(response(
                413,
                "text/plain",
                "Request body exceeds bound (transport.body_bytes)",
            ));
        }
        Ok(Err(_)) => {
            return Ok(response(
                400,
                "text/plain",
                "Invalid or interrupted request body",
            ));
        }
        Err(_) => {
            return Ok(response(
                408,
                "text/plain",
                "Request body deadline exceeded (transport.http_body_timeout_ms)",
            ));
        }
    };
    if path == "/mcp" {
        if method != "POST" {
            return Ok(response(405, "text/plain", "MCP requires POST"));
        }
        if request_origin.as_ref().is_some_and(|v| v != &origin) {
            return Ok(response(403, "text/plain", "Cross-origin MCP refused"));
        }
        let (status, bytes) = crate::mcp::handle(
            &controller,
            mcp_caller.as_deref(),
            mcp_version.as_deref(),
            &body,
        )
        .await;
        return Ok(response(status, "application/json", bytes));
    }
    if path == "/api/operations" {
        if method != "POST" {
            return Ok(response(405, "text/plain", "Use POST"));
        }
        if request_origin.as_ref().is_some_and(|x| x != &origin) {
            return Ok(response(
                403,
                "text/plain",
                "Cross-origin operation refused",
            ));
        }
        let value: serde_json::Value = match serde_json::from_slice(&body) {
            Ok(x) => x,
            Err(_) => return Ok(response(400, "text/plain", "Invalid request")),
        };
        let allowed = ["protocol_revision", "request_id", "operation", "input"];
        if !value
            .as_object()
            .is_some_and(|m| m.keys().all(|k| allowed.contains(&k.as_str())))
        {
            return Ok(response(400, "text/plain", "Unknown request field"));
        }
        let command: Request = match serde_json::from_value(value) {
            Ok(x) => x,
            Err(_) => return Ok(response(400, "text/plain", "Invalid operation")),
        };
        let reply = controller.handle(command).await;
        return Ok(match serde_json::to_vec(&reply) {
            Ok(bytes) if bytes.len() <= controller.limits().transport.body_bytes => {
                response(200, "application/json", bytes)
            }
            Ok(_) => response(
                500,
                "text/plain",
                "Response exceeds transport.body_bytes; reconcile original operation",
            ),
            Err(_) => response(
                500,
                "text/plain",
                "Response encoding failure; reconcile original operation",
            ),
        });
    }
    let result = product::handle(
        &controller,
        UiRequest {
            method,
            path: product_path,
            body: body.to_vec(),
            origin: request_origin,
            expected_origin: origin,
        },
    )
    .await;
    let mut reply = response(result.status, &result.content_type, result.body);
    if let Some(location) = result.location {
        if let Ok(value) = location.parse() {
            reply.headers_mut().insert("location", value);
        }
    }
    Ok(reply)
}

pub async fn serve(
    controller: Controller,
    listener: TcpListener,
    origin: String,
) -> Result<(), String> {
    let local = listener.local_addr().map_err(|e| e.to_string())?;
    if !local.ip().is_loopback() {
        return Err(
            "v2 private adapter requires a loopback listener; use authenticated SSH forwarding"
                .into(),
        );
    }
    let expected = format!("http://{local}");
    if origin != expected {
        return Err(format!("configured origin must equal {expected}"));
    }
    let slots = Arc::new(Semaphore::new(
        controller.limits().transport.http_connections,
    ));
    loop {
        let permit = slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "server shutting down")?;
        let (stream, peer) = listener.accept().await.map_err(|e| e.to_string())?;
        if !peer.ip().is_loopback() {
            continue;
        }
        let policy = controller.limits().transport.clone();
        let c = controller.clone();
        let o = origin.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let service = service_fn(move |r| handle(c.clone(), o.clone(), r));
            let builder = connection_builder(&policy);
            let connection = builder.serve_connection(TokioIo::new(stream), service);
            if let Err(error) = connection.await {
                // Header parsing precedes the service and its operation identity.
                // Report a closed diagnostic without request/error body content.
                eprintln!("{}", connection_diagnostic(&error));
            }
        });
    }
}

fn connection_builder(
    policy: &crate::limits::TransportLimits,
) -> hyper::server::conn::http1::Builder {
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .max_headers(policy.http_header_count)
        .max_buf_size(policy.http_header_bytes)
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(std::time::Duration::from_millis(
            policy.http_header_timeout_ms,
        ));
    builder
}

fn connection_diagnostic(error: &hyper::Error) -> &'static str {
    use std::error::Error;
    // In pinned Hyper1.11.1, source-free is_timeout() is exactly HeaderTimeout.
    // Every other true timeout predicate requires a private TimedOut source.
    // Keep this tied to the unchanged HTTP1/TokioTimer builder above.
    if error.is_timeout() && error.source().is_none() {
        "http_connection_refused: transport.http_header_timeout_ms"
    } else if error.is_parse_too_large() {
        // Hyper erases both count and byte overflow to Parse::TooLarge; this
        // predicate also includes its private URI-length representation bound.
        // No public typed cause distinguishes those three cases here.
        "http_connection_refused: request_head_limit_cause_unavailable"
    } else {
        "http_connection_failed: cause_unavailable"
    }
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncWriteExt;

    fn policy() -> crate::limits::TransportLimits {
        crate::limits::RuntimeLimits::seed().unwrap().transport
    }

    // Actual selected HTTP1 parser/timer, in-memory IO, no listener or network.
    async fn refused_head(
        policy: crate::limits::TransportLimits,
        bytes: &[u8],
        eof: bool,
    ) -> hyper::Error {
        let (mut client, server) = tokio::io::duplex(policy.http_header_bytes * 2);
        client.write_all(bytes).await.unwrap();
        if eof {
            client.shutdown().await.unwrap();
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let service = service_fn(move |_request: HttpRequest<Incoming>| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, Infallible>(response(200, "text/plain", "unexpected service call")) }
        });
        let builder = connection_builder(&policy);
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            builder.serve_connection(TokioIo::new(server), service),
        )
        .await
        .expect("finite in-memory parser fixture")
        .expect_err("head must be refused before service invocation");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(client);
        error
    }

    #[tokio::test]
    async fn header_deadline_identifies_only_its_metadata_field() {
        let mut policy = policy();
        policy.http_header_timeout_ms = 10;
        let error = refused_head(
            policy,
            b"GET / HTTP/1.1\r\nX-Private-Fixture: never disclose this\r\n",
            false,
        )
        .await;
        assert!(error.is_timeout());
        assert_eq!(
            connection_diagnostic(&error),
            "http_connection_refused: transport.http_header_timeout_ms"
        );
    }

    #[tokio::test]
    async fn header_count_and_buffer_errors_remain_explicitly_unattributed() {
        let mut count = policy();
        count.http_header_count = 1;
        let count_error = refused_head(
            count,
            b"GET / HTTP/1.1\r\nHost: fixture\r\nX-Private-Fixture: undisclosed\r\n\r\n",
            false,
        )
        .await;
        let mut bytes = policy();
        bytes.http_header_bytes = crate::limits::RuntimeLimits::seed()
            .unwrap()
            .constraints
            .http1_minimum_buffer_bytes;
        let mut partial = b"GET / HTTP/1.1\r\nX-Private-Fixture: ".to_vec();
        partial.resize(bytes.http_header_bytes + 1, b'x');
        let bytes_error = refused_head(bytes, &partial, false).await;
        for error in [count_error, bytes_error] {
            assert!(error.is_parse_too_large());
            assert_eq!(
                connection_diagnostic(&error),
                "http_connection_refused: request_head_limit_cause_unavailable"
            );
        }
    }

    #[tokio::test]
    async fn malformed_and_interrupted_headers_do_not_claim_a_metadata_limit() {
        let malformed = refused_head(
            policy(),
            b"GET / HTTP/1.1\r\nBad Header: undisclosed\r\n\r\n",
            false,
        )
        .await;
        let interrupted =
            refused_head(policy(), b"GET / HTTP/1.1\r\nHost: undisclosed", true).await;
        assert!(malformed.is_parse());
        for error in [malformed, interrupted] {
            assert!(!error.is_timeout());
            assert!(!error.is_parse_too_large());
            assert_eq!(
                connection_diagnostic(&error),
                "http_connection_failed: cause_unavailable"
            );
        }
    }
}
// This adapter refuses exposure before bind; host authentication remains external.
//= spec/impl/v2/implementation.md#transport-and-access-controls-remain
//# An endpoint relying on the host's authenticated access MUST NOT be exposed directly on an untrusted network.
pub async fn listen(address: SocketAddr) -> Result<TcpListener, String> {
    if !address.ip().is_loopback() {
        return Err("only private loopback listeners are supported by this adapter".into());
    }
    TcpListener::bind(address).await.map_err(|e| e.to_string())
}
