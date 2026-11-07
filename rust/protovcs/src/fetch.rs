//! A connect-rust client transport over the browser's `fetch`.
//!
//! [`FetchTransport`] is a [`ClientTransport`], so [`crate::ConnectTransport`]
//! and any generated connect-rust client take it:
//!
//! ```ignore
//! let config = ClientConfig::new("https://example.com/api".parse()?);
//! let transport = ConnectTransport::new(FetchTransport, config, store_id);
//! ```
//!
//! A response body is read as it arrives, so a server stream such as
//! `WatchRefs` delivers each message when the server sends it. Dropping the
//! body cancels the read, which closes the stream.
//!
//! Built only for wasm32 with the `fetch` feature.

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use connectrpc::client::{BoxFuture, ClientBody, ClientTransport};
use futures::FutureExt as _;
use http::{Request, Response};
use http_body::{Body, Frame};
use http_body_util::BodyExt as _;
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast as _, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::ReadableStreamDefaultReader;

/// Sends each request with the global `fetch`, in a window or a worker.
#[derive(Clone, Copy, Debug, Default)]
pub struct FetchTransport;

/// Why a fetch failed.
#[derive(Debug)]
pub enum FetchError {
    /// The browser rejected the request or the response, with its message.
    Browser(String),
    /// Reading the request body failed.
    Body(connectrpc::ConnectError),
    /// A request header value was not visible ASCII.
    Header(http::header::ToStrError),
    /// The response could not be turned into an `http::Response`.
    Response(String),
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Browser(m) => write!(f, "fetch failed: {m}"),
            Self::Body(e) => write!(f, "reading the request body: {e}"),
            Self::Header(e) => write!(f, "a request header is not ASCII: {e}"),
            Self::Response(m) => write!(f, "reading the response: {m}"),
        }
    }
}

impl std::error::Error for FetchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Body(e) => Some(e),
            Self::Header(e) => Some(e),
            Self::Browser(_) | Self::Response(_) => None,
        }
    }
}

impl From<JsValue> for FetchError {
    fn from(value: JsValue) -> Self {
        Self::Browser(value.as_string().unwrap_or_else(|| format!("{value:?}")))
    }
}

impl ClientTransport for FetchTransport {
    type ResponseBody = FetchBody;
    type Error = FetchError;

    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
        // The browser's futures are not Send. A wasm32 module has one thread,
        // and SendWrapper panics rather than allow a move to another one.
        Box::pin(SendWrapper::new(fetch(request)))
    }
}

async fn fetch(request: Request<ClientBody>) -> Result<Response<FetchBody>, FetchError> {
    let (parts, body) = request.into_parts();
    let body = body.collect().await.map_err(FetchError::Body)?.to_bytes();

    let headers = web_sys::Headers::new()?;
    for (name, value) in &parts.headers {
        headers.append(name.as_str(), value.to_str().map_err(FetchError::Header)?)?;
    }
    let init = web_sys::RequestInit::new();
    init.set_method(parts.method.as_str());
    init.set_headers(&headers);
    init.set_body(&js_sys::Uint8Array::from(body.as_ref()));
    let request = web_sys::Request::new_with_str_and_init(&parts.uri.to_string(), &init)?;

    let fetch_fn: js_sys::Function =
        js_sys::Reflect::get(&js_sys::global(), &"fetch".into())?.dyn_into()?;
    let promise: js_sys::Promise = fetch_fn.call1(&JsValue::UNDEFINED, &request)?.dyn_into()?;
    let response: web_sys::Response = JsFuture::from(promise).await?.dyn_into()?;
    to_http_response(&response)
}

/// Copies the browser's response status and headers into an `http::Response`
/// whose body reads the browser's stream.
fn to_http_response(response: &web_sys::Response) -> Result<Response<FetchBody>, FetchError> {
    let mut builder = Response::builder().status(response.status());
    if let Some(entries) = js_sys::try_iter(&response.headers())? {
        for entry in entries {
            let pair: js_sys::Array = entry?.into();
            let (Some(name), Some(value)) = (pair.get(0).as_string(), pair.get(1).as_string())
            else {
                continue;
            };
            // fetch has already decoded the body, so these describe bytes
            // that are not what the body yields.
            if name.eq_ignore_ascii_case("content-encoding")
                || name.eq_ignore_ascii_case("content-length")
            {
                continue;
            }
            builder = builder.header(name, value);
        }
    }
    let reader = match response.body() {
        Some(stream) => Some(
            stream
                .get_reader()
                .dyn_into::<ReadableStreamDefaultReader>()
                .map_err(JsValue::from)?,
        ),
        None => None,
    };
    builder
        .body(FetchBody(SendWrapper::new(Reader {
            reader,
            pending: None,
        })))
        .map_err(|e| FetchError::Response(e.to_string()))
}

/// A response body, read from the browser's stream a chunk at a time.
pub struct FetchBody(SendWrapper<Reader>);

struct Reader {
    /// None once the stream ended, or when the response had no body.
    reader: Option<ReadableStreamDefaultReader>,
    /// The read in flight.
    pending: Option<JsFuture>,
}

impl fmt::Debug for FetchBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FetchBody")
    }
}

impl Body for FetchBody {
    type Data = Bytes;
    type Error = FetchError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, FetchError>>> {
        let r = &mut *self.get_mut().0;
        loop {
            let Some(reader) = &r.reader else {
                return Poll::Ready(None);
            };
            let read = r
                .pending
                .get_or_insert_with(|| JsFuture::from(reader.read()));
            let result = match read.poll_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result,
            };
            r.pending = None;
            let result = match result {
                Ok(v) => v,
                Err(e) => {
                    r.reader = None;
                    return Poll::Ready(Some(Err(e.into())));
                }
            };
            let done = js_sys::Reflect::get(&result, &"done".into())
                .ok()
                .and_then(|d| d.as_bool())
                .unwrap_or(true);
            if done {
                r.reader = None;
                return Poll::Ready(None);
            }
            let value = js_sys::Reflect::get(&result, &"value".into()).unwrap_or(JsValue::NULL);
            let Ok(chunk) = value.dyn_into::<js_sys::Uint8Array>() else {
                continue;
            };
            if chunk.length() == 0 {
                continue;
            }
            return Poll::Ready(Some(Ok(Frame::data(Bytes::from(chunk.to_vec())))));
        }
    }

    fn is_end_stream(&self) -> bool {
        self.0.reader.is_none()
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        // Cancelling closes the connection, which ends a server stream.
        if let Some(reader) = self.reader.take() {
            let _ = reader.cancel();
        }
    }
}
