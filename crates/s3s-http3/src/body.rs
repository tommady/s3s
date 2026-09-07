// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 The s3s Authors

use bytes::{Buf, Bytes};
use h3::server::RequestStream;
use http::StatusCode;
use http_body::{Body as HttpBody, Frame, SizeHint};

use std::pin::Pin;
use std::task::{Context, Poll};

type RecvStream = RequestStream<h3_quinn::RecvStream, Bytes>;

// #[derive(Clone, Copy)]
enum State {
    Data,
    Trailers,
    Done,
}

pub(crate) struct Body {
    stream: Option<RecvStream>,
    state: State,
}

impl Body {
    pub(crate) fn new(stream: RecvStream) -> Self {
        Self {
            stream: Some(stream),
            state: State::Data,
        }
    }
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = h3::error::StreamError;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        loop {
            match this.state {
                State::Data => {
                    let Some(stream) = this.stream.as_mut() else {
                        this.state = State::Done;
                        return Poll::Ready(None);
                    };

                    match stream.poll_recv_data(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                        Poll::Ready(Ok(Some(mut data))) => {
                            return Poll::Ready(Some(Ok(Frame::data(data.copy_to_bytes(data.remaining())))));
                        }
                        Poll::Ready(Ok(None)) => this.state = State::Trailers,
                    }
                }
                State::Trailers => {
                    let Some(stream) = this.stream.as_mut() else {
                        this.state = State::Done;
                        return Poll::Ready(None);
                    };

                    match stream.poll_recv_trailers(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                        Poll::Ready(Ok(Some(trailers))) => {
                            this.state = State::Done;
                            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
                        }
                        Poll::Ready(Ok(None)) => {
                            this.state = State::Done;
                            return Poll::Ready(None);
                        }
                    }
                }
                State::Done => return Poll::Ready(None),
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self.state, State::Done)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

impl Drop for Body {
    fn drop(&mut self) {
        if matches!(self.state, State::Done) {
            return;
        }

        let Some(mut stream) = self.stream.take() else {
            return;
        };

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        let _drain_task = handle.spawn(async move {
            loop {
                match stream.recv_data().await {
                    Ok(Some(_data)) => {}
                    Ok(None) => {
                        let _ = stream.recv_trailers().await;
                        break;
                    }
                    Err(_) => break,
                }
            }
        });
    }
}
