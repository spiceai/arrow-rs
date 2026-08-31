// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::error::Error;

use arrow_schema::ArrowError;

/// Errors for the Apache Arrow Flight crate
#[derive(Debug)]
pub enum FlightError {
    /// Underlying arrow error
    Arrow(ArrowError),
    /// Returned when functionality is not yet available.
    NotYetImplemented(String),
    /// Error from the underlying tonic library
    Tonic(Box<tonic::Status>),
    /// Some unexpected message was received
    ProtocolError(String),
    /// An error occurred during decoding
    DecodeError(String),
    /// External error that can provide source of error by calling `Error::source`.
    ExternalError(Box<dyn Error + Send + Sync>),
    /// An error annotated with the operation that produced it.
    ///
    /// The annotated error is kept intact rather than rendered into text, so a
    /// caller can still classify it: reach a [`tonic::Status`] through any number
    /// of these layers with [`FlightError::tonic_status`], or walk
    /// [`Error::source`] for anything else.
    Context(Box<str>, Box<FlightError>),
}

impl FlightError {
    /// Generate a new `FlightError::ProtocolError` variant.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::ProtocolError(message.into())
    }

    /// Wraps an external error in an `ArrowError`.
    pub fn from_external_error(error: Box<dyn Error + Send + Sync>) -> Self {
        Self::ExternalError(error)
    }

    /// Annotate this error with the operation that produced it.
    ///
    /// ```
    /// # use arrow_flight::error::FlightError;
    /// let err = FlightError::from(tonic::Status::unavailable("transport error"))
    ///     .context("Can't handshake");
    ///
    /// // the context says which call failed
    /// assert!(err.to_string().starts_with("Can't handshake: Tonic error:"));
    /// // and the status is still there, typed
    /// assert_eq!(err.tonic_status().unwrap().code(), tonic::Code::Unavailable);
    /// ```
    pub fn context(self, context: impl Into<Box<str>>) -> Self {
        Self::Context(context.into(), Box::new(self))
    }

    /// The [`tonic::Status`] this error carries, looking through any
    /// [`FlightError::Context`] layers, or `None` if it carries none.
    ///
    /// Use this to classify a failure — for example to decide whether a
    /// transport failure is worth retrying — without matching on the rendered
    /// error text.
    pub fn tonic_status(&self) -> Option<&tonic::Status> {
        match self {
            Self::Tonic(status) => Some(status),
            Self::Context(_, source) => source.tonic_status(),
            _ => None,
        }
    }
}

impl std::fmt::Display for FlightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlightError::Arrow(source) => write!(f, "Arrow error: {source}"),
            FlightError::NotYetImplemented(desc) => write!(f, "Not yet implemented: {desc}"),
            FlightError::Tonic(source) => write!(f, "Tonic error: {source}"),
            FlightError::ProtocolError(desc) => write!(f, "Protocol error: {desc}"),
            FlightError::DecodeError(desc) => write!(f, "Decode error: {desc}"),
            FlightError::ExternalError(source) => write!(f, "External error: {source}"),
            FlightError::Context(context, source) => write!(f, "{context}: {source}"),
        }
    }
}

impl Error for FlightError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            FlightError::Arrow(source) => Some(source),
            // `as_ref` so this downcasts to `tonic::Status` rather than to
            // `Box<tonic::Status>`
            FlightError::Tonic(source) => Some(source.as_ref()),
            FlightError::ExternalError(source) => Some(source.as_ref()),
            FlightError::Context(_, source) => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl From<tonic::Status> for FlightError {
    fn from(status: tonic::Status) -> Self {
        Self::Tonic(Box::new(status))
    }
}

impl From<prost::DecodeError> for FlightError {
    fn from(error: prost::DecodeError) -> Self {
        Self::DecodeError(error.to_string())
    }
}

impl From<ArrowError> for FlightError {
    fn from(value: ArrowError) -> Self {
        Self::Arrow(value)
    }
}

// default conversion from FlightError to tonic treats everything
// other than `Status` as an internal error
impl From<FlightError> for tonic::Status {
    fn from(value: FlightError) -> Self {
        match value {
            FlightError::Arrow(e) => tonic::Status::internal(e.to_string()),
            FlightError::NotYetImplemented(e) => tonic::Status::internal(e),
            FlightError::Tonic(status) => *status,
            FlightError::ProtocolError(e) => tonic::Status::internal(e),
            FlightError::DecodeError(e) => tonic::Status::internal(e),
            FlightError::ExternalError(e) => tonic::Status::internal(e.to_string()),
            // the annotated error decides the outgoing status, so a `Status` still
            // round trips unchanged through any number of context layers
            FlightError::Context(_, source) => (*source).into(),
        }
    }
}

/// Result type for the Apache Arrow Flight crate
pub type Result<T> = std::result::Result<T, FlightError>;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn error_source() {
        let e1 = FlightError::DecodeError("foo".into());
        assert!(e1.source().is_none());

        // one level of wrapping
        let e2 = FlightError::ExternalError(Box::new(e1));
        let source = e2.source().unwrap().downcast_ref::<FlightError>().unwrap();
        assert!(matches!(source, FlightError::DecodeError(_)));

        let e3 = FlightError::ExternalError(Box::new(e2));
        let source = e3
            .source()
            .unwrap()
            .downcast_ref::<FlightError>()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<FlightError>()
            .unwrap();

        assert!(matches!(source, FlightError::DecodeError(_)));
    }

    #[test]
    fn error_through_arrow() {
        // flight error that wraps an arrow error that wraps a flight error
        let e1 = FlightError::DecodeError("foo".into());
        let e2 = ArrowError::ExternalError(Box::new(e1));
        let e3 = FlightError::ExternalError(Box::new(e2));

        // ensure we can find the lowest level error by following source()
        let mut root_error: &dyn Error = &e3;
        while let Some(source) = root_error.source() {
            // walk the next level
            root_error = source;
        }

        let source = root_error.downcast_ref::<FlightError>().unwrap();
        assert!(matches!(source, FlightError::DecodeError(_)));
    }

    #[test]
    fn tonic_status_through_context() {
        let status = tonic::Status::new(tonic::Code::Unknown, "transport error");
        let err = FlightError::from(status).context("inner").context("outer");

        // the status survives any number of context layers, typed
        let found = err.tonic_status().expect("status should survive");
        assert_eq!(found.code(), tonic::Code::Unknown);
        assert_eq!(found.message(), "transport error");

        // and the context is still readable
        assert!(
            err.to_string().starts_with("outer: inner: Tonic error:"),
            "{err}"
        );

        // an error carrying no status says so rather than guessing
        assert!(
            FlightError::DecodeError("foo".into())
                .context("outer")
                .tonic_status()
                .is_none()
        );
    }

    #[test]
    fn context_keeps_status_on_the_wire() {
        let err = FlightError::from(tonic::Status::unavailable("gone")).context("Can't handshake");
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "gone");
    }

    #[test]
    fn context_source_chain_reaches_status() {
        // a caller that knows nothing about FlightError can still find the status
        let err = FlightError::from(tonic::Status::internal("boom")).context("Can't handshake");

        let mut source = err.source();
        let status = loop {
            let current = source.expect("status should be reachable through source()");
            if let Some(status) = current.downcast_ref::<tonic::Status>() {
                break status;
            }
            source = current.source();
        };

        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_error_size() {
        // use Box in variants to keep this size down
        assert_eq!(std::mem::size_of::<FlightError>(), 32);
    }
}
