//! Extractors that keep rejection status codes consistent with the handlers.
//!
//! Axum's built-in `Query` rejects a malformed query string with `400 Bad
//! Request` before a handler runs, while every validation a handler performs
//! itself surfaces as `AppError::Validation` and so `422 Unprocessable
//! Entity`. That split is invisible in the source and produced two codes for
//! the same class of mistake: `?search_type=popularity` on `/memories/search`
//! answered 422 (the handler parses it), `?sort_by=popularity` on `/memories`
//! answered 400 (the extractor parses it).
//!
//! `ValidatedQuery` closes that gap: a query string the server cannot make
//! sense of is a validation error wherever it is caught.

use axum::async_trait;
use axum::extract::rejection::QueryRejection;
use axum::extract::{FromRequestParts, Query};
use axum::http::request::Parts;
use serde::de::DeserializeOwned;

use crate::error::AppError;

/// `Query`, with deserialization failures reported as validation errors.
pub struct ValidatedQuery<T>(pub T);

#[async_trait]
impl<S, T> FromRequestParts<S> for ValidatedQuery<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(value)) => Ok(ValidatedQuery(value)),
            // The rejection's own message names the offending field and why it
            // failed — including the messages the custom `deserialize_with`
            // helpers produce — so it is worth more to the caller than any
            // wording invented here.
            Err(rejection) => Err(AppError::Validation(match &rejection {
                QueryRejection::FailedToDeserializeQueryString(err) => err.body_text(),
                other => other.body_text(),
            })),
        }
    }
}
