//! Camelmailer delivery worker — the Rust port of `script/worker.rb`,
//! `app/lib/message_dequeuer` and `app/senders`.

pub mod arf;
pub mod dkim;
pub mod dmarc;
pub mod inspection;
pub mod sender;
pub mod server_filter;
pub mod signer;
pub mod smtp_client;
pub mod ssrf;
pub mod tracking;
pub mod worker;

pub use sender::SmtpSender;
pub use server_filter::resolve_server_filter;
pub use signer::Signer;
pub use smtp_client::SendOutcome;
pub use worker::{ProcessOutcome, WebhookOutcome, Worker};
