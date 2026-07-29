// Vendored from datafusion-flight-sql-server v0.4.16.

use std::fmt::Display;

use arrow_flight::error::FlightError;
use arrow_flight::sql::{self, Any, Command};
use prost::Message;
use prost::bytes::Bytes;

pub type Result<T, E = FlightError> = std::result::Result<T, E>;

#[derive(Debug, PartialEq, Clone)]
pub struct CommandTicket {
    pub command: sql::Command,
    /// Server-minted UUID keying the cached `GetFlightInfo` logical plan.
    /// `Some` when `get_flight_info_statement` registered a plan and
    /// stamped its UUID on the ticket; `DoGet` looks it up to reuse the plan.
    /// `None` for tickets that carry only the SQL string (older clients, or any
    /// path that did not register a plan) — `DoGet` then re-plans.
    pub statement_uuid: Option<String>,
    /// The auto-commit statement's pinned read snapshot as a
    /// `commit_seq_num` frontier (the branch's max committed seq, captured once at
    /// `GetFlightInfo`). `Some` for an auto-commit statement; `None` in-tx (the
    /// open tx carries the snapshot) or for older clients. `DoGet` decodes it
    /// and installs the pin guard so every scan in the execution reads at this
    /// one seq snapshot.
    pub as_of_seq: Option<i64>,
}

impl CommandTicket {
    pub fn new(cmd: sql::Command) -> Self {
        Self {
            command: cmd,
            statement_uuid: None,
            as_of_seq: None,
        }
    }

    /// Stamp a cached-plan UUID on the ticket.
    pub fn with_statement_uuid(mut self, statement_uuid: String) -> Self {
        self.statement_uuid = Some(statement_uuid);
        self
    }

    /// Stamp the pinned auto-commit read snapshot on the ticket.
    pub fn with_as_of(mut self, as_of_seq: i64) -> Self {
        self.as_of_seq = Some(as_of_seq);
        self
    }

    pub fn try_decode(msg: Bytes) -> Result<Self> {
        let msg = CommandTicketMessage::decode(msg).map_err(decode_error_flight_error)?;
        let mut ticket = Self::try_decode_command(msg.command)?;
        ticket.statement_uuid = msg.statement_uuid;
        ticket.as_of_seq = msg.as_of_seq;
        Ok(ticket)
    }

    pub fn try_decode_command(cmd: Bytes) -> Result<Self> {
        let content_msg = Any::decode(cmd).map_err(decode_error_flight_error)?;
        let command = Command::try_from(content_msg).map_err(FlightError::Arrow)?;
        Ok(Self {
            command,
            statement_uuid: None,
            as_of_seq: None,
        })
    }

    pub fn try_encode(self) -> Result<Bytes> {
        let content_msg = self.command.into_any().encode_to_vec();
        let msg = CommandTicketMessage {
            command: content_msg.into(),
            statement_uuid: self.statement_uuid,
            as_of_seq: self.as_of_seq,
        };
        Ok(msg.encode_to_vec().into())
    }
}

#[derive(Clone, PartialEq, Message)]
struct CommandTicketMessage {
    #[prost(bytes = "bytes", tag = "2")]
    command: Bytes,
    /// Cached-plan UUID. `optional` so an absent field (old client,
    /// or any path that registered no plan) decodes to `None` — `DoGet` then
    /// re-plans. Tag 3; tag 2 (`command`) is unchanged for wire compatibility.
    #[prost(string, optional, tag = "3")]
    statement_uuid: Option<String>,
    /// Pinned auto-commit snapshot. `optional` tag 4; tags 2/3 are
    /// unchanged so older tickets decode with `as_of_seq = None`.
    #[prost(int64, optional, tag = "4")]
    as_of_seq: Option<i64>,
}

fn decode_error_flight_error(err: prost::DecodeError) -> FlightError {
    FlightError::DecodeError(format!("{err:?}"))
}

/// Represents a query handle for use in prepared statements.
/// All state required to run the prepared statement is passed
/// back and forth to the client, so any service instance can run it.
#[derive(Debug, Clone)]
pub struct QueryHandle {
    query: String,
    parameters: Option<Bytes>,
    /// Server-minted UUID keying the plan `do_action_create_prepared_statement`
    /// already built and cached, so `get_flight_info_prepared_statement` reuses
    /// it instead of re-planning the same statement at the same snapshot. `Some`
    /// only for the Select / Set arms (which reach GetFlightInfo + DoGet);
    /// `None` otherwise, and for older handles that lack the field — a `None`
    /// simply re-plans.
    statement_uuid: Option<String>,
}

impl QueryHandle {
    pub fn new(query: String, parameters: Option<Bytes>) -> Self {
        Self {
            query,
            parameters,
            statement_uuid: None,
        }
    }

    /// Stamp the cross-pass plan-cache UUID on the handle so
    /// `get_flight_info_prepared_statement` can reuse the PREPARE-built plan.
    pub fn with_statement_uuid(mut self, statement_uuid: String) -> Self {
        self.statement_uuid = Some(statement_uuid);
        self
    }

    pub fn query(&self) -> &str {
        self.query.as_ref()
    }

    pub fn parameters(&self) -> Option<&[u8]> {
        self.parameters.as_deref()
    }

    /// The cached-plan UUID, if PREPARE stamped one. `None` → the
    /// GetFlightInfo leg re-plans from `query()`.
    pub fn statement_uuid(&self) -> Option<&str> {
        self.statement_uuid.as_deref()
    }

    pub fn set_parameters(&mut self, parameters: Option<Bytes>) {
        self.parameters = parameters;
    }

    pub fn try_decode(msg: Bytes) -> Result<Self> {
        let msg = QueryHandleMessage::decode(msg).map_err(decode_error_flight_error)?;
        Ok(Self {
            query: msg.query,
            parameters: msg.parameters,
            statement_uuid: msg.statement_uuid,
        })
    }

    pub fn encode(self) -> Bytes {
        let msg = QueryHandleMessage {
            query: self.query,
            parameters: self.parameters,
            statement_uuid: self.statement_uuid,
        };
        msg.encode_to_vec().into()
    }
}

impl From<QueryHandle> for Bytes {
    fn from(value: QueryHandle) -> Self {
        value.encode()
    }
}

impl Display for QueryHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Query({})", self.query)
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct QueryHandleMessage {
    #[prost(string, tag = "1")]
    query: String,
    #[prost(bytes = "bytes", optional, tag = "2")]
    parameters: Option<Bytes>,
    /// Cross-pass cached-plan UUID. `optional` tag 3; tags 1/2 are
    /// unchanged so handles minted before this field decode with
    /// `statement_uuid = None` and re-plan, preserving wire compatibility.
    #[prost(string, optional, tag = "3")]
    statement_uuid: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::sql::CommandStatementQuery;

    fn statement_command(sql: &str) -> sql::Command {
        sql::Command::CommandStatementQuery(CommandStatementQuery {
            query: sql.to_string(),
            transaction_id: None,
        })
    }

    #[test]
    fn statement_uuid_survives_encode_decode_roundtrip() {
        let encoded = CommandTicket::new(statement_command("SELECT 1"))
            .with_statement_uuid("42".to_string())
            .try_encode()
            .expect("encode");
        let decoded = CommandTicket::try_decode(encoded).expect("decode");
        assert_eq!(decoded.statement_uuid.as_deref(), Some("42"));
    }

    #[test]
    fn ticket_without_statement_uuid_decodes_to_none() {
        // A ticket built the old way (no statement_uuid) must decode as None so
        // the DoGet path treats it as a cache miss and re-plans (back-compat).
        let encoded = CommandTicket::new(statement_command("SELECT 1"))
            .try_encode()
            .expect("encode");
        let decoded = CommandTicket::try_decode(encoded).expect("decode");
        assert_eq!(decoded.statement_uuid, None);
    }

    #[test]
    fn query_handle_statement_uuid_survives_encode_decode_roundtrip() {
        let decoded = QueryHandle::try_decode(
            QueryHandle::new("SELECT 1".to_string(), None)
                .with_statement_uuid("abc-123".to_string())
                .encode(),
        )
        .expect("decode");
        assert_eq!(decoded.query(), "SELECT 1");
        assert_eq!(decoded.statement_uuid(), Some("abc-123"));
    }

    #[test]
    fn query_handle_without_statement_uuid_decodes_to_none() {
        // A handle minted without a statement_uuid must decode as None so
        // get_flight_info_prepared_statement re-plans (back-compat).
        let decoded =
            QueryHandle::try_decode(QueryHandle::new("SELECT 1".to_string(), None).encode())
                .expect("decode");
        assert_eq!(decoded.statement_uuid(), None);
    }

    // The pinned auto-commit snapshot rides the ticket / handle beside
    // `statement_uuid` (server->client->server relay). Round-trip parity.
    #[test]
    fn as_of_seq_survives_command_ticket_roundtrip() {
        let encoded = CommandTicket::new(statement_command("SELECT 1"))
            .with_as_of(123)
            .try_encode()
            .expect("encode");
        let decoded = CommandTicket::try_decode(encoded).expect("decode");
        assert_eq!(decoded.as_of_seq, Some(123));
    }

    #[test]
    fn command_ticket_without_as_of_decodes_to_none() {
        // A ticket built without a pin (in-tx statement, or old client)
        // decodes as None so the DoGet leg installs no pin guard.
        let encoded = CommandTicket::new(statement_command("SELECT 1"))
            .try_encode()
            .expect("encode");
        let decoded = CommandTicket::try_decode(encoded).expect("decode");
        assert_eq!(decoded.as_of_seq, None);
    }
}
