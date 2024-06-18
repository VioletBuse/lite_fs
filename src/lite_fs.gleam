import gleam/bit_array
import gleam/bool
import gleam/bytes_builder.{type BytesBuilder}
import gleam/dynamic
import gleam/erlang/process
import gleam/http.{Get}
import gleam/http/request.{type Request, Request}
import gleam/int
import gleam/option.{None, Some}
import gleam/result
import gleam/string
import httpp/streaming
import jackson

pub type Message {
  Shutdown
}

fn build_request(port: Int) -> Request(BytesBuilder) {
  Request(
    method: Get,
    headers: [#("connection", "keep-alive")],
    body: bytes_builder.new(),
    scheme: http.Http,
    host: "localhost",
    port: Some(port),
    path: "/events",
    query: None,
  )
}

fn create_on_data(callback: fn(Event) -> Nil) {
  fn(msg: streaming.Message, _, _) {
    case msg {
      streaming.Done -> Error(process.Normal)
      streaming.Bits(bits) -> {
        let evt = {
          use str <- result.try(bit_array.to_string(bits) |> result.nil_error)
          use event <- result.try(parse_event(string.trim(str)))

          Ok(event)
        }

        case evt, bit_array.to_string(bits) {
          Error(_), Ok(evt_string) ->
            Error(process.Abnormal("unable to parse event " <> evt_string))
          Error(_), _ ->
            Error(process.Abnormal(
              "received bits from server that could not be parsed into string",
            ))
          Ok(event), _ -> {
            callback(event)
            Ok(Nil)
          }
        }
      }
    }
  }
}

fn on_message(message: Message, _, _) {
  case message {
    Shutdown -> Error(process.Normal)
  }
}

fn on_error(_, _, _) {
  Error(process.Abnormal("error received"))
}

fn build_request_handler(port: Int, on_event_callback: fn(Event) -> Nil) {
  let req = build_request(port)

  streaming.StreamingRequestHandler(
    initial_state: Nil,
    req: req,
    on_data: create_on_data(on_event_callback),
    on_message: on_message,
    on_error: on_error,
    initial_response_timeout: 10_000,
  )
}

/// start the event listener
pub fn start(port port: Int, with callback: fn(Event) -> Nil) {
  build_request_handler(port, callback)
  |> streaming.start
}

pub type Primary {
  Local
  Remote(hostname: String)
}

/// The different types of events that you can get from the event stream
pub type Event {
  Init(primary_data: Primary)
  Tx(
    db: String,
    tx_id: Int,
    post_apply_checksum: Int,
    commit: Int,
    page_size: Int,
    timestamp: String,
  )
  PrimaryChange(primary_data: Primary)
}

fn parse_event(json: String) -> Result(Event, Nil) {
  use parsed_json <- result.try(jackson.parse(json) |> result.nil_error)
  use resolved_type <- result.try(jackson.resolve_pointer(parsed_json, "/type"))
  use event_type <- result.try(
    jackson.decode(resolved_type, dynamic.string) |> result.nil_error,
  )

  case event_type {
    "init" -> parse_primary(json, Init)
    "primaryChange" -> parse_primary(json, PrimaryChange)
    "tx" -> parse_tx_event(json)
    _ -> Error(Nil)
  }
}

fn parse_primary(
  event: String,
  constructor: fn(Primary) -> Event,
) -> Result(Event, Nil) {
  use parsed_json <- result.try(jackson.parse(event) |> result.nil_error)
  use resolved_is_primary <- result.try(jackson.resolve_pointer(
    parsed_json,
    "/data/is_primary",
  ))
  use is_primary <- result.try(
    jackson.decode(resolved_is_primary, dynamic.bool) |> result.nil_error,
  )

  use <- bool.guard(when: is_primary, return: Ok(constructor(Local)))

  use resolved_hostname <- result.try(jackson.resolve_pointer(
    parsed_json,
    "/data/hostname",
  ))
  use hostname <- result.try(
    jackson.decode(resolved_hostname, dynamic.string) |> result.nil_error,
  )

  Ok(constructor(Remote(hostname)))
}

type TxData {
  TxData(
    txid: String,
    checksum: String,
    page_size: Int,
    commit: Int,
    timestamp: String,
  )
}

fn parse_tx_event(event: String) -> Result(Event, Nil) {
  use json <- result.try(jackson.parse(event) |> result.nil_error)
  use db_resolved <- result.try(jackson.resolve_pointer(json, "/db"))
  use db <- result.try(
    jackson.decode(db_resolved, dynamic.string) |> result.nil_error,
  )
  use data <- result.try(jackson.resolve_pointer(json, "/data"))

  use raw_data <- result.try(
    jackson.decode(
      data,
      dynamic.decode5(
        TxData,
        dynamic.field("txId", dynamic.string),
        dynamic.field("postApplyChecksum", dynamic.string),
        dynamic.field("pageSize", dynamic.int),
        dynamic.field("commit", dynamic.int),
        dynamic.field("timestamp", dynamic.string),
      ),
    )
    |> result.nil_error,
  )

  use tx_id <- result.try(int.base_parse(raw_data.txid, 16))
  use checksum <- result.try(int.base_parse(raw_data.checksum, 16))

  Ok(Tx(
    db,
    tx_id,
    checksum,
    raw_data.page_size,
    raw_data.commit,
    raw_data.timestamp,
  ))
}
