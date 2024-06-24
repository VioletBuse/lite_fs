import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom.{type Atom}
import gleam/erlang/node.{type Node}
import gleam/erlang/process.{type Subject}
import gleam/function
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/uri

import lite_fs.{type Event, type Primary, Init, Local, PrimaryChange, Remote, Tx}

pub fn start(port: Int, name: Atom) {
  let res =
    actor.start(State(None, port, name, Local, dict.new()), handle_message)
  let _ = result.map(res, fn(sub) { process.send(sub, Initialize(sub)) })
  res
}

pub opaque type Message {
  Initialize(self: Subject(Message))
  LitefsEvent(Event)
  InternalRpc(function: Dynamic, client: Dynamic)
  AheadOfTransaction(db: String, tx_id: Int, client: Subject(Bool))
}

pub fn ref(name: String) {
  atom.create_from_string(name)
}

pub fn exec_on_primary(manager: Atom, function: fn() -> a, timeout: Int) {
  let client = process.new_subject()
  node.send(
    node.self(),
    manager,
    InternalRpc(dynamic.from(function), dynamic.from(client)),
  )

  process.receive(client, timeout)
}

pub fn local_ahead_of_txn(manager: Atom, db: String, tx_id: Int, timeout: Int) {
  let client = process.new_subject()
  node.send(node.self(), manager, AheadOfTransaction(db, tx_id, client))

  process.receive(client, timeout)
}

fn selecting_message(
  selector: process.Selector(a),
  mapping handler: fn(Message) -> a,
) -> process.Selector(a) {
  process.selecting_record2(
    selector,
    atom.create_from_string("initialize"),
    fn(dyn) { dynamic_to_subject(dyn) |> Initialize |> handler },
  )
  |> process.selecting_record3(
    atom.create_from_string("internal_rpc"),
    fn(d1, d2) { InternalRpc(d1, d2) |> handler },
  )
  |> process.selecting_record4(
    atom.create_from_string("ahead_of_transaction"),
    fn(d0, d1, d2) {
      let assert Ok(db) = dynamic.string(d0)
      let assert Ok(tx_id) = dynamic.int(d1)
      let client = dynamic_to_subject(d2)

      AheadOfTransaction(db, tx_id, client) |> handler
    },
  )
}

type State {
  State(
    self: Option(Subject(Message)),
    port: Int,
    name: Atom,
    primary: Primary,
    tx_ids: Dict(String, Int),
  )
}

@external(erlang, "lite_fs_ffi", "dynamic_to_subject")
fn dynamic_to_subject(dyn: Dynamic) -> Subject(a)

@external(erlang, "lite_fs_ffi", "execute_function")
fn execute_function(dyn: Dynamic) -> a

fn primary_to_node(primary: Primary) -> Result(Node, Nil) {
  case primary {
    Local -> Error(Nil)
    Remote(hostname) -> {
      let hostname = case string.split(hostname, ":") {
        [] -> Error(Nil)
        [name] -> Ok(name)
        [name, _] -> Ok(name)
        _ ->
          case uri.parse(hostname) {
            Ok(uri) ->
              case uri.host {
                Some(host) -> Ok(host)
                None -> Error(Nil)
              }
            Error(_) -> Error(Nil)
          }
      }

      use hostname <- result.try(hostname)

      node.visible()
      |> list.find(fn(node) {
        node.to_atom(node) |> atom.to_string |> string.contains(hostname)
      })
    }
  }
}

fn handle_message(message: Message, state: State) -> actor.Next(Message, State) {
  case message {
    Initialize(self) -> {
      let assert Ok(_) = process.register(process.self(), state.name)

      let selector =
        process.new_selector()
        |> process.selecting(self, function.identity)
        |> selecting_message(function.identity)
        |> process.selecting_record3(
          atom.create_from_string("lite_fs_internal"),
          fn(function: Dynamic, client: Dynamic) {
            InternalRpc(function, client)
          },
        )

      let assert Ok(litefs_process) =
        lite_fs.start(state.port, fn(event) {
          process.send(self, LitefsEvent(event))
        })

      process.link(process.subject_owner(litefs_process))

      actor.Continue(State(..state, self: Some(self)), Some(selector))
    }
    LitefsEvent(evt) ->
      case evt {
        Init(primary) -> actor.continue(State(..state, primary: primary))
        PrimaryChange(primary) ->
          actor.continue(State(..state, primary: primary))
        Tx(..) -> {
          let assert Tx(db, tx_id, _, _, _, _) = evt

          let new_tx_ids = dict.insert(state.tx_ids, db, tx_id)

          actor.continue(State(..state, tx_ids: new_tx_ids))
        }
      }
    InternalRpc(function, client) -> {
      let client = dynamic_to_subject(client)

      let primary = primary_to_node(state.primary)

      case primary {
        Ok(node) ->
          node.send(node, state.name, #(
            atom.create_from_string("lite_fs_internal"),
            function,
            client,
          ))
        Error(_) -> {
          let res = execute_function(function)
          process.send(client, res)
        }
      }

      actor.continue(state)
    }
    AheadOfTransaction(db, id, client) -> {
      let res = case dict.get(state.tx_ids, db) {
        Ok(local_tx) if local_tx >= id -> True
        _ -> False
      }

      process.send(client, res)

      actor.continue(state)
    }
  }
}
