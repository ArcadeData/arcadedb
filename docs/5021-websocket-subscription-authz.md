# Issue #5021 - WebSocket change-stream subscription lacks per-database authorization

## Symptom
The WebSocket change-stream endpoint authenticates the user during the HTTP handshake but then
discards that identity. When a client sends a `SUBSCRIBE` message, the server subscribes to the
client-supplied database name with no authorization check, so any authenticated user can stream the
full live change feed (record contents) of a database they have no rights to read
(cross-tenant / cross-database data disclosure).

## Root cause
- `WebSocketConnectionHandler.execute(exchange, user, payload)` receives an authenticated
  `ServerSecurityUser` but the handshake callback only stores a random channel id; the user is dropped.
- `WebSocketReceiveListener` `SUBSCRIBE` branch subscribes with the client-supplied database name and
  performs no `user.canAccessToDatabase(database)` gate (unlike the REST path).

## Fix
- Add a `WebSocketEventBus.USER` channel-attribute key.
- `WebSocketConnectionHandler` stores the authenticated `user` on the channel inside the handshake
  callback.
- `WebSocketReceiveListener` `SUBSCRIBE` branch reads the user back and rejects the subscription with an
  error frame when the user is missing or `!user.canAccessToDatabase(database)`. The authorization check
  runs before any database watcher is started, so an unauthorized subscribe delivers zero events.
- `UNSUBSCRIBE` needs no additional check (it only affects the caller's own channel).

## Tests
`server/src/test/java/com/arcadedb/server/ws/WebSocketEventBusIT.java`:
- `subscribeToUnauthorizedDatabaseIsRejected`: a user authorized only for another database subscribes to
  `graph`, asserts an error frame and that no change events are delivered.
- `subscribeToAuthorizedDatabaseWorksForNonRootUser`: a non-root user authorized for `graph` subscribes
  and asserts events flow (no regression).

## Impact
Closes a HIGH-severity cross-database data-disclosure hole in the change stream. No new auth model:
reuses the existing `ServerSecurityUser.canAccessToDatabase` primitive already used by the REST layer.
