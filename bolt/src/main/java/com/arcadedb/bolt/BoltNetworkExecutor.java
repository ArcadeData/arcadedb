/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.bolt;

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.message.*;
import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.bolt.structure.BoltStructureMapper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.logging.Level;

/**
 * Handles a single BOLT protocol connection.
 * Implements the BOLT server state machine and processes client messages.
 */
public class BoltNetworkExecutor extends Thread {
  // BOLT magic bytes
  private static final byte[] BOLT_MAGIC = { 0x60, 0x60, (byte) 0xB0, 0x17 };

  // Supported protocol versions (in order of preference)
  private static final int[] SUPPORTED_VERSIONS = { 0x00000104, 0x00000004, 0x00000003 }; // v4.4, v4.0, v3.0

  // Server states
  private enum State {
    DISCONNECTED,
    NEGOTIATION,
    AUTHENTICATION,
    READY,
    STREAMING,
    TX_READY,
    TX_STREAMING,
    FAILED,
    INTERRUPTED
  }

  private final ArcadeDBServer    server;
  private final Socket            socket;
  private final BoltChunkedInput  input;
  private final BoltChunkedOutput output;
  private final boolean           debug;

  private State             state = State.DISCONNECTED;
  private int               protocolVersion;
  private ServerSecurityUser user;
  private Database          database;
  private String            databaseName;

  // Transaction state
  private boolean explicitTransaction = false;

  // Current result set for streaming
  private ResultSet      currentResultSet;
  private List<String>   currentFields;
  private Result         firstResult; // Buffered first result for field name extraction
  private int            recordsStreamed;

  public BoltNetworkExecutor(final ArcadeDBServer server, final Socket socket) throws IOException {
    super("BOLT-" + socket.getRemoteSocketAddress());
    this.server = server;
    this.socket = socket;
    this.input = new BoltChunkedInput(socket.getInputStream());
    this.output = new BoltChunkedOutput(socket.getOutputStream());
    this.debug = GlobalConfiguration.BOLT_DEBUG.getValueAsBoolean();
  }

  @Override
  public void run() {
    try {
      state = State.NEGOTIATION;

      // Perform handshake
      if (!performHandshake()) {
        return;
      }

      state = State.AUTHENTICATION;

      // Main message loop
      while (state != State.DISCONNECTED) {
        try {
          final byte[] messageData = input.readMessage();
          if (messageData.length == 0) {
            continue;
          }

          final PackStreamReader reader = new PackStreamReader(messageData);
          final Object value = reader.readValue();

          if (!(value instanceof PackStreamReader.StructureValue structure)) {
            sendFailure(BoltException.PROTOCOL_ERROR, "Expected structure, got: " + (value != null ? value.getClass().getSimpleName() : "null"));
            continue;
          }

          final BoltMessage message = BoltMessage.parse(structure);
          if (debug) {
            LogManager.instance().log(this, Level.INFO, "BOLT << %s (state=%s)", message, state);
          }

          processMessage(message);

        } catch (final EOFException | SocketException e) {
          // Client disconnected
          if (debug) {
            LogManager.instance().log(this, Level.INFO, "BOLT client disconnected: %s", e.getMessage());
          }
          break;
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "BOLT error processing message", e);
          try {
            sendFailure(BoltException.DATABASE_ERROR, e.getMessage());
            state = State.FAILED;
          } catch (final IOException ioe) {
            break;
          }
        }
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "BOLT connection error", e);
    } finally {
      cleanup();
    }
  }

  /**
   * Perform BOLT handshake with version negotiation.
   */
  private boolean performHandshake() throws IOException {
    // Read magic bytes
    final byte[] magic = input.readRaw(4);
    if (!Arrays.equals(magic, BOLT_MAGIC)) {
      LogManager.instance().log(this, Level.WARNING, "Invalid BOLT magic bytes");
      return false;
    }

    // Read 4 proposed versions (each 4 bytes, big-endian)
    final int[] clientVersions = new int[4];
    for (int i = 0; i < 4; i++) {
      clientVersions[i] = input.readRawInt();
    }

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "BOLT client versions: %s",
          Arrays.toString(Arrays.stream(clientVersions).mapToObj(v -> String.format("0x%08X", v)).toArray()));
    }

    // Select best matching version
    protocolVersion = 0;
    for (final int clientVersion : clientVersions) {
      for (final int supportedVersion : SUPPORTED_VERSIONS) {
        // Check major version match (upper 16 bits for BOLT 4.x)
        if (clientVersion == supportedVersion ||
            (clientVersion >> 8) == (supportedVersion >> 8)) {
          protocolVersion = supportedVersion;
          break;
        }
      }
      if (protocolVersion != 0) break;
    }

    // Send selected version
    output.writeRawInt(protocolVersion);

    if (protocolVersion == 0) {
      LogManager.instance().log(this, Level.WARNING, "BOLT no compatible version found");
      return false;
    }

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "BOLT negotiated version: 0x%08X", protocolVersion);
    }

    return true;
  }

  /**
   * Process a BOLT message based on current state.
   */
  private void processMessage(final BoltMessage message) throws IOException {
    switch (message.getSignature()) {
    case BoltMessage.HELLO:
      handleHello((HelloMessage) message);
      break;
    case BoltMessage.LOGON:
      handleLogon((LogonMessage) message);
      break;
    case BoltMessage.LOGOFF:
      handleLogoff();
      break;
    case BoltMessage.GOODBYE:
      handleGoodbye();
      break;
    case BoltMessage.RESET:
      handleReset();
      break;
    case BoltMessage.RUN:
      handleRun((RunMessage) message);
      break;
    case BoltMessage.PULL:
      handlePull((PullMessage) message);
      break;
    case BoltMessage.DISCARD:
      handleDiscard((DiscardMessage) message);
      break;
    case BoltMessage.BEGIN:
      handleBegin((BeginMessage) message);
      break;
    case BoltMessage.COMMIT:
      handleCommit();
      break;
    case BoltMessage.ROLLBACK:
      handleRollback();
      break;
    case BoltMessage.ROUTE:
      handleRoute((RouteMessage) message);
      break;
    default:
      sendFailure(BoltException.PROTOCOL_ERROR, "Unknown message: " + BoltMessage.signatureName(message.getSignature()));
    }
  }

  /**
   * Handle HELLO message - authenticate and initialize connection.
   */
  private void handleHello(final HelloMessage message) throws IOException {
    if (state != State.AUTHENTICATION && state != State.NEGOTIATION) {
      sendFailure(BoltException.PROTOCOL_ERROR, "HELLO not expected in state: " + state);
      return;
    }

    final String scheme = message.getScheme();
    final String principal = message.getPrincipal();
    final String credentials = message.getCredentials();

    // Extract database from routing if present
    final Map<String, Object> extra = message.getExtra();
    if (extra.containsKey("routing")) {
      @SuppressWarnings("unchecked")
      final Map<String, Object> routing = (Map<String, Object>) extra.get("routing");
      if (routing != null && routing.containsKey("db")) {
        databaseName = (String) routing.get("db");
      }
    }

    // Try to authenticate
    if ("basic".equals(scheme) && principal != null && credentials != null) {
      try {
        user = server.getSecurity().authenticate(principal, credentials, databaseName);
        if (user == null) {
          sendFailure(BoltException.AUTHENTICATION_ERROR, "Invalid credentials");
          state = State.FAILED;
          return;
        }
      } catch (final ServerSecurityException e) {
        sendFailure(BoltException.AUTHENTICATION_ERROR, e.getMessage());
        state = State.FAILED;
        return;
      }
    } else if ("none".equals(scheme)) {
      // No authentication - reject (authentication is always required)
      sendFailure(BoltException.AUTHENTICATION_ERROR, "Authentication required");
      state = State.FAILED;
      return;
    } else if (principal != null && credentials != null) {
      // Try basic auth even without explicit scheme
      try {
        user = server.getSecurity().authenticate(principal, credentials, databaseName);
        if (user == null) {
          sendFailure(BoltException.AUTHENTICATION_ERROR, "Invalid credentials");
          state = State.FAILED;
          return;
        }
      } catch (final ServerSecurityException e) {
        sendFailure(BoltException.AUTHENTICATION_ERROR, e.getMessage());
        state = State.FAILED;
        return;
      }
    }

    // Build success response with server info
    // Use "Neo4j" prefix for compatibility with official Neo4j drivers
    final Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("server", "Neo4j/5.26.0 compatible (ArcadeDB " + Constants.getRawVersion() + ")");
    metadata.put("connection_id", "bolt-" + Thread.currentThread().getId());

    sendSuccess(metadata);
    state = State.READY;
  }

  /**
   * Handle LOGON message (BOLT 5.1+).
   */
  private void handleLogon(final LogonMessage message) throws IOException {
    if (state != State.AUTHENTICATION && state != State.READY) {
      sendFailure(BoltException.PROTOCOL_ERROR, "LOGON not expected in state: " + state);
      return;
    }

    final String scheme = message.getScheme();
    final String principal = message.getPrincipal();
    final String credentials = message.getCredentials();

    if (principal != null && credentials != null) {
      try {
        user = server.getSecurity().authenticate(principal, credentials, databaseName);
        if (user == null) {
          sendFailure(BoltException.AUTHENTICATION_ERROR, "Invalid credentials");
          state = State.FAILED;
          return;
        }
      } catch (final ServerSecurityException e) {
        sendFailure(BoltException.AUTHENTICATION_ERROR, e.getMessage());
        state = State.FAILED;
        return;
      }
    }

    sendSuccess(Map.of());
    state = State.READY;
  }

  /**
   * Handle LOGOFF message.
   */
  private void handleLogoff() throws IOException {
    user = null;
    sendSuccess(Map.of());
    state = State.AUTHENTICATION;
  }

  /**
   * Handle GOODBYE message - close connection gracefully.
   */
  private void handleGoodbye() {
    state = State.DISCONNECTED;
  }

  /**
   * Handle RESET message - reset to initial state.
   */
  private void handleReset() throws IOException {
    // Rollback any open transaction
    if (explicitTransaction && database != null) {
      try {
        database.rollback();
      } catch (final Exception e) {
        // Ignore
      }
    }

    explicitTransaction = false;
    currentResultSet = null;
    currentFields = null;
    firstResult = null;

    sendSuccess(Map.of());
    state = State.READY;
  }

  /**
   * Handle RUN message - execute a Cypher query.
   */
  private void handleRun(final RunMessage message) throws IOException {
    if (state == State.FAILED) {
      sendIgnored();
      return;
    }

    if (state != State.READY && state != State.TX_READY) {
      sendFailure(BoltException.PROTOCOL_ERROR, "RUN not expected in state: " + state);
      state = State.FAILED;
      return;
    }

    // Get database from message extra if specified
    final String db = message.getDatabase();
    if (db != null && !db.isEmpty()) {
      databaseName = db;
    }

    // Ensure database is open
    if (!ensureDatabase()) {
      return;
    }

    try {
      final String query = message.getQuery();
      final Map<String, Object> params = message.getParameters();

      if (debug) {
        LogManager.instance().log(this, Level.INFO, "BOLT executing: %s with params %s", query, params);
      }

      // Execute using native OpenCypher engine
      currentResultSet = database.query("opencypher", query, params);
      currentFields = extractFieldNames(currentResultSet);
      recordsStreamed = 0;

      // Build success response with query metadata
      final Map<String, Object> metadata = new LinkedHashMap<>();
      metadata.put("fields", currentFields);
      metadata.put("t_first", 0L); // Time to first record (placeholder)

      sendSuccess(metadata);
      state = explicitTransaction ? State.TX_STREAMING : State.STREAMING;

    } catch (final CommandParsingException e) {
      sendFailure(BoltException.SYNTAX_ERROR, e.getMessage());
      state = State.FAILED;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "BOLT query error", e);
      sendFailure(BoltException.DATABASE_ERROR, e.getMessage());
      state = State.FAILED;
    }
  }

  /**
   * Handle PULL message - fetch records from result stream.
   */
  private void handlePull(final PullMessage message) throws IOException {
    if (state == State.FAILED) {
      sendIgnored();
      return;
    }

    if (state != State.STREAMING && state != State.TX_STREAMING) {
      sendFailure(BoltException.PROTOCOL_ERROR, "PULL not expected in state: " + state);
      state = State.FAILED;
      return;
    }

    if (currentResultSet == null) {
      sendFailure(BoltException.PROTOCOL_ERROR, "No active result set");
      state = State.FAILED;
      return;
    }

    try {
      final long n = message.getN();
      long count = 0;

      // First, return the buffered first result if present
      if (firstResult != null && (n < 0 || count < n)) {
        final List<Object> values = new ArrayList<>();
        for (final String field : currentFields) {
          final Object value = firstResult.getProperty(field);
          values.add(BoltStructureMapper.toPackStreamValue(value));
        }
        sendRecord(values);
        count++;
        recordsStreamed++;
        firstResult = null;
      }

      // Then continue with the rest of the result set
      while (currentResultSet.hasNext() && (n < 0 || count < n)) {
        final Result record = currentResultSet.next();
        final List<Object> values = new ArrayList<>();

        for (final String field : currentFields) {
          final Object value = record.getProperty(field);
          values.add(BoltStructureMapper.toPackStreamValue(value));
        }

        sendRecord(values);
        count++;
        recordsStreamed++;
      }

      final boolean hasMore = firstResult != null || currentResultSet.hasNext();

      // Build success metadata
      final Map<String, Object> metadata = new LinkedHashMap<>();
      if (!hasMore) {
        metadata.put("type", "r"); // Read-only query type
        metadata.put("t_last", 0L); // Time to last record
        currentResultSet = null;
        currentFields = null;
        firstResult = null;
        state = explicitTransaction ? State.TX_READY : State.READY;
      }
      metadata.put("has_more", hasMore);

      sendSuccess(metadata);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "BOLT PULL error", e);
      sendFailure(BoltException.DATABASE_ERROR, e.getMessage());
      state = State.FAILED;
    }
  }

  /**
   * Handle DISCARD message - discard remaining records.
   */
  private void handleDiscard(final DiscardMessage message) throws IOException {
    if (state == State.FAILED) {
      sendIgnored();
      return;
    }

    if (state != State.STREAMING && state != State.TX_STREAMING) {
      sendFailure(BoltException.PROTOCOL_ERROR, "DISCARD not expected in state: " + state);
      state = State.FAILED;
      return;
    }

    // Discard all remaining records
    if (currentResultSet != null) {
      while (currentResultSet.hasNext()) {
        currentResultSet.next();
      }
    }

    currentResultSet = null;
    currentFields = null;
    firstResult = null;

    final Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("has_more", false);

    sendSuccess(metadata);
    state = explicitTransaction ? State.TX_READY : State.READY;
  }

  /**
   * Handle BEGIN message - start explicit transaction.
   */
  private void handleBegin(final BeginMessage message) throws IOException {
    if (state == State.FAILED) {
      sendIgnored();
      return;
    }

    if (state != State.READY) {
      sendFailure(BoltException.PROTOCOL_ERROR, "BEGIN not expected in state: " + state);
      state = State.FAILED;
      return;
    }

    // Get database from message if specified
    final String db = message.getDatabase();
    if (db != null && !db.isEmpty()) {
      databaseName = db;
    }

    if (!ensureDatabase()) {
      return;
    }

    try {
      database.begin();
      explicitTransaction = true;

      sendSuccess(Map.of());
      state = State.TX_READY;

    } catch (final Exception e) {
      sendFailure(BoltException.TRANSACTION_ERROR, e.getMessage());
      state = State.FAILED;
    }
  }

  /**
   * Handle COMMIT message - commit explicit transaction.
   */
  private void handleCommit() throws IOException {
    if (state == State.FAILED) {
      sendIgnored();
      return;
    }

    if (state != State.TX_READY) {
      sendFailure(BoltException.PROTOCOL_ERROR, "COMMIT not expected in state: " + state);
      state = State.FAILED;
      return;
    }

    try {
      if (database != null) {
        database.commit();
      }
      explicitTransaction = false;

      final Map<String, Object> metadata = new LinkedHashMap<>();
      metadata.put("bookmark", generateBookmark());

      sendSuccess(metadata);
      state = State.READY;

    } catch (final Exception e) {
      sendFailure(BoltException.TRANSACTION_ERROR, e.getMessage());
      state = State.FAILED;
    }
  }

  /**
   * Handle ROLLBACK message - rollback explicit transaction.
   */
  private void handleRollback() throws IOException {
    if (state == State.FAILED) {
      sendIgnored();
      return;
    }

    if (state != State.TX_READY && state != State.TX_STREAMING) {
      sendFailure(BoltException.PROTOCOL_ERROR, "ROLLBACK not expected in state: " + state);
      state = State.FAILED;
      return;
    }

    try {
      if (database != null) {
        database.rollback();
      }
      explicitTransaction = false;
      currentResultSet = null;
      currentFields = null;
      firstResult = null;

      sendSuccess(Map.of());
      state = State.READY;

    } catch (final Exception e) {
      sendFailure(BoltException.TRANSACTION_ERROR, e.getMessage());
      state = State.FAILED;
    }
  }

  /**
   * Handle ROUTE message - return routing table for cluster-aware drivers.
   */
  private void handleRoute(final RouteMessage message) throws IOException {
    if (state == State.FAILED) {
      sendIgnored();
      return;
    }

    // For single-server setup, return this server as the only endpoint
    final String host = socket.getLocalAddress().getHostAddress();
    final int port = GlobalConfiguration.BOLT_PORT.getValueAsInteger();
    final String address = host + ":" + port;

    final Map<String, Object> rt = new LinkedHashMap<>();
    rt.put("ttl", 300L); // 5 minute TTL
    rt.put("db", message.getDatabase() != null ? message.getDatabase() : databaseName);

    final List<Map<String, Object>> servers = new ArrayList<>();

    // Writer server
    final Map<String, Object> writer = new LinkedHashMap<>();
    writer.put("addresses", List.of(address));
    writer.put("role", "WRITE");
    servers.add(writer);

    // Reader server (same as writer for single-server)
    final Map<String, Object> reader = new LinkedHashMap<>();
    reader.put("addresses", List.of(address));
    reader.put("role", "READ");
    servers.add(reader);

    // Route server
    final Map<String, Object> router = new LinkedHashMap<>();
    router.put("addresses", List.of(address));
    router.put("role", "ROUTE");
    servers.add(router);

    rt.put("servers", servers);

    sendSuccess(Map.of("rt", rt));
  }

  /**
   * Ensure database is open and accessible.
   */
  private boolean ensureDatabase() throws IOException {
    if (database != null && database.isOpen()) {
      return true;
    }

    if (databaseName == null || databaseName.isEmpty()) {
      // Try to get default database or first available
      final Collection<String> databases = server.getDatabaseNames();
      if (databases.isEmpty()) {
        sendFailure(BoltException.DATABASE_ERROR, "No database available");
        state = State.FAILED;
        return false;
      }
      databaseName = databases.iterator().next();
    }

    try {
      database = server.getDatabase(databaseName);
      if (database == null || !database.isOpen()) {
        sendFailure(BoltException.DATABASE_ERROR, "Database not found: " + databaseName);
        state = State.FAILED;
        return false;
      }
      return true;
    } catch (final Exception e) {
      sendFailure(BoltException.DATABASE_ERROR, "Cannot open database: " + databaseName + " - " + e.getMessage());
      state = State.FAILED;
      return false;
    }
  }

  /**
   * Extract field names from result set by peeking at the first result.
   * The first result is buffered and will be returned first during PULL.
   */
  private List<String> extractFieldNames(final ResultSet resultSet) {
    if (resultSet == null) {
      return List.of();
    }

    // Peek at first result to get field names
    if (resultSet.hasNext()) {
      firstResult = resultSet.next();
      final Set<String> propertyNames = firstResult.getPropertyNames();
      return propertyNames != null ? new ArrayList<>(propertyNames) : List.of();
    }

    return List.of();
  }

  /**
   * Generate a bookmark for the current transaction.
   */
  private String generateBookmark() {
    return "arcade:tx:" + System.currentTimeMillis();
  }

  /**
   * Send a SUCCESS response message.
   */
  private void sendSuccess(final Map<String, Object> metadata) throws IOException {
    final SuccessMessage success = new SuccessMessage(metadata);
    sendMessage(success);
  }

  /**
   * Send a FAILURE response message.
   */
  private void sendFailure(final String code, final String message) throws IOException {
    final FailureMessage failure = new FailureMessage(code, message);
    sendMessage(failure);
  }

  /**
   * Send an IGNORED response message.
   */
  private void sendIgnored() throws IOException {
    sendMessage(new IgnoredMessage());
  }

  /**
   * Send a RECORD message.
   */
  private void sendRecord(final List<Object> data) throws IOException {
    final RecordMessage record = new RecordMessage(data);
    sendMessage(record);
  }

  /**
   * Send a message to the client.
   */
  private void sendMessage(final BoltMessage message) throws IOException {
    if (debug) {
      LogManager.instance().log(this, Level.INFO, "BOLT >> %s", message);
    }

    final PackStreamWriter writer = new PackStreamWriter();
    message.writeTo(writer);
    output.writeMessage(writer.toByteArray());
  }

  /**
   * Cleanup resources when connection closes.
   */
  private void cleanup() {
    try {
      if (explicitTransaction && database != null) {
        database.rollback();
      }
    } catch (final Exception e) {
      // Ignore
    }

    try {
      socket.close();
    } catch (final Exception e) {
      // Ignore
    }

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "BOLT connection closed");
    }
  }
}
