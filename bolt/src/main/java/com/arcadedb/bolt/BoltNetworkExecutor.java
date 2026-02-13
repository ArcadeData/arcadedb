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
import com.arcadedb.bolt.message.BeginMessage;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.bolt.message.DiscardMessage;
import com.arcadedb.bolt.message.FailureMessage;
import com.arcadedb.bolt.message.HelloMessage;
import com.arcadedb.bolt.message.IgnoredMessage;
import com.arcadedb.bolt.message.LogonMessage;
import com.arcadedb.bolt.message.PullMessage;
import com.arcadedb.bolt.message.RecordMessage;
import com.arcadedb.bolt.message.RouteMessage;
import com.arcadedb.bolt.message.RunMessage;
import com.arcadedb.bolt.message.SuccessMessage;
import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.bolt.structure.BoltStructureMapper;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.CollectionUtils;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import static com.arcadedb.query.opencypher.executor.steps.FinalProjectionStep.PROJECTION_NAME_METADATA;

/**
 * Handles a single BOLT protocol connection.
 * Implements the BOLT server state machine and processes client messages.
 */
public class BoltNetworkExecutor extends Thread {
  // BOLT magic bytes
  private static final byte[] BOLT_MAGIC = { 0x60, 0x60, (byte) 0xB0, 0x17 };

  // Supported protocol versions (in order of preference)
  // Encoding: [unused(8)][range(8)][minor(8)][major(8)] — major = value & 0xFF, minor = (value >> 8) & 0xFF
  private static final int[] SUPPORTED_VERSIONS = { 0x00000404, 0x00000004, 0x00000003 }; // v4.4, v4.0, v3.0

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

  private final ArcadeDBServer      server;
  private final Socket              socket;
  private final BoltChunkedInput    input;
  private final BoltChunkedOutput   output;
  private final boolean             debug;
  private final BoltNetworkListener listener; // For notifying when connection closes

  private State              state = State.DISCONNECTED;
  private int                protocolVersion;
  private ServerSecurityUser user;
  private Database           database;
  private String             databaseName;

  // Transaction state
  private boolean explicitTransaction = false;

  /**
   * Current result set for streaming results.
   * Thread-safety: This class is designed to handle a single connection in a dedicated thread.
   * All state variables are accessed only by the executor thread and do not require synchronization.
   */
  private ResultSet    currentResultSet;
  private List<String> currentFields;
  private Result       firstResult; // Buffered first result for field name extraction
  private int          recordsStreamed;
  private long         queryStartTime; // Nanosecond timestamp when query execution started
  private long         firstRecordTime; // Nanosecond timestamp when first record was retrieved
  private boolean      isWriteOperation; // Whether the current query performs writes

  public BoltNetworkExecutor(final ArcadeDBServer server, final Socket socket, final BoltNetworkListener listener)
      throws IOException {
    super("BOLT-" + socket.getRemoteSocketAddress());
    this.server = server;
    this.socket = socket;
    this.listener = listener;
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
            sendFailure(BoltException.PROTOCOL_ERROR,
                "Expected structure, got: " + (value != null ? value.getClass().getSimpleName() : "null"));
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

    // Select best matching version using Bolt version negotiation with range support.
    // Each client version entry encodes: major = value & 0xFF, minor = (value >> 8) & 0xFF,
    // range = (value >> 16) & 0xFF. The range means the client supports minor versions
    // from (minor - range) up to minor (inclusive) for the given major version.
    protocolVersion = 0;
    for (final int clientVersion : clientVersions) {
      if (clientVersion == 0)
        continue;

      final int clientMajor = clientVersion & 0xFF;
      final int clientMinor = (clientVersion >> 8) & 0xFF;
      final int clientRange = (clientVersion >> 16) & 0xFF;

      for (final int supportedVersion : SUPPORTED_VERSIONS) {
        final int serverMajor = supportedVersion & 0xFF;
        final int serverMinor = (supportedVersion >> 8) & 0xFF;

        if (clientMajor == serverMajor && serverMinor <= clientMinor && serverMinor >= clientMinor - clientRange) {
          protocolVersion = supportedVersion;
          break;
        }
      }
      if (protocolVersion != 0)
        break;
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
      if (!authenticateUser(principal, credentials)) {
        return;
      }
    } else if ("none".equals(scheme)) {
      // No authentication - reject (authentication is always required)
      sendFailure(BoltException.AUTHENTICATION_ERROR, "Authentication required");
      state = State.FAILED;
      return;
    } else if (principal != null && credentials != null) {
      // Try basic auth even without explicit scheme
      if (!authenticateUser(principal, credentials)) {
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

    if (!authenticateUser(principal, credentials)) {
      return;
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
    // Close any open result set
    if (currentResultSet != null) {
      try {
        currentResultSet.close();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to close ResultSet during RESET", e);
      }
    }

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

    // Check database health and force re-authentication if database is unavailable
    if (database == null || !database.isOpen()) {
      state = State.AUTHENTICATION;
    } else {
      state = State.READY;
    }
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

      // Start timing for performance metrics
      queryStartTime = System.nanoTime();
      firstRecordTime = 0;

      // Determine if this is a write query using the query analyzer
      isWriteOperation = isWriteQuery(query);

      // Use command() for writes, query() for reads
      if (isWriteOperation) {
        currentResultSet = database.command("opencypher", query, params);
      } else {
        currentResultSet = database.query("opencypher", query, params);
      }
      currentFields = extractFieldNames(currentResultSet);
      recordsStreamed = 0;

      // Build success response with query metadata
      final Map<String, Object> metadata = new LinkedHashMap<>();
      metadata.put("fields", currentFields);

      // Calculate time to first record if we already have one buffered
      if (firstResult != null && firstRecordTime > 0) {
        final long tFirstMs = (firstRecordTime - queryStartTime) / 1_000_000;
        metadata.put("t_first", tFirstMs);
      } else {
        metadata.put("t_first", 0L);
      }

      sendSuccess(metadata);
      state = explicitTransaction ? State.TX_STREAMING : State.STREAMING;

    } catch (final CommandParsingException e) {
      final String parseMsg = e.getMessage() != null ? e.getMessage() : "Query parsing error";
      sendFailure(BoltException.SYNTAX_ERROR, parseMsg);
      state = State.FAILED;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "BOLT query error", e);
      final String errorMsg = e.getMessage() != null ? e.getMessage() : "Database error";
      sendFailure(BoltException.DATABASE_ERROR, errorMsg);
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
        final List<Object> values = extractRecordValues(firstResult);
        sendRecord(values);
        count++;
        recordsStreamed++;
        firstResult = null;
      }

      // Then continue with the rest of the result set
      while (currentResultSet.hasNext() && (n < 0 || count < n)) {
        final Result record = currentResultSet.next();
        final List<Object> values = extractRecordValues(record);

        sendRecord(values);
        count++;
        recordsStreamed++;
      }

      final boolean hasMore = firstResult != null || currentResultSet.hasNext();

      // Build success metadata
      final Map<String, Object> metadata = new LinkedHashMap<>();
      if (!hasMore) {
        // Determine query type based on whether it performed writes
        // r=read, w=write (for simplicity, we use binary classification)
        metadata.put("type", isWriteOperation ? "w" : "r");

        // Calculate time to last record
        final long tLastMs = (System.nanoTime() - queryStartTime) / 1_000_000;
        metadata.put("t_last", tLastMs);

        try {
          currentResultSet.close();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "Failed to close ResultSet during PULL completion", e);
        }
        currentResultSet = null;
        currentFields = null;
        firstResult = null;
        state = explicitTransaction ? State.TX_READY : State.READY;
      }
      metadata.put("has_more", hasMore);

      sendSuccess(metadata);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "BOLT PULL error", e);
      final String errorMsg = e.getMessage() != null ? e.getMessage() : "Error fetching records";
      sendFailure(BoltException.DATABASE_ERROR, errorMsg);
      state = State.FAILED;
    }
  }

  /**
   * Handle DISCARD message - discard remaining records.
   */
  private void handleDiscard(final DiscardMessage discardMessage) throws IOException {
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
      try {
        currentResultSet.close();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to close ResultSet during DISCARD", e);
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
  private void handleBegin(final BeginMessage beginMessage) throws IOException {
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
    final String db = beginMessage.getDatabase();
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
      // Attempt to rollback in case transaction was partially started
      try {
        if (database != null) {
          database.rollback();
        }
      } catch (final Exception rollbackError) {
        LogManager.instance().log(this, Level.WARNING, "Failed to rollback after BEGIN error", rollbackError);
      }
      final String errorMsg = e.getMessage() != null ? e.getMessage() : "Transaction error";
      sendFailure(BoltException.TRANSACTION_ERROR, errorMsg);
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
      final String message = e.getMessage() != null ? e.getMessage() : "Commit error";
      sendFailure(BoltException.TRANSACTION_ERROR, message);
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
      if (currentResultSet != null) {
        try {
          currentResultSet.close();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "Failed to close ResultSet during ROLLBACK", e);
        }
      }
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
      final String message = e.getMessage() != null ? e.getMessage() : "Rollback error";
      sendFailure(BoltException.TRANSACTION_ERROR, message);
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
    rt.put("ttl", GlobalConfiguration.BOLT_ROUTING_TTL.getValueAsLong());
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

    sendSuccess(CollectionUtils.singletonMap("rt", rt));
  }

  /**
   * Ensure database is open and accessible.
   */
  private boolean ensureDatabase() throws IOException {
    if (database != null && database.isOpen()) {
      return true;
    }

    if (databaseName == null || databaseName.isEmpty()) {
      // Try to use configured default database
      databaseName = GlobalConfiguration.BOLT_DEFAULT_DATABASE.getValueAsString();

      if (databaseName == null || databaseName.isEmpty()) {
        // If no default configured, use the first available database
        final Collection<String> databases = server.getDatabaseNames();
        if (databases.isEmpty()) {
          sendFailure(BoltException.DATABASE_ERROR, "No database available");
          state = State.FAILED;
          return false;
        }
        databaseName = databases.iterator().next();
      }
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
      final String message = e.getMessage() != null ? e.getMessage() : "Unknown error";
      sendFailure(BoltException.DATABASE_ERROR, "Cannot open database: " + databaseName + " - " + message);
      state = State.FAILED;
      return false;
    }
  }

  /**
   * Extract field names from result set by peeking at the first result.
   * The first result is buffered and will be returned first during PULL.
   * <p>
   * For single-element results (e.g., RETURN n), the projection name is stored
   * in metadata by FinalProjectionStep and used here to preserve field names.
   */
  private List<String> extractFieldNames(final ResultSet resultSet) {
    if (resultSet == null) {
      return List.of();
    }

    // Peek at first result to get field names
    if (resultSet.hasNext()) {
      firstResult = resultSet.next();
      firstRecordTime = System.nanoTime(); // Capture time when first record is available

      // Check if this is an unwrapped element with a projection name in metadata
      // This happens for queries like "MATCH (n) RETURN n" where the vertex is
      // returned directly but we need to preserve the field name "n" for Bolt protocol
      if (firstResult.isElement()) {
        final Object projectionName = firstResult.getMetadata(PROJECTION_NAME_METADATA);
        if (projectionName instanceof String name) {
          return List.of(name);
        }
      }

      final Set<String> propertyNames = firstResult.getPropertyNames();
      return propertyNames != null ? new ArrayList<>(propertyNames) : List.of();
    }

    return List.of();
  }

  /**
   * Extract values from a result for sending as a BOLT RECORD.
   * Handles both projection results and element results.
   * <p>
   * For element results (e.g., RETURN n where n is a vertex), the whole element
   * is returned as a single value, converted to BoltNode/BoltRelationship.
   */
  private List<Object> extractRecordValues(final Result result) {
    final List<Object> values = new ArrayList<>();

    // Check if this is an unwrapped element result
    // (single vertex/edge returned directly from RETURN clause)
    if (result.isElement() && result.getMetadata(PROJECTION_NAME_METADATA) != null) {
      // Return the element as a single value
      values.add(BoltStructureMapper.toPackStreamValue(result.getElement().orElse(null)));
    } else {
      // Standard projection result - extract each field
      for (final String field : currentFields) {
        final Object value = result.getProperty(field);
        values.add(BoltStructureMapper.toPackStreamValue(value));
      }
    }

    return values;
  }

  /**
   * Determine if a Cypher query contains write operations.
   * Uses ArcadeDB's query analyzer for accurate detection.
   */
  private boolean isWriteQuery(final String query) {
    if (query == null || query.isEmpty()) {
      return false;
    }
    try {
      // Use the query engine's analyzer to determine if the query is idempotent (read-only)
      return !database.getQueryEngine("opencypher").analyze(query).isIdempotent();
    } catch (final Exception e) {
      // If analysis fails, assume it's a write operation to be safe
      // Log at FINE level to avoid spam for complex but valid queries
      LogManager.instance().log(this, Level.FINE,
          "Query analysis failed for: " + (query.length() > 100 ? query.substring(0, 100) + "..." : query) +
              " - assuming write operation", e);
      return true;
    }
  }

  /**
   * Generate a bookmark for the current transaction.
   */
  private String generateBookmark() {
    return "arcade:tx:" + System.currentTimeMillis();
  }

  /**
   * Authenticate user with provided credentials.
   *
   * @param principal   the username
   * @param credentials the password
   *
   * @return true if authentication succeeded, false otherwise (failure already sent)
   *
   * @throws IOException if sending failure message fails
   */
  private boolean authenticateUser(final String principal, final String credentials) throws IOException {
    if (principal == null || credentials == null) {
      sendFailure(BoltException.AUTHENTICATION_ERROR, "Missing credentials");
      state = State.FAILED;
      return false;
    }

    try {
      user = server.getSecurity().authenticate(principal, credentials, databaseName);
      if (user == null) {
        sendFailure(BoltException.AUTHENTICATION_ERROR, "Authentication failed");
        state = State.FAILED;
        return false;
      }
      return true;
    } catch (final ServerSecurityException e) {
      // Sanitize error message to avoid information disclosure
      sendFailure(BoltException.AUTHENTICATION_ERROR, "Authentication failed");
      state = State.FAILED;
      return false;
    }
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
      if (currentResultSet != null) {
        currentResultSet.close();
        currentResultSet = null;
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Failed to close ResultSet during cleanup", e);
    }

    try {
      if (explicitTransaction && database != null) {
        database.rollback();
      }
    } catch (final Exception e) {
      // Ignore
    }

    // Database is managed by the server - just release our reference
    // DO NOT close the shared database instance
    database = null;

    try {
      socket.close();
    } catch (final Exception e) {
      // Ignore
    }

    // Notify listener that this connection is closed
    if (listener != null) {
      listener.removeConnection(this);
    }

    if (debug) {
      LogManager.instance().log(this, Level.INFO, "BOLT connection closed");
    }
  }
}
