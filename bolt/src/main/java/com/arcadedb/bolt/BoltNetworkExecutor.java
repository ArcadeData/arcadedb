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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.CollectionUtils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
  private       BoltChunkedInput    input;
  private       BoltChunkedOutput   output;
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
  private ResultSet          currentResultSet;
  private List<String>       currentFields;
  private Result             firstResult; // Buffered first result for field name extraction
  private List<List<Object>> syntheticResults; // For system queries that return synthetic data
  private int                recordsStreamed;
  private long               queryStartTime; // Nanosecond timestamp when query execution started
  private long               firstRecordTime; // Nanosecond timestamp when first record was retrieved
  private boolean            isWriteOperation; // Whether the current query performs writes

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
            LogManager.instance().log(this, Level.FINE, "BOLT << %s (state=%s)", message, state);
          }

          processMessage(message);

        } catch (final EOFException | SocketException e) {
          // Client disconnected
          if (debug) {
            LogManager.instance().log(this, Level.FINE, "BOLT client disconnected: %s", e.getMessage());
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
   * Supports both raw TCP and WebSocket transport (for Neo4j Desktop/Browser).
   */
  private boolean performHandshake() throws IOException {
    // Read magic bytes
    byte[] magic = input.readRaw(4);

    // Check if this is an HTTP request (WebSocket upgrade or plain HTTP probe)
    if (isHttpRequest(magic)) {
      final Map<String, String> headers = readHttpHeaders();
      final String upgrade = headers.get("upgrade");

      if (upgrade != null && "websocket".equalsIgnoreCase(upgrade)) {
        // WebSocket upgrade - Neo4j Desktop/Browser uses WebSocket transport for Bolt
        completeWebSocketUpgrade(headers);

        // Reinitialize I/O with WebSocket framing and read Bolt magic from WebSocket stream
        input = new BoltChunkedInput(new BoltWebSocketInputStream(socket.getInputStream()));
        output = new BoltChunkedOutput(new BoltWebSocketOutputStream(socket.getOutputStream()));
        try {
          magic = input.readRaw(4);
        } catch (final EOFException e) {
          // Client closed WebSocket without sending Bolt data (e.g. Neo4j Desktop health/SSO probe)
          if (debug)
            LogManager.instance().log(this, Level.FINE, "BOLT WebSocket closed without Bolt handshake from %s",
                socket.getRemoteSocketAddress());
          return false;
        }
      } else {
        // Plain HTTP request (e.g. SSO/OIDC discovery probe)
        handleHttpOnBoltPort();
        return false;
      }
    }

    if (!Arrays.equals(magic, BOLT_MAGIC)) {
      if (magic[0] == 0x16 && magic[1] == 0x03)
        LogManager.instance().log(this, Level.WARNING,
            "TLS/SSL connection attempted on BOLT port. ArcadeDB BOLT does not support encryption. "
                + "Configure the client to use bolt:// (unencrypted) instead of bolt+s:// or bolt+ssc://");
      else
        LogManager.instance().log(this, Level.WARNING,
            "Invalid BOLT magic bytes: [%d, %d, %d, %d]", magic[0], magic[1], magic[2], magic[3]);
      return false;
    }

    return negotiateVersion();
  }

  /**
   * Negotiate BOLT protocol version with the client.
   */
  private boolean negotiateVersion() throws IOException {
    // Read 4 proposed versions (each 4 bytes, big-endian)
    final int[] clientVersions = new int[4];
    for (int i = 0; i < 4; i++)
      clientVersions[i] = input.readRawInt();

    if (debug)
      LogManager.instance().log(this, Level.FINE, "BOLT client versions: %s",
          Arrays.toString(Arrays.stream(clientVersions).mapToObj(v -> String.format("0x%08X", v)).toArray()));

    // Select best matching version using Bolt version negotiation with range support.
    // The range means the client supports minor versions from (minor - range) up to minor
    // (inclusive) for the given major version. Zero entries are trailing padding per the Bolt spec.
    protocolVersion = 0;
    for (final int clientVersion : clientVersions) {
      if (clientVersion == 0)
        break;

      final int clientMajor = getMajorVersion(clientVersion);
      final int clientMinor = getMinorVersion(clientVersion);
      final int clientRange = getVersionRange(clientVersion);

      for (final int supportedVersion : SUPPORTED_VERSIONS) {
        final int serverMajor = getMajorVersion(supportedVersion);
        final int serverMinor = getMinorVersion(supportedVersion);

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

    if (debug)
      LogManager.instance().log(this, Level.FINE, "BOLT connection from %s, negotiated version %d.%d",
          socket.getRemoteSocketAddress(), getMajorVersion(protocolVersion), getMinorVersion(protocolVersion));

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

    // If the user has authenticated, go to READY (the database may not be selected yet
    // and will be resolved on the next RUN/BEGIN). Only revert to AUTHENTICATION if
    // the user hasn't authenticated at all.
    if (user != null) {
      state = State.READY;
    } else {
      state = State.AUTHENTICATION;
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

    final String query = message.getQuery();
    final Map<String, Object> params = message.getParameters();

    if (debug)
      LogManager.instance().log(this, Level.FINE, "BOLT executing: %s with params %s (db=%s)", query, params, databaseName);

    // Start timing for performance metrics
    queryStartTime = System.nanoTime();
    firstRecordTime = 0;
    syntheticResults = null;

    // Ensure database is open (maps "system"/"neo4j" to default database)
    if (!ensureDatabase())
      return;

    // Intercept known system queries (CALL dbms.components(), SHOW DATABASES, etc.)
    if (handleSystemQuery(query)) {
      currentResultSet = null;
      firstResult = null;
      recordsStreamed = 0;
      isWriteOperation = false;

      final Map<String, Object> metadata = new LinkedHashMap<>();
      metadata.put("fields", currentFields);
      metadata.put("t_first", 0L);
      sendSuccess(metadata);
      state = explicitTransaction ? State.TX_STREAMING : State.STREAMING;
      return;
    }

    try {
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

      if (debug) {
        LogManager.instance().log(this, Level.FINE, "BOLT query fields=%s firstResult=%s", currentFields,
            firstResult != null ? firstResult.toJSON() : "null");
      }

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

    if (currentResultSet == null && syntheticResults == null) {
      sendFailure(BoltException.PROTOCOL_ERROR, "No active result set");
      state = State.FAILED;
      return;
    }

    try {
      final long n = message.getN();
      long count = 0;

      // Handle synthetic results (from system queries)
      if (syntheticResults != null) {
        while (!syntheticResults.isEmpty() && (n < 0 || count < n)) {
          sendRecord(syntheticResults.remove(0));
          count++;
          recordsStreamed++;
        }
      } else {
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
      }

      final boolean hasMore = syntheticResults != null ? !syntheticResults.isEmpty()
          : (firstResult != null || currentResultSet.hasNext());

      // Build success metadata
      final Map<String, Object> metadata = new LinkedHashMap<>();
      if (!hasMore) {
        // Determine query type based on whether it performed writes
        // r=read, w=write (for simplicity, we use binary classification)
        metadata.put("type", isWriteOperation ? "w" : "r");

        // Calculate time to last record
        final long tLastMs = (System.nanoTime() - queryStartTime) / 1_000_000;
        metadata.put("t_last", tLastMs);

        if (currentResultSet != null) {
          try {
            currentResultSet.close();
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.WARNING, "Failed to close ResultSet during PULL completion", e);
          }
        }
        currentResultSet = null;
        currentFields = null;
        firstResult = null;
        syntheticResults = null;
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
    syntheticResults = null;

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
    final String address = getBoltAddress(GlobalConfiguration.BOLT_PORT.getValueAsInteger());

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
      // Check if we need to switch to a different database
      final String currentDbName = database.getName();
      if (databaseName != null && !databaseName.isEmpty()
          && !"system".equals(databaseName) && !"neo4j".equals(databaseName)
          && !currentDbName.equals(databaseName)) {
        // Database name changed, need to switch
        database = null;
      } else {
        return true;
      }
    }

    // Resolve the target database name, mapping virtual names to real databases
    String targetName = databaseName;
    if (targetName == null || targetName.isEmpty() || "system".equals(targetName) || "neo4j".equals(targetName)) {
      // "system" and "neo4j" are Neo4j virtual databases; map to default ArcadeDB database
      targetName = GlobalConfiguration.BOLT_DEFAULT_DATABASE.getValueAsString();

      if (targetName == null || targetName.isEmpty()) {
        // If no default configured, use the first available database
        final Collection<String> databases = server.getDatabaseNames();
        if (databases.isEmpty()) {
          sendFailure(BoltException.DATABASE_ERROR, "No database available");
          state = State.FAILED;
          return false;
        }
        targetName = databases.iterator().next();
      }
    }

    try {
      database = server.getDatabase(targetName);
      if (database == null || !database.isOpen()) {
        sendFailure(BoltException.DATABASE_ERROR, "Database not found: " + targetName);
        state = State.FAILED;
        return false;
      }
      return true;
    } catch (final Exception e) {
      final String message = e.getMessage() != null ? e.getMessage() : "Unknown error";
      sendFailure(BoltException.DATABASE_ERROR, "Cannot open database: " + targetName + " - " + message);
      state = State.FAILED;
      return false;
    }
  }

  /**
   * Handles known Neo4j system queries (e.g., dbms.components, SHOW DATABASES).
   * Returns true if the query was handled as a system query, false if it should be executed normally.
   */
  private boolean handleSystemQuery(final String query) throws IOException {
    final String normalized = query.trim().toLowerCase().replaceAll("\\s+", " ");

    if (normalized.contains("dbms.components")) {
      // CALL dbms.components() - returns server version info
      currentFields = List.of("name", "versions", "edition");
      syntheticResults = new ArrayList<>();
      syntheticResults.add(List.of("Neo4j Kernel", List.of("5.26.0"), "community"));
      return true;

    } else if (normalized.startsWith("show database") || normalized.contains("dbms.showdatabase")
        || normalized.contains("dbms.listdatabases")) {
      // SHOW DATABASES or CALL dbms.listDatabases()
      currentFields = List.of("name", "type", "aliases", "access", "address", "role",
          "writer", "requestedStatus", "currentStatus", "statusMessage", "default", "home",
          "constituents");
      syntheticResults = new ArrayList<>();
      for (final String dbName : server.getDatabaseNames()) {
        syntheticResults.add(List.of(dbName, "standard", List.of(), "read-write",
            getBoltAddress(GlobalConfiguration.BOLT_PORT.getValueAsInteger()), "primary",
            true, "online", "online", "", dbName.equals(database != null ? database.getName() : ""), false,
            List.of()));
      }
      // Also add the virtual "system" database entry
      syntheticResults.add(List.of("system", "system", List.of(), "read-write",
          getBoltAddress(GlobalConfiguration.BOLT_PORT.getValueAsInteger()), "primary",
          false, "online", "online", "", false, false, List.of()));
      return true;

    } else if (normalized.contains("show current user") || normalized.contains("dbms.showcurrentuser")) {
      // SHOW CURRENT USER or CALL dbms.showCurrentUser()
      currentFields = List.of("user", "roles", "passwordChangeRequired", "suspended", "home");
      syntheticResults = new ArrayList<>();
      final List<Object> userRecord = new ArrayList<>();
      userRecord.add(user != null ? user.getName() : "anonymous");
      userRecord.add(List.of("admin"));
      userRecord.add(false);
      userRecord.add(false);
      userRecord.add(null); // home database (null = use default)
      syntheticResults.add(userRecord);
      return true;

    } else if (normalized.contains("dbms.info")) {
      // CALL dbms.info() - returns basic server info
      currentFields = List.of("id", "name", "creationDate");
      syntheticResults = new ArrayList<>();
      syntheticResults.add(List.of("arcadedb-" + server.getServerName(), server.getServerName(), ""));
      return true;

    } else if (normalized.contains("db.ping")) {
      // CALL db.ping() - health check
      currentFields = List.of("success");
      syntheticResults = new ArrayList<>();
      syntheticResults.add(List.of(true));
      return true;

    } else if (normalized.contains("dbms.clientconfig")) {
      // CALL dbms.clientConfig() - client configuration
      currentFields = List.of("name", "value");
      syntheticResults = new ArrayList<>();
      return true;

    } else if (normalized.startsWith("show procedure")) {
      // SHOW PROCEDURES YIELD * - return empty list
      currentFields = List.of("name", "description", "mode", "worksOnSystem", "argumentDescription",
          "returnDescription", "admin", "option");
      syntheticResults = new ArrayList<>();
      return true;

    } else if (normalized.startsWith("show function")) {
      // SHOW FUNCTIONS YIELD * - return empty list
      currentFields = List.of("name", "category", "description", "isBuiltIn", "argumentDescription",
          "returnDescription", "aggregating");
      syntheticResults = new ArrayList<>();
      return true;

    } else if (normalized.contains("db.labels") && normalized.contains("db.relationshiptypes")
        && normalized.contains("db.propertykeys")) {
      // Combined UNION query from Neo4j Desktop: collects labels, relationship types, property keys as lists
      currentFields = List.of("result");
      syntheticResults = new ArrayList<>();
      if (database != null) {
        // Labels (vertex types, excluding composite ~ types)
        final List<Object> labels = new ArrayList<>();
        for (final DocumentType type : database.getSchema().getTypes())
          if (type instanceof VertexType && !type.getName().contains("~"))
            labels.add(type.getName());
        syntheticResults.add(List.of((Object) labels));

        // Relationship types (edge types)
        final List<Object> relTypes = new ArrayList<>();
        for (final DocumentType type : database.getSchema().getTypes())
          if (type instanceof EdgeType)
            relTypes.add(type.getName());
        syntheticResults.add(List.of((Object) relTypes));

        // Property keys (from all non-composite types)
        final Set<String> allKeys = new TreeSet<>();
        for (final DocumentType type : database.getSchema().getTypes())
          if (!type.getName().contains("~"))
            allKeys.addAll(type.getPropertyNames());
        syntheticResults.add(List.of((Object) new ArrayList<>(allKeys)));
      }
      return true;

    } else if (normalized.contains("db.labels")) {
      // CALL db.labels() - return vertex type names (excluding composite types with ~)
      currentFields = List.of("label");
      syntheticResults = new ArrayList<>();
      if (database != null) {
        for (final DocumentType type : database.getSchema().getTypes())
          if (type instanceof VertexType && !type.getName().contains("~"))
            syntheticResults.add(List.of(type.getName()));
      }
      return true;

    } else if (normalized.contains("db.relationshiptypes")) {
      // CALL db.relationshipTypes() - return edge type names
      currentFields = List.of("relationshipType");
      syntheticResults = new ArrayList<>();
      if (database != null) {
        for (final DocumentType type : database.getSchema().getTypes())
          if (type instanceof EdgeType)
            syntheticResults.add(List.of(type.getName()));
      }
      return true;

    } else if (normalized.contains("db.propertykeys")) {
      // CALL db.propertyKeys() - return all property key names
      currentFields = List.of("propertyKey");
      syntheticResults = new ArrayList<>();
      if (database != null) {
        final Set<String> allKeys = new TreeSet<>();
        for (final DocumentType type : database.getSchema().getTypes()) {
          if (!type.getName().contains("~"))
            allKeys.addAll(type.getPropertyNames());
        }
        for (final String key : allKeys)
          syntheticResults.add(List.of(key));
      }
      return true;

    } else if (normalized.startsWith("show index") || normalized.startsWith("show vector index")) {
      // SHOW INDEXES / SHOW VECTOR INDEXES - return empty list
      currentFields = List.of("id", "name", "state", "populationPercent", "type", "entityType",
          "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "lastRead", "readCount");
      syntheticResults = new ArrayList<>();
      return true;

    } else if (normalized.contains("dbms.licenseagreementdetails")) {
      // CALL dbms.licenseAgreementDetails() - return empty/default
      currentFields = List.of("name", "status", "version");
      syntheticResults = new ArrayList<>();
      syntheticResults.add(List.of("ArcadeDB", "active", "community"));
      return true;
    }

    return false;
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
      LogManager.instance().log(this, Level.FINE, "BOLT >> %s", message);
    }

    final PackStreamWriter writer = new PackStreamWriter();
    message.writeTo(writer);
    output.writeMessage(writer.toByteArray());
  }

  /**
   * Checks if the first 4 bytes look like an HTTP request method.
   * Neo4j Desktop and some clients may send HTTP/WebSocket requests to the Bolt port.
   */
  private static boolean isHttpRequest(final byte[] magic) {
    final String prefix = new String(magic, StandardCharsets.US_ASCII);
    return "GET ".equals(prefix) || "POST".equals(prefix) || "PUT ".equals(prefix)
        || "HEAD".equals(prefix) || "DELE".equals(prefix) || "OPTI".equals(prefix);
  }

  /**
   * Reads HTTP request headers from the socket after the first 4 bytes (method prefix) were already consumed.
   * Returns headers as a map with lowercased keys.
   */
  private Map<String, String> readHttpHeaders() throws IOException {
    final InputStream rawIn = socket.getInputStream();
    final Map<String, String> headers = new LinkedHashMap<>();
    final StringBuilder line = new StringBuilder();
    boolean firstLine = true;
    final int maxBytes = 8192;
    int bytesRead = 0;

    while (bytesRead < maxBytes) {
      final int b = rawIn.read();
      if (b == -1)
        break;
      bytesRead++;

      if (b == '\n') {
        final String l = line.toString().trim();
        line.setLength(0);

        if (l.isEmpty())
          break; // End of headers

        if (firstLine) {
          firstLine = false; // Skip request line (e.g., "/ HTTP/1.1")
        } else {
          final int colon = l.indexOf(':');
          if (colon > 0)
            headers.put(l.substring(0, colon).trim().toLowerCase(), l.substring(colon + 1).trim());
        }
      } else if (b != '\r') {
        line.append((char) b);
      }
    }

    return headers;
  }

  /**
   * Completes the WebSocket upgrade handshake. After this, the connection speaks WebSocket frames.
   * Echoes back Sec-WebSocket-Protocol if the client requested one (required by Neo4j Desktop).
   */
  private void completeWebSocketUpgrade(final Map<String, String> headers) throws IOException {
    final String key = headers.get("sec-websocket-key");
    if (key == null)
      throw new IOException("Missing Sec-WebSocket-Key header in WebSocket upgrade request");

    final String acceptKey = computeWebSocketAccept(key);
    final String protocol = headers.get("sec-websocket-protocol");

    final StringBuilder response = new StringBuilder();
    response.append("HTTP/1.1 101 Switching Protocols\r\n");
    response.append("Upgrade: websocket\r\n");
    response.append("Connection: Upgrade\r\n");
    response.append("Sec-WebSocket-Accept: ").append(acceptKey).append("\r\n");
    if (protocol != null && !protocol.isEmpty())
      response.append("Sec-WebSocket-Protocol: ").append(protocol).append("\r\n");
    response.append("\r\n");

    final OutputStream rawOut = socket.getOutputStream();
    rawOut.write(response.toString().getBytes(StandardCharsets.UTF_8));
    rawOut.flush();

    if (debug)
      LogManager.instance().log(this, Level.FINE, "BOLT WebSocket upgrade completed for %s (protocol=%s)",
          socket.getRemoteSocketAddress(), protocol != null ? protocol : "none");
  }

  /**
   * Computes the Sec-WebSocket-Accept value per RFC 6455.
   */
  private static String computeWebSocketAccept(final String key) {
    try {
      final MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
      final byte[] hash = sha1.digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.UTF_8));
      return Base64.getEncoder().encodeToString(hash);
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-1 not available", e);
    }
  }

  /**
   * Responds to a plain HTTP request on the Bolt port with a JSON error.
   * Headers have already been read by {@link #readHttpHeaders()}.
   */
  private void handleHttpOnBoltPort() throws IOException {
    final String address = getBoltAddress(socket.getLocalPort());
    final String body = "{\"error\":\"This is a Bolt connector. Connect using bolt://" + address + "\"}";

    final String httpResponse = "HTTP/1.1 400 Bad Request\r\n"
        + "Content-Type: application/json\r\n"
        + "Content-Length: " + body.getBytes(StandardCharsets.UTF_8).length + "\r\n"
        + "Access-Control-Allow-Origin: *\r\n"
        + "Access-Control-Allow-Methods: GET, OPTIONS\r\n"
        + "Access-Control-Allow-Headers: *\r\n"
        + "Connection: close\r\n"
        + "\r\n"
        + body;

    output.writeRaw(httpResponse.getBytes(StandardCharsets.UTF_8));

    if (debug)
      LogManager.instance().log(this, Level.FINE,
          "HTTP request on BOLT port from %s, responded with Bolt endpoint info for %s",
          socket.getRemoteSocketAddress(), address);
  }

  /**
   * Returns a Bolt-compatible host:port address for the current connection.
   * Handles IPv6 by using "localhost" for loopback addresses and bracketing otherwise.
   */
  private String getBoltAddress(final int port) {
    final InetAddress addr = socket.getLocalAddress();
    final String hostAddress = addr.getHostAddress();
    if (addr.isLoopbackAddress())
      return "localhost:" + port;
    if (hostAddress.contains(":"))
      return "[" + hostAddress + "]:" + port;
    return hostAddress + ":" + port;
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
      LogManager.instance().log(this, Level.FINE, "BOLT connection closed");
    }
  }

  // Bolt version encoding: [unused(8)][range(8)][minor(8)][major(8)]

  static int getMajorVersion(final int version) {
    return version & 0xFF;
  }

  static int getMinorVersion(final int version) {
    return (version >> 8) & 0xFF;
  }

  static int getVersionRange(final int version) {
    return (version >> 16) & 0xFF;
  }
}
