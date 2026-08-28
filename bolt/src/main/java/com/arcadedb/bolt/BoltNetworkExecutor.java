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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.network.PreAuthConnectionGate;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.CollectionUtils;

import javax.net.ssl.SSLSocket;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  // Supported protocol versions (in order of preference). Package-private so the negotiation unit
  // test asserts against the real advertised set rather than a drifting copy.
  // Encoding: [unused(8)][range(8)][minor(8)][major(8)] — major = value & 0xFF, minor = (value >> 8) & 0xFF
  static final int[] SUPPORTED_VERSIONS = {
      0x00000405, 0x00000305, 0x00000205, 0x00000105, 0x00000005, // v5.4, v5.3, v5.2, v5.1, v5.0
      0x00000404, 0x00000004, 0x00000003                          // v4.4, v4.0, v3.0
  };

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
  private volatile Socket           socket; // Reassigned to the SSLSocket once TLS negotiation completes
  private final BoltSslHelper       sslHelper;
  /**
   * The listener's permit for a connection that has not authenticated yet, handed back the moment it does -
   * or when it goes away without ever doing so (issue #6412).
   */
  private final PreAuthConnectionGate.Ticket preAuthTicket;
  private       BoltChunkedInput    input;
  private       BoltChunkedOutput   output;
  private final boolean             debug;
  private final BoltNetworkListener listener; // For notifying when connection closes

  /**
   * Ceiling on result streams held open at once by one connection. Every one of them pins an engine ResultSet
   * (cursors, pages) for as long as its transaction lives, and nothing in the protocol obliges a client ever to
   * consume one, so an unbounded map here is a resource leak a single authenticated session could drive. No real
   * driver holds more than a handful open.
   */
  private static final int MAX_OPEN_STREAMS = 1024;

  private State              state = State.DISCONNECTED;
  private int                protocolVersion;
  private ServerSecurityUser user;
  private Database           database;
  private String             databaseName;

  // Transaction state
  private boolean explicitTransaction = false;

  // GQL session state for this connection (SESSION SET/RESET/CLOSE parameters).
  private final BoltSession session = new BoltSession();

  /**
   * Result streams open on this connection, keyed by qid and iterated in the order they were opened.
   * <p>
   * BOLT 4.0+ allows several open streams inside one explicit transaction, told apart by the qid a RUN returns
   * and a PULL/DISCARD names (issue #6804). Outside a transaction there is at most one entry here.
   * <p>
   * Thread-safety: This class is designed to handle a single connection in a dedicated thread.
   * All state variables are accessed only by the executor thread and do not require synchronization.
   */
  private final Map<Long, BoltQueryStream> openStreams = new LinkedHashMap<>();

  /** Stream a PULL/DISCARD acts on when it carries no qid (or qid -1): the most recently opened one. */
  private BoltQueryStream currentStream;

  /** Next qid to hand out, numbered from 0 per explicit transaction as a Neo4j server does. */
  private long nextQid;

  public BoltNetworkExecutor(final ArcadeDBServer server, final Socket socket, final BoltNetworkListener listener) {
    this(server, socket, listener, null);
  }

  public BoltNetworkExecutor(final ArcadeDBServer server, final Socket socket, final BoltNetworkListener listener,
      final BoltSslHelper sslHelper) {
    this(server, socket, listener, sslHelper, null);
  }

  public BoltNetworkExecutor(final ArcadeDBServer server, final Socket socket, final BoltNetworkListener listener,
      final BoltSslHelper sslHelper, final PreAuthConnectionGate.Ticket preAuthTicket) {
    super("BOLT-" + socket.getRemoteSocketAddress());
    this.server = server;
    this.socket = socket;
    this.listener = listener;
    this.sslHelper = sslHelper;
    this.preAuthTicket = preAuthTicket;
    this.debug = GlobalConfiguration.BOLT_DEBUG.getValueAsBoolean();
    // NOTE: transport (TLS) negotiation and the socket I/O streams are intentionally set up in run(), on this
    // per-connection thread, so a slow/failed/hostile TLS handshake can never block the shared accept thread.
  }

  @Override
  public void run() {
    ProtocolContext.set("bolt");
    try {
      // Detect plaintext vs TLS and, if TLS, complete the handshake here (never on the listener accept thread).
      if (!negotiateTransport())
        return;

      state = State.NEGOTIATION;

      // Perform handshake. Bound by the same pre-auth read timeout negotiateTransport() armed (issue #5978):
      // a client that never completes the magic-bytes/version handshake must not hold this thread forever.
      try {
        if (!performHandshake())
          return;
      } catch (final EOFException | SocketException | SocketTimeoutException e) {
        if (debug) {
          LogManager.instance().log(this, Level.FINE, "BOLT connection closed during handshake: %s", e.getMessage());
        }
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
        } catch (final SocketTimeoutException e) {
          // No data received within the bounded pre-authentication window (issue #5978, mirroring #5912's
          // fix for the Redis wrapper): close instead of holding the connection thread open indefinitely.
          // Authenticated connections never reach here - their read timeout is lifted to infinite in
          // markAuthenticated().
          if (debug) {
            LogManager.instance().log(this, Level.FINE, "BOLT closing idle unauthenticated connection %s", socket.getRemoteSocketAddress());
          }
          break;
        } catch (final Exception e) {
          // Top-level safety net for unexpected dispatch/protocol failures. NeedRetryException (MVCC)
          // classification is handled where those conflicts actually arise - the RUN/PULL/COMMIT
          // query-execution handlers - so this fallback intentionally keeps the generic DATABASE_ERROR.
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
      ProtocolContext.clear();
      // Whatever ended the loop, the listener's permit must go back: a connection that dies without ever
      // authenticating would otherwise keep its slot for the life of the server (issue #6412).
      releasePreAuthTicket();
      cleanup();
    }
  }

  /**
   * Hands the listener's pre-authentication permit back. Idempotent, so authenticating and then terminating -
   * the normal life of a connection - releases exactly one permit.
   */
  private void releasePreAuthTicket() {
    if (preAuthTicket != null)
      preAuthTicket.release();
  }

  /**
   * Negotiates the transport for this connection on the per-connection thread: peeks the first bytes to tell
   * plaintext from TLS and, when TLS is used, completes the (blocking) handshake here. Running this off the
   * listener accept thread is what prevents a single slow, aborted or untrusted TLS handshake from wedging the
   * shared BOLT listener for all other clients (issue #5106).
   *
   * @return {@code true} if the transport is ready and the BOLT handshake can proceed; {@code false} if the
   * connection was rejected or closed (the caller must stop).
   */
  private boolean negotiateTransport() {
    try {
      Socket connectionSocket = socket;
      byte[] preReadBytes = null;

      // Bound transport detection, the BOLT handshake and the AUTH/HELLO-LOGON phase that follows it with a
      // read timeout (issue #5978, mirroring RedisNetworkExecutor's #5912 fix), so a stalled/hostile client
      // cannot hold this connection thread (and its file descriptors) open forever. Lifted to infinite once
      // authentication succeeds - see markAuthenticated() - and re-armed on LOGOFF - see markUnauthenticated().
      final int handshakeTimeout = GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.getValueAsInteger();
      if (handshakeTimeout > 0)
        socket.setSoTimeout(handshakeTimeout);

      if (sslHelper != null && sslHelper.getTlsMode() != BoltSslHelper.TlsMode.DISABLED) {
        final byte[] header = new byte[4];
        final InputStream rawIn = socket.getInputStream();
        int bytesRead = 0;
        while (bytesRead < 4) {
          final int n = rawIn.read(header, bytesRead, 4 - bytesRead);
          if (n == -1) {
            // Client closed before sending the 4 transport-detection bytes: abandon this connection cleanly
            // instead of re-reading a closed stream (the previous busy-spin that pinned a CPU core).
            return false;
          }
          bytesRead += n;
        }

        final boolean isTls = header[0] == 0x16 && header[1] == 0x03;

        if (isTls) {
          // Handshake happens here, on this per-connection thread. A failure throws and only affects us.
          final SSLSocket sslSocket = sslHelper.wrapWithTls(socket, header);
          this.socket = sslSocket; // route all subsequent I/O and cleanup through the TLS socket
          connectionSocket = sslSocket;
        } else if (sslHelper.getTlsMode() == BoltSslHelper.TlsMode.REQUIRED) {
          LogManager.instance().log(this, Level.WARNING,
              """
              BOLT rejecting non-TLS connection from %s (TLS is REQUIRED). \
              Configure the client to use bolt+s:// or bolt+ssc://""",
              socket.getRemoteSocketAddress());
          return false;
        } else {
          // OPTIONAL mode with a plaintext connection: replay the peeked bytes into the BOLT handshake.
          preReadBytes = header;
        }

        // Keep the pre-auth window armed through AUTH/HELLO-LOGON: a layered SSLSocket does not reliably
        // inherit the underlying socket's timeout, so re-apply it explicitly on whichever socket subsequent
        // reads actually use. Lifted to infinite only once authentication succeeds (markAuthenticated()).
        if (handshakeTimeout > 0)
          connectionSocket.setSoTimeout(handshakeTimeout);
      }

      final InputStream inputStream = preReadBytes != null
          ? new SequenceInputStream(new ByteArrayInputStream(preReadBytes), connectionSocket.getInputStream())
          : connectionSocket.getInputStream();
      this.input = new BoltChunkedInput(inputStream);
      this.output = new BoltChunkedOutput(connectionSocket.getOutputStream());
      return true;

    } catch (final Exception e) {
      // Any failure here (aborted/untrusted TLS handshake, I/O error, handshake timeout) is scoped to THIS
      // connection. Logging at FINE keeps a hostile client from flooding the log; the shared listener is fine.
      if (debug)
        LogManager.instance().log(this, Level.FINE, "BOLT transport negotiation failed: %s", e.getMessage());
      return false;
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

      if ("websocket".equalsIgnoreCase(upgrade)) {
        // WebSocket upgrade - Neo4j Desktop/Browser uses WebSocket transport for Bolt
        completeWebSocketUpgrade(headers);

        // Reinitialize I/O with WebSocket framing and read Bolt magic from WebSocket stream
        input = new BoltChunkedInput(
            new BoltWebSocketInputStream(socket.getInputStream(), GlobalConfiguration.BOLT_WEBSOCKET_MAX_FRAME_SIZE.getValueAsInteger()));
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
            """
                TLS/SSL connection attempted on BOLT port but TLS is disabled. \
                Configure arcadedb.bolt.ssl=OPTIONAL or REQUIRED to enable TLS, \
                or use bolt:// (unencrypted) on the client""");
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

    protocolVersion = selectVersion(clientVersions);

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
    case BoltMessage.TELEMETRY:
      // A FAILED connection must respond IGNORED to every request except RESET (Bolt state machine),
      // consistent with the other request handlers; otherwise acknowledge with SUCCESS.
      if (state == State.FAILED)
        sendIgnored();
      else
        sendSuccess(Map.of());
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

    if (deferAuthToLogon(protocolVersion, scheme, principal, credentials)) {
      // Bolt 5.1+ handshake: accept HELLO now, authenticate on the subsequent LOGON.
      sendSuccess(buildHelloSuccessMetadata());
      state = State.AUTHENTICATION;
      return;
    }

    // Try to authenticate
    if ("none".equals(scheme)) {
      // Explicit no-auth is always rejected.
      sendFailure(BoltException.AUTHENTICATION_ERROR, "Authentication required");
      state = State.FAILED;
      return;
    }
    // Covers "basic" with credentials, any other/missing scheme with credentials, and
    // the missing-scheme/missing-credentials case (authenticateUser null-checks and
    // rejects with "Missing credentials" rather than treating it as implicitly
    // authenticated). A legitimate Bolt 5.1+ HELLO with no auth fields never reaches this
    // point - the deferAuthToLogon() check above already routed it to await LOGON.
    if (!authenticateUser(principal, credentials)) {
      return;
    }

    sendSuccess(buildHelloSuccessMetadata());
    state = State.READY;
  }

  /**
   * Builds the HELLO success metadata shared by the authenticated-success path and the Bolt 5.1+
   * auth-deferral path. The "Neo4j" server prefix is used for compatibility with official Neo4j drivers.
   * Insertion order (server first, then connection_id) is significant for wire equality.
   */
  private Map<String, Object> buildHelloSuccessMetadata() {
    final Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("server", "Neo4j/5.26.0 compatible (ArcadeDB " + Constants.getRawVersion() + ")");
    metadata.put("connection_id", "bolt-" + Thread.currentThread().threadId());
    return metadata;
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
   * <p>
   * BOLT 5.1 lists LOGOFF as valid only from READY, which is precisely the state where no result stream and no
   * explicit transaction can be open. This handler used to be the one request handler with neither a state check
   * nor any cleanup: a LOGOFF sent mid-stream or mid-transaction answered SUCCESS and left the ResultSet and the
   * ArcadeDB transaction pinned on a connection the server had just stopped counting as authenticated. A
   * following HELLO/LOGON as a different user came back to READY with that transaction still open, and its
   * COMMIT then committed writes made before the user changed (issue #6803).
   */
  private void handleLogoff() throws IOException {
    if (state == State.FAILED) {
      sendIgnored();
      return;
    }

    if (state != State.READY) {
      sendFailure(BoltException.PROTOCOL_ERROR, "LOGOFF not expected in state: " + state);
      state = State.FAILED;
      return;
    }

    // Belt and braces: READY already implies no open stream and no open transaction, so nothing should be left
    // to release here. Doing it anyway is what keeps "the server considers this connection unauthenticated" from
    // ever meaning "and it still holds a transaction".
    closeAllStreams("LOGOFF");
    rollbackExplicitTransaction();

    user = null;
    markUnauthenticated();
    sendSuccess(Map.of());
    state = State.AUTHENTICATION;
  }

  /**
   * Rolls back the explicit transaction, if any, and forgets it. Never throws: it runs on the teardown paths,
   * where a rollback failure must not stop the rest of the teardown.
   */
  private void rollbackExplicitTransaction() {
    if (explicitTransaction && database != null) {
      try {
        database.rollback();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE, "Failed to roll back the open transaction during teardown", e);
      }
    }
    explicitTransaction = false;
  }

  /**
   * Selects the stream a PULL/DISCARD acts on. A qid of -1 - the default a driver sends when it omits the field
   * - means the most recently opened stream; any other value names one explicitly, which is how a client
   * interleaves several streams inside one transaction. A qid naming nothing fails the session, exactly as a
   * PULL with no result set behind it did before.
   * <p>
   * Outside an explicit transaction the qid is ignored altogether: only one stream can be open there (RUN is not
   * valid in STREAMING), no qid is ever published in an auto-commit RUN SUCCESS, and the BOLT docs are explicit
   * that qid does not apply to auto-commit. Honouring it would let a client that sent one anyway be answered
   * "No active result set for qid ..." for the stream it is plainly asking about.
   */
  private BoltQueryStream resolveStream(final long qid) throws IOException {
    final BoltQueryStream stream = qid < 0 || !explicitTransaction ? currentStream : openStreams.get(qid);
    if (stream == null) {
      sendFailure(BoltException.PROTOCOL_ERROR, qid < 0 ? "No active result set" : "No active result set for qid " + qid);
      state = State.FAILED;
      return null;
    }
    return stream;
  }

  /**
   * Registers a freshly executed query as an open stream and makes it the target of a qid-less PULL/DISCARD.
   */
  private void openStream(final BoltQueryStream stream) {
    openStreams.put(stream.qid, stream);
    currentStream = stream;
    ++nextQid;
  }

  /**
   * Completes one stream: releases it and, once nothing is left open, leaves STREAMING / TX_STREAMING. With
   * several streams open in one transaction the session stays in TX_STREAMING until the last one is drained or
   * discarded.
   */
  private void closeStream(final BoltQueryStream stream, final String phase) {
    stream.close(this, phase);
    openStreams.remove(stream.qid);

    if (currentStream == stream) {
      // Fall back to the newest stream still open, so a following qid-less PULL/DISCARD keeps meaning "the last
      // one opened". The scan is over a map that holds a handful of entries in the worst realistic case.
      currentStream = null;
      for (final BoltQueryStream open : openStreams.values())
        currentStream = open;
    }

    if (openStreams.isEmpty()) {
      state = explicitTransaction ? State.TX_READY : State.READY;
      if (!explicitTransaction)
        nextQid = 0; // an auto-commit stream is always qid 0: the numbering restarts per transaction
    }
  }

  /**
   * Releases every open result stream, for the teardown paths (RESET, LOGOFF, ROLLBACK, connection close) where
   * whatever the client left open has to go regardless of how many streams there were.
   */
  private void closeAllStreams(final String phase) {
    for (final BoltQueryStream stream : openStreams.values())
      stream.close(this, phase);
    openStreams.clear();
    currentStream = null;
    nextQid = 0;
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
    // Close every open result set
    closeAllStreams("RESET");

    // Rollback any open transaction
    rollbackExplicitTransaction();

    // NOTE: do not clear the GQL session parameters here. The Bolt RESET message is connection-level
    // housekeeping the driver sends when recycling a pooled connection, not a user request to reset GQL
    // session state; clearing here would drop SESSION SET parameters between auto-commit queries. GQL
    // session parameters are cleared only by an explicit SESSION RESET / SESSION CLOSE statement.

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

    // TX_STREAMING accepts RUN: BOLT 4.0+ lets a client hold several result streams open inside one explicit
    // transaction and tell them apart by qid, which is the entire reason that field exists (issue #6804). Any
    // driver configured with a fetch size smaller than the first query's row count leaves the first stream open,
    // so a second query in the same transaction used to be answered with a protocol error that failed the
    // session and lost the transaction. STREAMING deliberately does NOT accept RUN, matching the BOLT state
    // machine: outside a transaction there is nothing to multiplex onto.
    if (state != State.READY && state != State.TX_READY && state != State.TX_STREAMING) {
      sendFailure(BoltException.PROTOCOL_ERROR, "RUN not expected in state: " + state);
      state = State.FAILED;
      return;
    }

    if (openStreams.size() >= MAX_OPEN_STREAMS) {
      sendFailure(BoltException.PROTOCOL_ERROR,
          "Too many result streams open at once (max " + MAX_OPEN_STREAMS + "): consume or discard one first");
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

    final BoltQueryStream stream = new BoltQueryStream(nextQid);

    // Start timing for performance metrics
    stream.queryStartTime = System.nanoTime();

    // Ensure database is open (maps "system"/"neo4j" to default database). This also attaches the
    // connection's GQL session to the thread context, which the engine reads for SESSION statements.
    if (!ensureDatabase())
      return;

    // Intercept known system queries (CALL dbms.components(), SHOW DATABASES, etc.)
    if (handleSystemQuery(query, stream)) {
      openStream(stream);

      final Map<String, Object> metadata = new LinkedHashMap<>();
      metadata.put("fields", stream.fields);
      metadata.put("t_first", 0L);
      addQidMetadata(metadata, stream);
      sendSuccess(metadata);
      state = explicitTransaction ? State.TX_STREAMING : State.STREAMING;
      return;
    }

    try {
      // Determine if this is a write query using the query analyzer
      stream.writeOperation = isWriteQuery(query);

      // Detect EXPLAIN / PROFILE prefix so we can later surface the execution plan
      // in PULL SUCCESS metadata (consumed by Neo4j drivers as ResultSummary#plan / #profile).
      // The OpenCypher engine itself also strips the prefix, but we need to know which one
      // was present here to choose between record-streaming (PROFILE) and plan-only (EXPLAIN)
      // and to pick the correct metadata key.
      final String trimmedQuery = query == null ? "" : query.trim();
      final String upperQuery = trimmedQuery.toUpperCase();
      final boolean explainMode = upperQuery.startsWith("EXPLAIN ");
      final boolean profileMode = !explainMode && upperQuery.startsWith("PROFILE ");

      // Use command() for writes, query() for reads
      if (stream.writeOperation) {
        stream.resultSet = database.command("opencypher", query, params);
      } else {
        stream.resultSet = database.query("opencypher", query, params);
      }

      // Capture the plan from the engine. EXPLAIN returns ExplainResultSet (one synthetic row
      // exposing getExecutionPlan()); PROFILE returns InternalResultSet with setPlan() set.
      if (explainMode || profileMode) {
        stream.planMetadataKey = explainMode ? "plan" : "profile";
        stream.planMetadata = buildPlanMetadata(stream.resultSet, profileMode);
        if (explainMode) {
          // For EXPLAIN, the only "row" in the result set is the plan itself: drain it so that
          // the client sees zero records, matching Neo4j's EXPLAIN semantics.
          drainResultSet(stream.resultSet);
        }
      }

      stream.fields = extractFieldNames(stream.resultSet, stream);

      // The plan above is necessarily built before this query's column list is known (for EXPLAIN the rows have
      // to be drained first), so its identifiers are filled in here. Reading them from a connection-wide field
      // at build time, as this used to, meant the plan carried whatever the PREVIOUS query returned.
      if (stream.planMetadata != null)
        stream.planMetadata.put("identifiers", stream.fields);

      if (debug) {
        LogManager.instance().log(this, Level.FINE, "BOLT query fields=%s firstResult=%s", stream.fields,
            stream.firstResult != null ? stream.firstResult.toJSON() : "null");
      }

      openStream(stream);

      // Build success response with query metadata
      final Map<String, Object> metadata = new LinkedHashMap<>();
      metadata.put("fields", stream.fields);

      // Calculate time to first record if we already have one buffered
      if (stream.firstResult != null && stream.firstRecordTime > 0) {
        final long tFirstMs = (stream.firstRecordTime - stream.queryStartTime) / 1_000_000;
        metadata.put("t_first", tFirstMs);
      } else {
        metadata.put("t_first", 0L);
      }
      addQidMetadata(metadata, stream);

      sendSuccess(metadata);
      state = explicitTransaction ? State.TX_STREAMING : State.STREAMING;

    } catch (final CommandParsingException e) {
      stream.close(this, "RUN failure");
      final String parseMsg = e.getMessage() != null ? e.getMessage() : "Query parsing error";
      sendFailure(classifyParsingError(e), parseMsg);
      state = State.FAILED;
    } catch (final Exception e) {
      // MVCC conflicts (NeedRetryException) are expected under contention and auto-retried by the driver,
      // so log them at FINE to avoid flooding WARNING with normal, recoverable flow; genuine errors stay WARNING.
      stream.close(this, "RUN failure");
      LogManager.instance().log(this, isRetryableConflict(e) ? Level.FINE : Level.WARNING, "BOLT query error", e);
      final String errorMsg = e.getMessage() != null ? e.getMessage() : "Database error";
      sendFailure(classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR), errorMsg);
      state = State.FAILED;
    }
  }

  /**
   * Adds the stream's qid to a RUN SUCCESS, but only inside an explicit transaction: that is where a client may
   * open more than one stream and therefore needs to name them, and it matches what a Neo4j server sends.
   */
  private void addQidMetadata(final Map<String, Object> metadata, final BoltQueryStream stream) {
    if (explicitTransaction)
      metadata.put("qid", stream.qid);
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

    final BoltQueryStream stream = resolveStream(message.getQid());
    if (stream == null)
      return;

    try {
      final long n = message.getN();
      long count = 0;

      // Handle synthetic results (from system queries)
      if (stream.syntheticResults != null) {
        while (!stream.syntheticResults.isEmpty() && (n < 0 || count < n)) {
          sendRecord(stream.syntheticResults.remove(0));
          count++;
        }
      } else {
        // First, return the buffered first result if present
        if (stream.firstResult != null && (n < 0 || count < n)) {
          sendRecord(extractRecordValues(stream.firstResult, stream.fields));
          count++;
          stream.firstResult = null;
        }

        // Then continue with the rest of the result set
        while (stream.resultSet.hasNext() && (n < 0 || count < n)) {
          sendRecord(extractRecordValues(stream.resultSet.next(), stream.fields));
          count++;
        }
      }

      final boolean hasMore = stream.hasMore();

      // Build success metadata
      final Map<String, Object> metadata = new LinkedHashMap<>();
      if (!hasMore) {
        // Determine query type based on whether it performed writes
        // r=read, w=write (for simplicity, we use binary classification)
        metadata.put("type", stream.writeOperation ? "w" : "r");

        // Calculate time to last record
        final long tLastMs = (System.nanoTime() - stream.queryStartTime) / 1_000_000;
        metadata.put("t_last", tLastMs);

        // Surface execution plan from EXPLAIN / PROFILE (PME) so neo4j drivers populate
        // ResultSummary#plan() / #profile() instead of returning null.
        if (stream.planMetadata != null && stream.planMetadataKey != null)
          metadata.put(stream.planMetadataKey, stream.planMetadata);

        if (stream.resultSet != null) {
          final Optional<QueryStatistics> stats = stream.resultSet.getStatistics();
          if (stats.isPresent() && stats.get().containsUpdates())
            metadata.put("stats", BoltResultStats.toStatsMap(stats.get()));
        }

        // Leaves TX_STREAMING only once this connection has no other stream still open.
        closeStream(stream, "PULL completion");
      }
      metadata.put("has_more", hasMore);

      sendSuccess(metadata);

    } catch (final Exception e) {
      // MVCC conflicts (incl. those raised by an implicit auto-commit here) are expected and auto-retried
      // by the driver, so log at FINE to avoid flooding WARNING with recoverable flow; real errors stay WARNING.
      LogManager.instance().log(this, isRetryableConflict(e) ? Level.FINE : Level.WARNING, "BOLT PULL error", e);
      final String errorMsg = e.getMessage() != null ? e.getMessage() : "Error fetching records";
      sendFailure(classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR), errorMsg);
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

    final BoltQueryStream stream = resolveStream(discardMessage.getQid());
    if (stream == null)
      return;

    // Discard all remaining records
    Optional<QueryStatistics> stats = Optional.empty();
    if (stream.resultSet != null) {
      // Statistics are computed eagerly when the write is materialized in the query plan, so they
      // are valid to read before draining/closing the result set.
      stats = stream.resultSet.getStatistics();
      while (stream.resultSet.hasNext()) {
        stream.resultSet.next();
      }
    }

    final Map<String, Object> metadata = new LinkedHashMap<>();
    // Even on DISCARD we still surface the plan so EXPLAIN clients that DISCARD instead
    // of PULL still get ResultSummary#plan / #profile populated. Read before the stream is released,
    // which clears it.
    if (stream.planMetadata != null && stream.planMetadataKey != null)
      metadata.put(stream.planMetadataKey, stream.planMetadata);
    if (stats.isPresent() && stats.get().containsUpdates())
      metadata.put("stats", BoltResultStats.toStatsMap(stats.get()));
    metadata.put("has_more", false);

    // Leaves TX_STREAMING only once this connection has no other stream still open.
    closeStream(stream, "DISCARD");

    sendSuccess(metadata);
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
      nextQid = 0; // qids are numbered per explicit transaction

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
      sendFailure(classifyExecutionError(e, BoltErrorCodes.TRANSACTION_ERROR), message);
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
      closeAllStreams("ROLLBACK");
      if (database != null) {
        database.rollback();
      }
      explicitTransaction = false;

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

    if (state != State.READY) {
      // ROUTE enumerates every peer's Bolt endpoint, so it must not run for an unauthenticated caller.
      // Require an authenticated (READY) session, matching the other request handlers. A Bolt driver
      // always sends ROUTE after HELLO/LOGON, so this does not affect legitimate routing.
      sendFailure(BoltException.PROTOCOL_ERROR, "ROUTE not expected in state: " + state);
      state = State.FAILED;
      return;
    }

    final Map<String, Object> rt = new LinkedHashMap<>();
    rt.put("ttl", GlobalConfiguration.BOLT_ROUTING_TTL.getValueAsLong());
    rt.put("db", message.getDatabase() != null ? message.getDatabase() : databaseName);

    final List<Map<String, Object>> servers = new ArrayList<>();

    final HAServerPlugin ha = server.getHA();
    final HAServerPlugin.RoutingTable table = ha != null ?
        ha.getRoutingTable(HAServerPlugin.ROUTING_PROTOCOL.BOLT) : null;

    if (table != null) {
      // HA cluster with a known leader: the leader is the writer and a router, followers are readers and
      // routers. Writer and readers come from one leader snapshot, so they cannot disagree about the leader.
      final String writer = table.writer();
      final List<String> readers = table.readers();

      final List<String> routers = new ArrayList<>();
      routers.add(writer);
      routers.addAll(readers);

      servers.add(roleEntry(List.of(writer), "WRITE"));
      servers.add(roleEntry(readers.isEmpty() ? List.of(writer) : readers, "READ"));
      servers.add(roleEntry(routers, "ROUTE"));
    } else {
      // No usable routing table. Advertise this node using the actual bound Bolt port of this connection
      // rather than the global default.
      final String address = getBoltAddress(socket.getLocalPort());
      if (ha != null) {
        // HA is active but the cluster cannot name a writer to advertise: either no leader is known yet
        // (mid-election), or the leader's address cannot be told apart from a follower's because the nodes
        // share a host and no bolt: port was declared (issue #6183). Either way this node goes out as reader
        // and router only - never writer, since it may be a follower. The driver keeps reading and re-routes
        // after the TTL, receiving a writer once the cluster can name one, instead of sending a write to a
        // follower and getting an error.
        servers.add(roleEntry(List.of(address), "READ"));
        servers.add(roleEntry(List.of(address), "ROUTE"));
      } else {
        // True single-node deployment: this node is writer, reader, and router.
        servers.add(roleEntry(List.of(address), "WRITE"));
        servers.add(roleEntry(List.of(address), "READ"));
        servers.add(roleEntry(List.of(address), "ROUTE"));
      }
    }

    rt.put("servers", servers);

    sendSuccess(CollectionUtils.singletonMap("rt", rt));
  }

  /**
   * Builds a single ROUTE routing-table server entry pairing a list of client-reachable addresses with
   * a Bolt routing role (WRITE, READ, or ROUTE).
   */
  private static Map<String, Object> roleEntry(final List<String> addresses, final String role) {
    final Map<String, Object> entry = new LinkedHashMap<>();
    entry.put("addresses", addresses);
    entry.put("role", role);
    return entry;
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
        // Update current user on the existing context to handle LOGOFF/LOGON re-authentication
        // on the same connection without disrupting any open transactions.
        if (user != null) {
          final DatabaseContext.DatabaseContextTL ctx =
              DatabaseContext.INSTANCE.getContextIfExists(((DatabaseInternal) database).getDatabasePath());
          if (ctx != null) {
            ctx.setCurrentUser(user.getDatabaseUser(database));
            // Attach this connection's GQL session so SESSION statements and param merging can reach it.
            ctx.setQuerySession(session);
          }
        }
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
      if (user != null) {
        final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.init((DatabaseInternal) database);
        ctx.setCurrentUser(user.getDatabaseUser(database));
        // Attach this connection's GQL session so SESSION statements and param merging can reach it.
        ctx.setQuerySession(session);
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
   * Returns true if the query was handled as a system query - populating {@code stream} with the synthetic
   * fields and rows to serve - false if it should be executed normally.
   */
  private boolean handleSystemQuery(final String query, final BoltQueryStream stream) throws IOException {
    final String normalized = BoltSystemProcedures.normalize(query);

    if (normalized.contains("dbms.components")) {
      // CALL dbms.components() - returns server version info
      stream.fields = List.of("name", "versions", "edition");
      stream.syntheticResults = new ArrayList<>();
      stream.syntheticResults.add(List.of("Neo4j Kernel", List.of("5.26.0"), "community"));
      return true;

    } else if (normalized.startsWith("show database") || normalized.contains("dbms.showdatabase")
        || normalized.contains("dbms.listdatabases")) {
      // SHOW DATABASES or CALL dbms.listDatabases()
      stream.fields = List.of("name", "type", "aliases", "access", "address", "role",
          "writer", "requestedStatus", "currentStatus", "statusMessage", "default", "home",
          "constituents");
      stream.syntheticResults = new ArrayList<>();
      for (final String dbName : server.getDatabaseNames()) {
        stream.syntheticResults.add(List.of(dbName, "standard", List.of(), "read-write",
            getBoltAddress(GlobalConfiguration.BOLT_PORT.getValueAsInteger()), "primary",
            true, "online", "online", "", dbName.equals(database != null ? database.getName() : ""), false,
            List.of()));
      }
      // Also add the virtual "system" database entry
      stream.syntheticResults.add(List.of("system", "system", List.of(), "read-write",
          getBoltAddress(GlobalConfiguration.BOLT_PORT.getValueAsInteger()), "primary",
          false, "online", "online", "", false, false, List.of()));
      return true;

    } else if (normalized.contains("show current user") || normalized.contains("dbms.showcurrentuser")) {
      // SHOW CURRENT USER or CALL dbms.showCurrentUser()
      stream.fields = List.of("user", "roles", "passwordChangeRequired", "suspended", "home");
      stream.syntheticResults = new ArrayList<>();
      final List<Object> userRecord = new ArrayList<>();
      userRecord.add(user != null ? user.getName() : "anonymous");
      userRecord.add(List.of("admin"));
      userRecord.add(false);
      userRecord.add(false);
      userRecord.add(null); // home database (null = use default)
      stream.syntheticResults.add(userRecord);
      return true;

    } else if (normalized.contains("dbms.info")) {
      // CALL dbms.info() - returns basic server info
      stream.fields = List.of("id", "name", "creationDate");
      stream.syntheticResults = new ArrayList<>();
      stream.syntheticResults.add(List.of("arcadedb-" + server.getServerName(), server.getServerName(), ""));
      return true;

    } else if (normalized.contains("db.ping")) {
      // CALL db.ping() - health check
      stream.fields = List.of("success");
      stream.syntheticResults = new ArrayList<>();
      stream.syntheticResults.add(List.of(true));
      return true;

    } else if (normalized.contains("dbms.clientconfig")) {
      // CALL dbms.clientConfig() - client configuration
      stream.fields = List.of("name", "value");
      stream.syntheticResults = new ArrayList<>();
      return true;

    } else if (normalized.startsWith("show procedure")) {
      // SHOW PROCEDURES YIELD * - return empty list
      stream.fields = List.of("name", "description", "mode", "worksOnSystem", "argumentDescription",
          "returnDescription", "admin", "option");
      stream.syntheticResults = new ArrayList<>();
      return true;

    } else if (normalized.startsWith("show function")) {
      // SHOW FUNCTIONS YIELD * - return empty list
      stream.fields = List.of("name", "category", "description", "isBuiltIn", "argumentDescription",
          "returnDescription", "aggregating");
      stream.syntheticResults = new ArrayList<>();
      return true;

    } else if (BoltSystemProcedures.isSchemaProcedureQuery(normalized)) {
      // CALL db.labels() / db.relationshipTypes() / db.propertyKeys(), plus the combined UNION form Neo4j
      // Desktop sends. Answered from CypherProcedureRegistry, so the Bolt wire and the native Cypher CALL
      // path run the very same procedure and cannot drift apart (issue #6151). A null answer means the call
      // is not ours to serve - it carries arguments the registry entries do not accept - so it falls through
      // to the engine, which reports the same arity error it reports for any other client.
      final BoltSystemProcedures.Served served = BoltSystemProcedures.serveSchemaProcedure(database, normalized);
      if (served == null)
        return false;
      stream.fields = served.fields();
      stream.syntheticResults = served.rows();
      return true;

    } else if (normalized.startsWith("show index")
        || normalized.startsWith("show all index")
        || normalized.startsWith("show range index")
        || normalized.startsWith("show text index")
        || normalized.startsWith("show point index")
        || normalized.startsWith("show lookup index")
        || normalized.startsWith("show fulltext index")
        || normalized.startsWith("show vector index")
        || normalized.startsWith("show sparse_vector index")) {
      // SHOW INDEXES / SHOW ... INDEXES - list indexes from ArcadeDB schema
      stream.fields = List.of("id", "name", "state", "populationPercent", "type", "entityType",
          "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "lastRead", "readCount");
      stream.syntheticResults = buildShowIndexesResults(normalized);
      return true;

    } else if (normalized.startsWith("show constraint")
        || normalized.startsWith("show all constraint")
        || normalized.startsWith("show unique constraint")
        || normalized.startsWith("show uniqueness constraint")
        || normalized.startsWith("show exist constraint")
        || normalized.startsWith("show existence constraint")
        || normalized.startsWith("show node exist constraint")
        || normalized.startsWith("show node existence constraint")
        || normalized.startsWith("show node key constraint")
        || normalized.startsWith("show relationship exist constraint")
        || normalized.startsWith("show relationship existence constraint")
        || normalized.startsWith("show relationship key constraint")
        || normalized.startsWith("show rel exist constraint")
        || normalized.startsWith("show rel key constraint")
        || normalized.startsWith("show key constraint")
        || normalized.startsWith("show property type constraint")
        || normalized.startsWith("show node property type constraint")
        || normalized.startsWith("show relationship property type constraint")) {
      // SHOW CONSTRAINTS / SHOW ... CONSTRAINTS - list constraints from ArcadeDB schema
      stream.fields = List.of("id", "name", "type", "entityType", "labelsOrTypes", "properties",
          "ownedIndex", "propertyType");
      stream.syntheticResults = buildShowConstraintsResults(normalized);
      return true;

    } else if (normalized.contains("dbms.licenseagreementdetails")) {
      // CALL dbms.licenseAgreementDetails() - return empty/default
      stream.fields = List.of("name", "status", "version");
      stream.syntheticResults = new ArrayList<>();
      stream.syntheticResults.add(List.of("ArcadeDB", "active", "community"));
      return true;
    }

    return false;
  }

  /**
   * Builds the SHOW INDEXES result rows by iterating the ArcadeDB schema.
   * <p>
   * Each row mirrors Neo4j's SHOW INDEXES output shape so the Neo4j driver can parse it.
   * The {@code filter} argument is the normalized query text and is used to honor typed
   * variants like {@code SHOW VECTOR INDEXES}, {@code SHOW FULLTEXT INDEXES}, etc.
   */
  private List<List<Object>> buildShowIndexesResults(final String filter) {
    final List<List<Object>> rows = new ArrayList<>();
    if (database == null)
      return rows;

    final Schema schema = database.getSchema();
    final Set<String> visited = new HashSet<>();
    int idSeq = 1;

    for (final DocumentType type : schema.getTypes()) {
      if (type.getName().contains("~"))
        continue;

      final String entityType = type instanceof EdgeType ? "RELATIONSHIP" : "NODE";
      for (final TypeIndex typeIndex : type.getAllIndexes(false)) {
        if (!visited.add(typeIndex.getName()))
          continue;

        final String neoType = mapIndexTypeToNeo4j(typeIndex.getType());
        if (!indexTypeMatchesFilter(filter, neoType))
          continue;

        final String owningConstraint = typeIndex.isUnique() ? typeIndex.getName() : null;
        final List<Object> row = new ArrayList<>(12);
        row.add((long) idSeq++);
        row.add(typeIndex.getName());
        row.add("ONLINE");
        row.add(100.0);
        row.add(neoType);
        row.add(entityType);
        row.add(List.of(type.getName()));
        row.add(new ArrayList<>(typeIndex.getPropertyNames()));
        row.add(mapIndexTypeToProvider(typeIndex.getType()));
        row.add(owningConstraint);
        row.add(null);
        row.add(null);
        rows.add(row);
      }
    }
    return rows;
  }

  /**
   * Builds the SHOW CONSTRAINTS result rows by iterating the ArcadeDB schema.
   * <p>
   * ArcadeDB models constraints on the {@link Property} object: {@code mandatory},
   * {@code notNull}, and indirectly unique via a unique {@link Index}. This method
   * maps those to Neo4j constraint types so the Neo4j driver can parse the result.
   */
  private List<List<Object>> buildShowConstraintsResults(final String filter) {
    final List<List<Object>> rows = new ArrayList<>();
    if (database == null)
      return rows;

    final Schema schema = database.getSchema();
    int idSeq = 1;

    for (final DocumentType type : schema.getTypes()) {
      if (type.getName().contains("~"))
        continue;

      final boolean isEdge = type instanceof EdgeType;
      final String entityType = isEdge ? "RELATIONSHIP" : "NODE";

      // Uniqueness constraints: derived from unique indexes.
      for (final TypeIndex typeIndex : type.getAllIndexes(false)) {
        if (!typeIndex.isUnique())
          continue;

        final String constraintType = isEdge ? "RELATIONSHIP_UNIQUENESS" : "UNIQUENESS";
        if (!constraintTypeMatchesFilter(filter, constraintType))
          continue;

        final List<Object> row = new ArrayList<>(8);
        row.add((long) idSeq++);
        row.add(typeIndex.getName());
        row.add(constraintType);
        row.add(entityType);
        row.add(List.of(type.getName()));
        row.add(new ArrayList<>(typeIndex.getPropertyNames()));
        row.add(typeIndex.getName());
        row.add(null);
        rows.add(row);
      }

      // Property existence (mandatory / notNull) and property type constraints.
      for (final Property property : type.getProperties()) {
        final boolean mandatory = property.isMandatory();
        final boolean notNull = property.isNotNull();

        if (mandatory || notNull) {
          final String constraintType = isEdge ? "RELATIONSHIP_PROPERTY_EXISTENCE" : "NODE_PROPERTY_EXISTENCE";
          if (constraintTypeMatchesFilter(filter, constraintType)) {
            final List<Object> row = new ArrayList<>(8);
            row.add((long) idSeq++);
            row.add(buildConstraintName(type.getName(), property.getName(), "existence"));
            row.add(constraintType);
            row.add(entityType);
            row.add(List.of(type.getName()));
            row.add(List.of(property.getName()));
            row.add(null);
            row.add(null);
            rows.add(row);
          }
        }

        if (property.getType() != null) {
          final String constraintType = isEdge ? "RELATIONSHIP_PROPERTY_TYPE" : "NODE_PROPERTY_TYPE";
          if (constraintTypeMatchesFilter(filter, constraintType)) {
            final List<Object> row = new ArrayList<>(8);
            row.add((long) idSeq++);
            row.add(buildConstraintName(type.getName(), property.getName(), "type"));
            row.add(constraintType);
            row.add(entityType);
            row.add(List.of(type.getName()));
            row.add(List.of(property.getName()));
            row.add(null);
            row.add(property.getType().name());
            rows.add(row);
          }
        }
      }
    }
    return rows;
  }

  private static String mapIndexTypeToNeo4j(final Schema.INDEX_TYPE type) {
    if (type == null)
      return "RANGE";
    return switch (type) {
      case LSM_TREE -> "RANGE";
      case HASH -> "HASH";
      case FULL_TEXT -> "FULLTEXT";
      case LSM_VECTOR -> "VECTOR";
      case LSM_SPARSE_VECTOR -> "SPARSE_VECTOR";
      case GEOSPATIAL -> "POINT";
    };
  }

  private static String mapIndexTypeToProvider(final Schema.INDEX_TYPE type) {
    if (type == null)
      return "range-1.0";
    return switch (type) {
      case LSM_TREE -> "range-1.0";
      case HASH -> "hash-1.0";
      case FULL_TEXT -> "fulltext-1.0";
      case LSM_VECTOR -> "vector-2.0";
      case LSM_SPARSE_VECTOR -> "sparse-vector-1.0";
      case GEOSPATIAL -> "point-1.0";
    };
  }

  private static boolean indexTypeMatchesFilter(final String filter, final String neoType) {
    if (filter.startsWith("show range index"))
      return "RANGE".equals(neoType);
    if (filter.startsWith("show text index"))
      return "TEXT".equals(neoType);
    if (filter.startsWith("show point index"))
      return "POINT".equals(neoType);
    if (filter.startsWith("show lookup index"))
      return "LOOKUP".equals(neoType);
    if (filter.startsWith("show fulltext index"))
      return "FULLTEXT".equals(neoType);
    if (filter.startsWith("show vector index"))
      return "VECTOR".equals(neoType);
    if (filter.startsWith("show sparse_vector index"))
      return "SPARSE_VECTOR".equals(neoType);
    return true;
  }

  private static boolean constraintTypeMatchesFilter(final String filter, final String neoType) {
    if (filter.startsWith("show unique constraint") || filter.startsWith("show uniqueness constraint"))
      return neoType.endsWith("UNIQUENESS");
    if (filter.startsWith("show node exist") || filter.startsWith("show node existence"))
      return "NODE_PROPERTY_EXISTENCE".equals(neoType);
    if (filter.startsWith("show relationship exist") || filter.startsWith("show relationship existence")
        || filter.startsWith("show rel exist"))
      return "RELATIONSHIP_PROPERTY_EXISTENCE".equals(neoType);
    if (filter.startsWith("show exist constraint") || filter.startsWith("show existence constraint"))
      return neoType.endsWith("PROPERTY_EXISTENCE");
    if (filter.startsWith("show key constraint") || filter.startsWith("show node key constraint")
        || filter.startsWith("show relationship key constraint") || filter.startsWith("show rel key constraint"))
      return neoType.endsWith("_KEY");
    if (filter.startsWith("show node property type"))
      return "NODE_PROPERTY_TYPE".equals(neoType);
    if (filter.startsWith("show relationship property type"))
      return "RELATIONSHIP_PROPERTY_TYPE".equals(neoType);
    if (filter.startsWith("show property type constraint"))
      return neoType.endsWith("PROPERTY_TYPE");
    return true;
  }

  private static String buildConstraintName(final String typeName, final String propertyName, final String kind) {
    return "constraint_" + typeName + "_" + propertyName + "_" + kind;
  }

  /**
   * Extract field names from result set by peeking at the first result.
   * The first result is buffered and will be returned first during PULL.
   * <p>
   * For single-element results (e.g., RETURN n), the projection name is stored
   * in metadata by FinalProjectionStep and used here to preserve field names.
   */
  private List<String> extractFieldNames(final ResultSet resultSet, final BoltQueryStream stream) {
    if (resultSet == null) {
      return List.of();
    }

    // Peek at first result to get field names
    if (resultSet.hasNext()) {
      stream.firstResult = resultSet.next();
      stream.firstRecordTime = System.nanoTime(); // Capture time when first record is available

      // Check if this is an unwrapped element with a projection name in metadata
      // This happens for queries like "MATCH (n) RETURN n" where the vertex is
      // returned directly but we need to preserve the field name "n" for Bolt protocol
      if (stream.firstResult.isElement()) {
        final Object projectionName = stream.firstResult.getMetadata(PROJECTION_NAME_METADATA);
        if (projectionName instanceof String name) {
          return List.of(name);
        }
      }

      final Set<String> propertyNames = stream.firstResult.getPropertyNames();
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
  private List<Object> extractRecordValues(final Result result, final List<String> fields) {
    final List<Object> values = new ArrayList<>();

    // Check if this is an unwrapped element result
    // (single vertex/edge returned directly from RETURN clause)
    if (result.isElement() && result.getMetadata(PROJECTION_NAME_METADATA) != null) {
      // Return the element as a single value
      values.add(BoltStructureMapper.toPackStreamValue(result.getElement().orElse(null)));
    } else {
      // Standard projection result - extract each field
      for (final String field : fields) {
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
    if (query == null || query.isEmpty())
      return false;

    try {
      // Use the statement cache directly to avoid creating an AnalyzedQuery wrapper object.
      // The statement cache returns the parsed CypherStatement with isReadOnly() already computed.
      return !((DatabaseInternal) database).getCypherStatementCache().isIdempotent(query);
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
   * Build a Bolt plan map (matches the Plan / ProfilePlan structure expected by
   * {@code org.neo4j.driver.summary.ResultSummary#plan()} / {@code #profile()}).
   * The map has keys: {@code operatorType}, {@code identifiers}, {@code args},
   * and {@code children}. We do not currently produce a true operator tree from
   * the OpenCypher engine output: instead we synthesize a single root node and
   * embed the textual plan plus the column list under {@code args}, which is
   * what every Neo4j driver consumes verbatim.
   */
  private Map<String, Object> buildPlanMetadata(final ResultSet resultSet, final boolean profileMode) {
    if (resultSet == null)
      return null;

    final ExecutionPlan plan = resultSet.getExecutionPlan().orElse(null);
    if (plan == null)
      return null;

    final Map<String, Object> args = new LinkedHashMap<>();
    final String prettyPrint = plan.prettyPrint(0, 2);
    if (prettyPrint != null && !prettyPrint.isEmpty())
      args.put("string-representation", prettyPrint);

    if (profileMode) {
      // PROFILE plans expose per-step counters via ExecutionStep; surface a flat list
      // alongside the textual representation so DBaaS-style introspection still works.
      final List<ExecutionStep> steps = plan.getSteps();
      if (steps != null && !steps.isEmpty()) {
        final List<String> stepNames = new ArrayList<>(steps.size());
        for (final ExecutionStep step : steps)
          stepNames.add(step.getClass().getSimpleName());
        args.put("steps", stepNames);
      }
    }

    final Map<String, Object> root = new LinkedHashMap<>();
    root.put("operatorType", profileMode ? "ArcadeDB.OpenCypher.ProfilePlan" : "ArcadeDB.OpenCypher.Plan");
    // Placeholder: the caller overwrites this with the query's own column list once it is known (the map keeps
    // the key's position). The plan has to be captured before the EXPLAIN rows are drained, i.e. before the
    // columns can be read off the result set.
    root.put("identifiers", List.<String>of());
    root.put("args", args);
    root.put("children", List.<Map<String, Object>>of());

    if (profileMode) {
      // ProfilePlan inherits Plan and adds dbHits/rows/pageCacheHits/etc. We do not yet
      // collect per-operator stats, so report 0 instead of leaving the fields absent —
      // some drivers (e.g. neo4j-go-driver) check field presence and would otherwise
      // surface "no profile" even though we returned one.
      root.put("dbHits", 0L);
      root.put("rows", 0L);
      root.put("pageCacheMisses", 0L);
      root.put("pageCacheHits", 0L);
      root.put("pageCacheHitRatio", 0.0d);
      root.put("time", 0L);
    }
    return root;
  }

  /**
   * Drain (without sending) all remaining rows of the supplied result set so that
   * subsequent {@code hasNext()} returns false. Does not close: the existing PULL
   * completion path closes the result set after streaming. Used for EXPLAIN, where
   * the engine emits a synthetic row carrying the plan that the Bolt client should
   * not see as a record (Neo4j EXPLAIN returns zero records).
   */
  private void drainResultSet(final ResultSet resultSet) {
    if (resultSet == null)
      return;
    try {
      while (resultSet.hasNext())
        resultSet.next();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Failed to drain ResultSet for EXPLAIN", e);
    }
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
      markAuthenticated();
      return true;
    } catch (final ServerSecurityException e) {
      // Sanitize error message to avoid information disclosure
      sendFailure(BoltException.AUTHENTICATION_ERROR, "Authentication failed");
      state = State.FAILED;
      return false;
    }
  }

  /**
   * Lifts the pre-authentication read timeout (issue #5978, mirroring RedisNetworkExecutor.markAuthenticated
   * from #5912): an authenticated BOLT client is expected to keep a long-lived, often idle connection open
   * between requests, so the bounded handshake/auth window armed by {@link #negotiateTransport()} must not
   * keep applying past a successful HELLO/LOGON.
   */
  private void markAuthenticated() {
    releasePreAuthTicket();
    try {
      socket.setSoTimeout(0);
    } catch (final SocketException e) {
      // setSoTimeout() only throws on an already-broken/closed socket: there is nothing left to hold open in
      // that case, so logging and moving on - rather than failing the whole authentication - is safe. The
      // connection dies on its next read/write either way.
      LogManager.instance().log(this, Level.FINE, "BOLT unable to lift the idle read timeout after authentication", e);
    }
  }

  /**
   * (Re-)arms the bounded pre-authentication read timeout (issue #5978, mirroring
   * RedisNetworkExecutor.markUnauthenticated from #5912): a connection that authenticated once has its
   * timeout lifted to infinite by {@link #markAuthenticated()}. If it then sends LOGOFF, {@code user} goes
   * back to null - and without this, the infinite timeout would stay in place, breaking the invariant that
   * "unauthenticated" always implies the bounded handshake timeout applies.
   */
  private void markUnauthenticated() {
    final int handshakeTimeout = GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.getValueAsInteger();
    try {
      socket.setSoTimeout(Math.max(handshakeTimeout, 0));
    } catch (final SocketException e) {
      // As in markAuthenticated(): setSoTimeout() only throws on an already-broken/closed socket, so there is
      // no live connection left for the un-restored timeout to matter on.
      LogManager.instance().log(this, Level.FINE, "BOLT unable to restore the idle read timeout after LOGOFF", e);
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
   * Classify a query/transaction execution error into a Bolt status code. ArcadeDB's
   * optimistic-concurrency conflicts ({@link NeedRetryException}, e.g. a page-version
   * {@code ConcurrentModificationException} or a {@code LockTimeoutException}) map to a Neo4j
   * transient status so managed-transaction drivers auto-retry; an {@link ArithmeticErrorException}
   * (64-bit overflow, division by zero) maps to Neo4j's ArithmeticError so a driver reports the caller's
   * values rather than a server fault (issue #5602); anything else keeps the given default.
   */
  static String classifyExecutionError(final Throwable error, final String defaultCode) {
    if (isRetryableConflict(error))
      return BoltErrorCodes.TRANSIENT_CONFLICT_ERROR;
    return isArithmeticError(error) ? BoltErrorCodes.ARITHMETIC_ERROR : defaultCode;
  }

  /**
   * Whether the error (or any wrapped cause) is an arithmetic error. Walks the chain for the same reason
   * {@link #isRetryableConflict} does: the exception reaches here wrapped by the auto-commit transaction wrapper,
   * and carries the JDK {@code ArithmeticException} it came from as its own cause.
   */
  static boolean isArithmeticError(final Throwable error) {
    return CauseChain.contains(error, ArithmeticErrorException.class);
  }

  /**
   * Classify a query-parsing error into a Bolt status code. {@link CommandParameterMissingException} marks a
   * statement whose text is fine but whose {@code $parameter} the client never bound, and Neo4j gives that
   * its own ParameterMissing title - keep it distinct so a driver can tell "fix the query" from "send the
   * value". {@link CommandSemanticException} marks a statement that parsed correctly but violates a semantic
   * rule (e.g. an undefined variable), so it maps to Neo4j's SemanticError; every other
   * {@link CommandParsingException} is a genuine syntax error.
   */
  static String classifyParsingError(final CommandParsingException error) {
    if (error instanceof CommandParameterMissingException)
      return BoltErrorCodes.PARAMETER_MISSING_ERROR;
    return error instanceof CommandSemanticException ? BoltErrorCodes.SEMANTIC_ERROR : BoltErrorCodes.SYNTAX_ERROR;
  }

  /**
   * Whether the error (or any wrapped cause) is one of ArcadeDB's optimistic-concurrency conflicts
   * ({@link NeedRetryException}). Such conflicts are expected under contention and auto-retried by the
   * driver, so callers both classify them as transient and log them at a lower level.
   */
  static boolean isRetryableConflict(final Throwable error) {
    return CauseChain.contains(error, NeedRetryException.class);
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

    final PackStreamWriter writer = new PackStreamWriter().boltMajorVersion(getMajorVersion(protocolVersion));
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
    closeAllStreams("connection close");
    rollbackExplicitTransaction();

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

  /**
   * Select the highest-preference server version compatible with the client's proposals, or 0 if none match.
   * Client proposals are tried in order; for each, the range means the client supports minor versions from
   * (minor - range) up to minor inclusive for that major. A zero entry is trailing padding and stops the scan.
   * Pure function over {@link #SUPPORTED_VERSIONS} so the negotiation logic is exercised directly by tests.
   */
  static int selectVersion(final int[] clientVersions) {
    for (final int clientVersion : clientVersions) {
      if (clientVersion == 0)
        break;

      final int clientMajor = getMajorVersion(clientVersion);
      final int clientMinor = getMinorVersion(clientVersion);
      final int clientRange = getVersionRange(clientVersion);

      for (final int supportedVersion : SUPPORTED_VERSIONS) {
        final int serverMajor = getMajorVersion(supportedVersion);
        final int serverMinor = getMinorVersion(supportedVersion);

        if (clientMajor == serverMajor && serverMinor <= clientMinor && serverMinor >= clientMinor - clientRange)
          return supportedVersion;
      }
    }
    return 0;
  }

  /**
   * A Bolt 5.1+ HELLO carries no authentication - the driver authenticates with a separate LOGON.
   * Returns true only when the negotiated version is >= 5.1 and the HELLO omits all auth fields, so
   * such a HELLO is accepted (awaiting LOGON) instead of being rejected as "missing credentials".
   * Pre-5.1 keeps HELLO-embedded auth; an explicit scheme (incl. "none") is never a deferral.
   */
  static boolean deferAuthToLogon(final int protocolVersion, final String scheme, final String principal,
      final String credentials) {
    final int major = getMajorVersion(protocolVersion);
    final int minor = getMinorVersion(protocolVersion);
    // Lexicographic >= 5.1 so a higher major with minor 0 (e.g. a hypothetical 6.0) still defers,
    // rather than being excluded by an independent minor >= 1 test.
    final boolean atLeast51 = major > 5 || (major == 5 && minor >= 1);
    return atLeast51 && scheme == null && principal == null && credentials == null;
  }
}
