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
package com.arcadedb.postgres;

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.database.QueryMetricsRecorder;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.Limit;
import com.arcadedb.query.sql.parser.MatchStatement;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.monitor.QueryProfile;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.network.PreAuthConnectionGate;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.StringUtils;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.arcadedb.schema.Property.CAT_PROPERTY;
import static com.arcadedb.schema.Property.IN_PROPERTY;
import static com.arcadedb.schema.Property.OUT_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;

/**
 * Postgres Reference for Protocol Messages: https://www.postgresql.org/docs/9.6/protocol-message-formats.html
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostgresNetworkExecutor extends Thread {
  public enum ERROR_SEVERITY {FATAL, ERROR}

  public static final String PG_SERVER_VERSION = "12.0";

  private static final int                                            BUFFER_LENGTH    = 32 * 1024;
  /**
   * The cap PostgreSQL itself puts on a startup packet ({@code PQ_STARTUP_MSG_LIMIT} in {@code pqcomm.c}).
   */
  private static final int                                            MAX_STARTUP_MESSAGE_LENGTH = 10000;
  private static final Map<Long, Pair<Long, PostgresNetworkExecutor>> ACTIVE_SESSIONS  = new ConcurrentHashMap<>();
  /** Bind-message parameter length denoting a NULL value (wire value -1, read unsigned). */
  private static final long                                           NULL_PARAM_LENGTH = 0xFFFFFFFFL;
  private static final Object[]                                       NO_PARAMETERS     = new Object[0];
  /** Case-insensitive {@code TO} separator for the {@code SET <param> TO <value>} syntax. */
  private static final Pattern                                        SET_TO_SEPARATOR  = Pattern.compile("(?i)\\s+TO\\s+");
  /** Case-insensitive {@code SESSION}/{@code LOCAL} scope modifier leading a {@code SET} command (issue #6701). */
  private static final Pattern                                        SET_SCOPE_MODIFIER = Pattern.compile("(?i)^(?:SESSION|LOCAL)\\s+");

  private final ArcadeDBServer              server;
  private final ChannelBinaryServer         channel;
  private final byte[]                      buffer                = new byte[BUFFER_LENGTH];
  private final Map<String, PostgresPortal> portals               = new HashMap<>();
  // Prepared statements registered by PARSE, keyed by statement name (issue #6660 / CodeRabbit on #6658).
  // Read-only after PARSE creates the entry - bindCommand() clones from here via PostgresPortal.bindFrom()
  // rather than handing out this same instance, so that two portal names bound from one statement (or the
  // same portal name re-bound without a new Parse) never share mutable per-execution state. `portals` above
  // holds only the independent, already-bound portals that describeCommand('P')/executeCommand() operate on.
  private final Map<String, PostgresPortal> preparedStatements    = new HashMap<>();
  private final boolean                     DEBUG                 = GlobalConfiguration.POSTGRES_DEBUG.getValueAsBoolean();
  private final boolean                     QUOTED_IDENTIFIERS    = GlobalConfiguration.POSTGRES_QUOTED_IDENTIFIERS.getValueAsBoolean();
  private final Map<String, Object>         connectionProperties  = new HashMap<>();
  // The exact query spellings to answer with nothing, and the application_name values that gated them, used
  // to be listed here: see PostgresCatalog, which answers those questions by shape for every client (#6412).

  private volatile boolean shutdown = false;

  /**
   * The listener's permit for a connection that has not authenticated yet, handed back the moment it does -
   * or when it goes away without ever doing so (issue #6412). Null when the connection was not created by a
   * listener that caps them.
   */
  private final PreAuthConnectionGate.Ticket preAuthTicket;

  private Database database;
  private int      nextByte                   = 0;
  private boolean  reuseLastByte              = false;
  private String   userName                   = null;
  private String   databaseName               = null;
  private String   userPassword               = null;
  private int      consecutiveErrors          = 0;
  private long     processIdSequence          = 0;
  private boolean  explicitTransactionStarted = false;
  private boolean  errorInTransaction         = false;

  private interface ReadMessageCallback {
    void read(char type, long length) throws IOException;
  }

  private interface WriteMessageCallback {
    void write() throws IOException;
  }

  public PostgresNetworkExecutor(final ArcadeDBServer server, final Socket socket, final Database database) throws IOException {
    this(server, socket, database, null);
  }

  public PostgresNetworkExecutor(final ArcadeDBServer server, final Socket socket, final Database database,
      final PreAuthConnectionGate.Ticket preAuthTicket) throws IOException {
    setName(Constants.PRODUCT + "-postgres/" + socket.getInetAddress());
    this.server = server;
    this.channel = new ChannelBinaryServer(socket, server.getConfiguration());
    this.database = database;
    this.preAuthTicket = preAuthTicket;

    // Bound the pre-authentication window (issue #6377). One thread and one file descriptor are committed
    // per accepted connection, before anyone has proved who they are, and the listener caps neither; without
    // a read timeout a client that connects and then says nothing - or trickles bytes arbitrarily slowly -
    // holds both for as long as it likes. Lifted back to infinite once the database is open (see
    // markAuthenticated), because an authenticated client is expected to keep a long-lived, often idle
    // connection between statements. This is the same setSoTimeout(NETWORK_SOCKET_TIMEOUT)-then-
    // setSoTimeout(0) idiom RedisNetworkExecutor (#5912) and BoltNetworkExecutor (#5978) already use.
    final int handshakeTimeout = GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.getValueAsInteger();
    if (handshakeTimeout > 0)
      channel.socket.setSoTimeout(handshakeTimeout);
  }

  /**
   * Lifts the pre-authentication read timeout armed in the constructor (issue #6377).
   */
  private void markAuthenticated() {
    releasePreAuthTicket();
    try {
      channel.socket.setSoTimeout(0);
    } catch (final SocketException e) {
      // setSoTimeout() only throws on an already-broken/closed socket: there is nothing left to hold open in
      // that case, so logging and moving on - rather than failing the whole authentication - is safe. The
      // connection dies on its next read or write either way.
      LogManager.instance().log(this, Level.FINE, "PSQL: unable to lift the idle read timeout after authentication", e);
    }
  }

  public void close() {
    shutdown = true;
    releasePreAuthTicket();
    if (channel != null)
      channel.close();
  }

  /**
   * Hands the listener's pre-authentication permit back. Idempotent, so authenticating and then closing -
   * the normal life of a connection - releases exactly one permit.
   */
  private void releasePreAuthTicket() {
    if (preAuthTicket != null)
      preAuthTicket.release();
  }

  @Override
  public void run() {
    ProtocolContext.set("postgres");
    try {
      try {
        if (!readStartupMessage(true))
          return;

        writeMessage("request for password", () -> channel.writeUnsignedInt(3), 'R', 8);

        // The password read blocks on the socket, under the handshake timeout armed in the constructor: a
        // client that connects, receives the password request and then goes silent is closed by that timeout
        // (issue #6377). It used to need a deadline of its own here, because readMessage could not block
        // (issue #6410).
        if (!readMessage("password", (type, length) -> userPassword = readString(), 'p'))
          return;

        if (!openDatabase())
          return;
      } catch (final PostgresProtocolException e) {
        // A client that never finished the handshake is a non-event: it gets no error message it could read
        // anyway, and the connection is closed by the enclosing finally.
        LogManager.instance().log(this, Level.FINE, "PSQL: connection closed before authentication completed: %s", e,
            e.getMessage());
        return;
      }

      markAuthenticated();

      writeMessage("authentication ok", () -> channel.writeUnsignedInt(0), 'R', 8);

      // BackendKeyData
      final long pid = processIdSequence++;
      final long secret = Math.abs(ThreadLocalRandom.current().nextInt(10000000));
      writeMessage("backend key data", () -> {
        channel.writeUnsignedInt((int) pid);
        channel.writeUnsignedInt((int) secret);
      }, 'K', 12);

      ACTIVE_SESSIONS.put(pid, new Pair<>(secret, this));

      sendServerParameter("server_version", PG_SERVER_VERSION);
      sendServerParameter("server_encoding", "UTF8");
      sendServerParameter("client_encoding", "UTF8");

      try {
        writeReadyForQueryMessage();

        while (!shutdown) {
          try {

            // False means the connection is finished - the client closed it, or it was closed under this
            // thread's blocked read. There is nothing left to read from it, so the loop must not go round
            // again (issue #6410): before the read blocked, false only meant "no byte yet".
            if (!readMessage("any", (type, length) -> {
              consecutiveErrors = 0;

              switch (type) {
              case 'P' -> parseCommand();
              case 'B' -> bindCommand();
              case 'E' -> executeCommand();
              case 'Q' -> queryCommand();
              case 'S' -> syncCommand();
              case 'D' -> describeCommand();
              case 'C' -> closeCommand();
              case 'H' -> flushCommand();
              case 'X' -> {
                // TERMINATE
                shutdown = true;
                return;
              }
              default -> throw new PostgresProtocolException("Message '" + type + "' not managed");
              }

            }, 'P', 'B', 'E', 'Q', 'S', 'D', 'C', 'H', 'X'))
              return;

          } catch (final Exception e) {
            setErrorInTx();

            if (e instanceof PostgresProtocolException) {
              LogManager.instance().log(this, Level.SEVERE, e.getMessage(), e);
              LogManager.instance().log(this, Level.SEVERE, "PSQL: Closing connection with client");
              return;
            } else {
              LogManager.instance().log(this, Level.SEVERE, "PSQL: Error on reading request: %s", e, e.getMessage());
              if (++consecutiveErrors > 3) {
                LogManager.instance().log(this, Level.SEVERE, "PSQL: Closing connection with client");
                return;
              }
            }
          }
        }
      } finally {
        ACTIVE_SESSIONS.remove(pid);
      }

    } finally {
      ProtocolContext.clear();
      close();
    }
  }

  private void syncCommand() {
    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: sync (thread=%s)", Thread.currentThread().threadId());

    if (errorInTransaction) {
      // DISCARDED PREVIOUS MESSAGES TILL THIS POINT
      database.rollback();
      errorInTransaction = false;
    } else if (!explicitTransactionStarted) {
      if (database.isTransactionActive())
        database.commit();
    }
    writeReadyForQueryMessage();
  }

  private void flushCommand() throws IOException {
    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: flush (thread=%s)", Thread.currentThread().threadId());
    // Flush message does NOT generate any response according to PostgreSQL protocol.
    // It just forces the backend to deliver any data pending in its output buffers.
    // See: https://www.postgresql.org/docs/current/protocol-message-formats.html
    channel.flush();
  }

  private void closeCommand() throws IOException {
    final byte closeType = channel.readByte();
    final String prepStatementOrPortal = readString();

    if (errorInTransaction)
      return;

    if (closeType == 'P')
      getPortal(prepStatementOrPortal, true);
    else if (closeType == 'S')
      preparedStatements.remove(prepStatementOrPortal);

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: close '%s' type=%s (thread=%s)", prepStatementOrPortal, (char) closeType,
          Thread.currentThread().threadId());

    writeMessage("close complete", null, '3', 4);
  }

  private void describeCommand() throws IOException {
    final byte type = channel.readByte();
    final String portalName = readString();

    if (DEBUG)
      LogManager.instance()
          .log(this, Level.INFO, "PSQL: describe '%s' type=%s (errorInTransaction=%s thread=%s)", portalName, (char) type,
              errorInTransaction, Thread.currentThread().threadId());

    if (errorInTransaction)
      return;

    // Describe('S') names a PREPARED STATEMENT (registered by PARSE); Describe('P') names a bound PORTAL
    // (registered by BIND) - two different registries since #6660 / CodeRabbit split them apart so portals
    // stop sharing mutable state. A statement's column info, once resolved here from its schema, is a
    // property of the statement itself (independent of any parameter values), so it is cached directly on
    // the template - every portal bound from it afterwards inherits it via PostgresPortal.bindFrom(), the
    // same way queryTargetType/aliasToSourceProperty are already memoized per statement.
    final PostgresPortal portal = type == 'S' ? preparedStatements.get(portalName) : getPortal(portalName, false);
    if (portal == null) {
      writeNoData();
      return;
    }

    if (type == 'P') {
      // Every Describe is owed exactly one reply - RowDescription or NoData - and a client counting one reply
      // per request desynchronizes on anything else, so the three arms below are exhaustive by construction
      // (issue #6996). Which one applies is decided by whether rows are still coming, NOT by whether PARSE
      // left a parsed SQL statement behind: a portal in a non-"sql" language (cypher/gremlin/graphql) and a
      // catalog query whose filters are bound parameters both arrive here with no statement and no columns,
      // and both do produce rows.
      if (portal.isExpectingResult && !portal.executed && !portal.ignoreExecution) {
        // Runs the query now and answers from the rows it actually produced - the truthful answer, and the
        // only one available for a language whose columns no schema resolution can name ahead of execution.
        // Guarded on !executed so a second Describe('P') on the same portal (or one arriving after an Execute
        // already ran it) reuses the materialized result instead of running it again: issue #6458's follow-up
        // Execute(s) depend on this same fullResultSet being run exactly once.
        final ResultSet resultSet = runPortalQuery(portal);
        portal.executed = true;
        // Materializes the whole result now (issue #6458): Describe needs every row's columns, not just the
        // first, to catch a property that only shows up on a later row (documents can be sparse) - and the
        // portal keeps this list so the Execute(s) that follow slice it by their own row-limit instead of
        // re-running the statement or losing what Describe already had to read.
        portal.fullResultSet = browseAndCacheResultSet(resultSet, 0);
        resolvePortalColumns(portal);
        writeRowDescription(portal.columns, portal.resultFormats);
        portal.rowDescriptionSent = true;
      } else if (portal.isExpectingResult && portal.columns != null) {
        // Already materialized: a synthetic answer fixed at PARSE (SHOW/system/catalog), or a portal a
        // previous Describe/Execute already ran.
        if (portal.executed && portal.fullResultSet != null)
          resolvePortalColumns(portal);
        writeRowDescription(portal.columns, portal.resultFormats);
        portal.rowDescriptionSent = true;
      } else
        // No rows are coming: an INSERT/UPDATE/DELETE portal, or SAVEPOINT/RELEASE/ROLLBACK TO/SET, which
        // carry no statement and never produce a result (issue #6930).
        writeNoData();
    } else if (type == 'S') {
      // Describe Statement: send ParameterDescription followed by RowDescription/NoData
      // This tells the client how many parameters the prepared statement expects
      writeParameterDescription(portal);

      // Now send RowDescription or NoData
      // For SELECT queries, we need to determine the columns from the type schema
      if (portal.isExpectingResult && portal.columns == null && !portal.catalogQuery) {
        portal.columns = getColumnsFromQuerySchema(portal.query, portal.sqlStatement);
      }

      if (portal.columns != null && !portal.columns.isEmpty()) {
        writeRowDescription(portal.columns);
        portal.rowDescriptionSent = true;
        // This IS the statement-level contract executeCommand() must honor for every future Bind/Execute of
        // this statement, whatever row each one actually returns (issue #6725) - unlike portal.columns being
        // non-null for some other reason (a catalog answer recomputed per-Bind, or bindCommand()'s fallback
        // onto an already-executed portal), which carries no such promise.
        portal.columnsDescribed = true;
      } else {
        // We can't determine columns at DESCRIBE time (e.g., INSERT without schema info)
        // Send NoData, but keep isExpectingResult = true so EXECUTE can handle it properly
        // The actual query execution will determine if there are results
        writeNoData();
      }
    } else
      throw new PostgresProtocolException("Unexpected describe type '" + type + "'");
  }

  /**
   * Runs a portal's query, whichever of the three forms PARSE left it in, and returns its result set.
   * <p>
   * Shared by {@code Describe('P')} and {@code Execute} so a portal is run exactly once no matter which of
   * the two reaches it first (issue #6996): the other finds {@code portal.executed} already true and reads
   * what this left behind. Before the split existed, Describe could only run the parsed-SQL form, so a portal
   * in another language had no columns to announce and was answered with nothing at all.
   */
  private ResultSet runPortalQuery(final PostgresPortal portal) {
    if (portal.catalogQuery) {
      // Deferred from parseCommand because the query's filters are bound parameters (issue #6412).
      final CatalogAnswer catalogAnswer = handleCatalogQuery(portal.query, getParams(portal));
      if (catalogAnswer != null)
        portal.columns = catalogAnswer.columns();
      return new IteratorResultSet(
          (catalogAnswer != null ? catalogAnswer.rows() : Collections.<Result>emptyList()).iterator());
    }

    if (portal.sqlStatement == null)
      // No parsed statement: a non-"sql" language (cypher/gremlin/graphql) goes straight to its own engine.
      return database.command(portal.language, portal.query, server.getConfiguration(), getParams(portal));

    final Object[] parameters = portal.parameterValues != null ? portal.parameterValues.toArray() : new Object[0];
    final long metricsStart = QueryMetricsRecorder.Holder.startNanos();
    try {
      return portal.sqlStatement.execute(database, parameters, createCommandContext());
    } finally {
      QueryMetricsRecorder.Holder.record(metricsStart, database.getName(), portal.language, "command");
    }
  }

  /**
   * Resolves the columns a materialized portal must be announced under, from the rows it actually produced.
   * A catalog answer keeps its own columns, which are fixed by the catalog table being emulated rather than
   * by whichever rows happened to match; a query that came back empty falls back to the schema, so a client
   * probing a shape with {@code WHERE 1=0} or {@code LIMIT 0} still gets a typed result set.
   */
  private void resolvePortalColumns(final PostgresPortal portal) {
    final List<Result> rows = portal.fullResultSet != null ? portal.fullResultSet : Collections.emptyList();
    if (!portal.catalogQuery || portal.columns == null)
      portal.columns = getColumns(rows, resolveQueryTargetType(portal), resolveAliasToSourceProperty(portal));
    if (portal.columns.isEmpty() && rows.isEmpty()) {
      final Map<String, PostgresType> schemaColumns = resolveEmptyResultSchemaColumns(portal.query, portal.language,
          getParams(portal), portal.sqlStatement);
      if (schemaColumns != null)
        portal.columns = schemaColumns;
    }
  }

  private void executeCommand() {
    final QueryProfile profile = new QueryProfile();
    QueryProfile.pushCurrent(profile);
    PostgresPortal portal = null;
    try {
      final long deserStart = System.nanoTime();
      final String portalName = readString();
      final int limit = (int) channel.readUnsignedInt();
      profile.addDeserializationNanos(System.nanoTime() - deserStart);

      if (errorInTransaction)
        return;

      // Do NOT remove the portal here (issue #6458): a limit-hit Execute suspends rather than finishes, and
      // the follow-up Execute the client sends to continue fetching looks this same portal name up again.
      // The portal is only ever discarded by an explicit Close ('C') message or by a later Bind reusing the
      // name (closeCommand()/bindCommand()).
      portal = getPortal(portalName, false);
      if (portal == null) {
        writeNoData();
        return;
      }

      if (DEBUG)
        LogManager.instance()
            .log(this, Level.INFO, "PSQL: execute (portal=%s) (limit=%d)-> %s (thread=%s)", portalName, limit, portal,
                Thread.currentThread().threadId());

      if (portal.ignoreExecution)
        // SAVEPOINT/RELEASE/ROLLBACK TO/SET never produce rows: Execute must answer CommandComplete, not
        // NoData - NoData ('n') is a Describe-only reply and is never a legal answer to Execute (issue #6930).
        writeCommandComplete(portal.query, 0);
      else {
        if (!portal.executed) {
          final long engineStart = System.nanoTime();
          final ResultSet resultSet = runPortalQuery(portal);
          portal.executed = true;
          if (portal.isExpectingResult) {
            // Materializes the whole result now (issue #6458), the same way Describe('P') does: a client that
            // never Described this portal first (it already knows the row shape) reaches this branch instead,
            // and every Execute on this portal from here on - including this one - slices fullResultSet by its
            // own row-limit below rather than re-running the statement.
            portal.fullResultSet = browseAndCacheResultSet(resultSet, 0);
            profile.addEngineNanos(System.nanoTime() - engineStart);
            // Only send RowDescription if not already sent during DESCRIBE
            if (!portal.rowDescriptionSent) {
              final long serStart = System.nanoTime();
              // portal.columnsDescribed means a real Describe('S') already told the client this exact
              // shape/OIDs - and, for a schemaless type, a client that negotiated binary transfer off that
              // promise cannot have it silently swapped for a differently-typed one here (issue #6725):
              // re-deriving from this execution's own rows, which can differ in type per row for an
              // undeclared property, would desync that client's decoder. Keep the promised columns and just
              // mark this portal as having satisfied it, matching real PostgreSQL - a portal never gets a
              // second RowDescription once its statement was already Described.
              if (!portal.columnsDescribed) {
                resolvePortalColumns(portal);
                writeRowDescription(portal.columns, portal.resultFormats);
              }
              portal.rowDescriptionSent = true;
              profile.addSerializationNanos(System.nanoTime() - serStart);
            }
          } else {
            profile.addEngineNanos(System.nanoTime() - engineStart);
          }
        }

        // Computes this Execute's slice of the portal's materialized result (issue #6458). Runs on every
        // Execute, not only the one that just populated fullResultSet above: a follow-up Execute continuing a
        // previously suspended fetch reaches here with portal.executed already true and fullResultSet already
        // populated - by this same method on an earlier call, or by describeCommand() - and only needs the
        // next slice, never a re-run of the statement.
        if (portal.isExpectingResult && portal.fullResultSet != null) {
          final int total = portal.fullResultSet.size();
          final int start = portal.resultCursor;
          final int end = limit > 0 ? Math.min(start + limit, total) : total;
          portal.cachedResultSet = start < end ? new ArrayList<>(portal.fullResultSet.subList(start, end)) : Collections.emptyList();
          portal.resultCursor = end;
          // The protocol allows exactly one terminator after the data rows: PortalSuspended when this slice
          // stopped on the row-limit with rows still left, CommandComplete once the portal is fully drained.
          portal.suspended = limit > 0 && end < total;
        }

        if (portal.isExpectingResult && portal.cachedResultSet != null && !portal.cachedResultSet.isEmpty()) {
          final long serStart = System.nanoTime();
          // Query returned results - send them
          final Map<String, PostgresType> dataRowColumns = getColumns(portal.cachedResultSet, resolveQueryTargetType(portal),
              resolveAliasToSourceProperty(portal));

          if (DEBUG)
            LogManager.instance().log(this, Level.INFO,
                "PSQL: executeCommand columns - portal.columns=%s, dataRowColumns=%s, resultSize=%d (thread=%s)",
                portal.columns != null ? portal.columns.keySet() : "null",
                dataRowColumns.keySet(),
                portal.cachedResultSet.size(),
                Thread.currentThread().threadId());

          // If RowDescription wasn't sent during DESCRIBE (e.g., INSERT with RETURN), we need to send it now
          // before the data rows. portal.columnsDescribed means a real Describe('S') already told the client
          // this exact shape/OIDs (issue #6725) - keep it rather than silently swapping it for dataRowColumns,
          // which a schemaless type's undeclared property can compute differently per row.
          if (!portal.rowDescriptionSent) {
            if (!portal.columnsDescribed) {
              portal.columns = dataRowColumns;
              writeRowDescription(portal.columns, portal.resultFormats);
            }
            portal.rowDescriptionSent = true;
          }

          // Verify column count matches what was sent in RowDescription
          if (portal.columns != null && portal.columns.size() != dataRowColumns.size()) {
            // Column count mismatch - use the original columns from DESCRIBE
            // This can happen if sample query returned different properties than actual query
            if (DEBUG)
              LogManager.instance().log(this, Level.WARNING,
                  "PSQL: Column count mismatch - RowDesc=%d, DataRow=%d (thread=%s)",
                  portal.columns.size(), dataRowColumns.size(), Thread.currentThread().threadId());
          }

          // Use the columns that were sent in RowDescription for consistency
          final Map<String, PostgresType> columnsToUse = portal.columns != null ? portal.columns : dataRowColumns;
          writeDataRows(portal.cachedResultSet, columnsToUse, portal.resultFormats);
          // Exactly one terminator after the data rows (issue #6458), never both, and always after the rows
          // rather than before them: PortalSuspended when this slice stopped short of fullResultSet with the
          // row-limit reached, CommandComplete once the portal is fully drained - tagged with resultCursor,
          // the running total across every slice this portal has sent (matching PostgreSQL's own convention),
          // when this portal is the paginated fullResultSet-backed kind; a portal whose cachedResultSet was
          // set directly (a synthetic single-row answer - SHOW/catalog/etc.) never touches resultCursor, so it
          // keeps reporting its own size exactly as before this fix.
          if (portal.suspended)
            portalSuspendedResponse();
          else
            writeCommandComplete(portal.query, portal.fullResultSet != null ? portal.resultCursor : portal.cachedResultSet.size());
          profile.addSerializationNanos(System.nanoTime() - serStart);
        } else {
          final long serStart = System.nanoTime();
          // Query doesn't return data (INSERT/UPDATE/DELETE without RETURNING) or empty result
          final int affectedRows = portal.cachedResultSet != null ? portal.cachedResultSet.size() : 0;
          writeCommandComplete(portal.query, affectedRows);
          profile.addSerializationNanos(System.nanoTime() - serStart);
        }
      }
    } catch (final CommandParsingException e) {
      // The "Syntax error" wording assumes only genuine parse failures reach this arm, which holds because this
      // path runs an already-parsed statement and execution failures are CommandExecutionException. If a
      // CommandParsingException ever wraps a non-parse cause here, sqlStateFor reports that cause - correctly, and
      // clients branch on the SQLSTATE - while this text would still read "Syntax error".
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on executing query: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()), sqlStateFor(e));
    } catch (final Exception e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Error on executing query: " + e.getMessage(), sqlStateFor(e));
    } finally {
      if (portal != null)
        recordPostgresProfile(profile, portal.language, portal.query);
      QueryProfile.popCurrent();
    }
  }

  private CommandContext createCommandContext() {
    CommandContext commandContext = new BasicCommandContext();
    commandContext.setConfiguration(server.getConfiguration());
    return commandContext;
  }

  private void queryCommand() {
    final QueryProfile profile = new QueryProfile();
    QueryProfile.pushCurrent(profile);
    Query query = null;
    String queryText = null;
    CatalogAnswer catalogAnswer = null;
    try {
      final long deserStart = System.nanoTime();
      queryText = readString().trim();
      if (queryText.endsWith(";"))
        queryText = queryText.substring(0, queryText.length() - 1);

      if (errorInTransaction) {
        profile.addDeserializationNanos(System.nanoTime() - deserStart);
        final String abortedUpperCaseText = queryText.toUpperCase(Locale.ENGLISH);
        if (isTransactionEndStatement(abortedUpperCaseText)) {
          // Real Postgres treats a COMMIT of an aborted transaction the same as a ROLLBACK (with a
          // warning): there is nothing left to commit, so both end keywords just discard the transaction.
          if (database.isTransactionActive())
            database.rollback();
          explicitTransactionStarted = false;
          errorInTransaction = false;
          // The tag is always "ROLLBACK" here, even if the client sent COMMIT/END: see the comment above.
          writeCommandComplete("ROLLBACK", 0);
        } else if (queryText.isEmpty()) {
          emptyQueryResponse();
        } else {
          // Every other statement is refused, not silently swallowed (issue #6457): the client needs an
          // ErrorResponse to know its statement never ran, and errorInTransaction must stay set so the
          // session remains aborted until COMMIT/ROLLBACK/END ends the block.
          writeError(ERROR_SEVERITY.ERROR,
              "current transaction is aborted, commands ignored until end of transaction block", "25P02");
        }
        return;
      }

      if (queryText.isEmpty()) {
        profile.addDeserializationNanos(System.nanoTime() - deserStart);
        emptyQueryResponse();
        return;
      }

      query = getLanguageAndQuery(queryText);
      profile.addDeserializationNanos(System.nanoTime() - deserStart);
      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: query -> %s ", query);

      // Reused below for both the schema-fallback and the target-type resolution, but only set in the one
      // branch that reaches database.command(...) below: none of SET/SAVEPOINT/RELEASE/ROLLBACK TO/SHOW/a
      // system query/BEGIN are valid SQL productions (no bare "SET"/"SHOW" statement exists in SQLParser.g4),
      // so parsing any of them here would be a guaranteed parse-and-fail on every one of those statements -
      // exactly the ones connection setup (JDBC drivers, psql, poolers) sends most.
      Statement parsedStatement = null;

      final long engineStart = System.nanoTime();
      final ResultSet resultSet;
      final String upperCaseText = query.query.toUpperCase(Locale.ENGLISH);
      final PostgresSystemQuery systemQuery = PostgresSystemQuery.parse(query.query);
      if (upperCaseText.startsWith("SET ")) {
        setConfiguration(query.query);
        resultSet = new IteratorResultSet(createResultSet("STATUS", "Setting ignored").iterator());
      } else if (upperCaseText.startsWith("SAVEPOINT ") ||
          upperCaseText.startsWith("RELEASE ") ||
          upperCaseText.startsWith("ROLLBACK TO ")) {
        resultSet = new IteratorResultSet(Collections.emptyIterator());
      } else if (systemQuery != null)
        resultSet = new IteratorResultSet(
            createResultSet(systemQuery.columnName, systemQueryValue(systemQuery.function)).iterator());
      else if ("SHOW TRANSACTION ISOLATION LEVEL".equals(upperCaseText)) {
        final Database.TRANSACTION_ISOLATION_LEVEL dbIsolationLevel = database.getTransactionIsolationLevel();
        final String level = dbIsolationLevel.name().replace('_', ' ');
        resultSet = new IteratorResultSet(createResultSet("LEVEL", level).iterator());
      } else if (upperCaseText.startsWith("SHOW ")) {
        final String varName = query.query.substring(5).trim().toLowerCase(Locale.ENGLISH);
        resultSet = new IteratorResultSet(createResultSet(varName, getShowConfigValue(varName)).iterator());
      } else if (isBeginStatement(upperCaseText)) {
        explicitTransactionStarted = true;
        database.begin();
        resultSet = new IteratorResultSet(Collections.emptyIterator());
      } else if (isCommitStatement(upperCaseText)) {
        if (explicitTransactionStarted && database.isTransactionActive())
          database.commit();
        explicitTransactionStarted = false;
        resultSet = new IteratorResultSet(Collections.emptyIterator());
      } else if (isRollbackStatement(upperCaseText)) {
        if (explicitTransactionStarted && database.isTransactionActive())
          database.rollback();
        explicitTransactionStarted = false;
        resultSet = new IteratorResultSet(Collections.emptyIterator());
      } else {
        // A query about the emulated system catalog, which every client sends and which ArcadeDB's own SQL
        // engine has no tables for (issue #6412).
        catalogAnswer = handleCatalogQuery(query.query);
        if (catalogAnswer != null) {
          resultSet = new IteratorResultSet(catalogAnswer.rows().iterator());
        } else {
          parsedStatement = "sql".equalsIgnoreCase(query.language) ? parseStatement(query.query) : null;
          resultSet = database.command(query.language, query.query, server.getConfiguration());
        }
      }
      final List<Result> cachedResultSet = browseAndCacheBoundedResultSet(resultSet,
          GlobalConfiguration.POSTGRES_SIMPLE_QUERY_MAX_ROWS.getValueAsInteger());
      profile.addEngineNanos(System.nanoTime() - engineStart);

      final long serStart = System.nanoTime();
      Map<String, PostgresType> columns = catalogAnswer != null ? catalogAnswer.columns()
          : getColumns(cachedResultSet, resolveQueryTargetType(parsedStatement), resolveAliasToSourceProperty(parsedStatement));
      if (columns.isEmpty() && cachedResultSet.isEmpty()) {
        final Map<String, PostgresType> schemaColumns = resolveEmptyResultSchemaColumns(query.query, query.language, NO_PARAMETERS,
            parsedStatement);
        if (schemaColumns != null)
          columns = schemaColumns;
      }
      writeRowDescription(columns);
      writeDataRows(cachedResultSet, columns);
      writeCommandComplete(queryText, cachedResultSet.size());
      profile.addSerializationNanos(System.nanoTime() - serStart);

    } catch (final CommandParsingException e) {
      // See the note on the same arm in executeCommand about the "Syntax error" wording.
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on executing query: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()), sqlStateFor(e));
    } catch (final Exception e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Error on executing query: " + e.getMessage(), sqlStateFor(e));
    } finally {
      writeReadyForQueryMessage();
      if (query != null)
        recordPostgresProfile(profile, query.language, query.query);
      else if (queryText != null && !queryText.isEmpty())
        recordPostgresProfile(profile, "sql", queryText);
      QueryProfile.popCurrent();
    }
  }

  private void recordPostgresProfile(final QueryProfile profile, final String language, final String queryText) {
    Metrics.counter("postgres.query").increment();
    Metrics.timer("postgres.query.deserialization").record(profile.getDeserializationNanos(), TimeUnit.NANOSECONDS);
    Metrics.timer("postgres.query.engine").record(profile.getEngineNanos(), TimeUnit.NANOSECONDS);
    Metrics.timer("postgres.query.serialization").record(profile.getSerializationNanos(), TimeUnit.NANOSECONDS);

    final ServerQueryProfiler serverProfiler = server.getQueryProfiler();
    if (serverProfiler == null || !serverProfiler.isRecording())
      return;
    serverProfiler.recordQuery(database != null ? database.getName() : databaseName, language, queryText, profile, null);
  }

  private void writeReadyForQueryMessage() {
    final byte transactionStatus;
    if (errorInTransaction)
      transactionStatus = 'E';
    else if (explicitTransactionStarted)
      transactionStatus = 'T';
    else
      transactionStatus = 'I';

    writeMessage("ready for query", () -> channel.writeByte(transactionStatus), 'Z', 5);
  }

  private List<Result> browseAndCacheResultSet(final ResultSet resultSet, final int limit) {
    return browseAndCacheResultSet(resultSet, limit, true);
  }

  /**
   * Browse a result set and cache results up to the limit, then close the source ResultSet.
   * <p>
   * <b>Ownership:</b> this method takes ownership of the supplied ResultSet and closes it before returning, in
   * both the natural-exhaustion and the limit-hit paths. Both callers that matter for issue #6458 - Describe
   * ('P') and the first Execute of a portal that was never Described - call this with {@code limit=0} to
   * materialize the whole result into {@link PostgresPortal#fullResultSet}: Describe needs every row to
   * discover every column (documents can be sparse), and Execute needs the complete list to slice by whatever
   * row-limit each Execute call declares, including a follow-up one continuing a previously suspended fetch
   * (see the pagination step in {@code executeCommand()}, which slices {@code fullResultSet} rather than
   * calling this method again). The {@code limit>0} form remains for the unrelated internal callers that want
   * a bounded sample and are happy to see the rest of the result set discarded - {@code sendSuspendedOnLimit}
   * is {@code false} for all of them, so the always-{@code limit=0} callers are the only ones that can still
   * reach that branch.
   *
   * @param resultSet           The result set to browse (this method closes it)
   * @param limit               Maximum number of results to cache (0 = unlimited)
   * @param sendSuspendedOnLimit If true and limit is reached, sends PortalSuspended message.
   *                            Set to false for internal queries (like schema discovery) that
   *                            should not send protocol messages.
   */
  private List<Result> browseAndCacheResultSet(final ResultSet resultSet, final int limit, final boolean sendSuspendedOnLimit) {
    try (resultSet) {
      final List<Result> cachedResultSet = new ArrayList<>();
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        if (row == null)
          continue;

        cachedResultSet.add(row);

        if (limit > 0 && cachedResultSet.size() >= limit) {
          if (sendSuspendedOnLimit)
            portalSuspendedResponse();
          break;
        }
      }
      return cachedResultSet;
    }
  }

  /**
   * Browses and caches a result set for the simple-query ('Q' message) protocol path, refusing (rather than
   * silently truncating) a result larger than {@code maxRows} - and stopping the scan as soon as that is known,
   * rather than paying to read the rest of an oversized source just to reject it.
   * <p>
   * Unlike the extended query protocol - where a portal's {@code Execute} message carries its own client-chosen
   * max-rows and a limit hit is a normal, expected {@code PortalSuspended} the client explicitly asked for by
   * fetching in batches - the simple-query protocol has no such mechanism: a 'Q' message always means "give me
   * the complete result". This path is also why the whole result had to be held in memory in the first place:
   * determining the row description (the union of columns and their types across heterogeneous/schemaless rows)
   * genuinely requires having seen every row before the first one can be sent, since Postgres's wire protocol
   * fixes the column set in {@code RowDescription}, sent before any {@code DataRow}.
   * <p>
   * Aborting the scan early is safe for a write statement too: {@code UpdateExecutionPlan.executeInternal()} (and
   * {@code DeleteExecutionPlan}, which extends it) fully executes every matched row's write and buffers the whole
   * {@code RETURN AFTER}/{@code RETURN BEFORE} result internally <em>before</em> {@code UpdateStatement.execute()}
   * / {@code DeleteStatement.execute()} ever return a {@link ResultSet} to this method - so by the time this loop
   * runs, an {@code UPDATE}/{@code DELETE} ... {@code RETURN} statement has already fully applied every write
   * regardless of how many rows of its result this loop goes on to pull. Stopping early here only shortens how
   * much of that already-complete result gets copied into {@code cachedResultSet}; it can never leave a write
   * half-done.
   *
   * @param maxRows the maximum number of rows to buffer, 0 = unlimited (matches {@link GlobalConfiguration#POSTGRES_SIMPLE_QUERY_MAX_ROWS})
   */
  private List<Result> browseAndCacheBoundedResultSet(final ResultSet resultSet, final int maxRows) {
    try (resultSet) {
      final List<Result> cachedResultSet = new ArrayList<>();
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        if (row == null)
          continue;

        cachedResultSet.add(row);

        if (maxRows > 0 && cachedResultSet.size() > maxRows)
          throw new CommandExecutionException(
              "Result set exceeds the configured limit of " + maxRows + " rows for the Postgres simple-query protocol ("
                  + GlobalConfiguration.POSTGRES_SIMPLE_QUERY_MAX_ROWS.getKey()
                  + "); use the extended query protocol with a bounded portal fetch size for large result sets");
      }

      return cachedResultSet;
    }
  }

  private Object[] getParams(PostgresPortal portal) {
    Object[] parameters = portal.parameterValues != null ? portal.parameterValues.toArray() : new Object[0];

    if ("cypher".equals(portal.language) || "opencypher".equals(portal.language)) {
      Object[] parametersCypher = new Object[parameters.length * 2];
      for (int i = 0; i < parameters.length; i++) {
        parametersCypher[i * 2] = "" + (i + 1);
        parametersCypher[i * 2 + 1] = portal.parameterValues.get(i);
      }
      return parametersCypher;
    }

    return parameters;
  }

  /**
   * The answer to a catalog query: the rows to send, and the columns to announce them under. The columns are
   * carried rather than inferred from the rows so that an answer with no rows in it still describes itself,
   * and in the order the client projected - a DataRow is read positionally.
   */
  private record CatalogAnswer(List<Result> rows, Map<String, PostgresType> columns) {
  }

  /**
   * Answers a query about the emulated PostgreSQL system catalog, which is how every client - not only the
   * ones that were named in an application_name allow-list (issue #6412) - finds out what is in the database
   * it just connected to.
   * <p>
   * Two recognisers share the work: {@link PostgresTypeCatalog} for {@code pg_type}, whose element-type
   * self-join is a shape no plain projection can express, and {@link PostgresCatalog} for the rest. A shape
   * neither of them will answer gets the empty result set that a pg_catalog query it did not understand has
   * always been given, rather than being handed to ArcadeDB's SQL engine, which has no such tables and would
   * answer with a syntax error.
   *
   * @param parameters the values bound to the query's placeholders, empty at Parse time and supplied at
   *                   Execute time, when the client has sent them
   *
   * @return the answer, or null when the query is not about the catalog at all and must be executed normally
   */
  private CatalogAnswer handleCatalogQuery(final String query, final Object... parameters) {
    if (!PostgresCatalog.mightBeCatalogQuery(query))
      return null;

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: handling catalog query: %s (thread=%s)", query,
          Thread.currentThread().threadId());

    final List<Result> types = toResults(PostgresTypeCatalog.resolve(query));
    if (types != null)
      return new CatalogAnswer(types, getColumns(types));

    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, userName, parameters);
    if (answer != null && answer.rows != null)
      return new CatalogAnswer(toResults(answer.rows), answer.columns);

    if (answer == PostgresCatalog.DECLINED || PostgresCatalog.containsIgnoreCase(query, "pg_catalog")
        || PostgresCatalog.containsIgnoreCase(query, "pg_type"))
      return new CatalogAnswer(Collections.emptyList(), Map.of());

    return null;
  }

  private static List<Result> toResults(final List<Map<String, Object>> rows) {
    if (rows == null)
      return null;
    final List<Result> results = new ArrayList<>(rows.size());
    for (final Map<String, Object> row : rows)
      results.add(new ResultInternal(row));
    return results;
  }

  /**
   * Column names advertised for a row, kept deliberately independent of {@code Result.getPropertyNames()}.
   * <p>
   * A whole-entity projection (OpenCypher {@code RETURN n}) yields a row whose content holds only the
   * variable while the record's own properties live on the backing element. This surface flattens such a
   * row: {@code writeDataRows} reads those values straight off the element, so the names have to be
   * collected from the element as well. Relying on {@code getPropertyNames()} tied the announced columns
   * to that accessor's element/content precedence, and narrowing it for issue #5613 silently dropped every
   * flattened column here. Element names come first so the column order matches what this surface has
   * always emitted.
   */
  private Set<String> columnNamesOf(final Result row) {
    final Set<String> names = new LinkedHashSet<>();
    if (row.isElement())
      names.addAll(row.getElement().get().getPropertyNames());
    names.addAll(row.getPropertyNames());
    return names;
  }

  private Map<String, PostgresType> getColumns(final List<Result> resultSet) {
    return getColumns(resultSet, null, Map.of());
  }

  /**
   * @param queryTargetType       the schema type the query's FROM target names, or null when it is not a plain
   *                              "FROM &lt;type&gt;" or was not resolved. Falls back {@link #getDeclaredProperty}
   *                              to this type when a row is not itself an element - which a query that projects
   *                              specific columns ("SELECT col FROM Type") produces for every row, since only the
   *                              projected values travel with it, not the backing element.
   * @param aliasToSourceProperty maps an aliased projection's output column name back to the property it reads
   *                              (issue #6473), e.g. {@code "x" -> "amount"} for {@code SELECT amount AS x}; see
   *                              {@link #resolveAliasToSourceProperty(Statement)}.
   */
  private Map<String, PostgresType> getColumns(final List<Result> resultSet, final DocumentType queryTargetType,
      final Map<String, String> aliasToSourceProperty) {
    final Map<String, PostgresType> columns = new LinkedHashMap<>();

    boolean atLeastOneElement = false;
    for (final Result row : resultSet) {
      if (row.isElement())
        atLeastOneElement = true;

      for (final String p : columnNamesOf(row)) {
        if (!columns.containsKey(p)) {
          // Determine the PostgreSQL type based on the actual value.
          // Arrays/collections use proper array type codes; native scalar types (numeric, boolean,
          // temporal) are advertised with their native OID so Postgres clients (psycopg, JDBC, ...)
          // deserialize them as native values instead of strings. Without this, typed scalars
          // round-trip through clients as strings and parameter comparisons fail silently.
          // EMBEDDED documents and MAP values (issue #5253) are advertised as JSON so clients parse
          // the nested object instead of re-escaping it as an opaque VARCHAR string.
          final Object value = row.getProperty(p);
          PostgresType pgType = PostgresType.getTypeForValue(value);

          // An empty list carries no element to infer the type from, so getTypeForValue falls back to text[].
          // Prefer the declared "LIST OF <type>" (issue #5289) so a column's OID does not depend on whether
          // the first row's list happens to be empty.
          if (value instanceof Collection<?> collection && collection.isEmpty()) {
            final PostgresType declaredType = getDeclaredListType(row, p, queryTargetType, aliasToSourceProperty);
            if (declaredType != null)
              pgType = declaredType;
          } else if (pgType == PostgresType.DATE && isDeclaredAsDatetime(row, p, queryTargetType, aliasToSourceProperty)) {
            // java.util.Date is the default Java runtime type of both Type.DATE and Type.DATETIME* (issue
            // #6447), so getTypeForValue cannot tell them apart from the value alone and always answers DATE.
            // Prefer the schema's declared type when it can be found, the same way the empty-list case above
            // prefers the declared "LIST OF" over a value-based guess.
            pgType = PostgresType.TIMESTAMP;
          }

          if (pgType.isArrayType() || pgType.isNativeScalarType() || pgType == PostgresType.JSON)
            columns.put(p, pgType);
          else
            columns.put(p, PostgresType.VARCHAR);
        }
      }
    }

    if (atLeastOneElement) {
      columns.put(RID_PROPERTY, PostgresType.VARCHAR);
      columns.put(TYPE_PROPERTY, PostgresType.VARCHAR);
      columns.put(CAT_PROPERTY, PostgresType.CHAR);
    }

    return columns;
  }

  /**
   * Memoized on the portal (issue #6447): a portal can be described and executed - possibly executed
   * repeatedly, for a cursor-based fetch with a LIMIT - several times over its lifetime, and every one of
   * those calls resolves the same FROM-target type, so it is resolved at most once per portal rather than
   * once per call.
   */
  private DocumentType resolveQueryTargetType(final PostgresPortal portal) {
    if (!portal.queryTargetTypeResolved) {
      portal.queryTargetType = resolveQueryTargetType(portal.sqlStatement);
      portal.queryTargetTypeResolved = true;
    }
    return portal.queryTargetType;
  }

  /**
   * The schema type a SELECT statement's simple FROM target names, or null when the target is not a plain
   * "FROM &lt;type&gt;" (a subquery, a function call, a RID, a MATCH, ...) or does not resolve to a known type.
   * Resolved once per query and passed into {@link #getColumns(List, DocumentType)} as the fallback source for
   * a row that is not itself an element (issue #6447).
   */
  private DocumentType resolveQueryTargetType(final Statement statement) {
    if (!(statement instanceof SelectStatement select))
      return null;

    final FromClause target = select.getTarget();
    final FromItem item = target != null ? target.getItem() : null;
    if (item == null || item.getIdentifier() == null)
      return null;

    // getTypeOrNull, not getType: the identifier is very often not a registered type name at all (a bucket, a
    // catalog target, a typo, a RID-producing expression, ...), which getType() reports by throwing - control
    // flow this resolution runs on every query and so should not pay exception overhead for.
    return database.getSchema().getTypeOrNull(item.getIdentifier().getStringValue());
  }

  /**
   * Memoized on the portal, mirroring {@link #resolveQueryTargetType(PostgresPortal)}.
   */
  private Map<String, String> resolveAliasToSourceProperty(final PostgresPortal portal) {
    if (!portal.aliasToSourcePropertyResolved) {
      portal.aliasToSourceProperty = resolveAliasToSourceProperty(portal.sqlStatement);
      portal.aliasToSourcePropertyResolved = true;
    }
    return portal.aliasToSourceProperty;
  }

  /**
   * Maps a SELECT projection's output column name back to the bare property it reads, when the two differ
   * (issue #6473): {@code SELECT amount AS x} maps {@code "x" -> "amount"}. Used by {@link #getDeclaredProperty}
   * to resolve the schema-context fallback through an alias instead of missing it, since the row only ever
   * carries the OUTPUT name ({@code propertyName} there).
   * <p>
   * Only a bare property reference ({@link Expression#isBaseIdentifier()}) is mapped - a computed expression
   * (aliased or not) has no single source property to fall back to, and is correctly left out so the caller
   * keeps its existing value-only guess for that column. An explicit alias that happens to repeat the source
   * property name ({@code SELECT amount AS amount}) is also left out: {@code propertyName} already equals it,
   * so the direct lookup in {@link #getDeclaredProperty} already succeeds without this map.
   */
  private Map<String, String> resolveAliasToSourceProperty(final Statement statement) {
    if (!(statement instanceof SelectStatement select) || select.getProjection() == null)
      return Map.of();

    final List<ProjectionItem> items = select.getProjection().getItems();
    if (items == null || items.isEmpty())
      return Map.of();

    Map<String, String> aliasToProperty = null;
    for (final ProjectionItem item : items) {
      if (item.getAlias() == null || item.getExpression() == null || !item.getExpression().isBaseIdentifier())
        continue;

      final String alias = item.getProjectionAliasAsString();
      final String sourceProperty = item.getExpression().getDefaultAlias().getStringValue();
      if (alias.equals(sourceProperty))
        continue;

      if (aliasToProperty == null)
        aliasToProperty = new HashMap<>();
      aliasToProperty.put(alias, sourceProperty);
    }

    return aliasToProperty != null ? aliasToProperty : Map.of();
  }

  /**
   * Returns the schema property backing {@code propertyName}, or null when it cannot be found. {@code row}
   * itself is an element - and so carries its own {@link DocumentType} - only for a whole-entity projection
   * ("SELECT FROM Type" or "SELECT *"); a query that projects specific columns ("SELECT col FROM Type") produces
   * rows that carry only the projected values, so {@code queryTargetType} - the schema type the query's FROM
   * target names, resolved once by the caller - is the fallback for that (very common) case. Shared lookup
   * behind {@link #getDeclaredListType} and {@link #isDeclaredAsDatetime}, both of which prefer the schema's
   * declared type over a guess made from a single sample value.
   * <p>
   * {@code propertyName} is the row's own column name, which for an aliased or computed projection ({@code
   * SELECT amount AS x FROM Type}) is the alias, not the source property - so a direct lookup misses. When the
   * row is not itself an element (the {@code queryTargetType} fallback case, where this actually matters -
   * see below), {@code aliasToSourceProperty} - resolved once per query by
   * {@link #resolveAliasToSourceProperty(Statement)} - is tried next, closing the gap left by #5289's original
   * {@link #getDeclaredListType} and by #6447's {@code queryTargetType} fallback (issue #6473).
   */
  private Property getDeclaredProperty(final Result row, final String propertyName, final DocumentType queryTargetType,
      final Map<String, String> aliasToSourceProperty) {
    final Document element = row.getElement().orElse(null);
    final DocumentType documentType = element != null ? element.getType() : queryTargetType;
    if (documentType == null)
      return null;

    final Property declared = documentType.getPolymorphicPropertyIfExists(propertyName);
    if (declared != null || element != null)
      return declared;

    final String sourceProperty = aliasToSourceProperty.get(propertyName);
    return sourceProperty != null ? documentType.getPolymorphicPropertyIfExists(sourceProperty) : null;
  }

  /**
   * Returns the array type declared by the schema for a LIST property, or null when it is not a declared LIST.
   */
  private PostgresType getDeclaredListType(final Result row, final String propertyName, final DocumentType queryTargetType,
      final Map<String, String> aliasToSourceProperty) {
    final Property property = getDeclaredProperty(row, propertyName, queryTargetType, aliasToSourceProperty);
    if (property == null || property.getType() != Type.LIST)
      return null;

    return PostgresType.getTypeFromArcade(property.getType(), property.getOfType());
  }

  /**
   * True when the schema declares {@code propertyName} as one of the DATETIME variants (issue #6447). Used to
   * disambiguate a sampled {@code java.util.Date} value, which is the default Java runtime type of both
   * Type.DATE and every Type.DATETIME* and so cannot tell them apart on its own.
   */
  private boolean isDeclaredAsDatetime(final Result row, final String propertyName, final DocumentType queryTargetType,
      final Map<String, String> aliasToSourceProperty) {
    final Property property = getDeclaredProperty(row, propertyName, queryTargetType, aliasToSourceProperty);
    if (property == null)
      return false;

    return switch (property.getType()) {
      case DATETIME, DATETIME_MICROS, DATETIME_NANOS, DATETIME_SECOND -> true;
      default -> false;
    };
  }

  /**
   * Extract column schema from a SELECT query by parsing the type name and querying for a sample row.
   * This is used during DESCRIBE Statement to return RowDescription before the query is executed.
   * ArcadeDB is schema-less so we need to query actual data to discover dynamically-added properties.
   */
  private Map<String, PostgresType> getColumnsFromQuerySchema(final String query, final Statement alreadyParsed) {
    if (query == null || query.isEmpty()) {
      return null;
    }

    // the caller may already hold the parsed statement for this very text: parsing it a second time buys nothing
    final Statement parsed = alreadyParsed != null ? alreadyParsed : parseStatement(query);

    // Prefer the parsed statement: it resolves the FROM target reliably, including the subquery
    // wrapper Spark uses for its schema probe (issue #5368)
    if (parsed instanceof SelectStatement || parsed instanceof MatchStatement) {
      try {
        return getColumnsFromStatement(parsed);
      } catch (final Exception e) {
        if (DEBUG)
          LogManager.instance().log(this, Level.WARNING, "PSQL: cannot resolve the columns of '%s': %s", query, e.getMessage());
        return null;
      }
    }

    // Not parsable as an ArcadeDB SELECT: fall back to the textual FROM-target extraction
    // Patterns: "SELECT FROM TypeName", "SELECT * FROM TypeName", "SELECT ... FROM TypeName"
    final String upperQuery = query.toUpperCase();
    final int fromIndex = upperQuery.indexOf(" FROM ");
    if (fromIndex < 0) {
      return null;
    }

    String afterFrom = query.substring(fromIndex + 6).trim();

    // Extract type name (ends at WHERE, LIMIT, ORDER, or end of string)
    String typeName = afterFrom;
    for (String terminator : new String[]{" WHERE ", " LIMIT ", " ORDER ", " GROUP ", ";"}) {
      final int idx = typeName.toUpperCase().indexOf(terminator);
      if (idx > 0) {
        typeName = typeName.substring(0, idx);
      }
    }
    typeName = typeName.trim();

    // Skip schema: prefix if present
    if (typeName.toLowerCase().startsWith("schema:")) {
      return null; // Schema queries have different structure
    }

    return getColumnsFromType(typeName);
  }

  /**
   * Resolves the columns announced by a parsed SELECT. The FROM target is either a type, whose columns are
   * discovered from a sample row or from the declared schema, or a nested subquery, resolved recursively so
   * that a probe like {@code SELECT * FROM (SELECT name FROM Character) SPARK_GEN_SUBQ_0 WHERE 1=0} (the
   * shape Spark generates, issue #5368) exposes what the innermost query really projects. Each level then
   * narrows the columns with its own projection list. When the target carries no discoverable schema the
   * projection list alone names the columns (issue #6156).
   */
  private Map<String, PostgresType> getColumnsFromSelect(final SelectStatement select) {
    final FromClause target = select.getTarget();
    final FromItem item = target != null ? target.getItem() : null;

    Map<String, PostgresType> columns = null;
    if (item != null) {
      if (item.getStatement() != null)
        columns = getColumnsFromStatement(item.getStatement());
      else if (item.getIdentifier() != null)
        columns = getColumnsFromType(item.getIdentifier().getStringValue());
    }

    if (columns != null && !columns.isEmpty()) {
      final Map<String, PostgresType> projected = applyProjection(select.getProjection(), columns);
      if (projected != null && !projected.isEmpty())
        return projected;
    }

    // The row source is not a schema type: no FROM at all, a RID, a function call such as shortestPath(), a
    // TRAVERSE. Nothing can be discovered from the schema, but an explicit projection list still names every
    // column the query produces, which is what Spark's probe needs (issue #6156).
    return getColumnsFromProjection(select.getProjection());
  }

  /**
   * Resolves the columns announced by a parsed statement. Only the two statements that project a row shape of
   * their own are described here: SELECT through its FROM target and projection, MATCH through its RETURN list.
   */
  private Map<String, PostgresType> getColumnsFromStatement(final Statement statement) {
    if (statement instanceof SelectStatement select)
      return getColumnsFromSelect(select);
    if (statement instanceof MatchStatement match)
      return getColumnsFromMatch(match);
    return null;
  }

  /**
   * Columns announced by a MATCH statement: one per RETURN item, named after its alias or, when there is none,
   * after the expression's default alias - exactly the naming the executor applies. Types are unknowable without
   * running the pattern, so they are announced as text.
   */
  private Map<String, PostgresType> getColumnsFromMatch(final MatchStatement match) {
    final List<Expression> returnItems = match.getReturnItems();
    if (returnItems == null || returnItems.isEmpty())
      return null;

    final List<Identifier> returnAliases = match.getReturnAliases();
    final Map<String, PostgresType> columns = new LinkedHashMap<>();
    for (int i = 0; i < returnItems.size(); i++) {
      final Identifier alias = returnAliases != null && i < returnAliases.size() ? returnAliases.get(i) : null;
      final String name = alias != null ? alias.getStringValue() : defaultAliasOf(returnItems.get(i));
      if (name == null)
        // cannot tell what this item is named: announcing a partial row would be worse than announcing nothing
        return null;
      columns.put(name, PostgresType.VARCHAR);
    }

    return columns;
  }

  /**
   * Columns named by an explicit projection list, used when the row source itself carries no discoverable schema.
   * Returns null as soon as an item does not name a column on its own - a {@code *} or an exclusion, which both
   * need the row source, or an {@code expand()}, which replaces the row with whatever it expands.
   */
  private Map<String, PostgresType> getColumnsFromProjection(final Projection projection) {
    if (projection == null || projection.getItems() == null || projection.getItems().isEmpty())
      return null;

    if (projection.isExpand())
      return null;

    final Map<String, PostgresType> columns = new LinkedHashMap<>();
    for (final ProjectionItem item : projection.getItems()) {
      if (item.isAll() || item.exclude || item.getExpression() == null)
        return null;

      final String alias = item.getProjectionAliasAsString();
      if (alias == null)
        return null;

      columns.put(alias, PostgresType.VARCHAR);
    }

    return columns.isEmpty() ? null : columns;
  }

  private String defaultAliasOf(final Expression expression) {
    if (expression == null)
      return null;
    final Identifier defaultAlias = expression.getDefaultAlias();
    return defaultAlias != null ? defaultAlias.getStringValue() : null;
  }

  /**
   * Discovers the columns of a type from a sample row (ArcadeDB is schema-less, so actual data may carry
   * dynamically-added properties) or, when the type is empty, from the declared properties plus the system
   * columns. Returns null when the name does not identify a type.
   */
  private Map<String, PostgresType> getColumnsFromType(final String typeName) {
    try {
      // First verify the type exists
      final DocumentType docType = database.getSchema().getType(typeName);
      if (docType == null) {
        return null;
      }

      // Query for a sample row to discover all properties (including dynamically-added ones)
      // Use LIMIT 1 to minimize overhead
      // Use sendSuspendedOnLimit=false because this is an internal query for schema discovery,
      // not a client-initiated query that should send protocol messages
      final String sampleQuery = "SELECT FROM `" + typeName + "` LIMIT 1";
      final ResultSet resultSet = database.query("sql", sampleQuery, server.getConfiguration());
      final List<Result> sampleRows = browseAndCacheResultSet(resultSet, 1, false);

      if (!sampleRows.isEmpty()) {
        // Use the sample row to discover columns
        final Map<String, PostgresType> cols = getColumns(sampleRows, docType, Map.of());
        if (DEBUG)
          LogManager.instance().log(this, Level.INFO,
              "PSQL: getColumnsFromType('%s') -> sampleQuery='%s', found %d rows, columns=%s (thread=%s)",
              typeName, sampleQuery, sampleRows.size(), cols.keySet(), Thread.currentThread().threadId());
        return cols;
      }

      // If no rows exist, fall back to schema-defined properties
      final Map<String, PostgresType> columns = new LinkedHashMap<>();

      // Add system properties first (these are returned for document/vertex types)
      columns.put(RID_PROPERTY, PostgresType.VARCHAR);
      columns.put(TYPE_PROPERTY, PostgresType.VARCHAR);
      columns.put(CAT_PROPERTY, PostgresType.CHAR);

      // Add all defined properties from the type
      for (final String propName : docType.getPropertyNames()) {
        final Property prop = docType.getProperty(propName);
        if (prop != null && prop.getType() != null) {
          columns.put(propName, PostgresType.getTypeFromArcade(prop.getType(), prop.getOfType()));
        } else {
          columns.put(propName, PostgresType.VARCHAR);
        }
      }

      return columns;

    } catch (Exception e) {
      if (DEBUG)
        LogManager.instance().log(this, Level.WARNING, "PSQL: failed to get columns from schema for type '%s': %s",
            typeName, e.getMessage());
      return null;
    }
  }

  /**
   * Narrows the columns discovered for the queried target down to what the query really projects. The
   * discovery always looks at the whole target (a sample row or the declared properties), so without this
   * step a probe like {@code SELECT name FROM Character WHERE 1=0} would advertise every property plus the
   * system columns (issue #5367). The projection comes from the parsed statement instead of the raw text so
   * aliases, functions and expressions are handled the same way the executor handles them. When the query has
   * no projection ({@code SELECT FROM Type}), the full column set is returned unchanged.
   */
  private Map<String, PostgresType> applyProjection(final Projection projection, final Map<String, PostgresType> columns) {
    if (columns == null || columns.isEmpty())
      return columns;

    if (projection == null || projection.getItems() == null || projection.getItems().isEmpty())
      return columns;

    if (projection.isExpand())
      // expand() throws the row away and returns what it expands instead, so the target's columns say nothing
      // about the result. Announcing them would be worse than announcing nothing (issue #6156).
      return null;

    final Map<String, PostgresType> projected = new LinkedHashMap<>();
    for (final ProjectionItem item : projection.getItems()) {
      if (item.isAll()) {
        // "*" expands to every column discovered for the type
        projected.putAll(columns);
        continue;
      }

      final String alias = item.getProjectionAliasAsString();
      if (alias == null)
        // cannot tell what this item produces: rather than announce a wrong set, keep the full one
        return columns;

      if (item.exclude) {
        projected.remove(alias);
        continue;
      }

      // resolve the type from the projected expression when it is a plain property, else from the alias itself
      final String source = item.getExpression() != null ? item.getExpression().toString() : null;
      PostgresType type = source != null ? columns.get(source) : null;
      if (type == null)
        type = columns.getOrDefault(alias, PostgresType.VARCHAR);

      projected.put(alias, type);
    }

    return projected.isEmpty() ? columns : projected;
  }

  /**
   * Parses the query with the ArcadeDB SQL parser, returning null when it is not parsable.
   */
  private Statement parseStatement(final String query) {
    if (query == null || query.isEmpty())
      return null;

    try {
      final SQLQueryEngine sqlEngine = (SQLQueryEngine) database.getQueryEngine("sql");
      return sqlEngine.parse(query, (DatabaseInternal) database);
    } catch (final Exception e) {
      if (DEBUG)
        LogManager.instance().log(this, Level.WARNING, "PSQL: cannot parse query '%s': %s", query, e.getMessage());
      return null;
    }
  }

  /**
   * Schema-discovery fallback for queries that returned 0 rows. RowDescription must still carry column metadata
   * so JDBC clients - Spark and PySpark probe a schema with {@code WHERE 1=0}, Tableau and several JDBC/BI tools
   * with {@code LIMIT 0} - can build a typed result set. Returns null when no schema match is found, letting the
   * caller fall through to the empty-RowDescription default.
   * <p>
   * Two answers are available, and which one is asked first is decided by how the probe is spelled: a replay of
   * the query without the clause that empties it ({@link #sampleProbeColumns}) or a static resolution of the row
   * source ({@link #getColumnsFromQuerySchema}). See {@link ProbeSpelling}.
   */
  private Map<String, PostgresType> resolveEmptyResultSchemaColumns(final String query, final String language,
      final Object[] parameters, final Statement alreadyParsed) {
    if (query == null)
      return null;

    // Only the SQL engine is described here: the parser below, and every shape it recognizes, is SQL's
    if (language != null && !"sql".equalsIgnoreCase(language))
      return null;

    // the extended protocol parsed this very text when it prepared the portal: reuse it instead of parsing twice
    final Statement parsed = alreadyParsed != null ? alreadyParsed : parseStatement(query);

    final ProbeSpelling probe = probeSpelling(parsed);

    // A constant-false filter has no purpose other than probing, so it is answered by the replay first: replaying
    // the statement without it is the only way to learn the columns of a row source that is not a schema type - a
    // graph function, a TRAVERSE, a constant table (issue #6156) - and it is the most faithful answer for the
    // shapes the schema can describe too, because the columns and their types come from the very rows the
    // un-probed query would return.
    if (probe == ProbeSpelling.CONSTANT_FALSE_FILTER) {
      final Map<String, PostgresType> sampled = sampleProbeColumns(parsed, parameters);
      if (sampled != null && !sampled.isEmpty())
        return sampled;
    }

    if (query.toUpperCase(Locale.ENGLISH).trim().startsWith("SELECT") || parsed instanceof MatchStatement) {
      final Map<String, PostgresType> schemaColumns = getColumnsFromQuerySchema(query, parsed);
      if (schemaColumns != null && !schemaColumns.isEmpty())
        return schemaColumns;
    }

    // LIMIT 0 is the other spelling of a probe (issue #6185), but unlike a constant-false filter it is also how a
    // client asks an expensive query for nothing at all. So it earns the replay - which evaluates the projection
    // for real, once - only where nothing else can answer: after the static resolution above has come back empty.
    if (probe == ProbeSpelling.LIMIT_ZERO) {
      final Map<String, PostgresType> sampled = sampleProbeColumns(parsed, parameters);
      if (sampled != null && !sampled.isEmpty())
        return sampled;
    }

    return null;
  }

  /**
   * How a statement says, in its own text, that it cannot return a row. Both spellings mark a schema probe, and
   * the caller replays either of them, but they do not deserve the replay equally - see
   * {@link #resolveEmptyResultSchemaColumns}, which orders them against the static resolution.
   */
  private enum ProbeSpelling {
    NOT_A_PROBE, CONSTANT_FALSE_FILTER, LIMIT_ZERO
  }

  /**
   * Reads off the statement, or a subquery it selects from, whether it is empty by construction and how it says
   * so - the same question {@code SelectExecutionPlanner.emptyByConstructionReason} asks when it folds the fetch
   * away. A constant-false filter wins over a {@code LIMIT 0} found at another level, because it is the spelling
   * that can have no other purpose.
   * <p>
   * Nothing here evaluates a function or reads a bound parameter: {@code WhereClause.isAlwaysFalse} folds only
   * comparisons between literals, and {@link Limit#isAlwaysEmpty()} only a literal {@code LIMIT 0}. This is asked
   * of every query that comes back empty, the vast majority of which are not probes at all.
   */
  private ProbeSpelling probeSpelling(final Statement parsed) {
    if (!(parsed instanceof SelectStatement select))
      return ProbeSpelling.NOT_A_PROBE;

    // the database has to be on the context: the constant-folding below needs it
    final CommandContext context = createProbeContext();

    ProbeSpelling spelling = ProbeSpelling.NOT_A_PROBE;
    for (SelectStatement level = select; level != null; level = selectedSubQuery(level)) {
      if (isAlwaysFalseFilter(level.getWhereClause(), context))
        return ProbeSpelling.CONSTANT_FALSE_FILTER;

      final Limit limit = level.getLimit();
      if (limit != null && limit.isAlwaysEmpty())
        spelling = ProbeSpelling.LIMIT_ZERO;
    }

    return spelling;
  }

  /**
   * Runs the probe again with the clauses that empty it removed, and reports the columns of the first row it
   * returns. This is what makes a probe describable no matter how its rows are computed: whatever the un-probed
   * query would send on the wire is exactly what gets announced. Returns null when the replay finds no row.
   * <p>
   * Only a statement the caller has already classified as a probe ({@link #probeSpelling}) reaches this, so the
   * AST deep copy below is never paid for by a query that legitimately returns no rows - the common case on this
   * path by a wide margin.
   * <p>
   * Note that the replay evaluates the query's projection for real, once, on one row. The original probe never
   * did: its filter, or its LIMIT 0, discards every row before the projection runs. So a projected function that
   * has a side effect - a sequence's {@code next()}, a user-defined function that writes - is invoked once per
   * probe that takes this path. The alternative is to describe a computed projection by guessing, which is what
   * left this whole family of queries undescribable in the first place; the statically-resolvable shapes are
   * answered without a replay by {@link #getColumnsFromQuerySchema} whenever it can name their columns.
   */
  private Map<String, PostgresType> sampleProbeColumns(final Statement parsed, final Object[] parameters) {
    if (!(parsed instanceof SelectStatement select))
      return null;

    try {
      // the database has to be on the context: the replay runs against it
      final CommandContext context = createProbeContext();

      final SelectStatement sample = select.copy();
      stripProbe(sample, context);

      final ResultSet resultSet = sample.execute(database, parameters != null ? parameters : NO_PARAMETERS, context);
      final List<Result> sampleRows = browseAndCacheResultSet(resultSet, 1, false);
      return sampleRows.isEmpty() ? null : getColumns(sampleRows, resolveQueryTargetType(select), resolveAliasToSourceProperty(select));
    } catch (final Exception e) {
      if (DEBUG)
        LogManager.instance().log(this, Level.WARNING, "PSQL: cannot replay the schema probe '%s': %s", parsed, e.getMessage());
      return null;
    }
  }

  private CommandContext createProbeContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setConfiguration(server.getConfiguration());
    context.setDatabase(database);
    return context;
  }

  /**
   * Turns a copy of the probe into the query it is a probe of, in place: the constant-false filters go, and so do
   * the clauses that only choose which rows come back - the {@code LIMIT 0} of a probe spelled that way among them.
   * Dropping ORDER BY, SKIP and LIMIT costs nothing - the columns of a row do not depend on where it sits in the
   * result - and it keeps the replay from materializing and sorting a whole result set just to hand over its first
   * row.
   * <p>
   * A whole WHERE clause goes, not just the constant-false term inside it, so {@code WHERE 1=0 AND name = :n}
   * samples the unfiltered target. That rests on the caller reading nothing but column names and types off the
   * sampled row ({@link #getColumns}): the row itself never reaches the wire. Should RowDescription ever start
   * carrying anything derived from sample <i>values</i>, the predicate dropped here would begin to matter and
   * only the constant-false term could be removed.
   */
  private void stripProbe(final SelectStatement select, final CommandContext context) {
    if (isAlwaysFalseFilter(select.getWhereClause(), context))
      select.setWhereClause(null);

    select.orderBy = null;
    select.skip = null;
    select.limit = null;

    final SelectStatement subQuery = selectedSubQuery(select);
    if (subQuery != null)
      stripProbe(subQuery, context);
  }

  private boolean isAlwaysFalseFilter(final WhereClause where, final CommandContext context) {
    return where != null && where.isAlwaysFalse(context);
  }

  private SelectStatement selectedSubQuery(final SelectStatement select) {
    final FromClause target = select.getTarget();
    final FromItem item = target != null ? target.getItem() : null;
    return item != null && item.getStatement() instanceof SelectStatement subQuery ? subQuery : null;
  }

  private void writeRowDescription(final Map<String, PostgresType> columns) {
    writeRowDescription(columns, null);
  }

  private void writeRowDescription(final Map<String, PostgresType> columns, final List<Integer> resultFormats) {
    if (columns == null)
      return;

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL:-> RowDescription: %d columns: %s (thread=%s)",
          columns.size(), columns.keySet(), Thread.currentThread().threadId());

//    final ByteBuffer bufferDescription = ByteBuffer.allocate(64 * 1024).order(ByteOrder.BIG_ENDIAN);
    final Binary bufferDescription = new Binary();

    int colIndex = 0;
    for (final Map.Entry<String, PostgresType> col : columns.entrySet()) {
      final String columnName = col.getKey();
      final PostgresType columnType = col.getValue();

      bufferDescription.putByteArray(columnName.getBytes(DatabaseFactory.getDefaultCharset()));//The field name.
      bufferDescription.putByte((byte) 0);

      //If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
      bufferDescription.putInt(0);
      //If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
      bufferDescription.putShort((short) 0);
      // The object ID of the field's data type.
      bufferDescription.putInt(columnType.code);
      // The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
      bufferDescription.putShort((short) columnType.size);
      // The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
      bufferDescription.putInt(-1);
      // The format code being used for the field (0=text, 1=binary). Comes from the Bind message's
      // result-column formats when present; defaults to 0 (text) otherwise. Types that lack a
      // binary encoder (arrays) are forced to text so the announced format and DataRow agree.
      bufferDescription.putShort(effectiveResultFormat(resultFormats, colIndex++, columnType));
    }

    bufferDescription.flip();
    writeMessage("row description", () -> {
      channel.writeUnsignedShort((short) columns.size());
      channel.writeBuffer(bufferDescription.getByteBuffer());
    }, 'T', 4 + 2 + bufferDescription.limit());
  }

  /**
   * Returns the wire format code (0=text, 1=binary) requested for the column at {@code colIndex}.
   * Mirrors the Postgres protocol rules for the {@code formats} list in a Bind message:
   * empty list -> text, single entry -> applies to all columns, otherwise per-column.
   */
  private static short resolveResultFormat(final List<Integer> resultFormats, final int colIndex) {
    if (resultFormats == null || resultFormats.isEmpty())
      return 0;
    if (resultFormats.size() == 1)
      return resultFormats.get(0).shortValue();
    if (colIndex < resultFormats.size())
      return resultFormats.get(colIndex).shortValue();
    return 0;
  }

  /**
   * Same as {@link #resolveResultFormat} but forces text (0) for columns whose type lacks a
   * binary encoder. Used by both RowDescription and DataRow so the announced format code and the
   * written bytes always agree, even when the client requested binary.
   */
  private static short effectiveResultFormat(final List<Integer> resultFormats, final int colIndex,
      final PostgresType columnType) {
    if (!columnType.hasBinaryEncoding())
      return 0;
    return resolveResultFormat(resultFormats, colIndex);
  }

  private void writeDataRows(final List<Result> resultSet, final Map<String, PostgresType> columns) throws IOException {
    writeDataRows(resultSet, columns, null);
  }

  private void writeDataRows(final List<Result> resultSet, final Map<String, PostgresType> columns,
      final List<Integer> resultFormats) throws IOException {
    if (resultSet.isEmpty())
      return;

    final Binary bufferData = new Binary();
    final Binary bufferValues = new Binary();

    for (final Result row : resultSet) {
      bufferValues.putShort((short) columns.size()); // Int16 The number of column values that follow (possibly zero).

      int colIndex = 0;
      for (final Map.Entry<String, PostgresType> postgresTypeEntry : columns.entrySet()) {
        final String propertyName = postgresTypeEntry.getKey();

        Object value = switch (propertyName) {
          case RID_PROPERTY -> row.isElement() ? row.getElement().get().getIdentity() : row.getProperty(propertyName);
          case TYPE_PROPERTY -> row.isElement() ? row.getElement().get().getTypeName() : row.getProperty(propertyName);
          case OUT_PROPERTY -> {
            if (row.isElement()) {
              final Document record = row.getElement().get();
              if (record instanceof Vertex vertex)
                yield vertex.countEdges(Vertex.DIRECTION.OUT, null);
              else if (record instanceof Edge edge)
                yield edge.getOut();
            }
            yield row.getProperty(propertyName);
          }
          case IN_PROPERTY -> {
            if (row.isElement()) {
              final Document record = row.getElement().get();
              if (record instanceof Vertex vertex)
                yield vertex.countEdges(Vertex.DIRECTION.IN, null);
              else if (record instanceof Edge edge)
                yield edge.getIn();
            }
            yield row.getProperty(propertyName);
          }
          case CAT_PROPERTY -> {
            if (row.isElement()) {
              final Document record = row.getElement().get();
              if (record instanceof Vertex)
                yield "v";
              else if (record instanceof Edge)
                yield "e";
              else
                yield "d";
            }
            yield row.getProperty(propertyName);
          }
          default -> {
            Object v = row.getProperty(propertyName);
            // When content map exists but doesn't have the property (e.g., OpenCypher RETURN n
            // sets content with variable name but element has the actual properties), fall back to element
            if (v == null && row.isElement())
              v = row.getElement().get().get(propertyName);
            yield v;
          }
        };

        final PostgresType columnType = postgresTypeEntry.getValue();
        if (effectiveResultFormat(resultFormats, colIndex++, columnType) == 1)
          columnType.serializeAsBinary(columnType, bufferValues, value);
        else
          columnType.serializeAsText(columnType, bufferValues, value);
      }

      bufferValues.flip();
      final int dataRowLength = 4 + bufferValues.getByteBuffer().limit();
      bufferData.putByte((byte) 'D');
      bufferData.putInt(dataRowLength);
      bufferData.putBuffer(bufferValues.getByteBuffer());

      if (DEBUG)
        LogManager.instance().log(this, Level.INFO,
            "PSQL:-> DataRow: cols=%d, bufferValues=%d, dataRowLength=%d, bufferData=%d (thread=%s)",
            columns.size(), bufferValues.getByteBuffer().limit(), dataRowLength,
            bufferData.position(), Thread.currentThread().threadId());

      bufferData.flip();
      channel.writeBuffer(bufferData.getByteBuffer());

      bufferData.clear();
      bufferValues.clear();
    }

    channel.flush();

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL:-> %d row(s) data written (thread=%s)", resultSet.size(),
          Thread.currentThread().threadId());
  }

  private void bindCommand() {
    // Track read progress so a mid-message exception can drain the remaining Bind bytes
    // and keep the channel aligned for the next message.
    int totalParamValues = 0;
    int paramsConsumed = 0;
    boolean resultFormatSectionRead = false;
    try {
      // BIND
      final String portalName = readString();
      final String sourcePreparedStatement = readString();

      // Look up the prepared statement (stored during PARSE) and create THIS Bind's own independent portal
      // from it (issue #6660 / CodeRabbit review on #6658). PARSE's PostgresPortal is a read-only template
      // from here on - bindFrom() copies what PARSE already fixed for the statement (query, sqlStatement,
      // parameter types, and any response PARSE precomputed for BEGIN/COMMIT/ROLLBACK or a resolved catalog
      // answer) into a fresh object, so that two portal names bound from the same statement - or the same
      // portal name re-bound without a new Parse, which is exactly what asyncpg's/pgjdbc's statement caching
      // does for a repeated query - never share mutable execution state (parameterValues, fullResultSet,
      // resultCursor, suspended, rowDescriptionSent...). Without this, a later Bind on one portal name could
      // silently reset or overwrite another already-bound (possibly suspended) portal from the same statement,
      // since both names used to point at the very same object.
      // If the prepared statement is missing/closed, use a consume-only throwaway portal to drain parameters
      // off the wire and avoid channel framing corruption, without resurrecting any previously bound portal.
      final PostgresPortal preparedStatement = preparedStatements.get(sourcePreparedStatement);
      final PostgresPortal portal = preparedStatement != null ? PostgresPortal.bindFrom(preparedStatement) : new PostgresPortal("", "sql");

      if (DEBUG)
        LogManager.instance()
            .log(this, Level.INFO, "PSQL: bind (portal=%s) -> %s (thread=%s)", portalName, sourcePreparedStatement,
                Thread.currentThread().threadId());

      final int paramFormatCount = channel.readShort();
      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: bind paramFormatCount=%d (thread=%s)",
            paramFormatCount, Thread.currentThread().threadId());
      if (paramFormatCount > 0) {
        portal.parameterFormats = new ArrayList<>(paramFormatCount);
        for (int i = 0; i < paramFormatCount; i++) {
          final int formatCode = channel.readUnsignedShort();
          portal.parameterFormats.add(formatCode);
        }
      }

      final int paramValuesCount = channel.readShort();
      totalParamValues = paramValuesCount;
      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: bind paramValuesCount=%d (thread=%s)",
            paramValuesCount, Thread.currentThread().threadId());
      if (paramValuesCount > 0) {
        portal.parameterValues = new ArrayList<>(paramValuesCount);
        for (int i = 0; i < paramValuesCount; i++) {
          if (DEBUG)
            LogManager.instance().log(this, Level.INFO, "PSQL: bind reading param %d size (thread=%s)", i, Thread.currentThread().threadId());
          final long paramSize = channel.readUnsignedInt();
          if (DEBUG)
            LogManager.instance().log(this, Level.INFO, "PSQL: bind param %d size=%d (thread=%s)", i, paramSize, Thread.currentThread().threadId());

          if (paramSize == NULL_PARAM_LENGTH) {
            // Postgres protocol NULL sentinel: a declared length of -1 (0xFFFFFFFF unsigned), with no
            // value bytes following. Must be checked before the max-size guard below, since the unsigned
            // reading of -1 is far larger than any realistic configured limit.
            portal.parameterValues.add(null);
            paramsConsumed = i + 1;
            continue;
          }

          if (paramSize > GlobalConfiguration.POSTGRES_MAX_PARAM_SIZE.getValueAsInteger()) {
            // The value bytes for this parameter were never read, so the channel cannot be safely
            // resynchronized without draining a client(attacker)-controlled amount of data - that would
            // either reintroduce an unbounded read or block the connection thread indefinitely if the
            // declared bytes never arrive. Tell the client why, then close the connection outright
            // rather than gamble on realigning the stream.
            setErrorInTx();
            writeError(ERROR_SEVERITY.FATAL, "Postgres bind parameter too large: " + paramSize + " bytes (max "
                + GlobalConfiguration.POSTGRES_MAX_PARAM_SIZE.getValueAsInteger() + ")", "08P01");
            shutdown = true;
            return;
          }
          final byte[] paramValue = new byte[(int) paramSize];
          channel.readBytes(paramValue);
          // The length prefix and value bytes for this parameter are now fully consumed off the wire,
          // regardless of whether deserialize() below succeeds. Advance paramsConsumed here (rather than
          // after a successful deserialize()) so the catch block's drain/recovery loop always restarts at
          // the correct wire offset if deserialize() throws.
          paramsConsumed = i + 1;
          if (DEBUG)
            LogManager.instance().log(this, Level.INFO, "PSQL: bind param %d value read (thread=%s)", i, Thread.currentThread().threadId());

          // Determine format code according to PostgreSQL protocol:
          // - If paramFormatCount == 0: all parameters use text format (0)
          // - If paramFormatCount == 1: all parameters use that single format code
          // - Otherwise: each parameter uses its corresponding format code
          final int formatCode;
          if (portal.parameterFormats == null || portal.parameterFormats.isEmpty()) {
            formatCode = 0; // Default to text format
          } else if (portal.parameterFormats.size() == 1) {
            formatCode = portal.parameterFormats.get(0); // Single format for all
          } else {
            formatCode = portal.parameterFormats.get(i); // Per-parameter format
          }

          // Determine type code - use UNSPECIFIED (0) if not declared in PARSE
          final long typeCode = portal.parameterTypes != null && i < portal.parameterTypes.size()
              ? portal.parameterTypes.get(i)
              : 0L; // UNSPECIFIED type

          if (DEBUG)
            LogManager.instance().log(this, Level.INFO, "PSQL: bind deserializing param %d typeCode=%d formatCode=%d (thread=%s)",
                i, typeCode, formatCode, Thread.currentThread().threadId());
          portal.parameterValues.add(PostgresType.deserialize(typeCode, formatCode, paramValue));
          if (DEBUG)
            LogManager.instance().log(this, Level.INFO, "PSQL: bind param %d deserialized (thread=%s)", i, Thread.currentThread().threadId());
        }
      }

      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: bind reading resultFormatCount (thread=%s)", Thread.currentThread().threadId());
      final int resultFormatCount = channel.readShort();
      if (resultFormatCount > 0) {
        portal.resultFormats = new ArrayList<>(resultFormatCount);
        for (int i = 0; i < resultFormatCount; i++) {
          final int resultFormat = channel.readUnsignedShort();
          portal.resultFormats.add(resultFormat);
        }
        if (DEBUG)
          LogManager.instance().log(this, Level.INFO, "PSQL: bind resultFormats=%s (0=text, 1=binary) (thread=%s)",
              portal.resultFormats, Thread.currentThread().threadId());
      }
      resultFormatSectionRead = true;

      if (errorInTransaction) {
        // The Bind message is already fully consumed off the wire at this point (parameter values and
        // result-format codes), so no drain is needed. Mirror the simple-query fix from #6542/#6457: refuse
        // with an ErrorResponse instead of silently returning, so the client knows this Bind never ran
        // (issue #6545). errorInTransaction stays set until COMMIT/ROLLBACK/END ends the block.
        writeError(ERROR_SEVERITY.ERROR,
            "current transaction is aborted, commands ignored until end of transaction block", "25P02");
        return;
      }

      // Store this Bind's own portal under the portal name (which may be empty for unnamed portal) - always,
      // even when portalName equals sourcePreparedStatement, since this is a fresh clone now rather than the
      // statement's own template object (issue #6660 / CodeRabbit review on #6658).
      // This is necessary because EXECUTE looks up portals by portal name, not prepared statement name.
      // PostgreSQL protocol: PARSE creates "prepared statement", BIND creates "portal" from it.
      // If the source prepared statement was closed or non-existent, invalidate/remove any previously bound
      // portal under this name so Execute returns NoData.
      if (preparedStatement != null) {
        portals.put(portalName, portal);
        if (DEBUG)
          LogManager.instance().log(this, Level.INFO, "PSQL: bind stored portal under name '%s' (thread=%s)",
              portalName, Thread.currentThread().threadId());
      } else {
        portals.remove(portalName);
      }

      writeMessage("bind complete", null, '2', 4);

    } catch (final Exception e) {
      // Best-effort drain of the remaining Bind message so the channel stays aligned
      // for the next message in the pipelined client request (Describe, Execute, Sync).
      boolean drainedCleanly = true;
      try {
        for (int i = paramsConsumed; i < totalParamValues; i++) {
          final long sz = channel.readUnsignedInt();
          if (sz == NULL_PARAM_LENGTH)
            continue;
          if (sz > GlobalConfiguration.POSTGRES_MAX_PARAM_SIZE.getValueAsInteger()) {
            // Same reasoning as the main loop's guard: draining a declared-but-undelivered amount of
            // data risks an unbounded/blocking read, so give up on resyncing rather than attempt it.
            drainedCleanly = false;
            break;
          }
          if (sz > 0)
            channel.readBytes(new byte[(int) sz]);
        }
        if (drainedCleanly && !resultFormatSectionRead) {
          final int resultFormatCount = channel.readShort();
          for (int i = 0; i < resultFormatCount; i++)
            channel.readUnsignedShort();
        }
      } catch (final Exception ignored) {
        // If even the drain fails the channel is unrecoverable; the error response below
        // still goes out and the client will see the failure.
        drainedCleanly = false;
      }
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Error on parsing bind message: " + e.getMessage(), sqlStateFor(e));
      if (!drainedCleanly)
        shutdown = true;
    }
  }

  private void parseCommand() {
    try {
      // PARSE
      final String portalName = readString();

      final Query query = getLanguageAndQuery(readString());

      final PostgresPortal portal = new PostgresPortal(query.query, query.language);
      final int paramCount = channel.readShort();

      if (paramCount > 0) {
        portal.parameterTypes = new ArrayList<>(paramCount);
        for (int i = 0; i < paramCount; i++) {
          final long param = channel.readUnsignedInt();
          portal.parameterTypes.add(param);
        }
      } else {
        // Client sent paramCount=0 (e.g., asyncpg, node-postgres)
        // Detect $N placeholders in the query to determine actual parameter count
        final int detectedParams = detectParameterPlaceholders(query.query);
        if (detectedParams > 0) {
          portal.parameterTypes = new ArrayList<>(detectedParams);
          for (int i = 0; i < detectedParams; i++) {
            // Use VARCHAR (OID 1043) as default type instead of 0 (unspecified)
            // This prevents asyncpg from trying to introspect unknown types via pg_type
            portal.parameterTypes.add((long) PostgresType.VARCHAR.code);
          }
        }
      }

      final int actualParamCount = portal.parameterTypes != null ? portal.parameterTypes.size() : 0;
      if (DEBUG)
        LogManager.instance()
            .log(this, Level.INFO, "PSQL: parse (portal=%s) -> %s (params=%d, detected=%d) (errorInTransaction=%s thread=%s)",
                portalName, portal.query, paramCount, actualParamCount, errorInTransaction, Thread.currentThread().threadId());

      if (errorInTransaction) {
        // Mirror queryCommand()'s aborted-transaction dispatch (issue #6457/#6542): a client recovering
        // from an aborted transaction over the extended protocol sends its COMMIT/END/ROLLBACK through its
        // own Parse/Bind/Execute round trip rather than a single Query message, and the unconditional
        // return below dropped it silently - Sync's own errorInTransaction branch clears errorInTransaction
        // but never explicitTransactionStarted, so the client was reported status 'T' forever after
        // "recovering" (issue #6548). Every other statement still falls through to the silent return;
        // bindCommand()'s own errorInTransaction check (issue #6545) is what turns an attempt to Bind one
        // of those into an ErrorResponse.
        // Unlike the non-aborted BEGIN/COMMIT/ROLLBACK recognition below (gated inside the "sql" case of the
        // portal.language switch), this check runs regardless of portal.language - intentionally, matching
        // queryCommand()'s own aborted-transaction branch, which has no language gate either. No real client
        // sends a transaction-control statement under a non-"sql" language mid-session.
        // isCommitStatement()/isRollbackStatement() match by exact string equality, so the text they see must
        // be trimmed and stripped of a trailing ';' the same way queryCommand() strips it from its queryText
        // before its own aborted-transaction check runs (issue #6548 review follow-up) - otherwise a client
        // that sends "ROLLBACK;" (a real Postgres statement terminator many drivers append) falls through to
        // the silent return below, reproducing this exact issue's "wedged forever" symptom via a trailing
        // semicolon instead of via the missing dispatch. portal.query itself is left untouched here: the
        // matched branch below overwrites it outright, and the unmatched branch discards this portal.
        String abortedText = portal.query.trim();
        if (abortedText.endsWith(";"))
          abortedText = abortedText.substring(0, abortedText.length() - 1);
        final String abortedUpperCaseText = abortedText.toUpperCase(Locale.ENGLISH);
        if (isTransactionEndStatement(abortedUpperCaseText)) {
          if (database.isTransactionActive())
            database.rollback();
          explicitTransactionStarted = false;
          errorInTransaction = false;
          // Real Postgres treats a COMMIT/END of an aborted transaction the same as ROLLBACK - there is
          // nothing left to commit - so the command tag executeCommand() later writes must read ROLLBACK
          // regardless of which of the three keywords the client actually sent (see queryCommand()).
          portal.query = "ROLLBACK";
          setEmptyResultSet(portal);
          preparedStatements.put(portalName, portal);
          writeMessage("parse complete", null, '1', 4);
        }
        return;
      }

      if (portal.query.isEmpty()) {
        emptyQueryResponse();
        return;
      }

      final String upperCaseText = portal.query.toUpperCase(Locale.ENGLISH);
      final PostgresSystemQuery systemQuery = PostgresSystemQuery.parse(portal.query);

      if (upperCaseText.startsWith("SAVEPOINT ") ||
          upperCaseText.startsWith("RELEASE ") ||
          upperCaseText.startsWith("ROLLBACK TO ")) {
        portal.ignoreExecution = true;
      } else if (upperCaseText.startsWith("SET ")) {
        // Strip a trailing ';' before dispatch, mirroring what queryCommand() already does for its own
        // queryText on the simple-query protocol - a Parse message keeps the terminator glued onto the
        // text, which otherwise reaches setConfiguration() attached to the value (issue #6701).
        // portal.query itself is left untouched: nothing downstream needs the terminator removed.
        String setText = portal.query.trim();
        if (setText.endsWith(";"))
          setText = setText.substring(0, setText.length() - 1);
        setConfiguration(setText);
        portal.ignoreExecution = true;
      } else if (systemQuery != null) {
        createResultSet(portal, systemQuery.columnName, systemQueryValue(systemQuery.function));

      } else if ("SHOW TRANSACTION ISOLATION LEVEL".equals(upperCaseText)) {
        final Database.TRANSACTION_ISOLATION_LEVEL dbIsolationLevel = database.getTransactionIsolationLevel();
        final String level = dbIsolationLevel.name().replace('_', ' ');
        createResultSet(portal, "LEVEL", level);

      } else if (upperCaseText.startsWith("SHOW ")) {
        final String varName = portal.query.substring(5).trim().toLowerCase(Locale.ENGLISH);
        createResultSet(portal, varName, getShowConfigValue(varName));

      } else {
        // A query about the emulated system catalog. Every family of these used to be matched by string
        // equality here, several of them only when application_name was literally "dbvis", so the same
        // question asked by any other client fell through to ArcadeDB's SQL engine and failed (issue #6412).
        //
        // One that carries parameters cannot be answered yet: the JDBC driver puts the name patterns of its
        // table and column lists in them, and their values only arrive with the Bind message. It is marked
        // here and answered in executeCommand, with the values in hand.
        final CatalogAnswer catalogAnswer = handleCatalogQuery(portal.query);
        if (catalogAnswer != null && actualParamCount > 0) {
          // Recognised, but its filters are bound parameters whose values only arrive with Bind: the answer
          // computed here would ignore them, so it is thrown away and recomputed in executeCommand.
          portal.catalogQuery = true;
        } else if (catalogAnswer != null) {
          portal.executed = true;
          portal.cachedResultSet = catalogAnswer.rows();
          portal.columns = catalogAnswer.columns();
        } else {
          switch (portal.language) {
          case "sql":
            // BEGIN/COMMIT/ROLLBACK (in any of their recognized forms) are handled directly below and must
            // never reach sqlEngine.parse(): the grammar's beginStatement/commitStatement/rollbackStatement
            // productions accept only the bare keyword (or BEGIN's own ISOLATION clause and COMMIT's own RETRY
            // clause) - "BEGIN TRANSACTION"/"COMMIT WORK"/"ROLLBACK TRANSACTION"/etc. all fail to parse as SQL.
            // Checking first, the same way queryCommand's simple-query dispatch already does, keeps this
            // branch from ever calling parse() on text the grammar was never going to accept (issue #6543).
            if (isBeginStatement(upperCaseText)) {
              explicitTransactionStarted = true;
              setEmptyResultSet(portal);
            } else if (isCommitStatement(upperCaseText)) {
              // No explicit database.commit() here: clearing the flag makes the Sync that follows this
              // Execute take its implicit-commit branch (see syncCommand()), which is what actually
              // persists the transaction.
              explicitTransactionStarted = false;
              setEmptyResultSet(portal);
            } else if (isRollbackStatement(upperCaseText)) {
              // Unlike COMMIT, ROLLBACK cannot lean on Sync's implicit-commit branch - clearing the flag
              // without rolling back here would make the next Sync COMMIT the transaction instead (issue
              // #6543), the opposite of what the client asked for.
              if (explicitTransactionStarted && database.isTransactionActive())
                database.rollback();
              explicitTransactionStarted = false;
              setEmptyResultSet(portal);
            } else {
              final SQLQueryEngine sqlEngine = (SQLQueryEngine) database.getQueryEngine("sql");
              portal.sqlStatement = sqlEngine.parse(query.query, (DatabaseInternal) database);
            }
            break;

          default:
            //nooop
          }
        }
      }

      preparedStatements.put(portalName, portal);

      // ParseComplete
      writeMessage("parse complete", null, '1', 4);

    } catch (final CommandParsingException e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on parsing query: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()), sqlStateFor(e));
    } catch (final Exception e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Error on parsing query: " + e.getMessage(), sqlStateFor(e));
    }
  }

  /**
   * Parses a Postgres {@code SET <param> = <value>} or {@code SET <param> TO <value>} command into its
   * {@code {paramName, value}} pair, honoring the optional PostgreSQL {@code SESSION}/{@code LOCAL} scope
   * modifiers (issue #6701): {@code SET SESSION datestyle = 'ISO'} and {@code SET LOCAL datestyle = 'ISO'}
   * must resolve to the same {@code datestyle} parameter name as a plain {@code SET datestyle = 'ISO'}, not
   * a literal {@code "session datestyle"}/{@code "local datestyle"} that no special case ever matches.
   * ArcadeDB has no notion of transaction-scoped config distinct from session-scoped config, so both
   * modifiers - and no modifier at all - end up folded into the same connection-wide
   * {@link #connectionProperties} map; that already matches the de facto behavior of a plain {@code SET}
   * today.
   * <p>
   * A command can only use one of the two separators, but its value may legitimately contain the other
   * one as plain text (e.g. {@code SET search_path TO 'a=b'} or {@code SET x = 'a TO b'}), so this picks
   * whichever separator - the first '=' or the first case-insensitive ' TO ' - occurs FIRST in the string
   * and splits on that one only, leaving every other occurrence of either inside the value untouched
   * (issue #6423). {@code paramName} is lower-cased for case-insensitive comparison; a quoted {@code value}
   * has its surrounding quotes stripped. Returns null when the command has neither separator.
   */
  static String[] parseSetCommand(final String query) {
    final int setLength = "SET ".length();
    // Use original query to preserve case of values
    String q = query.substring(setLength);

    final Matcher scopeModifier = SET_SCOPE_MODIFIER.matcher(q);
    if (scopeModifier.find())
      q = q.substring(scopeModifier.end());

    final int eqPos = q.indexOf('=');
    final Matcher toMatcher = SET_TO_SEPARATOR.matcher(q);
    final boolean toFound = toMatcher.find();

    final String[] parts;
    if (toFound && (eqPos < 0 || toMatcher.start() < eqPos))
      parts = new String[] { q.substring(0, toMatcher.start()), q.substring(toMatcher.end()) };
    else if (eqPos >= 0)
      parts = StringUtils.splitKeyValue(q);
    else
      return null;

    final String paramName = parts[0].trim().toLowerCase(Locale.ENGLISH);
    if (paramName.isEmpty())
      // An empty parameter name (e.g. "SET = somevalue") is as malformed as a missing separator.
      return null;

    String value = parts[1].trim();
    if (value.startsWith("'") || value.startsWith("\"")) {
      // A quoted value needs a matching closing delimiter: a single stray quote (e.g. "SET x = '") is a
      // malformed command, not a closed quoted value, so it is rejected the same way a missing separator
      // is - rather than throw StringIndexOutOfBoundsException on an unconditional substring(1, length - 1),
      // or silently store a value still carrying its opening quote.
      final char quote = value.charAt(0);
      if (value.length() < 2 || value.charAt(value.length() - 1) != quote)
        return null;
      value = value.substring(1, value.length() - 1);
    }

    return new String[] { paramName, value };
  }

  private void setConfiguration(final String query) {
    final String[] parts = parseSetCommand(query);
    if (parts == null) {
      LogManager.instance().log(this, Level.WARNING, "Invalid SET command format: %s", query);
      return;
    }

    final String paramName = parts[0];
    final String value = parts[1];

    if ("datestyle".equals(paramName)) {
      if ("ISO".equalsIgnoreCase(value))
        database.getSchema().setDateTimeFormat(DateUtils.DATE_TIME_ISO_8601_FORMAT);
      else
        LogManager.instance().log(this, Level.INFO, "datestyle '%s' not supported", value);
    }

    connectionProperties.put(paramName, value);
  }

  /**
   * The value of an emulated system-information function (issue #5290). {@code current_schema} answers the
   * database name rather than PostgreSQL's {@code public}, and {@code current_database} answers the same
   * name: ArcadeDB has one namespace per database, so the distinction PostgreSQL draws between a catalog
   * and a schema inside it has nothing on this side to map onto, and a client using either to qualify a
   * name needs the one name that works.
   */
  private String systemQueryValue(final PostgresSystemQuery.Function function) {
    return switch (function) {
      case VERSION -> buildServerVersionString();
      case CURRENT_SCHEMA, CURRENT_DATABASE, CURRENT_CATALOG -> database.getName();
      case CURRENT_USER, SESSION_USER, CURRENT_ROLE, USER -> userName;
    };
  }

  private String buildServerVersionString() {
    return "PostgreSQL " + PG_SERVER_VERSION + " (ArcadeDB " + Constants.getRawVersion() + ")";
  }

  private String getShowConfigValue(final String varName) {
    return switch (varName) {
      case "server_version" -> PG_SERVER_VERSION;
      case "standard_conforming_strings" -> "on";
      case "integer_datetimes" -> "on";
      case "client_encoding" -> "UTF8";
      case "server_encoding" -> "UTF8";
      case "timezone" -> "UTC";
      default -> "";
    };
  }

  private void setEmptyResultSet(final PostgresPortal portal) {
    portal.executed = true;
    portal.isExpectingResult = true;
    portal.cachedResultSet = Collections.emptyList();
    portal.columns = getColumns(portal.cachedResultSet);
  }

  private void sendServerParameter(final String name, final String value) {
    final byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

    final int length = 4 + nameBytes.length + 1 + valueBytes.length + 1;

    writeMessage("parameter status", () -> {
      writeString(name);
      writeString(value);
    }, 'S', length);
  }

  private boolean openDatabase() {
    if (databaseName == null) {
      writeError(ERROR_SEVERITY.FATAL, "Database not selected", "HV00Q");
      return false;
    }

    try {
      final ServerSecurityUser dbUser = server.getSecurity().authenticate(userName, userPassword, databaseName);

      database = server.getDatabase(databaseName);

      DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(dbUser.getDatabaseUser(database));

      database.setAutoTransaction(true);

    } catch (final ServerSecurityException e) {
      writeError(ERROR_SEVERITY.FATAL, "Credentials not valid", "28P01");
      return false;
    } catch (final DatabaseOperationException e) {
      writeError(ERROR_SEVERITY.FATAL, "Database does not exist", "HV00Q");
      return false;
    }

    return true;
  }

  private boolean readStartupMessage(final boolean no2ssl) {
    try {
      final long len = channel.readUnsignedInt();
      // The declared length used to be read and then ignored, so the parameter loop below ran until the
      // client chose to stop it and every pair it sent was retained (issue #6377). PostgreSQL rejects an
      // over-long startup packet outright rather than counting what is inside it, and this does the same:
      // one rule bounds the pair count, the total bytes and the time spent reading them, and it is the
      // client's own declared length that is being held to, not a limit invented here.
      if (len < 8 || len > MAX_STARTUP_MESSAGE_LENGTH)
        throw new PostgresProtocolException("Invalid startup message length " + len);

      final long protocolVersion = channel.readUnsignedInt();
      if (protocolVersion == 80877103) {
        // REQUEST FOR SSL, NOT SUPPORTED
        if (no2ssl) {
          channel.writeByte((byte) 'N');
          channel.flush();

          LogManager.instance().log(this, Level.INFO,
              "PSQL: received not supported SSL connection request. Sending back error message to the client");

          // REPEAT
          return readStartupMessage(false);
        }

        throw new PostgresProtocolException("SSL authentication is not supported");
      } else if (protocolVersion == 80877102) {
        // CANCEL REQUEST, IGNORE IT
        final long pid = channel.readUnsignedInt();
        final long secret = channel.readUnsignedInt();

        LogManager.instance().log(this, Level.INFO, "PSQL: Received cancel request pid %d", pid);

        final Pair<Long, PostgresNetworkExecutor> session = ACTIVE_SESSIONS.get(pid);
        if (session != null) {
          if (session.getFirst() == secret) {
            LogManager.instance().log(this, Level.INFO, "PSQL: Canceling session " + pid);
            session.getSecond().close();
          } else
            LogManager.instance().log(this, Level.INFO, "PSQL: Blocked unauthorized canceling session " + pid);
        } else
          LogManager.instance().log(this, Level.INFO, "PSQL: Session " + pid + " not found");

        close();
        return false;
      }

      if (len > 8) {
        final byte[] body = new byte[(int) (len - 8)];
        channel.readBytes(body);
        readStartupParameters(body);
      }
    } catch (final IOException e) {
      setErrorInTx();
      throw new PostgresProtocolException("Error on parsing startup message", e);
    }
    return true;
  }

  /**
   * Reads the {@code name\0value\0...\0} parameter section of a startup message out of the bytes the
   * message declared, rather than off the socket until the client sends a terminator (issue #6377).
   */
  private void readStartupParameters(final byte[] body) {
    int pos = 0;
    while (pos < body.length && body[pos] != 0) {
      final int nameEnd = indexOfTerminator(body, pos);
      final String paramName = new String(body, pos, nameEnd - pos, DatabaseFactory.getDefaultCharset());
      pos = nameEnd + 1;

      final int valueEnd = indexOfTerminator(body, pos);
      final String paramValue = new String(body, pos, valueEnd - pos, DatabaseFactory.getDefaultCharset());
      pos = valueEnd + 1;

      switch (paramName) {
      case "user":
        userName = paramValue;
        break;
      case "database":
        databaseName = paramValue;
        break;
      case "options":
        // DEPRECATED, IGNORE IT
        break;
      case "replication":
        // NOT SUPPORTED, IGNORE IT
        break;
      }

      connectionProperties.put(paramName, paramValue);
    }
  }

  private static int indexOfTerminator(final byte[] body, final int from) {
    for (int i = from; i < body.length; i++)
      if (body[i] == 0)
        return i;
    throw new PostgresProtocolException("Unterminated string in startup message");
  }

  /**
   * The SQLSTATE for a failure raised while running a statement (issue #5628). Everything used to be reported as
   * {@code XX000} internal_error, which tells a driver the server broke - so a caller who divided by zero, hit a
   * unique index or lost an optimistic-concurrency race got a code that made their own mistake look like ours, and
   * a retryable conflict got a code no driver retries.
   * <p>
   * The classification itself lives in {@link ErrorCategory} so every wire protocol answers it the same way; only
   * the translation into Postgres' vocabulary is here.
   * <p>
   * {@code SCHEMA} reports {@code 42P01} undefined_table because the query-reachable case is overwhelmingly a
   * missing type, which is this database's table; the same category also covers a missing bucket or property, for
   * which a Postgres client would rather have seen {@code 42704}. As with the arithmetic split, the class - here
   * 42, syntax error or access rule violation - carries the client-vs-server verdict either way.
   */
  static String sqlStateFor(final Throwable error) {
    return switch (ErrorCategory.of(error)) {
      case RETRY -> "40001";          // serialization_failure - the code drivers auto-retry on
      case ARITHMETIC -> arithmeticSqlState(error);
      case DUPLICATED_KEY -> "23505"; // unique_violation
      case NOT_FOUND -> "P0002";      // no_data_found - class 02 is a completion condition, not an error
      case SCHEMA -> "42P01";         // undefined_table - a type is this database's table
      case SECURITY -> "42501";       // insufficient_privilege
      case VALIDATION -> "22023";     // invalid_parameter_value
      case PARSING -> "42601";        // syntax_error
      case TIMEOUT -> "57014";        // query_canceled
      case SERVER -> "XX000";         // internal_error
    };
  }

  /**
   * Splits ArcadeDB's two arithmetic failures into the two SQLSTATEs Postgres uses for them. Both live in class 22
   * (data exception), so the client-vs-server verdict does not depend on getting the split right; a message the
   * engine grows later and this does not recognise still reports as a data exception rather than a server fault.
   */
  private static String arithmeticSqlState(final Throwable error) {
    final ArithmeticErrorException arithmetic = CauseChain.find(error, ArithmeticErrorException.class);
    final String message = arithmetic != null ? arithmetic.getMessage() : null;
    return message != null && message.contains("by zero") ?
        "22012" :   // division_by_zero
        "22003";    // numeric_value_out_of_range
  }

  private void writeError(final ERROR_SEVERITY severity, final String errorMessage, final String errorCode) {
    try {
      final String sev = severity.toString();

      final int length = 4 + //
          1 + errorMessage.getBytes(StandardCharsets.UTF_8).length + 1 +//
          1 + sev.getBytes(StandardCharsets.UTF_8).length + 1 +//
          1 + errorCode.getBytes(StandardCharsets.UTF_8).length + 1 +//
          1;

      channel.writeByte((byte) 'E');
      channel.writeUnsignedInt(length);

      channel.writeByte((byte) 'M');
      writeString(errorMessage);

      channel.writeByte((byte) 'S');
      writeString(sev);

      channel.writeByte((byte) 'C');
      writeString(errorCode);

      channel.writeByte((byte) 0);
      channel.flush();
    } catch (final IOException e) {
      setErrorInTx();
      throw new PostgresProtocolException("Error on sending error '" + errorMessage + "' to the client", e);
    }
  }

  private void writeMessage(final String messageName, final WriteMessageCallback callback, final char messageCode,
      final long length) {
    try {
      channel.writeByte((byte) messageCode);
      channel.writeUnsignedInt((int) length);
      if (callback != null)
        callback.write();
      channel.flush();

      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL:-> %s (%s - %s) (thread=%s)", null, messageName, messageCode,
            FileUtils.getSizeAsString(length), Thread.currentThread().threadId());

    } catch (final IOException e) {
      setErrorInTx();
      throw new PostgresProtocolException("Error on sending '" + messageName + "' message", e);
    }
  }

  /**
   * Reads the client's next message, blocking on the socket until its first byte arrives (issue #6410).
   * <p>
   * This used to open with {@code if (!channel.inputHasData()) { sleep(100); return false; }}, which made
   * every idle connection wake its thread ten times a second to ask an empty socket whether anything had
   * turned up - and a Postgres client pool is expected to hold long-lived, mostly-idle connections, so the
   * cost was 10 wakeups per second per pooled connection, for no work. It also meant the end of the stream
   * was never read: a client that went away left its thread polling a socket that would never produce
   * another byte, because {@code available()} answers 0 for a closed peer exactly as it does for an idle one.
   * <p>
   * Blocking is safe on both of the paths that have to keep working. A server-side {@link #close()} - the
   * cancel-request path uses it on another connection's executor - closes the socket, and closing a socket
   * breaks a thread blocked reading it. A client that closes cleanly produces the EOF the arm below already
   * handled. Before authentication the socket carries the handshake timeout armed in the constructor, so a
   * silent client is bounded by it rather than by a poll loop of this method's own (issue #6377).
   *
   * @return false when the connection is finished - end of stream, or a socket closed under it - in which
   * case the caller must stop reading from it. True when a message was read and dispatched.
   */
  private boolean readMessage(final String messageName, final ReadMessageCallback callback, final char... expectedMessageCodes) {
    try {
      final char type = (char) readNextByte();
      final long length = channel.readUnsignedInt();

      if (expectedMessageCodes != null && expectedMessageCodes.length > 0) {
        // VALIDATE MESSAGES
        boolean valid = false;
        for (int i = 0; i < expectedMessageCodes.length; i++) {
          if (type == expectedMessageCodes[i]) {
            valid = true;
            break;
          }
        }

        if (!valid) {
          // READ TILL THE END OF THE MESSAGE
          if (length > 4)
            readBytes((int) (length - 4));
          throw new PostgresProtocolException("Unexpected message type '" + type + "' for message " + messageName);
        }
      }

      //if (length > 4)
      callback.read(type, length - 4);

      return true;

    } catch (final EOFException e) {
      // CLIENT CLOSES THE CONNECTION
      setErrorInTx();
      return false;
    } catch (final IOException e) {
      setErrorInTx();
      if (shutdown)
        // The socket was closed under the blocked read, by this executor's own close() - the cancel-request
        // path calls it from another connection's thread. That is a shutdown, not a protocol error, and it
        // must not be logged as one.
        return false;
      throw new PostgresProtocolException("Error on reading " + messageName + " message: " + e.getMessage(), e);
    }
  }

  private int readNextByte() throws IOException {
    if (reuseLastByte) {
      // USE THE BYTE ALREADY READ
      reuseLastByte = false;
      return nextByte;
    }

    return nextByte = channel.readUnsignedByte();
  }

  private void reuseLastByte() {
    reuseLastByte = true;
  }

  private String readString() throws IOException {
    int len = 0;
    for (; len < buffer.length; len++) {
      final int b = readNextByte();
      if (b == 0)
        return new String(buffer, 0, len, DatabaseFactory.getDefaultCharset());

      buffer[len] = (byte) b;
    }

    len = readUntilTerminator(len);

    throw new PostgresProtocolException("String content (" + len + ") too long (>" + BUFFER_LENGTH + ")");
  }

  private void writeString(final String text) throws IOException {
    channel.writeBytes(text.getBytes(StandardCharsets.UTF_8));
    channel.writeByte((byte) 0);
  }

  private int readUntilTerminator(int len) throws IOException {
    // OUT OF BUFFER SIZE, CONTINUE READING AND DISCARD THE CONTENT
    for (; readNextByte() != 0; len++) {
    }
    return len;
  }

  private void readBytes(final int len) throws IOException {
    for (int i = 0; i < len; i++)
      readNextByte();
  }

  private void writeCommandComplete(final String queryText, final int resultSetCount) {
    final String upperCaseText = queryText.toUpperCase(Locale.ENGLISH);
    final String tag = getTag(upperCaseText, resultSetCount);
    writeMessage("command complete",
        () -> writeString(tag), 'C', 4 + tag.length() + 1);
  }

  private String getTag(String upperCaseText, int resultSetCount) {
    if (upperCaseText.startsWith("CREATE VERTEX") || upperCaseText.startsWith("INSERT INTO")) {
      return "INSERT 0 " + resultSetCount;
    } else if (upperCaseText.startsWith("SELECT") || upperCaseText.startsWith("MATCH")) {
      return "SELECT " + resultSetCount;
    } else if (upperCaseText.startsWith("UPDATE")) {
      return "UPDATE " + resultSetCount;
    } else if (upperCaseText.startsWith("DELETE")) {
      return "DELETE " + resultSetCount;
    } else if (isBeginStatement(upperCaseText)) {
      return "BEGIN";
    } else if (isCommitStatement(upperCaseText)) {
      return "COMMIT";
    } else if (isRollbackStatement(upperCaseText)) {
      return "ROLLBACK";
    } else if (upperCaseText.startsWith("ROLLBACK TO ")) {
      return "ROLLBACK";
    } else if (upperCaseText.startsWith("SAVEPOINT ")) {
      return "SAVEPOINT";
    } else if (upperCaseText.startsWith("RELEASE ")) {
      return "RELEASE";
    } else if (upperCaseText.startsWith("SET ")) {
      return "SET";
    } else {
      return "";
    }
  }

  /**
   * Matches a BEGIN statement, including the SQL-standard {@code ... WORK} form alongside the bare and
   * {@code ... TRANSACTION} forms - the same three-way shape as {@link #isCommitStatement} and
   * {@link #isRollbackStatement} (issue #6543 review follow-up: PostgreSQL's own grammar is
   * {@code BEGIN [ WORK | TRANSACTION ]}).
   */
  private static boolean isBeginStatement(final String upperCaseText) {
    return "BEGIN".equals(upperCaseText) || "BEGIN TRANSACTION".equals(upperCaseText) || "BEGIN WORK".equals(upperCaseText);
  }

  /**
   * Matches a COMMIT/END statement, the counterpart to the existing BEGIN check (issue #6457). Covers the
   * SQL-standard {@code ... WORK} form alongside the bare and {@code ... TRANSACTION} forms (issue #6543).
   */
  private static boolean isCommitStatement(final String upperCaseText) {
    return "COMMIT".equals(upperCaseText) || "COMMIT TRANSACTION".equals(upperCaseText) || "COMMIT WORK".equals(upperCaseText) ||
        "END".equals(upperCaseText) || "END TRANSACTION".equals(upperCaseText) || "END WORK".equals(upperCaseText);
  }

  /**
   * Matches a ROLLBACK statement (issue #6457, extended to the extended-query protocol and the
   * SQL-standard {@code ... WORK} form by issue #6543). Deliberately excludes {@code ROLLBACK TO
   * <savepoint>}, which the dispatch already routes elsewhere (it does not end the transaction).
   */
  private static boolean isRollbackStatement(final String upperCaseText) {
    return "ROLLBACK".equals(upperCaseText) || "ROLLBACK TRANSACTION".equals(upperCaseText) || "ROLLBACK WORK".equals(upperCaseText);
  }

  /**
   * True for any statement that ends an explicit transaction block, whichever keyword the client used
   * (issue #6457). While the transaction is aborted, real Postgres treats all three identically - a plain
   * rollback - rather than distinguishing COMMIT (which would otherwise try to persist writes that
   * already failed).
   */
  private static boolean isTransactionEndStatement(final String upperCaseText) {
    return isCommitStatement(upperCaseText) || isRollbackStatement(upperCaseText);
  }

  private void writeNoData() {
    writeMessage("no data", null, 'n', 4);
  }

  /**
   * Writes ParameterDescription message ('t') describing the parameters of a prepared statement.
   * This is required by the PostgreSQL extended query protocol for DESCRIBE 'S' (Statement).
   */
  private void writeParameterDescription(final PostgresPortal portal) {
    final int paramCount = portal.parameterTypes != null ? portal.parameterTypes.size() : 0;
    // Message format: 't' + int32 length + int16 param count + int32[] type OIDs
    final int messageLength = 4 + 2 + (paramCount * 4);

    writeMessage("parameter description", () -> {
      channel.writeShort((short) paramCount);
      if (portal.parameterTypes != null) {
        for (final Long typeOid : portal.parameterTypes) {
          channel.writeUnsignedInt(typeOid != null ? typeOid.intValue() : 0); // 0 = unspecified type
        }
      }
    }, 't', messageLength);
  }

  /**
   * Detects $N style parameter placeholders in a query and returns the count.
   * PostgreSQL uses $1, $2, etc. for positional parameters.
   * Returns the highest parameter number found (e.g., "$3" returns 3).
   */
  private int detectParameterPlaceholders(final String query) {
    int maxParam = 0;
    final Pattern pattern = Pattern.compile("\\$(\\d+)");
    final Matcher matcher = pattern.matcher(query);
    while (matcher.find()) {
      final int paramNum = Integer.parseInt(matcher.group(1));
      if (paramNum > maxParam) {
        maxParam = paramNum;
      }
    }
    return maxParam;
  }

  private PostgresPortal getPortal(final String name, final boolean remove) {
    if (remove)
      return portals.remove(name);
    else
      return portals.get(name);
  }

  private void createResultSet(final PostgresPortal portal, final Object... elements) {
    portal.executed = true;
    portal.cachedResultSet = createResultSet(elements);
    portal.columns = getColumns(portal.cachedResultSet);
  }

  private List<Result> createResultSet(final Object... elements) {
    if (elements.length % 2 != 0)
      throw new IllegalArgumentException("Result set elements must be in pairs");

    final List<Result> resultSet = new ArrayList<>();
    for (int i = 0; i < elements.length; i += 2) {
      final Map<String, Object> map = new HashMap<>(2);
      map.put((String) elements[i], elements[i + 1]);
      resultSet.add(new ResultInternal(map));
    }
    return resultSet;
  }

  private Query getLanguageAndQuery(final String query) {
    String language = "sql";
    String queryText = query;

    // Regular expression to match language prefixes
    Pattern pattern = Pattern.compile("\\{(\\w+)\\}");
    Matcher matcher = pattern.matcher(query);

    if (matcher.find()) {
      language = matcher.group(1);
      queryText = query.substring(matcher.end()).trim();
    }

    if (QUOTED_IDENTIFIERS && ("sql".equals(language) || "sqlscript".equals(language)) && !isSessionCommand(queryText))
      // POSTGRES CLIENTS QUOTE IDENTIFIERS WITH DOUBLE QUOTES, WHILE ARCADEDB SQL USES BACK-TICKS (ISSUE #5369)
      queryText = PostgresQuotedIdentifierRewriter.rewrite(queryText);

    return new Query(language, queryText);
  }

  /**
   * Session commands such as {@code SET search_path TO "$user", public} are handled by the protocol itself and never
   * reach the SQL engine, so their double quotes must be left alone.
   * <p>
   * The emulated system-information queries are in the same category, and for the same reason: rewriting the
   * quotes in {@code SELECT current_schema() AS "schema"} left an alias in back-ticks that
   * {@link PostgresSystemQuery} no longer recognised, and the query fell through to a SQL engine that has no
   * such function (issue #5290).
   */
  private static boolean isSessionCommand(final String queryText) {
    return queryText.regionMatches(true, 0, "SET ", 0, 4) || queryText.regionMatches(true, 0, "SHOW ", 0, 5)
        || PostgresSystemQuery.parse(queryText) != null;
  }

  private void emptyQueryResponse() {
    writeMessage("empty query response", null, 'I', 4);
  }

  private void portalSuspendedResponse() {
    writeMessage("portal suspended response", null, 's', 4);
  }

  private void setErrorInTx() {
    if (explicitTransactionStarted)
      errorInTransaction = true;
  }

  private record Query(String language, String query) {
  }

}
