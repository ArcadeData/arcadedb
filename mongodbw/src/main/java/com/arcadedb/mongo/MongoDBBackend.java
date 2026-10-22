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
package com.arcadedb.mongo;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.exception.QueryAdmissionException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The ArcadeDB side of the bundled mongo-java-server. Every call the library's Netty handler makes with a client's request
 * enters through one of the {@code MongoBackend} methods overridden here, and each of them tags the thread with the
 * {@value #MONGO_PROTOCOL} protocol for its duration (issue #10390). The tag lives here, where the module's work begins,
 * and not on one of the {@code MongoDatabase} methods the library calls next: it reaches the database wrapper through
 * several doors ({@code handleCommand}; {@code handleQuery} for an OP_QUERY; {@code drop} for a {@code dropDatabase};
 * {@code dropCollection} and {@code unregisterCollection} for a {@code renameCollection}), and a thread that reaches one
 * of them untagged reads {@link ProtocolContext#INTERNAL}, which exempts it from the HA gate that refuses client work on a
 * database whose directory is being replaced from the leader's snapshot, and reports it as the engine's own work.
 */
public class MongoDBBackend extends AbstractMongoBackend {
  /** The protocol a MongoDB client's work is attributed to by the query metrics, the trace spans and the HA gate. */
  static final String MONGO_PROTOCOL = "mongo";

  // MongoDB error codes (see https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml)
  private static final int    AUTHENTICATION_FAILED = 18;
  private static final int    UNAUTHORIZED          = 13;
  // THE RETRYABLE CODE THE DRIVERS RETRY READS AND WRITES ON: THE CLOSEST TO "BUSY, TRY AGAIN" THE PROTOCOL HAS (ISSUE #9518)
  private static final int    EXCEEDED_TIME_LIMIT   = 262;
  private static final int    MECHANISM_UNAVAILABLE = 334;

  // The only SASL mechanism ArcadeDB can support: ArcadeDB stores passwords as one-way PBKDF2 hashes, so
  // challenge-response mechanisms (SCRAM-SHA-1/256) that require the clear-text password server-side are not feasible.
  private static final String PLAIN_MECHANISM       = "PLAIN";

  // SASL PLAIN (RFC 4616) separates [authzid] NUL authcid NUL passwd with the NUL character.
  private static final String SASL_PLAIN_SEPARATOR  = "\u0000";

  private static final String ADMIN_DATABASE = "admin";

  /**
   * Answered by {@code AbstractMongoBackend.handleCommand} itself, before it resolves any database, on whatever database
   * the client names. Lower case.
   */
  private static final Set<String> NO_DATABASE_COMMANDS = Set.of("whatsmyuri", "ismaster", "buildinfo", "ping", "serverstatus");

  /**
   * The admin commands a driver or shell sends while it connects, before it authenticates: answered from constants or not
   * at all, reading no database and no cursor. Lower case.
   */
  private static final Set<String> ADMIN_HANDSHAKE_COMMANDS = Set.of("hello", "getlasterror", "endsessions",
      "connectionstatus", "hostinfo", "getcmdlineopts", "getfreemonitoringstatus", "getlog", "replsetgetstatus",
      "getparameter", "atlasversion");

  private final ArcadeDBServer        server;
  private final MongoDBProtocolPlugin plugin;

  // Per-connection authenticated principal. SASL authentication runs once per channel (handleSaslStart)
  // and every subsequent data command is gated on this state, so the principal survives across commands
  // on the same connection. The entry is removed on channel close (handleClose).
  private final Map<Channel, ServerSecurityUser> authenticatedUsers = new ConcurrentHashMap<>();

  public MongoDBBackend(final ArcadeDBServer server, final MongoDBProtocolPlugin plugin) {
    this.server = server;
    this.plugin = plugin;
  }

  @Override
  protected MongoDatabase openOrCreateDatabase(final String databaseName) throws MongoServerException {
    return new MongoDBDatabaseWrapper(server.getDatabaseForClient(databaseName), plugin, this);
  }

  @Override
  public Document handleCommand(final Channel channel, final String databaseName, final String command, final Document query) {
    final String enclosing = tagAsMongo();
    try {
      return handleClientCommand(channel, databaseName, command, query);
    } finally {
      restoreProtocol(enclosing);
    }
  }

  /**
   * An OP_QUERY on a collection: the library hands it to the database's {@code handleQuery}, past every command, so it is
   * authorized here exactly as a command on that database is. Whatever the database is called: the library resolves (and
   * opens) it by name, so an {@code admin}, {@code local} or {@code $}-prefixed name is no exemption on this door.
   */
  @Override
  public QueryResult handleQuery(final MongoQuery query) {
    final String enclosing = tagAsMongo();
    try {
      final Channel channel = query.getChannel();
      final String databaseName = query.getDatabaseName();
      final ServerSecurityUser user = authorizeOnDatabase(channel, databaseName, "Query on '" + query.getFullCollectionName() + "'");
      return runAuthorized(channel, databaseName, user, true, () -> "query " + query.getFullCollectionName() + " " + query.getQuery(),
          () -> super.handleQuery(query));
    } finally {
      restoreProtocol(enclosing);
    }
  }

  /** An OP_MSG: the library unwraps it into {@link #handleCommand}, which gives this tag back when it returns. */
  @Override
  public Document handleMessage(final MongoMessage message) {
    final String enclosing = tagAsMongo();
    try {
      return super.handleMessage(message);
    } finally {
      restoreProtocol(enclosing);
    }
  }

  /**
   * Reads no database today ({@code AbstractMongoBackend} answers with an empty list), and is tagged anyway so that a
   * library version that starts reading one here does not run untagged.
   */
  @Override
  public Collection<Document> getCurrentOperations(final MongoQuery query) {
    final String enclosing = tagAsMongo();
    try {
      return super.getCurrentOperations(query);
    } finally {
      restoreProtocol(enclosing);
    }
  }

  private Document handleClientCommand(final Channel channel, final String databaseName, final String command,
      final Document query) {
    if ("saslStart".equalsIgnoreCase(command))
      return handleSaslStart(channel, databaseName, query);
    else if ("saslContinue".equalsIgnoreCase(command))
      // A single round-trip is enough for PLAIN, so a saslContinue is unexpected: treat it as a failed conversation.
      throw new MongoServerError(AUTHENTICATION_FAILED, "AuthenticationFailed", "Authentication conversation already completed");

    // The handshake and diagnostic commands the library answers itself, without resolving any database: let through so
    // drivers can complete the connection handshake before authenticating.
    final String normalizedCommand = command.toLowerCase(Locale.ENGLISH);
    if (NO_DATABASE_COMMANDS.contains(normalizedCommand))
      return super.handleCommand(channel, databaseName, command, query);

    // Commands that address a database read or write stored data: they require an authenticated principal that is
    // authorized for that database, and the principal is bound into the engine so per-type ACLs enforce - mirroring the
    // Postgres and BOLT transports. Every name but the virtual admin one is such a database: the library resolves (and
    // opens) "local" or "config" by name exactly like any other, so a database called that is no exemption.
    if (!isAdminDatabase(databaseName)) {
      final ServerSecurityUser user = authorizeOnDatabase(channel, databaseName, "Command '" + command + "'");
      // A cursor continuation is not admitted: it only serves the rows its find already read, and refusing it would break a
      // cursor the client cannot resume
      final boolean admit = !"getmore".equals(normalizedCommand) && !"killcursors".equals(normalizedCommand);
      return runAuthorized(channel, databaseName, user, admit, () -> command + " " + query, () -> super.handleCommand(channel,
          databaseName, command, query));
    }

    // The virtual admin database: the handshake commands stay public, anything else (listDatabases, renameCollection,
    // getMore, killCursors, dropDatabase...) touches the server's databases or another client's cursors and needs a login
    if (!ADMIN_HANDSHAKE_COMMANDS.contains(normalizedCommand)) {
      final ServerSecurityUser user = authenticated(channel, "Command '" + command + "'");
      // these two resolve the database the library is told about, which for the admin namespace is one named "admin"
      if (("dropdatabase".equals(normalizedCommand) || "getmore".equals(normalizedCommand)) && !user.canAccessToDatabase(
          databaseName))
        throw notAuthorizedOn(user, databaseName);
    }
    return super.handleCommand(channel, databaseName, command, query);
  }

  /**
   * The principal authenticated on {@code channel}, revalidated against the live security configuration, and authorized for
   * {@code databaseName}.
   *
   * @throws MongoServerError {@code Unauthorized} when the connection has not authenticated, when its user no longer
   *                          validates, or when the user has no grant on the database
   */
  private ServerSecurityUser authorizeOnDatabase(final Channel channel, final String databaseName, final String what) {
    final ServerSecurityUser user = authenticated(channel, what);
    if (!user.canAccessToDatabase(databaseName))
      throw notAuthorizedOn(user, databaseName);
    return user;
  }

  /**
   * The principal authenticated on {@code channel}, revalidated: a user deleted, re-passworded or stripped of a grant since
   * the SASL login must not keep the access it had when it authenticated.
   */
  private ServerSecurityUser authenticated(final Channel channel, final String what) {
    final ServerSecurityUser user = authenticatedUsers.get(channel);
    if (user == null)
      throw new MongoServerError(UNAUTHORIZED, "Unauthorized", what + " requires authentication");

    try {
      final ServerSecurityUser current = server.getSecurity().revalidate(user);
      if (current != user)
        authenticatedUsers.put(channel, current);
      return current;
    } catch (final ServerSecurityException e) {
      authenticatedUsers.remove(channel);
      throw new MongoServerError(UNAUTHORIZED, "Unauthorized", what + " requires authentication");
    }
  }

  private static MongoServerError notAuthorizedOn(final ServerSecurityUser user, final String databaseName) {
    return new MongoServerError(UNAUTHORIZED, "Unauthorized",
        "User '" + user.getName() + "' is not authorized to access database '" + databaseName + "'");
  }

  /**
   * Runs {@code work} for an authorized principal: through the query admission gate when {@code admit}, listed as a running
   * query, and with the principal bound into {@link DatabaseContext} for the target database so the engine's per-database and
   * per-type ACLs enforce, then cleared. The MongoDB backend processes each connection's requests synchronously on its Netty
   * channel thread, so the binding is confined to the current request.
   */
  private <T> T runAuthorized(final Channel channel, final String databaseName, final ServerSecurityUser user,
      final boolean admit, final Supplier<String> statement, final Supplier<T> work) {
    // The query admission gate (issue #9518), WITHOUT waiting: this runs on the Netty channel thread, which also serves the
    // other connections assigned to it, so it must never park. A request that cannot start at once is refused with a code
    // the drivers retry
    final QueryAdmissionGate.Ticket admission;
    try {
      admission = admit ? QueryAdmissionGate.getInstance().admit(0) : null;
    } catch (final QueryAdmissionException e) {
      throw new MongoServerError(EXCEEDED_TIME_LIMIT, "ExceededTimeLimit", e.getMessage());
    }

    // Listed by "list queries" / SHOW TRANSACTIONS and stopped by a terminate (issue #9689), over the same window as the
    // slot. The request is described only if somebody lists it
    final RunningQuery runningQuery = admission != null ? registerRunningQuery(channel, databaseName, statement, user) : null;
    try {
      final DatabaseInternal database = (DatabaseInternal) server.getDatabaseForClient(databaseName);
      final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.init(database);
      ctx.setCurrentUser(user.getDatabaseUser(database));
      try {
        return work.get();
      } finally {
        ctx.setCurrentUser(null);
      }
    } finally {
      try {
        if (admission != null)
          admission.close();
      } finally {
        // LAST: THE ENTRY LEAVING THE LIST IS THE PROOF THE WORK IS OVER
        if (runningQuery != null)
          runningQuery.close();
      }
    }
  }

  private RunningQuery registerRunningQuery(final Channel channel, final String databaseName, final Supplier<String> statement,
      final ServerSecurityUser user) {
    final RunningQuery runningQuery = server.getRunningQueries().register(databaseName, user.getName(), "mongodb", null, null,
        RunningQuery.Client.of(channel.id().asShortText(), channel.remoteAddress()));
    // Read on the listing thread while the request runs: a document the backend changes meanwhile can at worst fail the
    // read, which RunningQuery.getText() turns into an entry listed without its text
    runningQuery.setStatement("mongo", statement);
    return runningQuery;
  }

  @Override
  public void handleClose(final Channel channel) {
    final String enclosing = tagAsMongo();
    try {
      authenticatedUsers.remove(channel);
      super.handleClose(channel);
    } finally {
      restoreProtocol(enclosing);
    }
  }

  /**
   * Tags the current thread as serving a MongoDB client and returns the tag it carried before, for
   * {@link #restoreProtocol}. The entry points nest (an OP_MSG is unwrapped into a command), so the inner one must give the
   * outer one its tag back rather than clear it.
   */
  private static String tagAsMongo() {
    final String enclosing = ProtocolContext.get();
    ProtocolContext.set(MONGO_PROTOCOL);
    return enclosing;
  }

  private static void restoreProtocol(final String enclosing) {
    if (ProtocolContext.INTERNAL.equals(enclosing))
      // THE SAME VALUE AS set(INTERNAL), WHICH IS THE THREAD-LOCAL'S DEFAULT, BUT WITHOUT AN ENTRY LEFT ON THE POOLED NETTY THREAD
      ProtocolContext.clear();
    else
      ProtocolContext.set(enclosing);
  }

  /**
   * The virtual administrative namespace the library answers its admin commands on: {@code admin} exactly, as the library
   * compares it, or an authentication source such as {@code $external}. Any other name is resolved (and opened) as a database
   * by the library, so it requires authorization and principal binding.
   */
  private static boolean isAdminDatabase(final String databaseName) {
    return databaseName == null || databaseName.isEmpty() || databaseName.startsWith("$") || ADMIN_DATABASE.equals(databaseName);
  }

  /**
   * Implements the SASL PLAIN mechanism (RFC 4616). The payload carries {@code [authzid] NUL authcid NUL passwd}; the
   * credentials are validated against ArcadeDB server security.
   */
  private Document handleSaslStart(final Channel channel, final String databaseName, final Document query) {
    final Object mechanism = query.get("mechanism");
    if (mechanism == null || !PLAIN_MECHANISM.equalsIgnoreCase(mechanism.toString()))
      throw new MongoServerError(MECHANISM_UNAVAILABLE, "MechanismUnavailable",
          "Unsupported authentication mechanism '" + mechanism + "'. The ArcadeDB MongoDB plugin only supports the PLAIN (SASL) mechanism");

    final Object payload = query.get("payload");
    if (!(payload instanceof BinData))
      throw new MongoServerError(AUTHENTICATION_FAILED, "AuthenticationFailed", "Missing or invalid SASL payload");

    final byte[] data = ((BinData) payload).getData();
    final String decoded = new String(data, StandardCharsets.UTF_8);
    final String[] parts = decoded.split(SASL_PLAIN_SEPARATOR, -1);
    if (parts.length != 3)
      throw new MongoServerError(AUTHENTICATION_FAILED, "AuthenticationFailed", "Malformed SASL PLAIN payload");

    // parts[0] = authorization identity (optional, ignored), parts[1] = user, parts[2] = password
    final String userName = parts[1].isEmpty() ? parts[0] : parts[1];
    final String userPassword = parts[2];

    // An auth source of "admin" or "$external" maps to a server-wide credential check (database access is verified later).
    final String authDatabase =
        databaseName == null || "admin".equals(databaseName) || databaseName.startsWith("$") ? null : databaseName;

    try {
      final ServerSecurityUser user = server.getSecurity().authenticate(userName, userPassword, authDatabase);
      authenticatedUsers.put(channel, user);
    } catch (final ServerSecurityException e) {
      authenticatedUsers.remove(channel);
      throw new MongoServerError(AUTHENTICATION_FAILED, "AuthenticationFailed", "Authentication failed");
    }

    final Document response = new Document();
    response.put("conversationId", 1);
    response.put("done", Boolean.TRUE);
    response.put("payload", new BinData(new byte[0]));
    Utils.markOkay(response);
    return response;
  }
}
