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
import java.util.concurrent.ConcurrentHashMap;

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
    return new MongoDBDatabaseWrapper(server.getDatabase(databaseName), plugin, this);
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

  /** An OP_QUERY on a collection: the library hands it to the database's {@code handleQuery}, past every command. */
  @Override
  public QueryResult handleQuery(final MongoQuery query) {
    final String enclosing = tagAsMongo();
    try {
      return super.handleQuery(query);
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

    // Commands that address a real (user) database read or write stored data: they require an
    // authenticated principal that is authorized for that database, and the principal is bound into the
    // engine so per-type ACLs enforce - mirroring the Postgres and BOLT transports (GHSA-fq9c-x968-g278).
    // Handshake/diagnostic commands (hello, isMaster, ping, ...) target a virtual admin database and are
    // let through so drivers can complete the connection handshake before authenticating.
    if (isUserDatabase(databaseName)) {
      ServerSecurityUser user = authenticatedUsers.get(channel);
      if (user == null)
        throw new MongoServerError(UNAUTHORIZED, "Unauthorized", "Command '" + command + "' requires authentication");

      try {
        // The live principal: a user deleted, re-passworded or stripped of a grant since the SASL login must not keep
        // the access it had when it authenticated.
        final ServerSecurityUser current = server.getSecurity().revalidate(user);
        if (current != user) {
          user = current;
          authenticatedUsers.put(channel, current);
        }
      } catch (final ServerSecurityException e) {
        authenticatedUsers.remove(channel);
        throw new MongoServerError(UNAUTHORIZED, "Unauthorized", "Command '" + command + "' requires authentication");
      }

      if (!user.canAccessToDatabase(databaseName))
        throw new MongoServerError(UNAUTHORIZED, "Unauthorized",
            "User '" + user.getName() + "' is not authorized to access database '" + databaseName + "'");

      return handleAuthorizedCommand(channel, databaseName, command, query, user);
    }

    return super.handleCommand(channel, databaseName, command, query);
  }

  /**
   * Binds the authenticated principal into {@link DatabaseContext} for the target database so the engine's
   * per-database and per-type ACLs enforce while the command executes, then clears it. The MongoDB backend
   * processes each connection's commands synchronously on its Netty channel thread, so the binding is
   * confined to the current command.
   */
  private Document handleAuthorizedCommand(final Channel channel, final String databaseName, final String command,
      final Document query, final ServerSecurityUser user) {
    // The query admission gate (issue #9518), WITHOUT waiting: this runs on the Netty channel thread, which also serves the
    // other connections assigned to it, so it must never park. A command that cannot start at once is refused with a code
    // the drivers retry. A cursor continuation is not gated: it only serves the rows its find already read, and refusing
    // it would break a cursor the client cannot resume
    final QueryAdmissionGate.Ticket admission;
    try {
      admission = "getMore".equalsIgnoreCase(command) || "killCursors".equalsIgnoreCase(command) ?
          null :
          QueryAdmissionGate.getInstance().admit(0);
    } catch (final QueryAdmissionException e) {
      throw new MongoServerError(EXCEEDED_TIME_LIMIT, "ExceededTimeLimit", e.getMessage());
    }

    // Listed by "list queries" / SHOW TRANSACTIONS and stopped by a terminate (issue #9689), over the same window as the
    // slot. The command document is described only if somebody lists it
    final RunningQuery runningQuery = admission != null ? registerRunningQuery(channel, databaseName, command, query, user) : null;
    try {
      final DatabaseInternal database = (DatabaseInternal) server.getDatabase(databaseName);
      final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.init(database);
      ctx.setCurrentUser(user.getDatabaseUser(database));
      try {
        return super.handleCommand(channel, databaseName, command, query);
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

  private RunningQuery registerRunningQuery(final Channel channel, final String databaseName, final String command,
      final Document query, final ServerSecurityUser user) {
    final RunningQuery runningQuery = server.getRunningQueries().register(databaseName, user.getName(), "mongodb", null, null,
        RunningQuery.Client.of(channel.id().asShortText(), channel.remoteAddress()));
    // Read on the listing thread while the command runs: a document the backend changes meanwhile can at worst fail the
    // read, which RunningQuery.getText() turns into an entry listed without its text
    runningQuery.setStatement("mongo", () -> command + " " + query.toString());
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
   * A user (real) database is anything that is not one of MongoDB's virtual administrative databases.
   * Only these carry stored data, so only these require authorization and principal binding.
   */
  private static boolean isUserDatabase(final String databaseName) {
    if (databaseName == null || databaseName.isEmpty() || databaseName.startsWith("$"))
      return false;
    final String normalized = databaseName.toLowerCase(Locale.ENGLISH);
    return !"admin".equals(normalized) && !"local".equals(normalized) && !"config".equals(normalized);
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
