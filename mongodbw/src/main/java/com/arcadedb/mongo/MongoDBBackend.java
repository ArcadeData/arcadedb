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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import io.netty.channel.Channel;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MongoDBBackend extends AbstractMongoBackend {
  // MongoDB error codes (see https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml)
  private static final int    AUTHENTICATION_FAILED = 18;
  private static final int    UNAUTHORIZED          = 13;
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
      final ServerSecurityUser user = authenticatedUsers.get(channel);
      if (user == null)
        throw new MongoServerError(UNAUTHORIZED, "Unauthorized", "Command '" + command + "' requires authentication");

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
    final DatabaseInternal database = (DatabaseInternal) server.getDatabase(databaseName);
    final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.init(database);
    ctx.setCurrentUser(user.getDatabaseUser(database));
    try {
      return super.handleCommand(channel, databaseName, command, query);
    } finally {
      ctx.setCurrentUser(null);
    }
  }

  @Override
  public void handleClose(final Channel channel) {
    authenticatedUsers.remove(channel);
    super.handleClose(channel);
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
