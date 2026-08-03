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
package com.arcadedb.redis;

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.database.Record;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.NumberUtils;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class RedisNetworkExecutor extends Thread {
  private final    ArcadeDBServer      server;
  private final    ChannelBinaryServer channel;
  private volatile boolean             shutdown         = false;
  private          int                 posInBuffer      = 0;
  private final    StringBuilder       value            = new StringBuilder();
  private final    byte[]              buffer           = new byte[32 * 1024];
  private          int                 bytesRead        = 0;
  private final    Map<String, Object> defaultBucket    = new ConcurrentHashMap<>();
  private          String              selectedDatabaseName = null;
  private          ServerSecurityUser  authenticatedUser    = null;

  /**
   * Holds the resolved key and database from key resolution.
   */
  private record ResolvedKey(String key, DatabaseInternal database) {
  }

  public RedisNetworkExecutor(final ArcadeDBServer server, final Socket socket) throws IOException {
    setName(Constants.PRODUCT + "-redis/" + socket.getInetAddress());
    this.server = server;
    this.channel = new ChannelBinaryServer(socket, server.getConfiguration());

    // Initialize default database from configuration if set. The database access is authorized lazily,
    // once the connection has authenticated (see getAuthorizedDatabase), so here we only record the name.
    final String defaultDbName = GlobalConfiguration.REDIS_DEFAULT_DATABASE.getValueAsString();
    if (defaultDbName != null && !defaultDbName.isEmpty()) {
      if (server.existsDatabase(defaultDbName))
        this.selectedDatabaseName = defaultDbName;
      else
        LogManager.instance().log(this, Level.WARNING,
            "Redis wrapper: Default database '%s' not found, will use connection-local storage", defaultDbName);
    }
  }

  @Override
  public void run() {
    ProtocolContext.set("redis");
    try {
      while (!shutdown) {
        try {
          executeCommand(parseNext());

          replyToClient(value);

        } catch (final EOFException | SocketException e) {
          LogManager.instance().log(this, Level.FINE, "Redis wrapper: Error on reading request", e);
          close();
        } catch (final SocketTimeoutException e) {
          // IGNORE IT
        } catch (final IOException e) {
          LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Error on reading request", e);
        }
      }
    } finally {
      ProtocolContext.clear();
    }
  }

  public void replyToClient(final StringBuilder response) throws IOException {
    LogManager.instance().log(this, Level.FINE, "Redis wrapper: Sending response back to the client '%s'...", response);

    final byte[] buffer = response.toString().getBytes(DatabaseFactory.getDefaultCharset());

    channel.outStream.write(buffer);
    channel.flush();

    response.setLength(0);
  }

  public void close() {
    shutdown = true;
    if (channel != null)
      channel.close();
  }

  private void executeCommand(final Object command) {
    value.setLength(0);

    if (command instanceof List) {
      final List<Object> list = (List<Object>) command;
      if (list.isEmpty())
        return;

      final Object cmd = list.getFirst();
      if (!(cmd instanceof String)) {
        LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid command[0] %s (type=%s)", command,
            cmd.getClass());
        return;
      }

      final String cmdString = ((String) cmd).toUpperCase(Locale.ENGLISH);

      // Redis maps commands directly to engine operations rather than going through
      // Database.query/command, so the engine-boundary span never fires for it. Open one here so
      // Redis is traced like the other protocols (protocol=redis comes from ProtocolContext, set in
      // run()). No query language applies to a native Redis command, so language is null. No-op
      // unless the tracing plugin is active.
      try (final QueryTracer.Span span = QueryTracer.Holder.begin(
          selectedDatabaseName, null, "command", cmdString)) {

        // AUTH and HELLO are the only commands accepted before authentication (HELLO must still carry a
        // valid AUTH option, otherwise it is rejected with NOAUTH). Every other command is rejected with
        // NOAUTH until the connection has provided valid ArcadeDB credentials, mirroring the Postgres and
        // MongoDB wire-protocol wrappers.
        if ("AUTH".equals(cmdString)) {
          auth(list);
          appendCrLf();
          return;
        }

        if ("HELLO".equals(cmdString)) {
          hello(list);
          appendCrLf();
          return;
        }

        if (authenticatedUser == null) {
          value.append("-NOAUTH Authentication required.");
          appendCrLf();
          return;
        }

        switch (cmdString) {
          case "DECR":
            decrBy(list);
            break;

          case "DECRBY":
            decrBy(list);
            break;

          case "GET":
            get(list);
            break;

          case "GETDEL":
            getDel(list);
            break;

          case "EXISTS":
            exists(list);
            break;

          case "HDEL":
            hDel(list);
            break;

          case "HEXISTS":
            hExists(list);
            break;

          case "HGET":
            hGet(list);
            break;

          case "HMGET":
            hMGet(list);
            break;

          case "HSET":
          case "HMSET": // HMSET IS DEPRECATED IN FAVOUR OF HSET
            hSet(list);
            break;

          case "INCR":
            incrBy(list, false);
            break;

          case "INCRBY":
            incrBy(list, false);
            break;

          case "INCRBYFLOAT":
            incrBy(list, true);
            break;

          case "PING":
            ping(list);
            break;

          case "SELECT":
            select(list);
            break;

          case "SET":
            set(list);
            break;

          default:
            value.append("-Command not found");
        }

      } catch (final Exception e) {
        value.append('-');
        value.append(respErrorPrefix(e));
        value.append(' ');
        value.append(respErrorMessage(e));
      }

      appendCrLf();

    } else
      LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid command %s", command);
  }

  /**
   * The kind word a RESP error reply opens with (issue #5628). Errors used to go out as a bare {@code -<message>},
   * which carries no kind at all: a client had nothing to branch on, and an optimistic-concurrency conflict - the
   * one failure worth repeating the command for - looked exactly like a permanent one.
   * <p>
   * The classification itself lives in {@link ErrorCategory} so every wire protocol answers it the same way. RESP
   * has no vocabulary for the client-error categories Postgres and Bolt distinguish, so they all keep Redis'
   * generic {@code ERR}.
   * <p>
   * {@code TRYAGAIN} is the closest RESP2 offers, but in real Redis it is a cluster-mode error, so several client
   * libraries will not auto-retry on it. The retry hint is therefore weaker here than Postgres' {@code 40001} or
   * Bolt's transient status - it is the best signal the protocol has, not an equivalent one.
   */
  static String respErrorPrefix(final Throwable error) {
    return switch (ErrorCategory.of(error)) {
      case RETRY -> "TRYAGAIN";
      case SECURITY -> "NOPERM";
      default -> "ERR";
    };
  }

  /**
   * A RESP simple error is one line, so an embedded CR or LF would end the reply early and leave the remainder to
   * be read as the start of the next one.
   */
  static String respErrorMessage(final Throwable error) {
    final String message = error.getMessage();
    if (message == null || message.isEmpty())
      return error.getClass().getSimpleName();
    return message.replace('\r', ' ').replace('\n', ' ');
  }

  private void decrBy(final List<Object> list) {
    final String k = (String) list.get(1);
    final int by = list.size() > 2 ? Integer.parseInt((String) list.get(2)) : 1;

    Object number = getVariable(k);
    if (number == null) {
      number = 0L;
    } else if (!(number instanceof Number)) {
      if (NumberUtils.isIntegerNumber(number.toString()))
        number = Long.parseLong(number.toString());
      else
        throw new RedisException("Key '" + k + "' is not a number");
    }

    final Number newValue = Type.decrement((Number) number, by);
    setVariable(k, newValue);
    value.append(":");
    value.append(newValue);
  }

  private void exists(final List<Object> list) {
    int total = 0;
    for (int i = 1; i < list.size(); i++)
      total += containsVariable((String) list.get(i)) ? 1 : 0;

    respondValue(total, false);
  }

  private void get(final List<Object> list) {
    final String k = (String) list.get(1);
    final Object v = getVariable(k);
    respondValue(v, true);
  }

  private void getDel(final List<Object> list) {
    final String k = (String) list.get(1);
    final Object v = removeVariable(k);
    respondValue(v, true);
  }

  private void hDel(final List<Object> list) {
    final String bucketName = (String) list.get(1);

    final int pos = bucketName.indexOf(".");
    final int[] deleted = {0};

    if (pos < 0) {
      // Transient mode: delete from globalVariables atomically
      final DatabaseInternal database = getAuthorizedDatabase(bucketName);
      database.transaction(() -> {
        for (int i = 2; i < list.size(); i++) {
          final String key = (String) list.get(i);
          // Use setGlobalVariable which atomically returns the previous value
          final Object previous = database.setGlobalVariable(key, null);
          if (previous != null) {
            deleted[0]++;
          }
        }
      });
    } else {
      // Persistent mode: delete from database
      final String databaseName = bucketName.substring(0, pos);
      final String keyType = bucketName.substring(pos + 1);

      final Database database = getAuthorizedDatabase(databaseName);

      if (keyType.startsWith("#")) {
        database.lookupByRID(new RID(keyType), true).delete();
        deleted[0]++;
      } else {
        final Index index = database.getSchema().getIndexByName(keyType);

        for (int i = 2; i < list.size(); i++) {
          final String key = (String) list.get(i);

          final Object[] keys;
          if (key.startsWith("[")) {
            keys = new JSONArray(key).toList().toArray();
          } else if (key.startsWith("\"")) {
            keys = new String[]{key.substring(1, key.length() - 1)};
          } else
            keys = new String[]{key};

          final IndexCursor cursor = index.get(keys);
          if (cursor.hasNext()) {
            cursor.next().getRecord().delete();
            deleted[0]++;
          }
        }
      }
    }
    value.append(":");
    value.append(deleted[0]);
  }

  private void hExists(final List<Object> list) {
    final String bucketName = (String) list.get(1);
    final String key = (String) list.get(2);

    // Check for transient mode: no dot in bucketName and key doesn't start with #
    final int pos = bucketName.indexOf(".");
    if (pos < 0 && !key.startsWith("#")) {
      // Transient mode
      final String transientValue = getTransientValue(bucketName, key);
      respondValue(transientValue != null ? 1 : 0, false);
    } else {
      // Persistent mode
      final Record record = getRecord(bucketName, key);
      respondValue(record != null ? 1 : 0, false);
    }
  }

  private void hGet(final List<Object> list) {
    final String bucketName = (String) list.get(1);
    final String key = (String) list.get(2);

    // Check for transient mode: no dot in bucketName and key doesn't start with #
    final int pos = bucketName.indexOf(".");
    if (pos < 0 && !key.startsWith("#")) {
      // Transient mode
      final String transientValue = getTransientValue(bucketName, key);
      respondValue(transientValue, true);
    } else {
      // Persistent mode
      final Record record = getRecord(bucketName, key);
      respondValue(record != null ? record.toJSON(true) : null, true);
    }
  }

  private void hMGet(final List<Object> list) {
    final String bucketName = (String) list.get(1);
    final List<Object> keys = list.subList(2, list.size());

    // Check for transient mode: no dot in bucketName
    final int pos = bucketName.indexOf(".");
    if (pos < 0) {
      // Transient mode: get from globalVariables
      value.append("*");
      value.append(keys.size());

      for (final Object keyObj : keys) {
        appendCrLf();
        final String key = keyObj.toString();
        if (key.startsWith("#")) {
          // BY RID - persistent mode
          final Database database = getAuthorizedDatabase(bucketName);
          final Record record = database.lookupByRID(new RID(key), true);
          respondValue(record != null ? record.toJSON(true) : null, true);
        } else {
          // Transient mode
          final String transientValue = getTransientValue(bucketName, key);
          respondValue(transientValue, true);
        }
      }
    } else {
      // Persistent mode: get records from database
      final List<Record> records = getRecords(bucketName, keys);

      value.append("*");
      value.append(records.size());

      for (int i = 0; i < records.size(); i++) {
        appendCrLf();
        final Record record = records.get(i);
        respondValue(record != null ? record.toJSON(true) : null, true);
      }
    }
  }

  private void hSet(final List<Object> list) {
    final String databaseName = (String) list.get(1);
    final String secondArg = (String) list.get(2);

    // Check if transient mode: second argument is JSON (starts with '{')
    if (secondArg.startsWith("{")) {
      // Transient mode: store JSON objects in globalVariables atomically
      final DatabaseInternal database = getAuthorizedDatabase(databaseName);
      final int[] stored = {0};
      database.transaction(() -> {
        for (int i = 2; i < list.size(); i++) {
          final JSONObject json = new JSONObject((String) list.get(i));
          if (!json.has("id")) {
            throw new RedisException("JSON object must have an 'id' field for transient storage");
          }
          final String key = json.get("id").toString();
          database.setGlobalVariable(key, json.toString());
          stored[0]++;
        }
      });
      value.append(":");
      value.append(stored[0]);
    } else {
      // Persistent mode: store documents in database type
      final String typeName = secondArg;
      final Database database = getAuthorizedDatabase(databaseName);
      database.transaction(() -> {
        for (int i = 3; i < list.size(); i++) {
          final JSONObject v = new JSONObject((String) list.get(i));

          final DocumentType type = database.getSchema().getType(typeName);

          final MutableDocument document;

          if (type instanceof LocalVertexType)
            document = database.newVertex(typeName);
          else if (type instanceof LocalEdgeType edgeType)
            document = new MutableEdge(database, edgeType, null);
          else
            document = database.newDocument(typeName);

          document.fromJSON(v);
          document.save();
        }
      });
      value.append(":");
      value.append(list.size() - 3);
    }
  }

  private void incrBy(final List<Object> list, final boolean decimal) {
    final String k = (String) list.get(1);

    final Number by;
    if (list.size() > 2) {
      if (decimal)
        by = Double.valueOf((String) list.get(2));
      else
        by = Integer.valueOf((String) list.get(2));
    } else
      by = 1;

    Object number = getVariable(k);
    if (number == null) {
      number = 0L;
    } else if (!(number instanceof Number)) {
      if (NumberUtils.isIntegerNumber(number.toString()))
        number = Long.parseLong(number.toString());
      else
        throw new RedisException("Key '" + k + "' is not a number");
    }

    final Number newValue = Type.increment((Number) number, by);
    setVariable(k, newValue);
    value.append(newValue instanceof Long ? ":" : "+");
    value.append(newValue);
  }

  private void set(final List<Object> list) {
    final String k = (String) list.get(1);
    final String v = (String) list.get(2);
    setVariable(k, v);
    value.append("+");
    value.append("OK");
  }

  private void ping(final List<Object> list) {
    final String response = list.size() > 1 ? (String) list.get(1) : "PONG";
    value.append("+");
    value.append(response);
  }

  /**
   * Authenticates the connection against ArcadeDB's server security. Only the two-argument form
   * {@code AUTH <username> <password>} is supported: ArcadeDB has no anonymous "default" user, so the
   * single-argument Redis form is rejected. On success the authenticated principal is retained for the
   * life of the connection and bound into {@link DatabaseContext} on every database access, so the
   * engine's per-user permission gates enforce for Redis callers exactly as they do for the HTTP,
   * Postgres and MongoDB transports.
   */
  private void auth(final List<Object> list) {
    final String userName;
    final String password;
    if (list.size() == 3) {
      userName = (String) list.get(1);
      password = (String) list.get(2);
    } else if (list.size() == 2) {
      // Single-argument AUTH targets Redis' "default" user, which ArcadeDB does not model.
      this.authenticatedUser = null;
      throw new RedisException("WRONGPASS ArcadeDB requires the 'AUTH <username> <password>' form");
    } else
      throw new RedisException("ERR wrong number of arguments for 'auth' command");

    try {
      this.authenticatedUser = server.getSecurity().authenticate(userName, password, null);
      value.append("+OK");
    } catch (final ServerSecurityException e) {
      this.authenticatedUser = null;
      throw new RedisException("WRONGPASS invalid username-password pair or user is disabled");
    }
  }

  /**
   * Handles the {@code HELLO [protover [AUTH username password]]} handshake. Modern Redis clients use it
   * to authenticate and negotiate the protocol version in a single round-trip. Only the AUTH-carrying
   * form is accepted before authentication; a plain HELLO on an unauthenticated connection is rejected
   * with NOAUTH, matching Redis' own behaviour. Both RESP2 and RESP3 protocol versions are accepted but
   * the reply is always encoded as a RESP2 map (a flat array of key/value pairs).
   */
  private void hello(final List<Object> list) {
    int idx = 1;

    // Optional protocol version.
    if (list.size() > idx) {
      final String maybeVersion = (String) list.get(idx);
      if (NumberUtils.isIntegerNumber(maybeVersion)) {
        final int proto = Integer.parseInt(maybeVersion);
        if (proto < 2 || proto > 3)
          throw new RedisException("NOPROTO unsupported protocol version");
        idx++;
      }
    }

    // Optional AUTH <username> <password> option.
    if (list.size() > idx && "AUTH".equalsIgnoreCase((String) list.get(idx))) {
      if (list.size() < idx + 3)
        throw new RedisException("ERR Syntax error in HELLO");
      final String userName = (String) list.get(idx + 1);
      final String password = (String) list.get(idx + 2);
      try {
        this.authenticatedUser = server.getSecurity().authenticate(userName, password, null);
      } catch (final ServerSecurityException e) {
        this.authenticatedUser = null;
        throw new RedisException("WRONGPASS invalid username-password pair or user is disabled");
      }
    }

    if (authenticatedUser == null)
      throw new RedisException(
          "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");

    // RESP2 map reply: a flat array of alternating key/value elements (7 pairs = 14 elements).
    value.append("*14");
    appendHelloEntry("server", Constants.PRODUCT.toLowerCase(Locale.ENGLISH));
    appendHelloEntry("version", Constants.getVersion());
    appendHelloEntry("proto", 2L);
    appendHelloEntry("id", threadId());
    appendHelloEntry("mode", "standalone");
    appendHelloEntry("role", "master");
    // "modules" maps to an empty array.
    appendCrLf();
    respondValue("modules", true);
    appendCrLf();
    value.append("*0");
  }

  private void appendHelloEntry(final String key, final Object entryValue) {
    appendCrLf();
    respondValue(key, true);
    appendCrLf();
    respondValue(entryValue, !(entryValue instanceof Number));
  }

  private void select(final List<Object> list) {
    final String dbName = (String) list.get(1);
    if (!server.existsDatabase(dbName))
      throw new RedisException("Database '" + dbName + "' not found");

    // Authorize access (and bind the principal) before switching the connection's current database.
    getAuthorizedDatabase(dbName);
    this.selectedDatabaseName = dbName;
    value.append("+");
    value.append("OK");
  }

  /**
   * Resolves a database by name, enforcing that the authenticated user is allowed to access it and
   * binding the user into {@link DatabaseContext} so the engine's per-user permission gates enforce for
   * the subsequent record/index operations. Every command that touches a database MUST resolve it
   * through this method rather than calling {@code server.getDatabase(...)} directly.
   */
  private DatabaseInternal getAuthorizedDatabase(final String databaseName) {
    if (authenticatedUser == null)
      throw new RedisException("NOAUTH Authentication required.");

    if (!authenticatedUser.canAccessToDatabase(databaseName))
      throw new RedisException("NOPERM this user has no permissions to access the database '" + databaseName + "'");

    final DatabaseInternal database = (DatabaseInternal) server.getDatabase(databaseName);
    DatabaseContext.INSTANCE.init(database).setCurrentUser(authenticatedUser.getDatabaseUser(database));
    return database;
  }

  /**
   * Resolves the database and actual key from a potentially prefixed key.
   * Priority: key prefix (dbname.key) > SELECT > default config > connection-local bucket.
   *
   * @param key the key which may contain a database prefix (e.g., "mydb.mykey")
   * @return ResolvedKey containing the resolved key and database (database may be null if using local bucket)
   */
  private ResolvedKey resolveKeyAndDatabase(final String key) {
    // Check for database prefix (dbname.key)
    final int dotPos = key.indexOf('.');
    if (dotPos > 0) {
      final String dbName = key.substring(0, dotPos);
      final String actualKey = key.substring(dotPos + 1);
      if (server.existsDatabase(dbName))
        // A real database prefix: authorize access (throws if not permitted) and bind the principal.
        return new ResolvedKey(actualKey, getAuthorizedDatabase(dbName));
      // Not a database prefix, treat as regular key.
    }

    // Use selected database (from SELECT command or default config)
    if (selectedDatabaseName != null)
      return new ResolvedKey(key, getAuthorizedDatabase(selectedDatabaseName));

    return new ResolvedKey(key, null);
  }

  private Object getVariable(final String key) {
    final ResolvedKey resolved = resolveKeyAndDatabase(key);

    if (resolved.database() != null) {
      return resolved.database().getGlobalVariable(resolved.key());
    }
    return defaultBucket.get(resolved.key());
  }

  private void setVariable(final String key, final Object value) {
    final ResolvedKey resolved = resolveKeyAndDatabase(key);

    if (resolved.database() != null) {
      resolved.database().setGlobalVariable(resolved.key(), value);
    } else {
      defaultBucket.put(resolved.key(), value);
    }
  }

  private Object removeVariable(final String key) {
    final ResolvedKey resolved = resolveKeyAndDatabase(key);

    if (resolved.database() != null) {
      // Use setGlobalVariable which atomically returns the previous value
      return resolved.database().setGlobalVariable(resolved.key(), null);
    }
    return defaultBucket.remove(resolved.key());
  }

  private boolean containsVariable(final String key) {
    final ResolvedKey resolved = resolveKeyAndDatabase(key);

    if (resolved.database() != null) {
      return resolved.database().getGlobalVariable(resolved.key()) != null;
    }
    return defaultBucket.containsKey(resolved.key());
  }

  private Object parseNext() throws IOException {
    final byte b = readNext();

    if (b == '+')
      // SIMPLE STRING
      return parseValueUntilLF();
    else if (b == ':')
      // INTEGER
      return Integer.parseInt(parseValueUntilLF());
    else if (b == '$') {
      // BATCH STRING
      final String value = parseChars(Integer.parseInt(parseValueUntilLF()));
      skipLF();
      return value;
    } else if (b == '*') {
      // ARRAY
      final List<Object> array = new ArrayList<>();
      final int arraySize = Integer.parseInt(parseValueUntilLF());
      for (int i = 0; i < arraySize; ++i)
        array.add(parseNext());
      return array;
    } else {
      LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s'", (char) b);
      return null;
    }
  }

  private void skipLF() throws IOException {
    final byte b = readNext();
    if (b == '\r') {
      final byte b2 = readNext();
      if (b2 == '\n') {
      } else
        LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s' instead of expected \\n"
            , (char) b2);
    } else
      LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s' instead of expected \\r",
          (char) b);
  }

  private String parseValueUntilLF() throws IOException {
    value.setLength(0);

    boolean slashR = false;

    while (!shutdown) {
      final byte b = readNext();

      if (!slashR) {
        if (b == '\r')
          slashR = true;
        else
          value.append((char) b);
      } else {
        if (b == '\n')
          break;
        else
          LogManager.instance().log(this, Level.SEVERE, """
              Redis wrapper: Error on parsing value waiting for LF, but \
              found '%s' after /r""", (char) b);
      }
    }

    return value.toString();
  }

  private void respondValue(final Object v, final boolean forceString) {
    if (v == null)
      value.append("$-1");
    else if (!forceString && v instanceof Number) {
      value.append(":");
      value.append(v);
    } else {
      value.append("$");
      value.append(v.toString().length());
      appendCrLf();
      value.append(v);
    }
  }

  private void appendCrLf() {
    value.append("\r\n");
  }

  private String parseChars(final int size) throws IOException {
    value.setLength(0);

    for (int i = 0; i < size && !shutdown; ++i) {
      final byte b = readNext();
      value.append((char) b);
    }

    return value.toString();
  }

  private byte readNext() throws IOException {
    if (posInBuffer < bytesRead)
      return buffer[posInBuffer++];

    posInBuffer = 0;

    do {
      bytesRead = channel.inStream.read(buffer);

//      String debug = "";
//      for (int i = 0; i < bytesRead; ++i) {
//        debug += (char) buffer[i];
//      }
//      LogManager.instance().log(this, Level.INFO, "Redis wrapper: Read '%s'...", debug);

    } while (bytesRead == 0);

    if (bytesRead == -1)
      throw new EOFException();

    return buffer[posInBuffer++];
  }

  /**
   * Gets a record by RID or index.
   * Formats:
   * - bucketName = database, key = #rid -> get by RID
   * - bucketName = database.indexName, key = value -> get by index
   *
   * @return the record, or null if not found
   * @throws RedisException if key is not a RID when bucketName has no dot
   */
  private Record getRecord(final String bucketName, final String key) {
    final Record record;
    final int pos = bucketName.indexOf(".");
    if (pos < 0) {
      final Database database = getAuthorizedDatabase(bucketName);

      if (key.startsWith("#")) {
        // BY RID
        record = (Document) database.lookupByRID(new RID(key), true);
      } else {
        throw new RedisException(
            "Retrieving a record by RID, the key must be as #<bucket-id>:<bucket-position>. Example: #13:432");
      }
    } else {
      // BY INDEX
      final String databaseName = bucketName.substring(0, pos);
      final String keyType = bucketName.substring(pos + 1);

      final Database database = getAuthorizedDatabase(databaseName);

      final Index index = database.getSchema().getIndexByName(keyType);

      final Object[] keys;
      if (key.startsWith("[")) {
        keys = new JSONArray(key).toList().toArray();
      } else if (key.startsWith("\"")) {
        keys = new String[]{key.substring(1, key.length() - 1)};
      } else
        keys = new String[]{key};

      final IndexCursor cursor = index.get(keys);
      record = cursor.hasNext() ? cursor.next().asDocument() : null;
    }
    return record;
  }

  /**
   * Gets a transient value from globalVariables.
   * Used when bucketName has no dot and key doesn't start with #.
   */
  private String getTransientValue(final String databaseName, final String key) {
    final DatabaseInternal database = getAuthorizedDatabase(databaseName);
    final Object value = database.getGlobalVariable(key);
    return value != null ? value.toString() : null;
  }

  private List<Record> getRecords(final String bucketName, final List<Object> keys) {
    final List<Record> records = new ArrayList<>();

    final int pos = bucketName.indexOf(".");
    if (pos < 0) {
      // BY RID
      final Database database = getAuthorizedDatabase(bucketName);

      for (final Object key : keys) {
        final String k = key.toString();
        if (k.startsWith("#"))
          records.add((Document) database.lookupByRID(new RID(k), true));
        else
          throw new RedisException("""
              Retrieving a record by RID, the key must be as #<bucket-id>:<bucket-position>. \
              Example: #13:432""");
      }
    } else {
      // BY INDEX
      final String databaseName = bucketName.substring(0, pos);
      final String keyType = bucketName.substring(pos + 1);

      final Database database = getAuthorizedDatabase(databaseName);

      final Index index = database.getSchema().getIndexByName(keyType);

      for (final Object key : keys) {
        final String k = key.toString();
        final Object[] compositeKey;
        if (k.startsWith("[")) {
          compositeKey = new JSONArray((String[]) key).toList().toArray();
        } else if (k.startsWith("\"")) {
          compositeKey = new String[]{k.substring(1, k.length() - 1)};
        } else
          compositeKey = new String[]{k};

        final IndexCursor cursor = index.get(compositeKey);
        records.add(cursor.hasNext() ? cursor.next().asDocument() : null);
      }
    }
    return records;
  }
}
