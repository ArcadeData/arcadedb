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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.network.PreAuthConnectionGate;
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
import java.util.Set;
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
   * The listener's permit for a connection that has not authenticated yet, handed back the moment it does -
   * or when it goes away without ever doing so (issue #6412).
   */
  private final    PreAuthConnectionGate.Ticket preAuthTicket;
  private final    int                 maxMultiBulkDepth;
  private final    int                 maxMultiBulkLength;
  private final    int                 maxBulkLength;

  // A misconfigured protocol-limit setting is re-read (and re-validated) on every new connection, since the
  // value can change at runtime; this only bounds the WARNING about it to once per setting per JVM, so a busy
  // server churning through connections against a static bad value does not flood the log.
  private static final Set<GlobalConfiguration> WARNED_MISCONFIGURED_LIMITS = ConcurrentHashMap.newKeySet();

  // parseValueUntilLF() reads every RESP length/integer token (*, $, :) and simple-string reply value; all of
  // them are always short (a signed 64-bit decimal has at most 20 characters). Without a bound, a client that
  // never sends a terminating CRLF - e.g. "$" followed by megabytes of digits - grows this buffer unboundedly
  // and holds the thread before parseLength()/maxBulkLength/maxMultiBulkLength ever get a value to check,
  // since those only fire once a token has actually been parsed (issue #5895 review, round 6).
  private static final int MAX_TOKEN_LENGTH = 64;

  /**
   * Holds the resolved key and database from key resolution.
   */
  private record ResolvedKey(String key, DatabaseInternal database) {
  }

  public RedisNetworkExecutor(final ArcadeDBServer server, final Socket socket) throws IOException {
    this(server, socket, null);
  }

  public RedisNetworkExecutor(final ArcadeDBServer server, final Socket socket,
      final PreAuthConnectionGate.Ticket preAuthTicket) throws IOException {
    this.preAuthTicket = preAuthTicket;
    setName(Constants.PRODUCT + "-redis/" + socket.getInetAddress());
    this.server = server;
    this.channel = new ChannelBinaryServer(socket, server.getConfiguration());
    this.maxMultiBulkDepth = sanitizedLimit(GlobalConfiguration.REDIS_MAX_MULTIBULK_DEPTH, 2);
    this.maxMultiBulkLength = sanitizedLimit(GlobalConfiguration.REDIS_MAX_MULTIBULK_LENGTH, 1);
    this.maxBulkLength = sanitizedLimit(GlobalConfiguration.REDIS_MAX_BULK_LENGTH, 1);

    // Bound the pre-authentication window (issue #5912): without a read timeout, a client that opens a
    // connection and never completes AUTH/HELLO - or trickles bytes arbitrarily slowly - can hold this
    // connection thread open indefinitely. Lifted back to infinite once authentication succeeds (see
    // markAuthenticated), reusing the same setSoTimeout(NETWORK_SOCKET_TIMEOUT)-then-setSoTimeout(0) idiom
    // BoltNetworkExecutor.negotiateTransport() uses to bound its own TLS-detection window (review on #5965:
    // Bolt only bounds that window, not its own subsequent HELLO/LOGON auth phase, so this goes further -
    // it bounds the entire pre-auth phase here, through AUTH/HELLO itself). An authenticated RESP client is
    // expected to keep a long-lived, often idle connection open between commands, so the timeout must not
    // keep applying past that point.
    final int handshakeTimeout = GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.getValueAsInteger();
    if (handshakeTimeout > 0)
      channel.socket.setSoTimeout(handshakeTimeout);

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

  /**
   * Reads a protocol-limit setting, falling back to its built-in default (with a warning) if configured below
   * {@code floor}. A value that low would reject every command outright - e.g. {@code parseNext}'s depth check
   * is {@code depth >= maxMultiBulkDepth} (depth starts at 0 for the top-level array, so a flat command's single
   * argument is already parsed at depth 1), so a configured depth below 2 rejects even a flat {@code PING} - so
   * it is treated as a misconfiguration rather than an intentional (if impractical) lockdown. "Usable" here means
   * "a connection can still parse a command at all", not "usable for real traffic": the floor of 1 used for
   * {@code maxMultiBulkLength}/{@code maxBulkLength} still lets a configured value through that is far too low
   * for any real command (e.g. a 1-byte max bulk length rejects even the shortest command name) - that is an
   * intentionally low, if impractical, configuration rather than the 0-or-negative case this guards against.
   */
  private int sanitizedLimit(final GlobalConfiguration setting, final int floor) {
    final int configured = setting.getValueAsInteger();
    if (configured < floor) {
      final int fallback = ((Number) setting.getDefValue()).intValue();
      if (WARNED_MISCONFIGURED_LIMITS.add(setting))
        LogManager.instance().log(this, Level.WARNING,
            "Redis wrapper: '%s' is set to %d, below the minimum usable value (%d); falling back to the default (%d)",
            setting.getKey(), configured, floor, fallback);
      return fallback;
    }
    return configured;
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
          // No data received within the pre-authentication window (issue #5912): close instead of holding
          // the connection thread open indefinitely. Authenticated connections never reach here - their
          // read timeout is lifted to infinite in markAuthenticated().
          LogManager.instance().log(this, Level.FINE, "Redis wrapper: closing idle unauthenticated connection %s", channel.socket.getRemoteSocketAddress());
          close();
        } catch (final RedisProtocolLimitException e) {
          // The message violates a configured protocol limit (issue #5895): the stream position can no
          // longer be trusted, so report the error and close rather than trying to resync and continue.
          LogManager.instance().log(this, Level.WARNING, "Redis wrapper: %s, closing connection", e.getMessage());
          try {
            value.setLength(0);
            // respErrorMessage() strips embedded \r/\n: the malformed-length message embeds the raw
            // client-supplied token verbatim, and a bare \n (no preceding \r) survives parseValueUntilLF()
            // uncaught, so without this an adversarial token could break a RESP reply across two lines.
            value.append("-ERR ").append(respErrorMessage(e));
            appendCrLf();
            replyToClient(value);
          } catch (final IOException ignored) {
            // the connection is already gone; nothing left to notify
          }
          close();
        } catch (final IOException e) {
          LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Error on reading request", e);
        }
      }
    } finally {
      ProtocolContext.clear();
      // Whatever ended the loop, the listener's permit must go back: a connection that dies without ever
      // authenticating would otherwise keep its slot for the life of the server (issue #6412).
      releasePreAuthTicket();
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

  private void executeCommand(final Object command) {
    value.setLength(0);

    if (command instanceof List) {
      final List<Object> list = (List<Object>) command;
      if (list.isEmpty())
        return;

      final Object cmd = list.get(0);
      if (!(cmd instanceof String)) {
        // cmd can be null here (issue #5911: a RESP2 null bulk string, $-1, is now a legal array element),
        // so guard the diagnostic itself rather than calling getClass() on a possibly-null reference.
        LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid command[0] %s (type=%s)", command,
            cmd == null ? "null" : cmd.getClass());
        return;
      }

      final String cmdString = ((String) cmd).toUpperCase(Locale.ENGLISH);

      // A RESP2 null bulk string ($-1) is only meaningful as an "absent" value; no real command handler
      // below is written to expect one as an argument (issue #5911 made it reachable here at all, since the
      // $ branch used to desync instead of returning null). Reject it uniformly, with a clean protocol
      // error, rather than letting whichever handler happens to touch it first crash with a raw
      // NullPointerException it wasn't written to expect - e.g. defaultBucket is a ConcurrentHashMap, whose
      // put/get reject a null key or value outright, so "SET key $-1" or "GET $-1" would otherwise NPE deep
      // inside setVariable/getVariable instead of getting a clear, single reply.
      // Deliberately checks only top-level (depth-1) arguments: a $-1 nested inside a multibulk argument
      // (e.g. "SET key *1\r\n$-1\r\n") still reaches a handler as a null inside a List and would surface as a
      // raw ClassCastException from the generic catch-all instead of this message. No handler actually
      // consumes a nested array argument today, so that's an obscure, already-non-crashing edge case rather
      // than a gap worth a recursive check for.
      for (int i = 1; i < list.size(); i++) {
        if (list.get(i) == null) {
          value.append("-ERR Protocol error: unexpected null bulk string argument");
          appendCrLf();
          return;
        }
      }

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
   * A {@link RedisException} carrying an explicit kind (see {@link RedisException#withKind}) wins first - that
   * covers the kinds {@link ErrorCategory} has no concept of at all, such as {@code WRONGPASS}, {@code NOAUTH} and
   * {@code NOPROTO} (issue #6560). Everything else falls back to {@link ErrorCategory} so every wire protocol
   * answers the question the same way. RESP has no vocabulary for the client-error categories Postgres and Bolt
   * distinguish, so they all keep Redis' generic {@code ERR}.
   * <p>
   * {@code TRYAGAIN} is the closest RESP2 offers, but in real Redis it is a cluster-mode error, so several client
   * libraries will not auto-retry on it. The retry hint is therefore weaker here than Postgres' {@code 40001} or
   * Bolt's transient status - it is the best signal the protocol has, not an equivalent one.
   */
  static String respErrorPrefix(final Throwable error) {
    if (error instanceof RedisException redisException && redisException.getKind() != null)
      return redisException.getKind();

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
    final long by = list.size() > 2 ? Long.parseLong((String) list.get(2)) : 1L;

    Object number = getVariable(k);
    if (number == null) {
      number = 0L;
    } else if (!(number instanceof Number)) {
      if (NumberUtils.isIntegerNumber(number.toString()))
        number = Long.parseLong(number.toString());
      else
        throw new RedisException("Key '" + k + "' is not a number");
    }

    final Number newValue;
    if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte) {
      try {
        newValue = Math.subtractExact(((Number) number).longValue(), by);
      } catch (final ArithmeticException e) {
        throw new RedisException("increment or decrement would overflow", e);
      }
    } else
      newValue = Type.decrement((Number) number, by);

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

          final IndexCursor cursor = index.get(RedisIndexKeys.parse(key));
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
        by = Long.valueOf((String) list.get(2));
    } else
      by = 1L;

    Object number = getVariable(k);
    if (number == null) {
      number = 0L;
    } else if (!(number instanceof Number)) {
      if (NumberUtils.isIntegerNumber(number.toString()))
        number = Long.parseLong(number.toString());
      else
        throw new RedisException("Key '" + k + "' is not a number");
    }

    final Number newValue;
    if (!decimal && (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte)) {
      try {
        newValue = Math.addExact(((Number) number).longValue(), by.longValue());
      } catch (final ArithmeticException e) {
        throw new RedisException("increment or decrement would overflow", e);
      }
    } else
      newValue = Type.increment((Number) number, by);

    setVariable(k, newValue);
    if (decimal) {
      final String text = newValue.toString();
      value.append("$");
      value.append(text.getBytes(DatabaseFactory.getDefaultCharset()).length);
      appendCrLf();
      value.append(text);
    } else {
      value.append(newValue instanceof Long ? ":" : "+");
      value.append(newValue);
    }
  }

  /**
   * NX/XX must be a single atomic check-and-set: {@code SET k v NX} is the primitive a distributed-lock client
   * builds on, and evaluating "does k exist" and "write k" as two separate calls would let two racing clients both
   * observe "absent" and both believe they acquired the lock. {@link #setVariableIfAbsent} / {@link
   * #setVariableIfPresent} route to a single atomic map operation ({@code ConcurrentHashMap.putIfAbsent} /
   * {@code computeIfPresent}, or their {@code DatabaseInternal} equivalents) instead of a get-then-set pair.
   */
  private void set(final List<Object> list) {
    final String k = (String) list.get(1);
    final String v = (String) list.get(2);

    boolean nx = false;
    boolean xx = false;
    boolean get = false;
    for (int i = 3; i < list.size(); i++) {
      final String opt = ((String) list.get(i)).toUpperCase(Locale.ROOT);
      switch (opt) {
      case "NX":
        if (xx) throw new RedisException("syntax error");
        nx = true;
        break;
      case "XX":
        if (nx) throw new RedisException("syntax error");
        xx = true;
        break;
      case "GET":
        get = true;
        break;
      case "EX":
      case "PX":
      case "EXAT":
      case "PXAT":
      case "KEEPTTL":
        throw new RedisException("unsupported SET option '" + opt + "': ArcadeDB transient keys have no expiry");
      default:
        throw new RedisException("syntax error");
      }
    }

    final Object previous;
    final boolean applied;
    if (nx) {
      previous = setVariableIfAbsent(k, v);
      applied = previous == null;
    } else if (xx) {
      previous = setVariableIfPresent(k, v);
      applied = previous != null;
    } else {
      previous = setVariable(k, v);
      applied = true;
    }

    if (get) {
      // Real Redis returns the pre-existing value for GET regardless of whether NX/XX vetoed the write.
      respondValue(previous, true);
      return;
    }

    if (!applied) {
      value.append("$-1");
      return;
    }

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
      markUnauthenticated();
      throw RedisException.withKind("WRONGPASS", "ArcadeDB requires the 'AUTH <username> <password>' form");
    } else
      // ERR is already respErrorPrefix()'s default kind, so the message no longer needs to repeat it.
      throw new RedisException("wrong number of arguments for 'auth' command");

    try {
      markAuthenticated(server.getSecurity().authenticate(userName, password, null));
      value.append("+OK");
    } catch (final ServerSecurityException e) {
      markUnauthenticated();
      throw RedisException.withKind("WRONGPASS", "invalid username-password pair or user is disabled");
    }
  }

  /**
   * Marks the connection authenticated and lifts the pre-authentication idle read timeout (issue #5912):
   * unlike the bounded handshake window enforced before authentication, an authenticated RESP client is
   * expected to keep a long-lived, often idle connection open between commands.
   */
  private void markAuthenticated(final ServerSecurityUser user) {
    this.authenticatedUser = user;
    releasePreAuthTicket();
    try {
      channel.socket.setSoTimeout(0);
    } catch (final SocketException e) {
      // setSoTimeout() only throws on an already-broken/closed socket (review on #5965): there is nothing
      // left to hold open in that case, so logging and moving on - rather than failing the whole
      // authentication - is safe. The connection dies on its next read/write either way.
      LogManager.instance().log(this, Level.FINE, "Redis wrapper: unable to lift the idle read timeout after authentication", e);
    }
  }

  /**
   * Marks the connection unauthenticated and (re-)arms the bounded pre-authentication idle read timeout
   * (issue #5912 follow-up, review on #5965): a connection that authenticated once has its timeout lifted
   * to infinite by {@link #markAuthenticated}. If it then fails a subsequent AUTH/HELLO re-authentication
   * attempt on the same connection, {@code authenticatedUser} goes back to null - and without this, the
   * infinite timeout would stay in place, breaking the invariant that "unauthenticated" always implies the
   * bounded handshake timeout applies.
   */
  private void markUnauthenticated() {
    this.authenticatedUser = null;
    final int handshakeTimeout = GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.getValueAsInteger();
    try {
      channel.socket.setSoTimeout(Math.max(handshakeTimeout, 0));
    } catch (final SocketException e) {
      // As in markAuthenticated(): setSoTimeout() only throws on an already-broken/closed socket, so there
      // is no live connection left for the un-restored timeout to matter on - the "unauthenticated implies
      // bounded timeout" invariant this method exists for has nothing left to protect in that case.
      LogManager.instance().log(this, Level.FINE, "Redis wrapper: unable to restore the idle read timeout after failed authentication", e);
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
          throw RedisException.withKind("NOPROTO", "unsupported protocol version");
        idx++;
      }
    }

    // Optional AUTH <username> <password> option.
    if (list.size() > idx && "AUTH".equalsIgnoreCase((String) list.get(idx))) {
      if (list.size() < idx + 3)
        // ERR is already respErrorPrefix()'s default kind, so the message no longer needs to repeat it.
        throw new RedisException("Syntax error in HELLO");
      final String userName = (String) list.get(idx + 1);
      final String password = (String) list.get(idx + 2);
      try {
        markAuthenticated(server.getSecurity().authenticate(userName, password, null));
      } catch (final ServerSecurityException e) {
        markUnauthenticated();
        throw RedisException.withKind("WRONGPASS", "invalid username-password pair or user is disabled");
      }
    }

    if (authenticatedUser == null)
      throw RedisException.withKind("NOAUTH",
          "HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");

    // RESP2 map reply: a flat array of alternating key/value elements (7 pairs = 14 elements).
    value.append("*14");
    appendHelloEntry("server", Constants.PRODUCT.toLowerCase(Locale.ENGLISH));
    appendHelloEntry("version", Constants.getVersion());
    appendHelloEntry("proto", 2L);
    appendHelloEntry("id", getId());
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
      throw RedisException.withKind("NOAUTH", "Authentication required.");

    if (!authenticatedUser.canAccessToDatabase(databaseName))
      throw RedisException.withKind("NOPERM", "this user has no permissions to access the database '" + databaseName + "'");

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

  /**
   * @return the previous value of the key, or null if it had none
   */
  private Object setVariable(final String key, final Object value) {
    final ResolvedKey resolved = resolveKeyAndDatabase(key);

    if (resolved.database() != null)
      return resolved.database().setGlobalVariable(resolved.key(), value);
    return defaultBucket.put(resolved.key(), value);
  }

  /**
   * Atomic check-and-set: writes {@code value} only if the key is currently absent.
   *
   * @return the existing value if the key was already set (left untouched), or null if it was absent (now set)
   */
  private Object setVariableIfAbsent(final String key, final Object value) {
    final ResolvedKey resolved = resolveKeyAndDatabase(key);

    if (resolved.database() != null)
      return resolved.database().setGlobalVariableIfAbsent(resolved.key(), value);
    return defaultBucket.putIfAbsent(resolved.key(), value);
  }

  /**
   * Atomic check-and-set: writes {@code value} only if the key is currently present.
   *
   * @return the previous value if the key was set (now replaced), or null if it was absent (left untouched)
   */
  private Object setVariableIfPresent(final String key, final Object value) {
    final ResolvedKey resolved = resolveKeyAndDatabase(key);

    if (resolved.database() != null)
      return resolved.database().setGlobalVariableIfPresent(resolved.key(), value);

    final Object[] previous = new Object[1];
    defaultBucket.computeIfPresent(resolved.key(), (k, current) -> {
      previous[0] = current;
      return value;
    });
    return previous[0];
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
    return parseNext(0);
  }

  /**
   * Parses the next RESP value from the wire. {@code depth} counts RESP array nesting: a RESP array
   * element can itself be an array, so without a bound a maliciously deep, unauthenticated client payload
   * recurses once per nesting level and overflows the connection thread's JVM stack (issue #5895). The
   * array element count and the bulk-string byte length are bounded the same way, so a single
   * client-declared length like {@code *2000000000\r\n} or {@code $2000000000\r\n} cannot start a parse
   * loop (or a buffer growth) that runs for as long as the client is willing to trickle bytes.
   */
  private Object parseNext(final int depth) throws IOException {
    // depth starts at 0 for the top-level call, so ">=" (not ">") admits exactly maxMultiBulkDepth nested
    // levels (0 .. maxMultiBulkDepth-1) before rejecting - matching the setting's documented default.
    if (depth >= maxMultiBulkDepth)
      throw new RedisProtocolLimitException("Protocol error: RESP array nesting exceeds the maximum allowed depth (" + maxMultiBulkDepth + ")");

    final byte b = readNext();

    if (b == '+')
      // SIMPLE STRING
      return parseValueUntilLF();
    else if (b == ':')
      // INTEGER
      return parseLength(parseValueUntilLF(), "integer");
    else if (b == '$') {
      // BULK STRING
      final int size = parseLength(parseValueUntilLF(), "bulk length");
      if (size < 0)
        // RESP2 null bulk string ($-1): the header IS the complete, self-terminated token - unlike a
        // present bulk string, there is no trailing CRLF to skip (issue #5911). Mirrors the null/empty
        // array short-circuit below; skipping it here avoided consuming the next token's leading byte.
        // Deliberately treats every negative size as null, not just -1: RESP2 only defines -1, but nothing
        // else about "negative" is meaningful either, and rejecting e.g. $-2 would need its own protocol-
        // error branch for no behavioral benefit over just treating it the same as the one negative value
        // that is defined.
        return null;
      if (size > maxBulkLength)
        throw new RedisProtocolLimitException(
            "Protocol error: invalid bulk length " + size + " (maximum allowed is " + maxBulkLength + ")");

      final String value = parseChars(size);
      skipLF();
      return value;
    } else if (b == '*') {
      // ARRAY
      final int arraySize = parseLength(parseValueUntilLF(), "multibulk length");
      if (arraySize <= 0)
        // RESP2 null array (*-1) or an explicit empty array (*0): nothing to read.
        return new ArrayList<>();
      if (arraySize > maxMultiBulkLength)
        throw new RedisProtocolLimitException(
            "Protocol error: invalid multibulk length " + arraySize + " (maximum allowed is " + maxMultiBulkLength + ")");

      final List<Object> array = new ArrayList<>();
      for (int i = 0; i < arraySize; ++i)
        array.add(parseNext(depth + 1));
      return array;
    } else {
      LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s'", (char) b);
      return null;
    }
  }

  /**
   * Parses a RESP length/integer token as a plain {@code int}, wrapping a malformed (non-numeric) value in
   * {@link RedisProtocolLimitException} instead of letting {@link NumberFormatException} escape uncaught: a
   * bare {@code NumberFormatException} is not an {@link IOException}, so it would kill the connection thread
   * outright rather than getting the same clean {@code -ERR Protocol error} reply and close that {@link #run()}
   * already gives every other malformed-input case.
   */
  private int parseLength(final String raw, final String what) throws RedisProtocolLimitException {
    try {
      return Integer.parseInt(raw);
    } catch (final NumberFormatException e) {
      throw new RedisProtocolLimitException("Protocol error: invalid " + what + " '" + raw + "'");
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
        else {
          if (value.length() >= MAX_TOKEN_LENGTH)
            throw new RedisProtocolLimitException("Protocol error: token exceeds the maximum allowed length (" + MAX_TOKEN_LENGTH + ") without a terminating CRLF");
          value.append((char) b);
        }
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
      // The RESP bulk-length header is a byte count, not a char count (issue #5907 fallout): a multi-byte
      // UTF-8 character (e.g. accented Latin, CJK, emoji) is 1 String char but more than 1 byte once
      // replyToClient() encodes the whole reply, so v.toString().length() under-declares the length for any
      // non-ASCII value and desyncs the client exactly like a truncated bulk string would.
      // TODO(perf): this encodes `text` to bytes here just to measure its length, then replyToClient()
      // encodes the whole accumulated `value` StringBuilder (including this same text) to bytes again right
      // after - a double UTF-8 encode per non-numeric reply, noticeable for a value near maxBulkLength.
      // Avoiding it cleanly needs `value`'s reply-building to move off a single shared StringBuilder of
      // chars onto something byte-oriented, which is a larger change than this correctness fix warrants.
      final String text = v.toString();
      value.append("$");
      value.append(text.getBytes(DatabaseFactory.getDefaultCharset()).length);
      appendCrLf();
      value.append(text);
    }
  }

  private void appendCrLf() {
    value.append("\r\n");
  }

  /**
   * Reads {@code size} raw bytes and decodes them once with the same charset {@link #replyToClient} encodes
   * with, instead of the previous per-byte {@code (char) b} widening (issue #5907): that widening
   * sign-extended any byte {@code >= 0x80} into the wrong UTF-16 code unit rather than decoding it, mangling
   * every non-ASCII bulk string. Reading into a right-sized {@code byte[]} first - rather than building a
   * {@link StringBuilder} one UTF-16 char at a time and copying it via {@code toString()} - also keeps the
   * transient cost close to the wire size instead of roughly double it. {@code size} is assumed
   * non-negative: the RESP2 null bulk string ({@code $-1}) is handled by the caller before this is reached.
   */
  private String parseChars(final int size) throws IOException {
    final byte[] bytes = new byte[size];
    int read = 0;
    for (; read < size && !shutdown; ++read)
      bytes[read] = readNext();

    return new String(bytes, 0, read, DatabaseFactory.getDefaultCharset());
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

      final IndexCursor cursor = index.get(RedisIndexKeys.parse(key));
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
        final IndexCursor cursor = index.get(RedisIndexKeys.parse(key.toString()));
        records.add(cursor.hasNext() ? cursor.next().asDocument() : null);
      }
    }
    return records;
  }
}
