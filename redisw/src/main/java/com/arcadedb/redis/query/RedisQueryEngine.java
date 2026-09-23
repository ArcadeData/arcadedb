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
package com.arcadedb.redis.query;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.redis.RedisException;
import com.arcadedb.redis.RedisIndexKeys;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.NumberUtils;

import java.util.*;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Query engine for executing Redis commands via the HTTP API.
 * Supports Redis commands in text format (e.g., "GET key", "SET key value").
 * <p>
 * Supported commands:
 * <ul>
 *   <li>RAM commands (in-memory bucket): PING, SET, GET, GETDEL, EXISTS, INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY</li>
 *   <li>Persistent commands (database operations): HSET, HGET, HMGET, HEXISTS, HDEL</li>
 *   <li>Transaction commands: MULTI, EXEC, DISCARD</li>
 * </ul>
 * <p>
 * Multiple commands can be executed using:
 * <ul>
 *   <li>MULTI/EXEC transaction blocks (official Redis syntax)</li>
 *   <li>Newline-separated commands (batch execution)</li>
 * </ul>
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/1010">Issue #1010</a>
 */
public class RedisQueryEngine implements QueryEngine {
  public static final String ENGINE_NAME = "redis";

  private final DatabaseInternal database;

  // Pattern to parse Redis commands - handles quoted strings and JSON
  private static final Pattern COMMAND_PATTERN = Pattern.compile("(\\{[^{}]*(?:\\{[^{}]*\\}[^{}]*)*\\})|\"([^\"]*)\"|'([^']*)'|(\\S+)");

  protected RedisQueryEngine(final DatabaseInternal database) {
    this.database = database;
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    final List<String> parts = parseCommand(query);
    if (parts.isEmpty()) {
      return new AnalyzedQuery() {
        @Override
        public boolean isIdempotent() {
          return true;
        }

        @Override
        public boolean isDDL() {
          return false;
        }

        @Override
        public Set<OperationType> getOperationTypes() {
          return CollectionUtils.singletonSet(OperationType.READ);
        }
      };
    }

    final String cmd = parts.getFirst().toUpperCase(Locale.ENGLISH);
    final boolean isIdempotent = switch (cmd) {
      case "GET", "EXISTS", "HGET", "HEXISTS", "HMGET", "PING" -> true;
      default -> false;
    };
    final Set<OperationType> ops = detectRedisOperationTypes(cmd);

    return new AnalyzedQuery() {
      @Override
      public boolean isIdempotent() {
        return isIdempotent;
      }

      @Override
      public boolean isDDL() {
        return false;
      }

      @Override
      public Set<OperationType> getOperationTypes() {
        return ops;
      }
    };
  }

  private static Set<OperationType> detectRedisOperationTypes(final String cmd) {
    return switch (cmd) {
      case "GET", "EXISTS", "HGET", "HEXISTS", "HMGET", "PING" -> CollectionUtils.singletonSet(OperationType.READ);
      case "SET", "HSET", "HMSET" -> Set.of(OperationType.CREATE, OperationType.UPDATE);
      case "INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY" -> CollectionUtils.singletonSet(OperationType.UPDATE);
      case "GETDEL", "HDEL" -> CollectionUtils.singletonSet(OperationType.DELETE);
      default -> Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
    };
  }

  @Override
  public ResultSet query(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    checkIdempotent(query);
    return executeRedisCommand(query);
  }

  @Override
  public ResultSet query(final String query, final ContextConfiguration configuration, final Object... parameters) {
    checkIdempotent(query);
    return executeRedisCommand(query);
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    return executeRedisCommand(query);
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Object... parameters) {
    return executeRedisCommand(query);
  }

  private void checkIdempotent(final String query) {
    final AnalyzedQuery analyzed = analyze(query);
    if (!analyzed.isIdempotent())
      throw new CommandParsingException("Non-idempotent Redis command cannot be executed on the query endpoint. Use the command endpoint instead");
  }

  private ResultSet executeRedisCommand(final String query) {
    try {
      // Check if this is a multi-command query (contains newlines)
      final String[] lines = query.split("\\R");
      if (lines.length > 1) {
        return executeMultipleCommands(lines);
      }

      // Single command execution
      return executeSingleCommand(query);
    } catch (final RedisException | CommandParsingException e) {
      throw e;
    } catch (final NeedRetryException | DuplicatedKeyException e) {
      // Unwrapped rather than folded into the branch below: a conflict a write command's own
      // `database.transaction(...)` (or the HTTP auto-commit wrapper around this whole call) is set up to
      // retry is not "an error executing the Redis command" the way a malformed command is. Wrapping it into
      // CommandParsingException here - as every other exception is, below - defeated retrying at BOTH levels:
      // neither `LocalDatabase.transaction`'s own `catch (NeedRetryException | DuplicatedKeyException)` nor
      // DatabaseAbstractHandler's identical one recognizes the wrapped type, so an MVCC conflict that a second
      // attempt would have committed was answered as a hard failure on the first, whichever level the
      // transaction that took the conflict belonged to (found while fixing issue #8037's retry-duplication bug
      // in this same class).
      throw e;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing Redis command: " + query, e);
      throw new CommandParsingException("Error executing Redis command", e);
    }
  }

  /**
   * Executes multiple commands, either as a MULTI/EXEC transaction or as a batch.
   */
  private ResultSet executeMultipleCommands(final String[] lines) {
    final List<String> commands = new ArrayList<>();
    boolean inTransaction = false;

    for (final String line : lines) {
      final String trimmed = line.trim();
      if (trimmed.isEmpty() || trimmed.startsWith("#") || trimmed.startsWith("//")) {
        continue; // Skip empty lines and comments
      }

      final String upperCmd = trimmed.toUpperCase(Locale.ENGLISH);
      if ("MULTI".equals(upperCmd)) {
        inTransaction = true;
        continue;
      } else if ("EXEC".equals(upperCmd)) {
        // Execute all queued commands in a transaction
        return executeTransaction(commands);
      } else if ("DISCARD".equals(upperCmd)) {
        // Discard all queued commands
        commands.clear();
        return createResultSet("OK");
      }

      commands.add(trimmed);
    }

    // If we reach here without EXEC, execute as a batch (not a transaction)
    if (inTransaction) {
      throw new CommandParsingException("MULTI without EXEC - transaction not committed");
    }

    return executeBatch(commands);
  }

  /**
   * Executes commands in a database transaction (atomically).
   */
  private ResultSet executeTransaction(final List<String> commands) {
    // Filled fresh on every attempt and published only once the block has returned, the way
    // MCPToolUtils.collectInTransaction does: `database.transaction(...)` retries the block up to
    // arcadedb.txRetries times on an MVCC conflict, rolling the failed attempt back first, and an accumulator
    // declared outside the block is never reset between attempts - so a single retry publishes the discarded
    // attempt's replies alongside the committed ones (issue #8037). A one-element array rather than
    // MCPToolUtils's own JSONArray[1] holder, because List<Object> is generic and JSONArray is not - the
    // unchecked array-creation warning is suppressed rather than left to accumulate as noise, since nothing
    // else touches this array before the single assignment below.
    @SuppressWarnings("unchecked")
    final List<Object>[] committed = new List[1];

    database.transaction(() -> {
      final List<Object> attemptResults = new ArrayList<>(commands.size());
      for (final String command : commands) {
        final Object result = executeSingleCommandInternal(command);
        attemptResults.add(result);
      }
      committed[0] = attemptResults;
    });

    return createResultSet(committed[0]);
  }

  /**
   * Executes commands as a batch (sequentially, not atomically).
   */
  private ResultSet executeBatch(final List<String> commands) {
    final List<Object> results = new ArrayList<>();

    for (final String command : commands) {
      final Object result = executeSingleCommandInternal(command);
      results.add(result);
    }

    return createResultSet(results);
  }

  /**
   * Executes a single command and returns a ResultSet.
   */
  private ResultSet executeSingleCommand(final String query) {
    final Object result = executeSingleCommandInternal(query);
    return createResultSet(result);
  }

  /**
   * Executes a single command and returns the raw result.
   */
  private Object executeSingleCommandInternal(final String query) {
    final List<String> parts = parseCommand(query);
    if (parts.isEmpty()) {
      throw new CommandParsingException("Empty Redis command");
    }

    final String cmd = parts.getFirst().toUpperCase(Locale.ENGLISH);
    return switch (cmd) {
      case "PING" -> ping(parts);
      case "SET" -> set(parts);
      case "GET" -> get(parts);
      case "GETDEL" -> getDel(parts);
      case "EXISTS" -> exists(parts);
      case "INCR" -> incrBy(parts, false);
      case "INCRBY" -> incrBy(parts, false);
      case "INCRBYFLOAT" -> incrBy(parts, true);
      case "DECR" -> decrBy(parts);
      case "DECRBY" -> decrBy(parts);
      case "HSET", "HMSET" -> hSet(parts);
      case "HGET" -> hGet(parts);
      case "HMGET" -> hMGet(parts);
      case "HEXISTS" -> hExists(parts);
      case "HDEL" -> hDel(parts);
      default -> throw new CommandParsingException("Command not found: " + cmd);
    };
  }

  private List<String> parseCommand(final String command) {
    final List<String> parts = new ArrayList<>();
    final Matcher matcher = COMMAND_PATTERN.matcher(command.trim());

    while (matcher.find()) {
      if (matcher.group(1) != null) {
        parts.add(matcher.group(1)); // JSON object
      } else if (matcher.group(2) != null) {
        parts.add(matcher.group(2)); // Double-quoted string
      } else if (matcher.group(3) != null) {
        parts.add(matcher.group(3)); // Single-quoted string
      } else if (matcher.group(4) != null) {
        parts.add(matcher.group(4)); // Unquoted token
      }
    }

    return parts;
  }

  private ResultSet createResultSet(final Object result) {
    if (result == null)
      return new IteratorResultSet(Collections.emptyIterator());

    if (result instanceof Record record) {
      // Return documents directly (consistent with SQL/OpenCypher result format)
      final ResultInternal resultInternal = new ResultInternal(record);
      return new IteratorResultSet(Collections.singleton((Result) resultInternal).iterator());
    }

    final ResultInternal resultInternal = new ResultInternal();
    resultInternal.setProperty("value", result);
    return new IteratorResultSet(Collections.singleton((Result) resultInternal).iterator());
  }

  // --- RAM Commands (default bucket) ---

  private String ping(final List<String> parts) {
    return parts.size() > 1 ? parts.get(1) : "PONG";
  }

  private String set(final List<String> parts) {
    if (parts.size() < 3) {
      throw new CommandParsingException("SET requires key and value: SET <key> <value>");
    }
    final String key = parts.get(1);
    final String value = parts.get(2);
    database.setGlobalVariable(key, value);
    return "OK";
  }

  private Object get(final List<String> parts) {
    if (parts.size() < 2) {
      throw new CommandParsingException("GET requires a key: GET <key>");
    }
    return database.getGlobalVariable(parts.get(1));
  }

  private Object getDel(final List<String> parts) {
    if (parts.size() < 2) {
      throw new CommandParsingException("GETDEL requires a key: GETDEL <key>");
    }
    final String key = parts.get(1);
    // Use setGlobalVariable which atomically returns the previous value
    return database.setGlobalVariable(key, null);
  }

  private int exists(final List<String> parts) {
    if (parts.size() < 2) {
      throw new CommandParsingException("EXISTS requires at least one key: EXISTS <key> [key ...]");
    }
    int count = 0;
    for (int i = 1; i < parts.size(); i++) {
      if (database.getGlobalVariable(parts.get(i)) != null) {
        count++;
      }
    }
    return count;
  }

  /**
   * INCR/INCRBY/INCRBYFLOAT. The read, the addition and the write are ONE atomic operation on the key through
   * {@link DatabaseInternal#computeGlobalVariable}, the primitive the RESP wire path uses too (issue #7776): this
   * engine is cached per database and shared by every request thread, so a get followed by a set let two concurrent
   * callers read the same value, both write their own successor and each be answered a count that never happened
   * (issue #8248).
   */
  private Number incrBy(final List<String> parts, final boolean decimal) {
    if (parts.size() < 2) {
      throw new CommandParsingException("INCR/INCRBY requires a key: INCR <key> [increment]");
    }
    final String key = parts.get(1);
    final Number increment;
    if (parts.size() > 2) {
      increment = decimal ? Double.parseDouble(parts.get(2)) : Integer.parseInt(parts.get(2));
    } else {
      increment = 1;
    }

    return (Number) database.computeGlobalVariable(key, current -> Type.increment(toNumber(key, current), increment));
  }

  /**
   * DECR/DECRBY, atomic for the same reason as {@link #incrBy}.
   */
  private Number decrBy(final List<String> parts) {
    if (parts.size() < 2) {
      throw new CommandParsingException("DECR/DECRBY requires a key: DECR <key> [decrement]");
    }
    final String key = parts.get(1);
    final int decrement = parts.size() > 2 ? Integer.parseInt(parts.get(2)) : 1;

    return (Number) database.computeGlobalVariable(key, current -> Type.decrement(toNumber(key, current), decrement));
  }

  /**
   * Normalizes the stored value INCR/DECR operate on: an absent key reads as {@code 0} and an integral string is
   * parsed. Anything else is refused with a {@link RedisException}; since this runs inside
   * {@link DatabaseInternal#computeGlobalVariable}'s remapping, the refusal leaves the key unchanged.
   */
  private static Number toNumber(final String key, final Object current) {
    if (current == null)
      return 0L;
    if (current instanceof Number number)
      return number;
    if (NumberUtils.isIntegerNumber(current.toString()))
      return Long.parseLong(current.toString());
    throw new RedisException("Key '" + key + "' is not a number");
  }

  // --- Persistent Commands (database operations) ---

  /**
   * HSET command: Creates documents in the database.
   * Syntax: HSET <type> <json> [json ...]
   * Example: HSET Person {"name":"John","age":30}
   */
  private int hSet(final List<String> parts) {
    if (parts.size() < 3) {
      throw new CommandParsingException("HSET requires type and JSON: HSET <type> <json> [json ...]");
    }
    final String typeName = parts.get(1);

    // Counted into a local inside the block and published only once the block has returned, the same way
    // executeTransaction() now does: a holder incremented directly retries under `database.transaction(...)`
    // still carries the rolled-back attempt's count into the one that finally commits (issue #8037).
    final int[] count = {0};

    database.transaction(() -> {
      int created = 0;
      for (int i = 2; i < parts.size(); i++) {
        final JSONObject json = new JSONObject(parts.get(i));
        final DocumentType type = database.getSchema().getType(typeName);

        final MutableDocument document;
        if (type instanceof LocalVertexType) {
          document = database.newVertex(typeName);
        } else if (type instanceof LocalEdgeType edgeType) {
          document = new MutableEdge(database, edgeType, null);
        } else {
          document = database.newDocument(typeName);
        }

        document.fromJSON(json);
        document.save();
        created++;
      }
      count[0] = created;
    });

    return count[0];
  }

  /**
   * HGET command: Retrieves a document from the database.
   * Syntax: HGET <index> <key>  - retrieves by index
   *         HGET <rid>         - retrieves by RID
   * Examples:
   *   HGET Person[id] 1
   *   HGET #10:5
   */
  private Object hGet(final List<String> parts) {
    if (parts.size() < 2) {
      throw new CommandParsingException("HGET requires index and key: HGET <index> <key> or HGET <rid>");
    }

    final String firstArg = parts.get(1);

    // Check if it's a RID
    if (firstArg.startsWith("#")) {
      return (Document) database.lookupByRID(new RID(firstArg), true);
    }

    // It's an index lookup
    if (parts.size() < 3) {
      throw new CommandParsingException("HGET requires index and key: HGET <index> <key>");
    }

    final String indexName = firstArg;
    final String key = parts.get(2);

    return getRecordByIndex(indexName, key);
  }

  /**
   * HMGET command: Retrieves multiple documents from the database.
   * Syntax: HMGET <index> <key> [key ...]
   *         HMGET <rid> [rid ...]
   */
  private List<Object> hMGet(final List<String> parts) {
    if (parts.size() < 2) {
      throw new CommandParsingException("HMGET requires index/rids and keys: HMGET <index> <key> [key ...] or HMGET <rid> [rid ...]");
    }

    final String firstArg = parts.get(1);
    final List<Object> results = new ArrayList<>();

    // Check if it's RID mode
    if (firstArg.startsWith("#")) {
      for (int i = 1; i < parts.size(); i++) {
        final String rid = parts.get(i);
        if (!rid.startsWith("#")) {
          throw new CommandParsingException("All arguments must be RIDs when first argument is a RID");
        }
        results.add((Document) database.lookupByRID(new RID(rid), true));
      }
    } else {
      // It's an index lookup
      if (parts.size() < 3) {
        throw new CommandParsingException("HMGET requires index and keys: HMGET <index> <key> [key ...]");
      }

      final String indexName = firstArg;
      for (int i = 2; i < parts.size(); i++) {
        results.add(getRecordByIndex(indexName, parts.get(i)));
      }
    }

    return results;
  }

  /**
   * HEXISTS command: Checks if a document exists in the database.
   * Syntax: HEXISTS <index> <key>
   *         HEXISTS <rid>
   */
  private int hExists(final List<String> parts) {
    if (parts.size() < 2) {
      throw new CommandParsingException("HEXISTS requires index and key: HEXISTS <index> <key> or HEXISTS <rid>");
    }

    final String firstArg = parts.get(1);

    // Check if it's a RID
    if (firstArg.startsWith("#")) {
      try {
        final Record record = database.lookupByRID(new RID(firstArg), true);
        return record != null ? 1 : 0;
      } catch (Exception e) {
        return 0;
      }
    }

    // It's an index lookup
    if (parts.size() < 3) {
      throw new CommandParsingException("HEXISTS requires index and key: HEXISTS <index> <key>");
    }

    final String indexName = firstArg;
    final String key = parts.get(2);

    final Record record = getRecordByIndex(indexName, key);
    return record != null ? 1 : 0;
  }

  /**
   * HDEL command: Deletes documents from the database.
   * Syntax: HDEL <index> <key> [key ...]
   *         HDEL <rid> [rid ...]
   */
  private int hDel(final List<String> parts) {
    if (parts.size() < 2) {
      throw new CommandParsingException("HDEL requires index and keys: HDEL <index> <key> [key ...] or HDEL <rid> [rid ...]");
    }

    final String firstArg = parts.get(1);
    // Same reasoning as hSet() above: counted into a local inside the block, published only once the block has
    // returned (issue #8037).
    final int[] deleted = {0};

    database.transaction(() -> {
      int removed = 0;
      // Check if it's RID mode
      if (firstArg.startsWith("#")) {
        for (int i = 1; i < parts.size(); i++) {
          final String rid = parts.get(i);
          if (!rid.startsWith("#")) {
            throw new CommandParsingException("All arguments must be RIDs when first argument is a RID");
          }
          try {
            database.lookupByRID(new RID(rid), true).delete();
            removed++;
          } catch (Exception e) {
            // Record not found, ignore
          }
        }
      } else {
        // It's an index lookup
        if (parts.size() < 3) {
          throw new CommandParsingException("HDEL requires index and keys: HDEL <index> <key> [key ...]");
        }

        final String indexName = firstArg;
        final Index index = database.getSchema().getIndexByName(indexName);

        for (int i = 2; i < parts.size(); i++) {
          final String key = parts.get(i);
          final Object[] keys = RedisIndexKeys.parse(key);
          final IndexCursor cursor = index.get(keys);
          if (cursor.hasNext()) {
            cursor.next().getRecord().delete();
            removed++;
          }
        }
      }
      deleted[0] = removed;
    });

    return deleted[0];
  }

  private Record getRecordByIndex(final String indexName, final String key) {
    final Index index = database.getSchema().getIndexByName(indexName);
    final Object[] keys = RedisIndexKeys.parse(key);
    final IndexCursor cursor = index.get(keys);
    return cursor.hasNext() ? cursor.next().asDocument() : null;
  }

}
