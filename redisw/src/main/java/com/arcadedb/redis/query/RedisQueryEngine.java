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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.redis.RedisException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.NumberUtils;

import java.util.*;
import java.util.logging.*;
import java.util.regex.*;

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
      };
    }

    final String cmd = parts.getFirst().toUpperCase(Locale.ENGLISH);
    final boolean isIdempotent = switch (cmd) {
      case "GET", "EXISTS", "HGET", "HEXISTS", "HMGET", "PING" -> true;
      default -> false;
    };

    return new AnalyzedQuery() {
      @Override
      public boolean isIdempotent() {
        return isIdempotent;
      }

      @Override
      public boolean isDDL() {
        return false;
      }
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
      if (upperCmd.equals("MULTI")) {
        inTransaction = true;
        continue;
      } else if (upperCmd.equals("EXEC")) {
        // Execute all queued commands in a transaction
        return executeTransaction(commands);
      } else if (upperCmd.equals("DISCARD")) {
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
    final List<Object> results = new ArrayList<>();

    database.transaction(() -> {
      for (final String command : commands) {
        final Object result = executeSingleCommandInternal(command);
        results.add(result);
      }
    });

    return createResultSet(results);
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

    Object current = database.getGlobalVariable(key);
    if (current == null) {
      current = 0L;
    } else if (!(current instanceof Number)) {
      if (NumberUtils.isIntegerNumber(current.toString())) {
        current = Long.parseLong(current.toString());
      } else {
        throw new RedisException("Key '" + key + "' is not a number");
      }
    }

    final Number newValue = Type.increment((Number) current, increment);
    database.setGlobalVariable(key, newValue);
    return newValue;
  }

  private Number decrBy(final List<String> parts) {
    if (parts.size() < 2) {
      throw new CommandParsingException("DECR/DECRBY requires a key: DECR <key> [decrement]");
    }
    final String key = parts.get(1);
    final int decrement = parts.size() > 2 ? Integer.parseInt(parts.get(2)) : 1;

    Object current = database.getGlobalVariable(key);
    if (current == null) {
      current = 0L;
    } else if (!(current instanceof Number)) {
      if (NumberUtils.isIntegerNumber(current.toString())) {
        current = Long.parseLong(current.toString());
      } else {
        throw new RedisException("Key '" + key + "' is not a number");
      }
    }

    final Number newValue = Type.decrement((Number) current, decrement);
    database.setGlobalVariable(key, newValue);
    return newValue;
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

    final int[] count = {0};

    database.transaction(() -> {
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
        count[0]++;
      }
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
      final Record record = new RID(database, firstArg).asDocument();
      return record != null ? record.toJSON(true).toString() : null;
    }

    // It's an index lookup
    if (parts.size() < 3) {
      throw new CommandParsingException("HGET requires index and key: HGET <index> <key>");
    }

    final String indexName = firstArg;
    final String key = parts.get(2);

    final Record record = getRecordByIndex(indexName, key);
    return record != null ? record.toJSON(true).toString() : null;
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
        final Record record = new RID(database, rid).asDocument();
        results.add(record != null ? record.toJSON(true).toString() : null);
      }
    } else {
      // It's an index lookup
      if (parts.size() < 3) {
        throw new CommandParsingException("HMGET requires index and keys: HMGET <index> <key> [key ...]");
      }

      final String indexName = firstArg;
      for (int i = 2; i < parts.size(); i++) {
        final Record record = getRecordByIndex(indexName, parts.get(i));
        results.add(record != null ? record.toJSON(true).toString() : null);
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
        final Record record = new RID(database, firstArg).getRecord();
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
    final int[] deleted = {0};

    database.transaction(() -> {
      // Check if it's RID mode
      if (firstArg.startsWith("#")) {
        for (int i = 1; i < parts.size(); i++) {
          final String rid = parts.get(i);
          if (!rid.startsWith("#")) {
            throw new CommandParsingException("All arguments must be RIDs when first argument is a RID");
          }
          try {
            new RID(database, rid).getRecord().delete();
            deleted[0]++;
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
          final Object[] keys = parseIndexKey(key);
          final IndexCursor cursor = index.get(keys);
          if (cursor.hasNext()) {
            cursor.next().getRecord().delete();
            deleted[0]++;
          }
        }
      }
    });

    return deleted[0];
  }

  private Record getRecordByIndex(final String indexName, final String key) {
    final Index index = database.getSchema().getIndexByName(indexName);
    final Object[] keys = parseIndexKey(key);
    final IndexCursor cursor = index.get(keys);
    return cursor.hasNext() ? cursor.next().asDocument() : null;
  }

  private Object[] parseIndexKey(final String key) {
    if (key.startsWith("[")) {
      return new JSONArray(key).toList().toArray();
    } else if (key.startsWith("\"")) {
      return new String[]{key.substring(1, key.length() - 1)};
    } else {
      // Try to parse as number first, then fallback to string
      if (NumberUtils.isIntegerNumber(key)) {
        return new Object[]{Long.parseLong(key)};
      }
      return new String[]{key};
    }
  }
}
