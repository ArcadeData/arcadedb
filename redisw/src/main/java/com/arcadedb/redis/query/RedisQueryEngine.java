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
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.redis.RedisCounterOperations;
import com.arcadedb.redis.RedisException;
import com.arcadedb.redis.RedisIndexKeys;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;
import java.util.function.UnaryOperator;
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
  // Batch separator: any line break, as the executor has always split on (String.split("\\R")).
  private static final Pattern LINE_SEPARATOR = Pattern.compile("\\R");
  private static final Pattern COMMAND_PATTERN = Pattern.compile("(\\{[^{}]*(?:\\{[^{}]*\\}[^{}]*)*\\})|\"([^\"]*)\"|'([^']*)'|(\\S+)");

  protected RedisQueryEngine(final DatabaseInternal database) {
    this.database = database;
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  /**
   * Classifies what {@link #executeRedisCommand(String)} will actually run. A newline-separated batch runs EVERY
   * line, so it is analyzed line by line with the same split and the same skipped lines as the executor, and the
   * answers are folded: idempotent only if every command is, operation types the union of all of them - the way
   * {@code SQLScriptQueryEngine.analyze} folds a script. Classifying the whole text by its first verb declared a
   * batch opening with a read idempotent and read-only however many writes followed it, which defeated both the
   * query endpoint's refusal of writes and the streaming gate that reads this answer (issue #8247).
   * <p>
   * MULTI, EXEC and DISCARD are classified like any other verb outside the read list, so a transaction block is
   * never idempotent: a block only exists to contain writes, and one that holds only reads loses nothing by being
   * sent to the command endpoint.
   */
  @Override
  public AnalyzedQuery analyze(final String query) {
    final String[] lines = splitBatch(query);
    if (lines.length <= 1) {
      // Single command: the executor runs the text as-is, without the batch's comment skipping.
      final List<String> parts = parseCommand(query);
      if (parts.isEmpty())
        return analyzed(true, CollectionUtils.singletonSet(OperationType.READ));
      final String cmd = parts.getFirst().toUpperCase(Locale.ENGLISH);
      return analyzed(isIdempotentCommand(cmd), detectRedisOperationTypes(cmd));
    }

    boolean idempotent = true;
    final Set<OperationType> ops = EnumSet.noneOf(OperationType.class);
    for (final String line : lines) {
      final String trimmed = line.trim();
      if (isSkippedBatchLine(trimmed))
        continue;
      final List<String> parts = parseCommand(trimmed);
      if (parts.isEmpty())
        continue;
      final String cmd = parts.getFirst().toUpperCase(Locale.ENGLISH);
      idempotent &= isIdempotentCommand(cmd);
      ops.addAll(detectRedisOperationTypes(cmd));
    }
    if (ops.isEmpty())
      ops.add(OperationType.READ);

    return analyzed(idempotent, Collections.unmodifiableSet(ops));
  }

  private static AnalyzedQuery analyzed(final boolean isIdempotent, final Set<OperationType> ops) {
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

  /**
   * Splits the text into the lines the executor runs. Shared by {@link #analyze(String)} and
   * {@link #executeRedisCommand(String)} so the analysis cannot drift from what is executed: more than one element
   * means batch execution.
   */
  private static String[] splitBatch(final String query) {
    return LINE_SEPARATOR.split(query);
  }

  /**
   * A batch line the executor does not run: blank, or a {@code #} / {@code //} comment. Only applies in batch
   * mode; a single-line command is run as written.
   */
  private static boolean isSkippedBatchLine(final String trimmed) {
    return trimmed.isEmpty() || trimmed.startsWith("#") || trimmed.startsWith("//");
  }

  private static boolean isIdempotentCommand(final String cmd) {
    return switch (cmd) {
      case "GET", "EXISTS", "HGET", "HEXISTS", "HMGET", "PING" -> true;
      default -> false;
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
      // Check if this is a multi-command query (contains newlines). Same split as analyze(), which must see
      // exactly the commands that run here (issue #8247).
      final String[] lines = splitBatch(query);
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
      if (isSkippedBatchLine(trimmed))
        continue; // Skip empty lines and comments (analyze() skips the same ones)

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
   * Executes commands in a database transaction (atomically) - the persistent commands (HSET/HDEL) only. The RAM
   * commands (SET/GET/GETDEL/INCR/DECR and their variants) write straight to {@code database}'s global-variables map, a plain
   * {@code ConcurrentHashMap} that {@code database.transaction(...)}'s retry/rollback never touches (issue #8254):
   * buffered in {@code ramOverlay} during the attempt and published to the real map only once the block finally
   * commits, the same way {@code committed} below is - otherwise a retried {@code INCR} in the same block as a
   * write that hits an MVCC conflict or a duplicated key applies twice, once per attempt.
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
    final RamOverlay[] ramWrites = new RamOverlay[1];

    // #8254 follow-up (PR #8309 review): database.transaction(...) only actually commits when THIS call
    // creates the transaction (LocalDatabase.transaction()'s own createdNewTx); joining one already active -
    // the HTTP command endpoint's own auto-commit wrapper - returns here before that OUTER transaction
    // commits. Publishing RAM writes right after this call would be premature in that case: a conflict at the
    // outer level re-runs this whole command from scratch, and a write already published by the discarded
    // attempt would be picked up as the new baseline and re-applied by the fresh one - the same bug #8254
    // reported, one nesting level up. Sampled before the call, since nothing else on this thread can open or
    // close a transaction between the check and database.transaction() below.
    final boolean nested = database.isTransactionActive();

    database.transaction(() -> {
      // Falls back to writing straight through (this engine's behavior before #8254) when nested: a MULTI/EXEC
      // joined into an outer retryable transaction is not the shape #8254 reported, and buffering here without
      // a way to publish exactly at the OUTER commit would only trade one double-apply window for another.
      final RamOverlay ramOverlay = nested ? null : new RamOverlay();
      final List<Object> attemptResults = new ArrayList<>(commands.size());
      for (final String command : commands) {
        final Object result = executeSingleCommandInternal(command, ramOverlay);
        attemptResults.add(result);
      }
      committed[0] = attemptResults;
      ramWrites[0] = ramOverlay;
    });

    if (ramWrites[0] != null)
      ramWrites[0].publishTo(database);

    return createResultSet(committed[0]);
  }

  /**
   * Buffers the RAM mutations one MULTI/EXEC attempt performs (issue #8254), so a discarded attempt never
   * reaches the real global-variables map. {@code values} answers in-block reads (SET/GETDEL's overwrite, or
   * INCR/DECR's running total); {@code remaps} additionally tracks, for a key whose most recent operation in
   * this attempt was an INCR/DECR, the composed remapping to replay through {@code computeGlobalVariable} at
   * publish time - PR #8309 review: publishing a blind {@code setGlobalVariable} for a counter would silently
   * lose a concurrent standalone INCR/DECR on the same key that lands between this attempt finishing and its
   * publish. A SET/GETDEL clears any pending remap for its key: it supplies a fresh value this attempt itself
   * chose, no longer relative to whatever else is in the real map, so it publishes as a plain overwrite -
   * consistent with a bare SET's own last-write-wins semantics outside any block.
   */
  private static final class RamOverlay {
    private final Map<String, Object>              values = new HashMap<>();
    private final Map<String, UnaryOperator<Object>> remaps = new HashMap<>();

    boolean hasKey(final String key) {
      return values.containsKey(key);
    }

    Object get(final String key) {
      return values.get(key);
    }

    void setAbsolute(final String key, final Object value) {
      values.put(key, value);
      remaps.remove(key);
    }

    void setComputed(final String key, final Object newValue, final UnaryOperator<Object> remapping) {
      values.put(key, newValue);
      final UnaryOperator<Object> existing = remaps.get(key);
      remaps.put(key, existing == null ? remapping : value -> remapping.apply(existing.apply(value)));
    }

    void publishTo(final DatabaseInternal database) {
      for (final Map.Entry<String, Object> entry : values.entrySet()) {
        final UnaryOperator<Object> remap = remaps.get(entry.getKey());
        if (remap != null)
          database.computeGlobalVariable(entry.getKey(), remap);
        else
          database.setGlobalVariable(entry.getKey(), entry.getValue());
      }
    }
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
    return executeSingleCommandInternal(query, null);
  }

  /**
   * @param ramOverlay {@code null} outside {@link #executeTransaction} (or when it is nested, see there): a RAM
   *                   command then reads/writes {@code database}'s global-variables map directly. Non-null inside an
   *                   outermost MULTI/EXEC attempt: a RAM command reads/writes this overlay instead, so a discarded
   *                   attempt's mutations never reach the real one (issue #8254).
   */
  private Object executeSingleCommandInternal(final String query, final RamOverlay ramOverlay) {
    final List<String> parts = parseCommand(query);
    if (parts.isEmpty()) {
      throw new CommandParsingException("Empty Redis command");
    }

    final String cmd = parts.getFirst().toUpperCase(Locale.ENGLISH);
    return switch (cmd) {
      case "PING" -> ping(parts);
      case "SET" -> set(parts, ramOverlay);
      case "GET" -> get(parts, ramOverlay);
      case "GETDEL" -> getDel(parts, ramOverlay);
      case "EXISTS" -> exists(parts, ramOverlay);
      case "INCR" -> incrBy(parts, false, ramOverlay);
      case "INCRBY" -> incrBy(parts, false, ramOverlay);
      case "INCRBYFLOAT" -> incrBy(parts, true, ramOverlay);
      case "DECR" -> decrBy(parts, ramOverlay);
      case "DECRBY" -> decrBy(parts, ramOverlay);
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

  /**
   * The single normalization point for a RAM key (PR #8309 review): {@code LocalDatabase.getGlobalVariable}/
   * {@code setGlobalVariable}/{@code computeGlobalVariable} all strip a leading {@code $} and refuse a reserved
   * name via {@code SQLQueryEngine.validateVariableName}. Called once per command, on the raw key from the
   * parsed command, before it ever reaches {@code ramOverlay} or the real map - so both agree on the same key
   * (an overlay entry stored under the raw {@code "$seq"} would never be found by a later {@code GET seq} in the
   * same block) and a reserved name is refused at the point the offending command runs, not deferred to publish
   * time after other commands in the same block may already have committed real document writes.
   */
  private String normalizeRamKey(final String key) {
    return SQLQueryEngine.validateVariableName(key);
  }

  /**
   * Reads a RAM key: from {@code ramOverlay} if this key was already written earlier in the same MULTI/EXEC
   * attempt (a {@code hasKey} check, since a buffered {@code null} - GETDEL's delete - is a real value here,
   * not "absent"), otherwise from the database's global-variables map. See {@link #executeSingleCommandInternal}.
   */
  private Object readRamVariable(final String key, final RamOverlay ramOverlay) {
    if (ramOverlay != null && ramOverlay.hasKey(key))
      return ramOverlay.get(key);
    return database.getGlobalVariable(key);
  }

  /** Writes a RAM key: buffered in {@code ramOverlay} when inside a MULTI/EXEC attempt, applied immediately otherwise. */
  private void writeRamVariable(final String key, final Object value, final RamOverlay ramOverlay) {
    if (ramOverlay != null)
      ramOverlay.setAbsolute(key, value);
    else
      database.setGlobalVariable(key, value);
  }

  /**
   * Applies the INCR/DECR remapping to a RAM key: computed and buffered in {@code ramOverlay} when inside a
   * MULTI/EXEC attempt (that map is private to this attempt, so there is no concurrent access to race reading
   * it back within the block - {@link RamOverlay#publishTo} is what keeps the update atomic against the OUTSIDE
   * world once the block commits), or applied as one atomic {@code computeGlobalVariable} otherwise - the same
   * primitive #8248 gave the wire path, because a plain read-then-write would let two concurrent INCR calls both
   * read the same starting value.
   */
  private Number computeRamVariable(final String key, final UnaryOperator<Object> remapping, final RamOverlay ramOverlay) {
    if (ramOverlay != null) {
      final Object newValue = remapping.apply(readRamVariable(key, ramOverlay));
      ramOverlay.setComputed(key, newValue, remapping);
      return (Number) newValue;
    }
    return (Number) database.computeGlobalVariable(key, remapping);
  }

  private String set(final List<String> parts, final RamOverlay ramOverlay) {
    if (parts.size() < 3) {
      throw new CommandParsingException("SET requires key and value: SET <key> <value>");
    }
    final String key = normalizeRamKey(parts.get(1));
    final String value = parts.get(2);
    writeRamVariable(key, value, ramOverlay);
    return "OK";
  }

  private Object get(final List<String> parts, final RamOverlay ramOverlay) {
    if (parts.size() < 2) {
      throw new CommandParsingException("GET requires a key: GET <key>");
    }
    return readRamVariable(normalizeRamKey(parts.get(1)), ramOverlay);
  }

  private Object getDel(final List<String> parts, final RamOverlay ramOverlay) {
    if (parts.size() < 2) {
      throw new CommandParsingException("GETDEL requires a key: GETDEL <key>");
    }
    final String key = normalizeRamKey(parts.get(1));
    if (ramOverlay != null) {
      // ramOverlay is private to this MULTI/EXEC attempt - no concurrent access to race, so a plain
      // read-then-write is safe here even though it would not be against the real global-variables map below.
      final Object previous = readRamVariable(key, ramOverlay);
      ramOverlay.setAbsolute(key, null);
      return previous;
    }
    // setGlobalVariable atomically returns the previous value - the same reason GETDEL needs it that INCR needs
    // computeGlobalVariable (issue #8248): two concurrent GETDEL calls on the same key must not both read it.
    return database.setGlobalVariable(key, null);
  }

  private int exists(final List<String> parts, final RamOverlay ramOverlay) {
    if (parts.size() < 2) {
      throw new CommandParsingException("EXISTS requires at least one key: EXISTS <key> [key ...]");
    }
    int count = 0;
    for (int i = 1; i < parts.size(); i++) {
      if (readRamVariable(normalizeRamKey(parts.get(i)), ramOverlay) != null) {
        count++;
      }
    }
    return count;
  }

  /**
   * #8271: THE ARITHMETIC AND VALIDATION ARE THE SAME REMAPPING {@code RedisNetworkExecutor} (THE RESP WIRE
   * PATH) USES, so the two surfaces cannot answer this command differently again - a 64-bit increment, a checked
   * add that refuses to overflow silently, and real Redis' own error text.
   */
  private Number incrBy(final List<String> parts, final boolean decimal, final RamOverlay ramOverlay) {
    if (parts.size() < 2) {
      throw new CommandParsingException("INCR/INCRBY requires a key: INCR <key> [increment]");
    }
    final String key = normalizeRamKey(parts.get(1));

    if (decimal) {
      final double increment = parts.size() > 2 ? Double.parseDouble(parts.get(2)) : 1D;
      return computeRamVariable(key, RedisCounterOperations.incrementByFloat(increment), ramOverlay);
    }

    final long increment = parts.size() > 2 ? Long.parseLong(parts.get(2)) : 1L;
    return computeRamVariable(key, RedisCounterOperations.incrementBy(increment), ramOverlay);
  }

  /** See {@link #incrBy}. */
  private Number decrBy(final List<String> parts, final RamOverlay ramOverlay) {
    if (parts.size() < 2) {
      throw new CommandParsingException("DECR/DECRBY requires a key: DECR <key> [decrement]");
    }
    final String key = normalizeRamKey(parts.get(1));
    final long decrement = parts.size() > 2 ? Long.parseLong(parts.get(2)) : 1L;

    return computeRamVariable(key, RedisCounterOperations.decrementBy(decrement), ramOverlay);
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
