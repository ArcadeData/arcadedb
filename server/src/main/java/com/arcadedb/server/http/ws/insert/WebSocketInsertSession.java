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
package com.arcadedb.server.http.ws.insert;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.websockets.core.WebSocketChannel;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * One duplex insert session opened on {@code /ws} by a {@code start} frame (issue #7382). Mirrors the state the
 * gRPC {@code InsertBidirectional} RPC keeps for the lifetime of its stream: the transaction, the running totals,
 * and the chunk watermark that makes a replayed chunk idempotent.
 * <p>
 * <b>Conflicts.</b> The {@code conflictMode} / {@code keyColumns} / {@code updateColumnsOnConflict} /
 * {@code validateOnly} options are honoured the way {@code ArcadeDbGrpcService.insertRows} honours them (issue
 * #7404): a row is first matched on its {@code keyColumns} with a {@code SELECT} against its type, and the mode
 * decides whether a match is updated in place, dropped, or reported as a {@code CONFLICT}. A duplicate the engine
 * itself detects - a unique index the session named no key columns for - is answered by the same mode, wherever
 * it surfaces: at the row's own {@code save()} when the twin was written by this same transaction, at the chunk's
 * commit under {@code per_batch}, at the row's commit under {@code per_row}, and at the session's {@code commit}
 * frame under {@code per_stream}. That last case is the engine's contract, not this session's: a unique index is
 * checked against durable state when the transaction commits, so a session that wants a conflict reported on
 * the row that caused it names its {@code keyColumns}.
 * <p>
 * <b>Threading.</b> The session's {@link TransactionContext} is bound to whichever thread is currently running one
 * of its frames and detached again when that frame finishes - the same borrow-per-request lifecycle
 * {@code DatabaseAbstractHandler} gives an {@code arcadedb-session-id} transaction, and the reason no thread has to
 * be dedicated to a session. {@link WebSocketInsertSessionManager} guarantees the frames of one channel do not
 * overlap; {@link #lock} guards against the idle sweep and the connection-close hook, which do not come through
 * that ordering.
 *
 * @author Arcade Data Ltd
 */
public class WebSocketInsertSession {
  /** Record keys that name the record itself rather than one of its properties. */
  private static final String CLASS_KEY = "@class";
  private static final String FROM_KEY  = "@from";
  private static final String TO_KEY    = "@to";
  /** gRPC spells an edge's endpoints {@code out} / {@code in}; accepted so a ported loader keeps working. */
  private static final String OUT_KEY   = "out";
  private static final String IN_KEY    = "in";

  public final  String                        id;
  public final  String                        databaseName;
  public final  ServerSecurityUser            user;
  public final  UUID                          channelId;
  public final  InsertSessionOptions          options;
  private final DatabaseInternal              database;
  private final ReentrantLock                 lock       = new ReentrantLock();
  private final long                          startedAt  = System.currentTimeMillis();
  /** Null in every mode but {@code PER_STREAM}, where it is the transaction the client's frames decide. */
  private       TransactionContext            transaction;
  private       long                          received;
  private       long                          inserted;
  private       long                          updated;
  private       long                          ignored;
  private       long                          failed;
  /** Highest chunk sequence already applied. A chunk at or below it is acknowledged without being applied again. */
  private       long                          watermark;
  private volatile boolean                    closed;
  /** Set once the "out/in in updateColumnsOnConflict are ignored" warning has been logged for this session. */
  private          boolean                    warnedEdgeEndpointUpdateColumns;
  private volatile long                       lastUsed   = System.currentTimeMillis();
  /** The connection this session was opened on, so the idle sweep can tell its client it gave up on it. */
  private volatile WebSocketChannel            channel;

  WebSocketInsertSession(final String id, final DatabaseInternal database, final ServerSecurityUser user,
      final UUID channelId, final InsertSessionOptions options) {
    this.id = id;
    this.database = database;
    this.databaseName = database.getName();
    this.user = user;
    this.channelId = channelId;
    this.options = options;
  }

  /**
   * Begins the session's own transaction. Only {@code PER_STREAM} has one: the other modes open and commit a
   * transaction inside {@link #applyChunk}, which is what makes their acknowledged chunks durable before the
   * client has said anything.
   */
  void begin() {
    if (options.transactionMode != InsertSessionOptions.TransactionMode.PER_STREAM)
      return;

    DatabaseContext.INSTANCE.init(database);
    try {
      database.begin();
      transaction = database.getTransaction();
      // The requester is what lets a lock taken on this thread be released from another one, which is exactly
      // what a session whose frames land on different worker threads needs. Same reason PostBeginHandler sets it.
      transaction.setRequester(id);
    } finally {
      DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
    }
  }

  /**
   * Attaches the connection the session belongs to. Called by {@link WebSocketInsertSessionManager#start} BEFORE
   * the session is registered, so the idle sweep can never find a registered session with no channel to dispatch
   * its expiry to - and therefore never has to run a rollback on the one sweep thread for the whole server.
   */
  void setChannel(final WebSocketChannel channel) {
    this.channel = channel;
  }

  public WebSocketChannel getChannel() {
    return channel;
  }

  public long elapsedFromLastUse() {
    return System.currentTimeMillis() - lastUsed;
  }

  public boolean isClosed() {
    return closed;
  }

  /**
   * Applies one {@code chunk} frame and returns the {@code batchAck} to answer it with.
   * <p>
   * A chunk at or below the watermark is a replay: it is acknowledged with zero tallies and NOT applied again,
   * mirroring the gRPC path. A row that fails is counted in {@code failed} and described in {@code errors}; the
   * rest of the chunk still goes in, because a duplex session exists so the client can decide what to do about a
   * partial chunk rather than have the server decide by aborting.
   * <p>
   * What "applied" means differs by transaction mode, and the difference is visible to a client that replays.
   * Under {@code PER_STREAM} and {@code PER_BATCH} the chunk is all-or-nothing, so a chunk whose transaction failed
   * leaves the watermark where it was and replaying that sequence applies it. Under {@code PER_ROW} every row
   * commits on its own, so no chunk is all-or-nothing and the watermark advances on any chunk that was tried -
   * including one whose rows ALL failed. Resending that sequence is answered as a replay rather than retried; a
   * client that wants a failed row in resends it under a new sequence, reading which rows off {@code errors[]}.
   * Retrying the whole chunk instead would double-apply the rows of it that had succeeded.
   * <p>
   * Anything else - a sequence that skips ahead of the next one due - is REFUSED. A watermark that simply
   * follows whatever arrives would jump to 5 when a client sent chunk 5 before chunk 2, and chunk 2 would then
   * be acknowledged as a replay of something that never happened: every row it carried silently dropped, with a
   * successful-looking answer. Chunks are therefore contiguous from 1, which a client sending them in order
   * satisfies without doing anything, and a gap is an error rather than an undocumented hazard.
   */
  JSONObject applyChunk(final long chunkSeq, final JSONArray records) {
    lock.lock();
    try {
      requireOpen();
      // Stamped on entry as well as on the way out: a frame that throws - a skipped sequence, a malformed record -
      // never reaches the closing stamp, and a client whose chunks are being refused is still a client using the
      // session. Without this, a run of refusals would let the idle sweep roll the session back underneath it.
      lastUsed = System.currentTimeMillis();

      final JSONObject ack = new JSONObject();
      ack.put("result", "ok");
      ack.put("action", "batchAck");
      ack.put("sessionId", id);
      ack.put("chunkSeq", chunkSeq);

      if (chunkSeq > watermark + 1)
        throw new IllegalArgumentException(
            "Chunk " + chunkSeq + " skips ahead: session '" + id + "' has applied up to chunk " + watermark
                + " and expects " + (watermark + 1) + " next. Chunk sequences must be contiguous from 1");

      if (chunkSeq <= watermark) {
        ack.put("received", 0L);
        ack.put("inserted", 0L);
        ack.put("updated", 0L);
        ack.put("ignored", 0L);
        ack.put("failed", 0L);
        ack.put("replay", true);
        return ack;
      }

      final ChunkCounts counts = new ChunkCounts();
      final int rows = records == null ? 0 : records.length();

      DatabaseContext.INSTANCE.init(database, transaction);
      // The key lookups below are real SQL: tagged with the transport they serve so they are metered as such,
      // which is the misattribution issue #7407 found on the gRPC streaming inserts.
      ProtocolContext.set("ws");
      try {
        DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(user.getDatabaseUser(database));

        switch (options.transactionMode) {
        case PER_STREAM -> applyRows(records, rows, counts);
        case PER_BATCH -> {
          try {
            counts.absorb(inOwnTransaction(attempt -> applyRows(records, rows, attempt)));
          } catch (final Exception e) {
            // The chunk's own transaction failed to commit, so nothing in it is durable. Report the whole chunk
            // as failed - the per-row tallies the attempts produced describe transactions that no longer exist.
            counts.resetToWholeChunkFailure(rows, e);
          }
        }
        case PER_ROW -> {
          for (int i = 0; i < rows; i++) {
            final int row = i;
            try {
              counts.absorb(inOwnTransaction(attempt -> applyRowCounting(records, row, attempt)));
            } catch (final DuplicatedKeyException e) {
              // The row's own commit hit a unique index the session named no key columns for. The row IS the
              // transaction here, so the mode can still answer per row: dropped under ignore, a CONFLICT
              // otherwise.
              if (options.conflictMode == InsertSessionOptions.ConflictMode.IGNORE)
                counts.ignored++;
              else
                counts.conflict(row, e);
            } catch (final Exception e) {
              // Anything else that stops the row's commit is reported on the row, since the row is the transaction.
              counts.fail(row, e);
            }
          }
        }
        default -> throw new IllegalStateException("Unsupported transaction mode " + options.transactionMode);
        }
      } finally {
        ProtocolContext.clear();
        DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
      }

      received += rows;
      inserted += counts.inserted;
      updated += counts.updated;
      ignored += counts.ignored;
      failed += counts.failed;
      // The watermark advances only on a chunk that was applied without a whole-chunk failure, so a client that
      // replays a chunk whose transaction never committed gets it applied rather than acknowledged as a duplicate.
      // Only PER_STREAM and PER_BATCH can report a whole-chunk failure: PER_ROW commits row by row, so it advances
      // even when every row failed, and the client resends those rows under a new sequence. See the javadoc.
      if (!counts.wholeChunkFailed)
        watermark = chunkSeq;

      ack.put("received", (long) rows);
      ack.put("inserted", counts.inserted);
      ack.put("updated", counts.updated);
      ack.put("ignored", counts.ignored);
      ack.put("failed", counts.failed);
      if (!counts.errors.isEmpty())
        ack.put("errors", counts.errors);

      lastUsed = System.currentTimeMillis();
      return ack;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Ends the session, committing or discarding what it wrote, and returns the {@code committed} frame carrying the
   * full-session totals.
   *
   * @param commit {@code true} for a {@code commit} frame, {@code false} for a {@code rollback} one
   */
  JSONObject finish(final boolean commit) {
    lock.lock();
    try {
      requireOpen();

      // Closed BEFORE the commit is attempted: a commit the engine refuses - a unique index it checks only now,
      // see the class comment - rolls the transaction back, and a session whose transaction is gone must not
      // stay open to take more chunks.
      closed = true;

      if (transaction != null) {
        DatabaseContext.INSTANCE.init(database, transaction);
        try {
          if (commit)
            database.commit();
          else
            database.rollback();
        } finally {
          DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
          transaction = null;
        }
      }

      final JSONObject summary = new JSONObject();
      summary.put("received", received);
      summary.put("inserted", inserted);
      summary.put("updated", updated);
      summary.put("ignored", ignored);
      summary.put("failed", failed);
      summary.put("executionTimeMs", System.currentTimeMillis() - startedAt);
      // PER_BATCH and PER_ROW commit as they go, so a rollback frame cannot take back a chunk the client has
      // already been acknowledged for. Say so in the answer rather than letting the outcome imply otherwise.
      summary.put("partialCommit", options.transactionMode != InsertSessionOptions.TransactionMode.PER_STREAM);

      final JSONObject response = new JSONObject();
      response.put("result", "ok");
      response.put("action", "committed");
      response.put("sessionId", id);
      response.put("outcome", commit ? "commit" : "rollback");
      response.put("summary", summary);
      return response;
    } finally {
      lock.unlock();
    }
  }

  /**
   * What {@link #cancelIfIdle()} did, so the idle sweep can tell "leave it registered and look again next tick"
   * from "it is gone".
   */
  enum CancelOutcome {
    /** A frame is being applied right now: the session is NOT idle and must be left alone this sweep. */
    BUSY,
    /** This call is the one that closed the session. */
    CANCELLED,
    /** Somebody else had already closed it. */
    ALREADY_CLOSED
  }

  /**
   * Discards whatever the session wrote and marks it closed. Called by the connection-close hook and by server
   * shutdown - paths that end a session WITHOUT the client having said what to do with it, which is why the
   * answer is always rollback. Waits for a frame in flight rather than tearing its transaction down underneath
   * it, which is the race issue #4857 fixed for the HTTP sessions; callers therefore run it off the I/O thread.
   *
   * @return {@code true} when this call is the one that closed the session
   */
  boolean cancel() {
    lock.lock();
    try {
      return rollbackAndClose();
    } finally {
      lock.unlock();
    }
  }

  /**
   * The idle sweep's version: gives up rather than waiting. A session with a frame in flight is not idle at all -
   * its {@code lastUsed} was stamped when that frame STARTED, so a chunk that takes longer than the timeout looks
   * abandoned from outside - and rolling its transaction back from the timer thread while a worker thread is
   * still writing into it is the same defect issue #4857 closed for {@code HttpSession}.
   */
  CancelOutcome cancelIfIdle() {
    if (!lock.tryLock())
      return CancelOutcome.BUSY;
    try {
      return rollbackAndClose() ? CancelOutcome.CANCELLED : CancelOutcome.ALREADY_CLOSED;
    } finally {
      lock.unlock();
    }
  }

  /** Must be called holding {@link #lock}. */
  private boolean rollbackAndClose() {
    if (closed)
      return false;
    closed = true;

    if (transaction != null) {
      DatabaseContext.INSTANCE.init(database, transaction);
      try {
        database.rollback();
      } catch (final Exception e) {
        // The session is being torn down; a rollback that cannot run leaves nothing further to do here.
      } finally {
        DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
        transaction = null;
      }
    }
    return true;
  }

  private void requireOpen() {
    if (closed)
      throw new IllegalStateException("Insert session '" + id + "' is closed");
  }

  /**
   * Runs {@code work} inside a transaction of its own and returns the tallies of the attempt that COMMITTED.
   * <p>
   * {@link com.arcadedb.database.Database#transaction(com.arcadedb.database.Database.TransactionScope)} re-runs
   * its block up to {@code arcadedb.txRetries} times when the commit hits a transient conflict, and this engine
   * conflicts at page granularity, so under concurrent load that is an ordinary outcome rather than an exotic
   * one. Every attempt therefore gets a FRESH {@link ChunkCounts}: a single accumulator shared across attempts
   * counts every row of every attempt, so a chunk that landed once, correctly, on the second try would be
   * acknowledged with twice its true {@code inserted} and a duplicate entry per {@code errors} row - and a
   * bulk-load protocol whose whole value is a trustworthy acknowledgement cannot afford that. The tallies of the
   * abandoned attempts are dropped with the transaction that produced them.
   */
  private ChunkCounts inOwnTransaction(final Consumer<ChunkCounts> work) {
    final ChunkCounts[] committed = new ChunkCounts[1];
    database.transaction(() -> {
      final ChunkCounts attempt = new ChunkCounts();
      committed[0] = attempt;
      work.accept(attempt);
    });
    return committed[0];
  }

  private void applyRows(final JSONArray records, final int rows, final ChunkCounts counts) {
    for (int i = 0; i < rows; i++)
      applyRowCounting(records, i, counts);
  }

  /** {@link #applyRow} with its outcome tallied: a row that cannot be applied is counted, never thrown. */
  private void applyRowCounting(final JSONArray records, final int rowIndex, final ChunkCounts counts) {
    try {
      applyRow(records.getJSONObject(rowIndex), rowIndex, counts);
    } catch (final DuplicatedKeyException e) {
      // Reaches here only from the modes that report a duplicate rather than absorb it.
      counts.conflict(rowIndex, e);
    } catch (final Exception e) {
      counts.fail(rowIndex, e);
    }
  }

  /**
   * Applies one record under the session's conflict mode. Throws for a row that could not be applied; the
   * caller decides how that is tallied. A {@link DuplicatedKeyException} that escapes is one the mode wants
   * reported as a {@code CONFLICT}.
   */
  private void applyRow(final JSONObject record, final int rowIndex, final ChunkCounts counts) {
    final String typeName = record.getString(CLASS_KEY, options.targetType);
    if (typeName == null || typeName.isBlank())
      throw new IllegalArgumentException(
          "Record " + rowIndex + " has no type: set '@class' on it or 'targetType' in the start frame options");

    final DocumentType type = database.getSchema().getType(typeName);
    final boolean isEdge = type instanceof EdgeType;
    final Map<String, Object> properties = record.toMap();
    properties.remove(CLASS_KEY);

    String from = null;
    String to = null;
    if (isEdge) {
      from = firstNonBlank(record.getString(FROM_KEY, null), record.getString(OUT_KEY, null));
      to = firstNonBlank(record.getString(TO_KEY, null), record.getString(IN_KEY, null));
      if (from == null || to == null)
        throw new IllegalArgumentException(
            "Edge record " + rowIndex + " of type '" + typeName + "' needs both '@from' and '@to' (or 'out' and 'in')");

      properties.remove(FROM_KEY);
      properties.remove(TO_KEY);
      properties.remove(OUT_KEY);
      properties.remove(IN_KEY);

      warnAboutEdgeEndpointUpdateColumns(typeName);
    }

    // A dry run parses and validates the row - the type, the endpoints - and stops here, so `received` counts it
    // and nothing else does. Same as gRPC's validate_only.
    if (options.validateOnly)
      return;

    try {
      switch (options.conflictMode) {
      case UPDATE -> {
        if (updateExisting(typeName, properties)) {
          counts.updated++;
          return;
        }
      }
      case IGNORE -> {
        if (findByKey(typeName, properties) != null) {
          counts.ignored++;
          return;
        }
      }
      case ERROR, ABORT -> {
        // With key columns the conflict is found on the row that caused it rather than left to the engine,
        // which reports it only where the transaction commits.
        final RID taken = findByKey(typeName, properties);
        if (taken != null)
          throw new DuplicatedKeyException(typeName + options.keyColumns, keyValues(properties), taken);
      }
      }

      insert(type, typeName, properties, from, to);
      counts.inserted++;
    } catch (final DuplicatedKeyException dup) {
      switch (options.conflictMode) {
      case IGNORE -> counts.ignored++;
      // A concurrent writer landed this key after the lookup above; the index proves it exists now, so update
      // it rather than lose the row. If the match is gone again (a transient window), or a third writer races
      // the retry too, it is a retriable CONFLICT. Anything else is a real failure, not a conflict.
      case UPDATE -> {
        if (updateExisting(typeName, properties))
          counts.updated++;
        else
          throw dup;
      }
      case ERROR, ABORT -> throw dup;
      }
    }
  }

  private void insert(final DocumentType type, final String typeName, final Map<String, Object> properties,
      final String from, final String to) {
    if (type instanceof EdgeType) {
      final Vertex fromVertex = database.lookupByRID(database.newRID(from), false).asVertex(false);
      final MutableEdge edge = fromVertex.newEdge(typeName, database.newRID(to));
      edge.set(properties);
      edge.save();
    } else if (type instanceof VertexType) {
      final MutableVertex vertex = database.newVertex(typeName);
      vertex.set(properties);
      vertex.save();
    } else {
      final MutableDocument document = database.newDocument(typeName);
      document.set(properties);
      document.save();
    }
  }

  /**
   * Matches an existing record of {@code typeName} on the session's key columns and merges the incoming values
   * onto it: the {@code updateColumnsOnConflict} when the session names some, every non-key property sent
   * otherwise. A property whose incoming value is absent or null is left as it is, so nothing can be nulled
   * through this path. An edge's endpoints are never rewritten - {@code set()} would bypass the graph engine's
   * edge-list bookkeeping - which needs no check here: {@link #applyRow} strips {@code @from}/{@code @to} and
   * {@code out}/{@code in} out of {@code properties} before this runs, so naming them in
   * {@code updateColumnsOnConflict} finds no value and is skipped like any absent column. The same rules as
   * gRPC's {@code applyConflictUpdates}.
   *
   * @return {@code false} when no record matched, in which case the caller inserts
   */
  private boolean updateExisting(final String typeName, final Map<String, Object> properties) {
    try (final ResultSet rs = lookupByKey(typeName, properties)) {
      if (!rs.hasNext())
        return false;

      final Result match = rs.next();
      if (!match.isElement())
        return false;

      final MutableDocument existing = match.getElement().get().asDocument().modify();

      final boolean mergeAll = options.updateColumnsOnConflict.isEmpty();
      final Iterable<String> columns = mergeAll ? properties.keySet() : options.updateColumnsOnConflict;
      for (final String column : columns) {
        if (column.startsWith("@"))
          continue;
        if (mergeAll && options.keyColumnSet.contains(column))
          continue;
        final Object value = properties.get(column);
        if (value == null)
          continue;
        existing.set(column, value);
      }
      existing.save();
      return true;
    }
  }

  /** @return the record of {@code typeName} carrying this row's key values, or {@code null} when there is none */
  private RID findByKey(final String typeName, final Map<String, Object> properties) {
    if (options.keyColumns.isEmpty())
      return null;
    try (final ResultSet rs = lookupByKey(typeName, properties)) {
      return rs.hasNext() ? rs.next().getIdentity().orElse(null) : null;
    }
  }

  /**
   * {@code SELECT FROM type WHERE k1 = ? AND k2 = ?} on the session's key columns. Identifiers are
   * backtick-quoted so a client-supplied name cannot inject SQL; the values stay parameters.
   */
  private ResultSet lookupByKey(final String typeName, final Map<String, Object> properties) {
    final List<String> keys = options.keyColumns;
    final StringBuilder sql = new StringBuilder("SELECT FROM ").append(Identifier.quote(typeName)).append(" WHERE ");
    final Object[] params = new Object[keys.size()];
    for (int i = 0; i < params.length; i++) {
      if (i > 0)
        sql.append(" AND ");
      sql.append(Identifier.quote(keys.get(i))).append(" = ?");
      params[i] = properties.get(keys.get(i));
    }
    return database.query("sql", sql.toString(), params);
  }

  private String keyValues(final Map<String, Object> properties) {
    final List<String> keys = options.keyColumns;
    final Object[] values = new Object[keys.size()];
    for (int i = 0; i < values.length; i++)
      values[i] = properties.get(keys.get(i));
    return Arrays.toString(values);
  }

  /**
   * Tells the operator, once per session, that the endpoints named in {@code updateColumnsOnConflict} are not
   * going to be rewritten, rather than dropping them silently. Same warning gRPC logs for an edge target.
   */
  private void warnAboutEdgeEndpointUpdateColumns(final String typeName) {
    if (warnedEdgeEndpointUpdateColumns || options.conflictMode != InsertSessionOptions.ConflictMode.UPDATE)
      return;
    if (!options.updateColumnsOnConflict.contains(OUT_KEY) && !options.updateColumnsOnConflict.contains(IN_KEY))
      return;
    warnedEdgeEndpointUpdateColumns = true;
    LogManager.instance().log(this, Level.WARNING,
        "/ws insert session %s: upsert on edge type '%s' ignores 'out'/'in' in updateColumnsOnConflict (edge endpoints cannot be re-pointed by an upsert)",
        id, typeName);
  }

  private static String firstNonBlank(final String first, final String second) {
    if (first != null && !first.isBlank())
      return first;
    return second != null && !second.isBlank() ? second : null;
  }

  /** Per-chunk tallies, the shape of the gRPC {@code BatchAck} counters. */
  private static final class ChunkCounts {
    private       JSONArray errors = new JSONArray();
    private       long      inserted;
    private       long      updated;
    private       long      ignored;
    private       long      failed;
    private       boolean   wholeChunkFailed;

    /** Adds the tallies of a committed attempt to the chunk's running totals. */
    private void absorb(final ChunkCounts other) {
      inserted += other.inserted;
      updated += other.updated;
      ignored += other.ignored;
      failed += other.failed;
      for (final Object error : other.errors)
        errors.put(error);
    }

    private void fail(final int rowIndex, final Exception e) {
      failed++;
      errors.put(error(rowIndex, codeOf(e), e));
    }

    /** A row refused because its key is already taken: the {@code CONFLICT} code of the gRPC {@code InsertError}. */
    private void conflict(final int rowIndex, final DuplicatedKeyException e) {
      failed++;
      errors.put(error(rowIndex, "CONFLICT", e));
    }

    /** A duplicate the engine reported on a commit rather than on a row is still a conflict, not a server fault. */
    private static String codeOf(final Exception e) {
      return e instanceof DuplicatedKeyException ? "CONFLICT" : "DB_ERROR";
    }

    /**
     * A failure that belongs to the chunk's transaction rather than to any one row: nothing in the chunk is
     * durable, so the per-row tallies are replaced instead of added to. Reported with {@code rowIndex} -1, the
     * "not applicable" value the gRPC {@code InsertError} contract uses for the same case.
     */
    private void resetToWholeChunkFailure(final int rows, final Exception e) {
      inserted = 0;
      updated = 0;
      ignored = 0;
      failed = rows;
      wholeChunkFailed = true;
      errors = new JSONArray();
      errors.put(error(-1, codeOf(e), e));
    }

    private static JSONObject error(final int rowIndex, final String code, final Exception e) {
      final JSONObject error = new JSONObject();
      error.put("rowIndex", rowIndex);
      error.put("code", code);
      error.put("message", e.getMessage() != null ? e.getMessage() : e.toString());
      error.put("exception", e.getClass().getName());
      return error;
    }
  }
}
