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
package com.arcadedb.server.grpc;

import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * The running-query entry of one client-streaming load - {@code InsertStream}, {@code InsertBidirectional},
 * {@code GraphBatchLoad}, {@code TimeSeriesWriteStream} - for the whole life of the stream (issue #9875).
 * <p>
 * {@link GrpcAdmissionInterceptor} lists the RPCs that send one message; these four send many, and their handler keeps
 * per-stream state from the first chunk on, so they open their entry themselves, once the first chunk has said which
 * database and which user the load runs as. Listed for the whole stream rather than per chunk: a load in a
 * {@code PER_STREAM} transaction mode holds one write transaction from its first chunk to its last, between chunks
 * too, and that is what an administrator has to be able to see and stop.
 * <p>
 * <b>How a termination reaches the load.</b> Every chunk runs inside {@link #enter()}, which binds the entry on the
 * thread, so the rows' checks and the commits the chunk attempts obey it as any statement's do. A load that is idle
 * between two chunks is reached by the termination listener given to {@link #open}, which runs only when no chunk is
 * running ({@link #runIfIdle}): it closes the call, and the handler retracts what the stream's mode can still retract
 * on the stream's own thread, where its transaction lives, before it {@link #close() closes} the entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class StreamingLoadEntry {
  /** A binding that binds nothing: the server keeps no registry, or the first chunk has not opened the entry yet. */
  private static final Bound NOT_BOUND = () -> {
  };

  private final RunningQueryRegistry registry;
  private final String               rpc;
  /** The peer of the call, read where this is built (issue #9962). */
  private final RunningQuery.Client  client;
  /** Held by a chunk while it runs, so the termination listener of an idle stream never acts under a running chunk. */
  private final ReentrantLock        working = new ReentrantLock();
  private volatile RunningQuery      entry;

  /**
   * Must be built on the call's gRPC Context - inside the RPC handler, as every caller does - since that is where
   * {@link GrpcAdmissionInterceptor} puts the client: built anywhere else, the entry is listed without one.
   *
   * @param registry the server's running statements, or {@code null} when the service runs without a server
   * @param rpc      the RPC's name, as the listing and the termination message show it
   */
  StreamingLoadEntry(final RunningQueryRegistry registry, final String rpc) {
    this.registry = registry;
    this.rpc = rpc;
    final RunningQuery.Client current = GrpcAdmissionInterceptor.CLIENT_KEY.get();
    this.client = current != null ? current : RunningQuery.Client.NONE;
  }

  /**
   * Opens and lists the entry, once: later calls do nothing. Not published on the calling thread - a chunk binds it with
   * {@link #enter()} - so it can be closed from whichever thread the stream ends on.
   *
   * @param sessionId   the client transaction the load writes into, or {@code null}
   * @param text        what the listing shows as the statement, built only when listed
   * @param onTerminate what stops the load when it is terminated: runs on the terminating thread, so it must not block
   */
  void open(final String database, final String user, final String sessionId, final Supplier<String> text,
      final Runnable onTerminate) {
    if (registry == null || entry != null)
      return;
    final RunningQuery opened = registry.open(database, user, "grpc", sessionId, null, client);
    opened.setStatement("grpc", text);
    entry = opened;
    opened.onTerminate(onTerminate);
  }

  /**
   * Marks the start of a chunk: the entry, once opened, is bound on the calling thread until the returned binding is
   * closed, which must happen on the same thread, in a {@code finally} block. Blocks only behind another chunk of the
   * same call, which gRPC never runs concurrently, or behind the brief {@link #runIfIdle} of a termination. Never call it
   * from the termination listener: that one only ever tries the lock.
   */
  Bound enter() {
    working.lock();
    final RunningQuery current = entry;
    if (current == null)
      return working::unlock;
    final RunningQuery.Binding binding = current.bind();
    return () -> {
      try {
        binding.close();
      } finally {
        working.unlock();
      }
    };
  }

  /**
   * Binds the entry on the calling thread without excluding the termination listener, for a stream whose chunks are
   * already serialized on a thread of their own (the bidirectional insert).
   */
  Bound bind() {
    final RunningQuery current = entry;
    if (current == null)
      return NOT_BOUND;
    final RunningQuery.Binding binding = current.bind();
    return binding::close;
  }

  /**
   * Runs {@code action} if no chunk is running, and answers whether it ran. For the termination listener: a running chunk
   * sees the termination by itself and ends the stream with an exact account of what it did.
   */
  boolean runIfIdle(final Runnable action) {
    if (!working.tryLock())
      return false;
    try {
      action.run();
      return true;
    } finally {
      working.unlock();
    }
  }

  boolean isTerminated() {
    final RunningQuery current = entry;
    return current != null && current.isTerminated();
  }

  /** Fails the chunk if the load was asked to stop. */
  void checkNotTerminated() {
    final RunningQuery current = entry;
    if (current != null)
      current.checkNotTerminated(rpc);
  }

  /** The entry, or {@code null} before the first chunk opened it or when the server keeps no registry. */
  RunningQuery get() {
    return entry;
  }

  /**
   * What the client of a terminated load is told: mapped to {@code CANCELLED}, with the trailer the Java client rebuilds
   * a {@link QueryTerminatedException} from. {@code outcome} says what became of what the stream sent.
   */
  QueryTerminatedException terminated(final String outcome) {
    final RunningQuery current = entry;
    return new QueryTerminatedException(
        rpc + " has been terminated" + (current != null ? " (query " + current.getId() + ", terminated by " + current.getTerminatedBy() + ")" : "")
            + (outcome != null ? ": " + outcome : ""));
  }

  /** Records that what the load wrote was rolled back, so the termination counts as having stopped it. */
  void setRolledBack() {
    final RunningQuery current = entry;
    if (current != null)
      current.setRolledBack();
  }

  /** Ends the entry; idempotent. LAST, once the load is over: the entry leaving the list is the proof that it is. */
  void close() {
    final RunningQuery current = entry;
    if (current != null)
      current.close();
  }

  /** A binding of the entry, closed without a checked exception. */
  @FunctionalInterface
  interface Bound extends AutoCloseable {
    @Override
    void close();
  }
}
