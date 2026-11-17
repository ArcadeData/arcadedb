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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.server.HAReplicatedDatabase;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.Locale;

/**
 * Read-your-writes on a Raft cluster for gRPC clients, the contract HTTP has had since issue #5845 (issue #11948).
 * <ul>
 *   <li>Every response of a call that reached a replicated database carries {@code x-arcadedb-commit-index}: the last
 *   Raft index the answering server had applied when it answered. After a commit that is a bookmark covering it.</li>
 *   <li>A call that sends that value back as {@code x-arcadedb-read-after} makes a follower apply at least that much
 *   before it reads, and {@code x-arcadedb-read-consistency} ({@code eventual}, {@code read_your_writes},
 *   {@code linearizable}) picks the level. Without the header the server's {@code arcadedb.ha.readConsistency}
 *   applies, as it does to HTTP reads.</li>
 * </ul>
 * The consistency is a thread-local of the replicated database, so it is set where the statement runs: by
 * {@link ArcadeDbGrpcService} when it resolves the database on the call's thread, and on the dedicated thread of a
 * client transaction for the statements it runs there. This interceptor parses the headers, clears what the call's
 * thread was given after each callback, and writes the bookmark into the response.
 */
class GrpcReadConsistencyInterceptor implements ServerInterceptor {

  static final Metadata.Key<String> READ_CONSISTENCY_HEADER =
      Metadata.Key.of("x-arcadedb-read-consistency", Metadata.ASCII_STRING_MARSHALLER);
  static final Metadata.Key<String> READ_AFTER_HEADER       =
      Metadata.Key.of("x-arcadedb-read-after", Metadata.ASCII_STRING_MARSHALLER);
  static final Metadata.Key<String> COMMIT_INDEX_HEADER     =
      Metadata.Key.of("x-arcadedb-commit-index", Metadata.ASCII_STRING_MARSHALLER);

  private static final Context.Key<ReadConsistency> CALL = Context.key("arcadedb-read-consistency");

  /** What one call asked for, and the replicated database it reached, whose applied index its response carries. */
  static final class ReadConsistency {
    private final    Database.READ_CONSISTENCY requested;
    private final    long                      readAfter;
    private volatile HAReplicatedDatabase      database;

    ReadConsistency(final Database.READ_CONSISTENCY requested, final long readAfter) {
      this.requested = requested;
      this.readAfter = readAfter;
    }

    /**
     * Applies the call's consistency to the statements the current thread runs next on {@code database}; a no-op on a
     * database that is not replicated. Undone by {@link #clear()}.
     */
    void applyTo(final Database database) {
      final HAReplicatedDatabase haDb = replicated(database);
      if (haDb != null)
        applyTo(haDb, requested != null ? null :
            database.getConfiguration().getValueAsString(GlobalConfiguration.HA_READ_CONSISTENCY));
    }

    /**
     * Binds {@code haDb} and sets the call's consistency on the current thread: the requested level, or else
     * {@code configured} ({@code arcadedb.ha.readConsistency} of the database), the level a read without headers gets.
     */
    void applyTo(final HAReplicatedDatabase haDb, final String configured) {
      bind(haDb);
      final Database.READ_CONSISTENCY consistency = requested != null ? requested : parseConsistency(configured);
      if (consistency != null)
        haDb.setReadConsistencyContext(consistency, readAfter);
    }

    /** Records the replicated database the call reached, for the bookmark of its response, without touching the thread. */
    void bind(final Database database) {
      final HAReplicatedDatabase haDb = replicated(database);
      if (haDb != null)
        bind(haDb);
    }

    void bind(final HAReplicatedDatabase haDb) {
      this.database = haDb;
    }

    /**
     * Clears the current thread's consistency. The context is ONE thread-local shared by every replicated database
     * ({@code RaftReplicatedDatabase}), so clearing through the database bound last also clears what an earlier
     * database of the same call set on this thread.
     */
    void clear() {
      final HAReplicatedDatabase haDb = database;
      if (haDb != null)
        haDb.clearReadConsistencyContext();
    }

    long appliedIndex() {
      final HAReplicatedDatabase haDb = database;
      return haDb != null ? haDb.getLastAppliedIndex() : -1;
    }
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call, final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {
    final ReadConsistency state;
    try {
      state = new ReadConsistency(parseConsistency(headers.get(READ_CONSISTENCY_HEADER)),
          parseReadAfter(headers.get(READ_AFTER_HEADER)));
    } catch (final IllegalArgumentException e) {
      // The value is not echoed back, as over HTTP: it is client input
      call.close(Status.INVALID_ARGUMENT.withDescription(e.getMessage()), new Metadata());
      return new ServerCall.Listener<>() {
      };
    }

    final ServerCall<ReqT, RespT> withBookmark = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
      private boolean headersSent;

      @Override
      public void sendHeaders(final Metadata responseHeaders) {
        putBookmark(responseHeaders, state);
        headersSent = true;
        super.sendHeaders(responseHeaders);
      }

      @Override
      public void close(final Status status, final Metadata trailers) {
        // A call answered without headers (an error, or an empty stream) carries the bookmark in its trailers: on a
        // failure the commits made before it still have to be readable, the same rule as HTTP (issue #5862)
        if (!headersSent)
          putBookmark(trailers, state);
        super.close(status, trailers);
      }
    };

    final ServerCall.Listener<ReqT> delegate = Contexts.interceptCall(Context.current().withValue(CALL, state), withBookmark,
        headers, next);
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
      @Override
      public void onMessage(final ReqT message) {
        runAndClear(() -> super.onMessage(message), state);
      }

      @Override
      public void onHalfClose() {
        runAndClear(super::onHalfClose, state);
      }

      @Override
      public void onCancel() {
        runAndClear(super::onCancel, state);
      }

      @Override
      public void onComplete() {
        runAndClear(super::onComplete, state);
      }

      @Override
      public void onReady() {
        runAndClear(super::onReady, state);
      }
    };
  }

  /**
   * Runs one listener callback and clears what it gave the call's thread: whatever resolved a database there set the
   * consistency on that thread, which gRPC hands to the next call once the callback returns.
   */
  private static void runAndClear(final Runnable callback, final ReadConsistency state) {
    try {
      callback.run();
    } finally {
      state.clear();
    }
  }

  /**
   * Wraps work a call hands to a thread of its own (the executor of a bidirectional stream), which resolves the database
   * there and so sets the consistency on that thread, where no listener callback clears it. Run it inside the call's
   * {@link Context} ({@code Context.wrap}), so {@link #current()} finds the call's state.
   */
  static Runnable clearingAfter(final Runnable task) {
    return () -> {
      try {
        task.run();
      } finally {
        final ReadConsistency state = current();
        if (state != null)
          state.clear();
      }
    };
  }

  /** The call's state when the current thread runs inside one, for the work it hands to another thread. */
  static ReadConsistency current() {
    return CALL.get();
  }

  static Database.READ_CONSISTENCY parseConsistency(final String raw) {
    if (raw == null || raw.isBlank())
      return null;
    try {
      return Database.READ_CONSISTENCY.valueOf(raw.trim().toUpperCase(Locale.ROOT));
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid read consistency: expected eventual, read_your_writes or linearizable");
    }
  }

  static long parseReadAfter(final String raw) {
    if (raw == null || raw.isBlank())
      return -1;
    try {
      return Long.parseLong(raw.trim());
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Invalid read-consistency bookmark");
    }
  }

  private static void putBookmark(final Metadata metadata, final ReadConsistency state) {
    final long applied = state.appliedIndex();
    if (applied >= 0)
      metadata.put(COMMIT_INDEX_HEADER, String.valueOf(applied));
  }

  private static HAReplicatedDatabase replicated(final Database database) {
    if (database instanceof HAReplicatedDatabase haDb)
      return haDb;
    if (database instanceof DatabaseInternal internal && internal.getWrappedDatabaseInstance() instanceof HAReplicatedDatabase haDb)
      return haDb;
    return null;
  }
}
