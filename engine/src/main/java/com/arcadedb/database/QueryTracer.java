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
package com.arcadedb.database;

/**
 * Framework-agnostic hook the engine calls to open a tracing span around a query/command execution.
 * Lives in the engine (which has Micrometer only at test scope); the server registers a
 * Micrometer/OpenTelemetry-backed implementation. The default is a no-op, so when no tracer is
 * registered the engine pays only a volatile read and a sentinel check per query - no span, no
 * allocation. Mirrors {@link QueryMetricsRecorder}: a single engine-boundary instrumentation point
 * that serves every wire protocol, with the originating protocol read from {@link ProtocolContext}.
 */
@FunctionalInterface
public interface QueryTracer {
  QueryTracer NO_OP = (protocol, database, language, type, query) -> Span.NO_OP;

  /**
   * Opens a span for an executing query/command. The returned {@link Span} must be closed when the
   * execution completes (use try-with-resources).
   *
   * @param protocol originating wire protocol (e.g. http, bolt, postgres, internal)
   * @param database database name
   * @param language query language (sql, opencypher, ...)
   * @param type     {@code query} or {@code command}
   * @param query    the query/command text (recorded as a span attribute only, never a metric tag)
   */
  Span beginQuery(String protocol, String database, String language, String type, String query);

  /** Handle for an open span. Closing it ends the span; closing is idempotent and never throws. */
  interface Span extends AutoCloseable {
    Span NO_OP = () -> {
    };

    @Override
    void close();
  }

  /** Static registration + helpers, kept off the interface's public surface. */
  final class Holder {
    private static volatile QueryTracer current = NO_OP;

    private Holder() {
    }

    public static void register(final QueryTracer tracer) {
      current = tracer != null ? tracer : NO_OP;
    }

    public static QueryTracer get() {
      return current;
    }

    /**
     * Opens a span for the current execution, reading the originating protocol from
     * {@link ProtocolContext}. Returns the no-op span (zero allocation) when no tracer is registered.
     */
    public static Span begin(final String database, final String language, final String type, final String query) {
      final QueryTracer tracer = current;
      return tracer == NO_OP ? Span.NO_OP : tracer.beginQuery(ProtocolContext.get(), database, language, type, query);
    }
  }
}
