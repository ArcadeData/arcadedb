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
package com.arcadedb.logging;

import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Exports every record over OTLP, and hands it to the logger the configuration chose.
 *
 * <p>A decorator, not a replacement: the delegate is written to <em>first</em> and unconditionally, so
 * turning export on never costs a line of local logging, and a collector that is down or slow cannot
 * take the server's own logs with it.
 *
 * <p>Records carry the trace they belong to. {@code LogManager.Correlation} already holds the request
 * id, the database, and - while the optional tracing plugin is active - the trace and span ids, so a
 * log line in the collector sits next to the span that produced it without anything else being wired.
 *
 * @author Rui Pereira
 */
public class OtlpLogger implements Logger, AutoCloseable {

  /** The scope name log records are attributed to. */
  private static final String            SCOPE          = "com.arcadedb";

  /**
   * Records from the exporter itself are logged locally and never exported.
   *
   * <p>Without this the failure path feeds itself: the collector is unreachable, the SDK logs that
   * through ArcadeDB's logger, this decorator tries to export that line, and it fails the same way.
   * The SDK throttles its own complaints so it does not become unbounded, but the loop exists and the
   * line that matters - the one saying export is broken - is the one that must reach the operator
   * locally rather than be queued for a collector that cannot take it.
   */
  private static final String            SDK_PACKAGE    = "io.opentelemetry";

  /** Set from {@code LogManager.Correlation}, and only when present. */
  private static final AttributeKey<String> REQUEST_ID  = AttributeKey.stringKey("arcadedb.requestId");
  private static final AttributeKey<String> DATABASE    = AttributeKey.stringKey("arcadedb.database");
  private static final AttributeKey<String> TRACE_ID    = AttributeKey.stringKey("arcadedb.traceId");
  private static final AttributeKey<String> SPAN_ID     = AttributeKey.stringKey("arcadedb.spanId");

  /** The class that asked for the line, so the collector can filter by component. */
  private static final AttributeKey<String> LOGGER_NAME = AttributeKey.stringKey("logger.name");

  /** Written to first and always; export never comes at its expense. */
  private final Logger                  delegate;

  /** Null when the SDK could not be built, which turns this into a pass-through rather than a failure. */
  private final SdkLoggerProvider       provider;

  /**
   * Releases the provider if the process ends without anyone replacing this logger.
   *
   * <p>Held so {@link #close()} can remove it: a hook that outlives what it was registered for is the
   * leak it exists to prevent.
   */
  private final Thread                  shutdownHook;

  /** Closing twice must not shut a provider down twice, nor try to remove a hook already running. */
  private final AtomicBoolean           closed = new AtomicBoolean();

  /**
   * Creates the decorator.
   *
   * @param delegate the logger the configuration selected
   * @param endpoint the OTLP endpoint to export to
   */
  public OtlpLogger(final Logger delegate, final String endpoint) {
    this(delegate, SdkLoggerProvider.builder()
        .addLogRecordProcessor(
            BatchLogRecordProcessor.builder(OtlpGrpcLogRecordExporter.builder().setEndpoint(endpoint).build()).build())
        .build());
  }

  /**
   * Creates the decorator over a provider the caller built.
   *
   * <p>Exists so the tests can read back what was emitted - severity, body, attributes - against an
   * in-memory exporter instead of a collector. Without it the only testable half of this class would
   * be the half that does not export, which is the half that matters least.
   *
   * @param delegate the logger the configuration selected
   * @param provider where records are emitted; null turns this into a pass-through
   */
  OtlpLogger(final Logger delegate, final SdkLoggerProvider provider) {
    this.delegate = delegate;
    this.provider = provider;
    this.shutdownHook = provider == null ? null : new Thread(this::shutdownProvider, "arcadedb-otlp-logs-shutdown");
    if (shutdownHook != null)
      Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  /**
   * Flushes what is queued and releases the exporter's thread and connection.
   *
   * <p>The provider owns a batch processor thread and a gRPC channel, so an instance that is replaced
   * - by a runtime settings change - or left behind by a server stopping would keep both. Idempotent,
   * because the shutdown hook and an explicit close can both reach here.
   */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true))
      return;

    if (shutdownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (final IllegalStateException alreadyShuttingDown) {
        // The hook is what is calling us; there is nothing to remove.
      }
    }
    shutdownProvider();
  }

  /**
   * Whether {@link #close()} has run.
   *
   * <p>Package-private for the tests that check a replaced decorator was actually released, which is
   * otherwise invisible from the outside.
   *
   * @return true once closed
   */
  boolean isClosed() {
    return closed.get();
  }

  /**
   * Shuts the provider down, bounded, and never throwing.
   *
   * <p>Bounded because this runs on the shutdown path: a collector that stopped answering must not
   * hold the process open, and losing the last few records is the better trade.
   */
  private void shutdownProvider() {
    if (provider == null)
      return;
    try {
      provider.shutdown().join(5, java.util.concurrent.TimeUnit.SECONDS);
    } catch (final Throwable ignored) {
      // Nothing useful to do while going down, and logging it would go through this very logger.
    }
  }

  @Override
  @SuppressWarnings("PMD.ExcessiveParameterList") // 17 args are mandated by the Logger interface (allocation-free path)
  public void log(final Object requester, final Level level, final String message, final Throwable exception,
      final String context,
      final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
      final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
      final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
    delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
        arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    export(requester, level, message, exception, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11,
        arg12, arg13, arg14, arg15, arg16, arg17);
  }

  @Override
  public void log(final Object requester, final Level level, final String message, final Throwable exception,
      final String context, final Object... args) {
    delegate.log(requester, level, message, exception, context, args);
    export(requester, level, message, exception, args);
  }

  @Override
  public void flush() {
    delegate.flush();
    if (provider != null)
      provider.forceFlush().join(10, java.util.concurrent.TimeUnit.SECONDS);
  }

  /**
   * Emits one record, swallowing anything that goes wrong.
   *
   * <p>Deliberately silent on failure. This is called from every {@code log()} in the server, so a
   * collector that has gone away must not turn one broken log line into a flood of them on the way to
   * reporting it.
   */
  private void export(final Object requester, final Level level, final String message, final Throwable exception,
      final Object... args) {
    if (provider == null || message == null)
      return;

    final String origin = nameOf(requester);
    if (origin != null && origin.startsWith(SDK_PACKAGE))
      return;

    try {
      final LogRecordBuilder record = provider.get(SCOPE).logRecordBuilder()
          .setSeverity(severityOf(level))
          .setSeverityText(level.getName())
          .setBody(formatted(message, args))
          .setAllAttributes(attributesOf(requester, exception));
      record.emit();
    } catch (final Throwable ignored) {
      // A telemetry problem is not worth a log line, which would be exported too.
    }
  }

  /**
   * Applies the arguments to the message, and keeps the raw message when they do not fit.
   *
   * <p>Same tolerance the SLF4J logger has: a format string that does not match its arguments is a
   * defect worth seeing the text of, not a reason to lose the record.
   */
  private static String formatted(final String message, final Object... args) {
    if (args == null || args.length == 0)
      return message;
    try {
      return message.formatted(args);
    } catch (final Exception badFormat) {
      return message;
    }
  }

  /**
   * Builds the record's attributes from whatever the scenario has.
   *
   * @param requester the object that asked for the line
   * @param exception the exception logged with it, if any
   *
   * @return the attributes, possibly empty
   */
  private static Attributes attributesOf(final Object requester, final Throwable exception) {
    final AttributesBuilder attributes = Attributes.builder();

    final String name = nameOf(requester);
    if (name != null)
      attributes.put(LOGGER_NAME, name);

    final LogManager.Correlation correlation = LogManager.instance().getCorrelation();
    if (correlation != null) {
      put(attributes, REQUEST_ID, correlation.requestId());
      put(attributes, DATABASE, correlation.database());
      put(attributes, TRACE_ID, correlation.traceId());
      put(attributes, SPAN_ID, correlation.spanId());
    }

    if (exception != null)
      attributes.put(AttributeKey.stringKey("exception.type"), exception.getClass().getName());

    return attributes.build();
  }

  /**
   * The class name behind whatever asked for the line.
   *
   * @param requester a class, an instance, or null
   *
   * @return the fully qualified name, or null
   */
  private static String nameOf(final Object requester) {
    if (requester == null)
      return null;
    return requester instanceof Class<?> type ? type.getName() : requester.getClass().getName();
  }

  /**
   * Sets an attribute only when there is a value, so absent correlation does not become empty strings.
   */
  private static void put(final AttributesBuilder attributes, final AttributeKey<String> key, final String value) {
    if (value != null && !value.isBlank())
      attributes.put(key, value);
  }

  /**
   * Maps a {@code java.util.logging} level onto an OTLP severity.
   *
   * <p>{@code CONFIG} sits with {@code DEBUG} rather than {@code INFO}: it is emitted at start-up per
   * setting, and an operator watching INFO does not want the configuration dump.
   */
  private static Severity severityOf(final Level level) {
    if (level == null)
      return Severity.INFO;

    final int value = level.intValue();
    if (value >= Level.SEVERE.intValue())
      return Severity.ERROR;
    if (value >= Level.WARNING.intValue())
      return Severity.WARN;
    if (value >= Level.INFO.intValue())
      return Severity.INFO;
    if (value >= Level.FINE.intValue())
      return Severity.DEBUG;
    return Severity.TRACE;
  }
}
