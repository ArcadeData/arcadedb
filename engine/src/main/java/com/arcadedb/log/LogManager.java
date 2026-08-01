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
package com.arcadedb.log;

import java.util.Locale;
import java.util.logging.Level;

/**
 * Centralized Log Manager.
 *
 * @author Luca Garulli
 */
public class LogManager {
  /**
   * System property selecting the {@link Logger} implementation, applied at startup without any code
   * change. Unset (or any value other than {@code slf4j}) keeps the default {@link DefaultLogger}
   * (java.util.logging); {@code slf4j} installs {@link Slf4jLogger}, routing the engine's logs
   * through the SLF4J facade so an embedding application receives them in its own backend.
   * <p>
   * It is also the key of {@code GlobalConfiguration.LOG_IMPL}, which applies the same choice at any
   * time - including after this class has been loaded, which the system property cannot do because it
   * is read from the static initialiser below. The logger can additionally be swapped programmatically
   * via {@link #setLogger(Logger)}.
   */
  public  static final String                        LOG_IMPL_PROPERTY    = "arcadedb.log.impl";
  private static final LogContext                    CONTEXT_INSTANCE     = new LogContext();
  private static final ThreadLocal<Correlation>      CORRELATION_INSTANCE = new ThreadLocal<>();
  private static final LogManager                    instance             = new LogManager();
  private static volatile TraceContextSupplier       traceContextSupplier = null;
  private              boolean                        debug                = false;
  // VOLATILE BECAUSE setLogger() IS A RUNTIME PATH - GlobalConfiguration.LOG_IMPL SWAPS THE LOGGER FROM AN
  // ARBITRARY THREAD WHILE OTHERS ARE LOGGING - AND EVERY log() OVERLOAD READS IT
  private volatile     Logger                         logger;

  static class LogContext extends ThreadLocal<String> {
  }

  /**
   * Per-request correlation fields read by the log formatters. {@code requestId} and {@code database}
   * always work; {@code traceId}/{@code spanId} are populated only when the optional tracing plugin
   * is active (otherwise null). All fields may be null.
   */
  public record Correlation(String requestId, String database, String traceId, String spanId) {
  }

  /**
   * SPI letting the optional {@code tracing} plugin expose the currently active trace context to the
   * core logger without the core taking an OpenTelemetry dependency. Unset by default (no tracer).
   */
  @FunctionalInterface
  public interface TraceContextSupplier {
    /**
     * @return a 2-element array {@code [traceId, spanId]}, or {@code null} when no span is active.
     */
    String[] currentTraceContext();
  }

  /**
   * Registers (or, with {@code null}, clears) the supplier used to read the active trace context.
   * Called by the tracing plugin on configure/stop. Instance method (consistent with the rest of the
   * {@link LogManager} API) backed by a process-wide {@code volatile} field so worker threads observe it.
   */
  public void setTraceContextSupplier(final TraceContextSupplier supplier) {
    traceContextSupplier = supplier;
  }

  /**
   * Reads the active trace context via the registered supplier, swallowing any failure so a tracing
   * fault never breaks logging. Returns {@code null} when no supplier is registered or no span active.
   */
  public String[] currentTraceContext() {
    final TraceContextSupplier supplier = traceContextSupplier;
    if (supplier == null)
      return null;
    try {
      return supplier.currentTraceContext();
    } catch (final Exception e) {
      return null;
    }
  }

  public void setCorrelation(final String requestId, final String database, final String traceId, final String spanId) {
    CORRELATION_INSTANCE.set(new Correlation(requestId, database, traceId, spanId));
  }

  public Correlation getCorrelation() {
    return CORRELATION_INSTANCE.get();
  }

  public void clearCorrelation() {
    CORRELATION_INSTANCE.remove();
  }

  public String getRequestId() {
    final Correlation c = CORRELATION_INSTANCE.get();
    return c == null ? null : c.requestId();
  }

  public String getDatabaseContext() {
    final Correlation c = CORRELATION_INSTANCE.get();
    return c == null ? null : c.database();
  }

  public String getTraceId() {
    final Correlation c = CORRELATION_INSTANCE.get();
    return c == null ? null : c.traceId();
  }

  public String getSpanId() {
    final Correlation c = CORRELATION_INSTANCE.get();
    return c == null ? null : c.spanId();
  }

  protected LogManager() {
    logger = createLogger();
  }

  /**
   * Builds the {@link Logger} chosen by the {@link #LOG_IMPL_PROPERTY} system property.
   * <p>
   * This class reads the raw system property rather than {@code GlobalConfiguration} on purpose: it runs
   * from the static initialiser, and querying the configuration there would run the whole of
   * {@code GlobalConfiguration}'s own initialisation - callbacks included - while {@link #instance()} is
   * still {@code null}. {@code GlobalConfiguration.LOG_IMPL} drives the logger the other way round, by
   * calling {@link #setLogger(Logger)} with {@link #createLogger(String)} whenever it is set.
   *
   * @return the logger instance to install; never {@code null}
   */
  static Logger createLogger() {
    return createLogger(System.getProperty(LOG_IMPL_PROPERTY, ""));
  }

  /**
   * Builds the {@link Logger} named by {@code implementation}: {@link Slf4jLogger} for {@code slf4j},
   * {@link DefaultLogger} when null, empty or {@code default}. Matching is case-insensitive. Any other
   * value is reported on {@code System.err} and treated as {@code default}, so a typo does not silently
   * look like a working configuration. Any failure constructing the chosen implementation (e.g.
   * {@code slf4j-api} missing at runtime) is caught and falls back to {@link DefaultLogger}, so a logging
   * misconfiguration can never prevent startup.
   *
   * @param implementation the requested implementation name; may be {@code null}
   *
   * @return the logger instance to install; never {@code null}
   */
  public static Logger createLogger(final String implementation) {
    final String impl = implementation == null ? "" : implementation.trim().toLowerCase(Locale.ROOT);
    try {
      if ("slf4j".equals(impl))
        return new Slf4jLogger();

      if (!impl.isEmpty() && !"default".equals(impl))
        System.err.println("ArcadeDB: unknown value '" + impl + "' for " + LOG_IMPL_PROPERTY
            + ", using java.util.logging. Supported values: 'default', 'slf4j'.");

      return new DefaultLogger();
    } catch (final Throwable t) {
      // A logging-init problem must never take the process down: fall back to the dependency-free
      // java.util.logging implementation.
      System.err.println(
          "ArcadeDB: cannot initialize logger impl '" + impl + "', falling back to java.util.logging. Cause: " + t);
      return new DefaultLogger();
    }
  }

  public static LogManager instance() {
    return instance;
  }

  public String getContext() {
    return CONTEXT_INSTANCE.get();
  }

  public void setContext(final String context) {
    CONTEXT_INSTANCE.set(context);
  }

  /**
   * Installs {@code logger} as the destination of every subsequent {@code log()} call, replacing whatever
   * {@link #createLogger()} or {@code GlobalConfiguration.LOG_IMPL} had selected. This is the escape hatch
   * for an embedder that needs an implementation the configuration cannot name, and for a test swapping in
   * a capturing logger - such a caller should keep the {@link #getLogger()} it replaces and put it back.
   *
   * @param logger the logger to install; must not be {@code null}
   */
  public void setLogger(final Logger logger) {
    this.logger = logger;
  }

  /**
   * Returns the installed logger, so a caller that temporarily replaces it (a test asserting on what was logged, a
   * decorator wrapping it) can put the original back instead of guessing which implementation
   * {@code createLogger()} had chosen for this JVM.
   */
  public Logger getLogger() {
    return logger;
  }

  public void log(final Object requester, final Level level, final String message) {
    logger.log(requester, level, message, null, CONTEXT_INSTANCE.get());
  }

  public void log(final Object requester, final Level level, final String message, final Throwable throwable) {
    logger.log(requester, level, message, throwable, CONTEXT_INSTANCE.get());
  }

  public void log(final Object requester, final Level level, final String message, final Object... args) {
    logger.log(requester, level, message, null, CONTEXT_INSTANCE.get(), args);
  }

  public void log(final Object requester, final Level level, final String message, final Throwable throwable,
                  final Object... args) {
    logger.log(requester, level, message, throwable, CONTEXT_INSTANCE.get(), args);
  }

  public void log(final Object requester, final Level level, final String message, final Throwable throwable,
                  final Object arg1) {
    logger.log(requester, level, message, throwable, CONTEXT_INSTANCE.get(), arg1, null, null, null, null, null, null
        , null, null,
        null, null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Object arg1) {
    logger.log(requester, level, message, null, CONTEXT_INSTANCE.get(), arg1, null, null, null, null, null, null,
        null, null, null,
        null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Throwable throwable,
                  final Object arg1,
                  final Object arg2) {
    logger.log(requester, level, message, throwable, CONTEXT_INSTANCE.get(), arg1, arg2, null, null, null, null, null
        , null, null,
        null, null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Object arg1,
                  final Object arg2) {
    logger.log(requester, level, message, null, CONTEXT_INSTANCE.get(), arg1, arg2, null, null, null, null, null,
        null, null, null,
        null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Throwable throwable,
                  final Object arg1,
                  final Object arg2, final Object arg3) {
    logger.log(requester, level, message, throwable, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, null, null, null, null
        , null, null,
        null, null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2,
                  final Object arg3) {
    logger.log(requester, level, message, null, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, null, null, null, null,
        null, null, null,
        null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Throwable throwable,
                  final Object arg1,
                  final Object arg2, final Object arg3, final Object arg4) {
    logger.log(requester, level, message, throwable, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, null, null, null
        , null, null,
        null, null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2,
                  final Object arg3, final Object arg4) {
    logger.log(requester, level, message, null, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, null, null, null,
        null, null, null,
        null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Throwable throwable,
                  final Object arg1,
                  final Object arg2, final Object arg3, final Object arg4, final Object arg5) {
    logger.log(requester, level, message, throwable, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, null, null
        , null, null,
        null, null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2,
                  final Object arg3, final Object arg4, final Object arg5) {
    logger.log(requester, level, message, null, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, null, null,
        null, null, null,
        null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Throwable throwable,
                  final Object arg1,
                  final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6) {
    logger.log(requester, level, message, throwable, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, null
        , null, null,
        null, null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2,
                  final Object arg3, final Object arg4, final Object arg5, final Object arg6) {
    logger.log(requester, level, message, null, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, null,
        null, null, null,
        null, null, null, null, null, null, null);
  }

  public void log(final Object requester, final Level level, final String message, final Throwable throwable,
                  final Object arg1,
                  final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
                  final Object arg7) {
    logger.log(requester, level, message, throwable, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7
        , null, null,
        null, null, null, null, null, null, null, null);
  }

  public boolean isDebugEnabled() {
    return debug;
  }

  public void setDebugEnabled(boolean value) {
    debug = value;
  }

  public void flush() {
    logger.flush();
  }
}
