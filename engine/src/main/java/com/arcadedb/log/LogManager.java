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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.Set;
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

  /**
   * System property turning on log export, independently of {@link #LOG_IMPL_PROPERTY}: the chosen
   * logger keeps receiving every record and a {@link LogExporter} found on the classpath receives it
   * too. Also the key of {@code GlobalConfiguration.LOG_OTLP_ENABLED}.
   */
  public  static final String                        LOG_EXPORT_PROPERTY  = "arcadedb.log.otlp.enabled";

  /** Where the exporter sends to. Also the key of {@code GlobalConfiguration.LOG_OTLP_ENDPOINT}. */
  public  static final String                        LOG_EXPORT_ENDPOINT_PROPERTY = "arcadedb.log.otlp.endpoint";

  /** The exporter name looked up when export is on; the optional {@code arcadedb-logs} module provides it. */
  private static final String                        LOG_EXPORTER_NAME    = "otlp";

  /** Matches the tracing plugin's default, so one collector address serves both. */
  public  static final String                        LOG_EXPORT_ENDPOINT_DEFAULT = "http://localhost:4317";
  private static final LogContext                    CONTEXT_INSTANCE     = new LogContext();
  private static final ThreadLocal<Correlation>      CORRELATION_INSTANCE = new ThreadLocal<>();
  /**
   * Every decorator {@link #exporting(Logger)} built and nothing has closed yet: the loggers this
   * class is allowed to close.
   *
   * <p>A set, where a single "most recently built" field was not enough. Building is public and
   * unsynchronised - {@link #createLogger(String)} - so between building a decorator and installing it
   * any number of others can be built, by a second thread changing the configuration or by the same
   * caller building ahead. A single field is already pointing at one of those by then, and the
   * decorator being displaced is no longer recognised as ours: it is never closed, and its batch
   * thread and connection stay up for the life of the JVM.
   *
   * <p>Identity-keyed on purpose, as {@code Profiler.databases} is: membership is "this very object",
   * not "something equal to it", so an embedder whose {@link Logger} defines value equality cannot
   * have one decorator closed in another's place. {@link IdentityHashMap} is not thread-safe, hence
   * the wrapper; contention does not arise on a path reached at start-up and on configuration changes.
   */
  private static final Set<Logger>                   OURS                 =
      Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

  /**
   * Serialises installation, so two installs cannot both displace the same logger.
   *
   * <p>The publish side only. Readers - every {@code log()} overload - never take it: they read the
   * volatile field, as the engine-concurrency skill requires of a hot path.
   */
  private static final Object                        INSTALL_LOCK         = new Object();

  // DECLARED LAST OF THE STATICS ON PURPOSE, AND MUST STAY THERE: class-variable initialisers run in
  // textual order (JLS 12.4.2) AND THIS ONE RUNS THE CONSTRUCTOR, WHICH REACHES OURS THROUGH
  // exporting(). A static final DECLARED BELOW THIS LINE WOULD STILL BE NULL BY THEN, AND CLASS
  // INITIALISATION WOULD DIE BEFORE ANYTHING COULD LOG THE FAILURE
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
    // Nothing installed to displace, and no ownership to take: exporting() has already recorded
    // whatever it built. The decorator built at start-up - the common case, since export is turned on
    // before the process starts rather than during it - is therefore owned by construction.
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
        return exporting(new Slf4jLogger());

      if (!impl.isEmpty() && !"default".equals(impl))
        System.err.println("ArcadeDB: unknown value '" + impl + "' for " + LOG_IMPL_PROPERTY
            + ", using java.util.logging. Supported values: 'default', 'slf4j'.");

      return exporting(new DefaultLogger());
    } catch (final Throwable t) {
      // A logging-init problem must never take the process down: fall back to the dependency-free
      // java.util.logging implementation.
      System.err.println(
          "ArcadeDB: cannot initialize logger impl '" + impl + "', falling back to java.util.logging. Cause: " + t);
      // Exported like the two paths above. Which local logger we fell back to says nothing about
      // whether the records should still reach the collector, and exporting() cannot throw: it
      // reports and returns the delegate undecorated.
      return exporting(new DefaultLogger());
    }
  }

  /**
   * Wraps a logger so records are exported as well, when export is on and an exporter is available.
   *
   * <p>Read from system properties rather than from {@code GlobalConfiguration} on purpose: this runs
   * from the static initialiser below, before that class is necessarily loaded, and a logger that
   * needed the configuration system to be up could not log the configuration system coming up.
   * {@code GlobalConfiguration.LOG_OTLP_ENABLED} sets the same properties and rebuilds the logger, so
   * both routes agree.
   *
   * <p>Nothing here is allowed to fail the caller. Export turned on with no exporter on the classpath
   * is a misconfiguration worth saying out loud, not a reason to stop logging; an exporter that throws
   * while starting is the same. Both leave the undecorated logger in place.
   *
   * <p>Both are reported on {@code System.err}, as the two cases above it are, and the obvious
   * improvement does not work. Logging through {@code delegate} looks safe - it is fully constructed,
   * it is the argument - but this runs from the static initialiser below, where {@code instance} is
   * not yet assigned, and the formatter reaches back for it: {@code LogFormatter.appendTraceTag} calls
   * {@code LogManager.instance().getTraceId()} and throws. The record is then swallowed by
   * {@code java.util.logging}'s ErrorManager, so the warning is lost rather than misplaced. Measured,
   * not assumed. A start-up misconfiguration on {@code System.err} is the lesser problem.
   *
   * @param delegate the logger the configuration selected
   *
   * @return {@code delegate}, or a decorator around it
   */
  private static Logger exporting(final Logger delegate) {
    if (!Boolean.parseBoolean(System.getProperty(LOG_EXPORT_PROPERTY, "false")))
      return delegate;

    final String endpoint = System.getProperty(LOG_EXPORT_ENDPOINT_PROPERTY, LOG_EXPORT_ENDPOINT_DEFAULT);
    try {
      for (final LogExporter exporter : ServiceLoader.load(LogExporter.class))
        if (LOG_EXPORTER_NAME.equals(exporter.name())) {
          final Logger decorator = exporter.create(delegate, endpoint);
          if (decorator == null)
            // The SPI says never null. Without this it becomes the process's logger and fails on the
            // first line logged, a long way from the exporter that produced it.
            break;

          OURS.add(decorator);
          return decorator;
        }

      System.err.println("ArcadeDB: " + LOG_EXPORT_PROPERTY
          + " is set but no '" + LOG_EXPORTER_NAME + "' log exporter is on the classpath (add arcadedb-logging)."
          + " Logs are written locally and not exported.");
    } catch (final Throwable t) {
      System.err.println("ArcadeDB: cannot start log export to " + endpoint
          + ", logs are written locally only. Cause: " + t);
    }
    return delegate;
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
    release(swap(logger), logger);
  }

  /**
   * Installs a logger and returns what it displaced, as one step.
   *
   * <p>One step because reading the field and writing it separately loses an install: two threads both
   * read the same outgoing logger, one of the two incoming ones is overwritten without ever having
   * been displaced, and nothing closes it.
   *
   * @param incoming the logger to install
   *
   * @return the logger it replaced, or null if there was none
   */
  private Logger swap(final Logger incoming) {
    synchronized (INSTALL_LOCK) {
      final Logger outgoing = this.logger;
      this.logger = incoming;
      return outgoing;
    }
  }

  /**
   * Closes the displaced logger, if it is one this class built.
   *
   * <p>The removal is the token, not merely a test: exactly one caller can win it for a given
   * decorator, so however the installs interleaved it is closed once, by whoever actually displaced
   * it. A logger we did not build is not in the set and is left alone - {@link #setLogger(Logger)} is
   * public, and closing what an embedder installed through it would reach into something we were only
   * lent.
   *
   * <p>Outside {@link #INSTALL_LOCK} on purpose: closing reaches into code this class does not own
   * and, for the OTLP decorator, into a network client. Installing a logger must not stall another
   * installation for as long as a collector takes to answer.
   *
   * @param outgoing the logger just replaced, or null when there was none
   * @param incoming the logger that replaced it
   */
  private static void release(final Logger outgoing, final Logger incoming) {
    if (outgoing == null || outgoing == incoming || !OURS.remove(outgoing))
      return;

    if (outgoing instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (final Throwable t) {
        // Throwable, as createLogger() and exporting() already catch: a telemetry fault must not
        // escape into a GlobalConfiguration callback that has already written its system property.
        System.err.println("ArcadeDB: could not release the previous log exporter. Cause: " + t);
      }
    }
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
