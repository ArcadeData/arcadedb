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

import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.logging.Level;

/**
 * {@link Logger} implementation that routes ArcadeDB's logs through the SLF4J facade, so an
 * application embedding ArcadeDB receives the engine's logs in whatever backend it already uses
 * (Logback, Log4j2, reload4j, or {@code java.util.logging} via {@code slf4j-jdk14}). ArcadeDB itself
 * depends only on {@code slf4j-api} and pins no backend.
 * <p>
 * This is <b>opt-in</b>: the default logger remains {@link DefaultLogger} (java.util.logging, with
 * ArcadeDB's built-in text/ANSI and JSON formatting). Select this one, without any code change, with
 * {@code -Darcadedb.log.impl=slf4j}, or programmatically via
 * {@link LogManager#setLogger(Logger) LogManager.setLogger(new Slf4jLogger())}.
 * <p>
 * Behaviour that call sites rely on matches {@link DefaultLogger}: the requester is resolved to a
 * logger name the same way (String / Class / instance class-name, falling back to
 * {@code com.arcadedb}); a non-null {@code context} is prefixed as {@code <context> message}; and
 * message parameters use Java {@link String#formatted(Object...)} (printf-style {@code %s}), not
 * SLF4J's {@code {}} placeholders, because that is how ArcadeDB messages are written. Formatting is
 * skipped entirely when the target level is disabled.
 * <p>
 * The per-request correlation fields carried by {@link LogManager} (requestId, database, traceId,
 * spanId) are exposed to the backend through SLF4J's {@link MDC} under the keys
 * {@code arcadedb.requestId}, {@code arcadedb.database}, {@code arcadedb.traceId} and
 * {@code arcadedb.spanId}, so a pattern/layout can render them. Any value already present under those
 * keys is saved and restored around the call, leaving the host application's MDC untouched.
 */
public class Slf4jLogger implements Logger {
  private static final String DEFAULT_LOG    = "com.arcadedb";
  private static final String MDC_REQUEST_ID = "arcadedb.requestId";
  private static final String MDC_DATABASE   = "arcadedb.database";
  private static final String MDC_TRACE_ID   = "arcadedb.traceId";
  private static final String MDC_SPAN_ID    = "arcadedb.spanId";

  // SLF4J's five levels, ordered by severity; the target level is resolved to one of these once per
  // call and then reused for both the enabled-check and the dispatch.
  private static final int ERROR = 0;
  private static final int WARN  = 1;
  private static final int INFO  = 2;
  private static final int DEBUG = 3;
  private static final int TRACE = 4;

  /**
   * Logs {@code message} at {@code level} for {@code requester}, through SLF4J. This fixed-arity
   * overload (instead of varargs) keeps the common, no-parameter call path free of an {@code Object[]}
   * allocation. When the level is disabled the message is neither formatted nor emitted. Non-null
   * parameters are substituted printf-style ({@link String#formatted(Object...)}); a non-null
   * {@code context} is prefixed as {@code <context> message}; and a non-null {@code exception} is
   * passed to SLF4J as the throwable. A {@code null} message is ignored.
   *
   * @param requester the object, class or name the log line is attributed to (resolves the logger name)
   * @param level     the {@link java.util.logging.Level}, mapped onto one of SLF4J's five levels
   * @param message   the message, optionally a printf-style format string
   * @param exception an optional throwable to log with the message
   * @param context   an optional context tag prefixed to the message
   * @param arg1      the first format argument (through {@code arg17}); trailing {@code null}s are unused
   */
  @Override
  public void log(final Object requester, final Level level, final String message, final Throwable exception,
      final String context,
      final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
      final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
      final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
    if (message == null)
      return;

    final org.slf4j.Logger log = resolveLogger(requester);
    final int slf4jLevel = slf4jLevel(level);
    if (!isEnabled(log, slf4jLevel))
      return;

    // Kept as an explicit 17-arg call (no Object[] wrapping) to preserve the interface's
    // allocation-free path for the common, no-parameter case.
    final boolean hasParams = anyNonNull(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12,
        arg13, arg14, arg15, arg16, arg17);

    String msg = withContext(context, message);
    if (hasParams) {
      try {
        msg = msg.formatted(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
            arg15, arg16, arg17);
      } catch (final Exception e) {
        // Best-effort: keep the raw message if the arguments do not match the format string.
      }
    }

    write(log, slf4jLevel, msg, exception);
  }

  /**
   * Varargs form of {@link #log(Object, Level, String, Throwable, String, Object, Object, Object,
   * Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object,
   * Object, Object)} for call sites with an arbitrary number of arguments. Same semantics: level
   * mapping and gating, printf-style substitution of {@code args}, {@code <context>} prefixing,
   * optional throwable, and {@code null} messages ignored.
   *
   * @param requester the object, class or name the log line is attributed to
   * @param level     the {@link java.util.logging.Level}, mapped onto one of SLF4J's five levels
   * @param message   the message, optionally a printf-style format string
   * @param exception an optional throwable to log with the message
   * @param context   an optional context tag prefixed to the message
   * @param args      the format arguments (may be empty)
   */
  @Override
  public void log(final Object requester, final Level level, final String message, final Throwable exception,
      final String context, final Object... args) {
    if (message == null)
      return;

    final org.slf4j.Logger log = resolveLogger(requester);
    final int slf4jLevel = slf4jLevel(level);
    if (!isEnabled(log, slf4jLevel))
      return;

    String msg = withContext(context, message);
    if (args != null && args.length > 0) {
      try {
        msg = msg.formatted(args);
      } catch (final Exception e) {
        // Best-effort: keep the raw message if the arguments do not match the format string.
      }
    }

    write(log, slf4jLevel, msg, exception);
  }

  /**
   * No-op. Unlike the {@code java.util.logging} logger, this implementation holds no handlers to
   * flush: the SLF4J backend owns its appender lifecycle and flushing policy.
   */
  @Override
  public void flush() {
    // Intentionally empty.
  }

  /**
   * Resolves the SLF4J logger for a requester, deriving its name the same way {@link DefaultLogger} does:
   * a {@link String} is used verbatim, a {@link Class} contributes its fully-qualified name, any
   * other object contributes its class name, and {@code null} falls back to {@value #DEFAULT_LOG}.
   * Delegates straight to {@link LoggerFactory#getLogger(String)}, which already performs its own
   * context-aware caching; a local cache would risk stale loggers or a ClassLoader leak if the
   * logging context is reloaded.
   */
  private org.slf4j.Logger resolveLogger(final Object requester) {
    final String name;
    if (requester instanceof String string)
      name = string;
    else if (requester instanceof Class<?> clazz)
      name = clazz.getName();
    else if (requester != null)
      name = requester.getClass().getName();
    else
      name = DEFAULT_LOG;

    return LoggerFactory.getLogger(name);
  }

  /** Prefixes the message with {@code <context> } when a context is set, otherwise returns it unchanged. */
  private static String withContext(final String context, final String message) {
    return context != null ? "<" + context + "> " + message : message;
  }

  /**
   * Returns whether any of the (up to 17) format arguments is non-null, i.e. whether the message
   * needs printf substitution. Extracted from the fixed-arity {@code log} overload to keep that
   * method's complexity low; takes the arguments explicitly (no {@code Object[]}) so the
   * no-parameter path stays allocation-free.
   */
  private static boolean anyNonNull(final Object arg1, final Object arg2, final Object arg3, final Object arg4,
      final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
      final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15,
      final Object arg16, final Object arg17) {
    return arg1 != null || arg2 != null || arg3 != null || arg4 != null || arg5 != null || arg6 != null || arg7 != null
        || arg8 != null || arg9 != null || arg10 != null || arg11 != null || arg12 != null || arg13 != null
        || arg14 != null || arg15 != null || arg16 != null || arg17 != null;
  }

  /**
   * Maps a {@link java.util.logging.Level} onto one of SLF4J's five levels by numeric weight, so
   * custom or intermediate JUL levels also resolve: {@code >= SEVERE -> ERROR}, {@code >= WARNING ->
   * WARN}, {@code >= INFO -> INFO}, {@code >= FINE -> DEBUG}, otherwise {@code TRACE}.
   *
   * @return one of {@link #ERROR}, {@link #WARN}, {@link #INFO}, {@link #DEBUG} or {@link #TRACE}
   */
  private static int slf4jLevel(final Level level) {
    final int v = level.intValue();
    if (v >= Level.SEVERE.intValue())
      return ERROR;
    if (v >= Level.WARNING.intValue())
      return WARN;
    if (v >= Level.INFO.intValue())
      return INFO;
    if (v >= Level.FINE.intValue())
      return DEBUG;
    return TRACE;
  }

  /** Whether {@code log} would emit at the given resolved SLF4J level; used to skip formatting when disabled. */
  private static boolean isEnabled(final org.slf4j.Logger log, final int slf4jLevel) {
    return switch (slf4jLevel) {
      case ERROR -> log.isErrorEnabled();
      case WARN -> log.isWarnEnabled();
      case INFO -> log.isInfoEnabled();
      case DEBUG -> log.isDebugEnabled();
      default -> log.isTraceEnabled();
    };
  }

  /**
   * Emits the already-formatted message at the resolved SLF4J level, with the current
   * {@link LogManager} correlation published to the MDC for the duration of the call and restored
   * afterwards (see {@link #pushCorrelation}/{@link #restoreCorrelation}).
   */
  private void write(final org.slf4j.Logger log, final int slf4jLevel, final String msg, final Throwable exception) {
    final LogManager.Correlation correlation = LogManager.instance().getCorrelation();
    final String[] saved = pushCorrelation(correlation);
    try {
      switch (slf4jLevel) {
      case ERROR -> {
        if (exception != null) log.error(msg, exception); else log.error(msg);
      }
      case WARN -> {
        if (exception != null) log.warn(msg, exception); else log.warn(msg);
      }
      case INFO -> {
        if (exception != null) log.info(msg, exception); else log.info(msg);
      }
      case DEBUG -> {
        if (exception != null) log.debug(msg, exception); else log.debug(msg);
      }
      default -> {
        if (exception != null) log.trace(msg, exception); else log.trace(msg);
      }
      }
    } finally {
      restoreCorrelation(correlation, saved);
    }
  }

  /**
   * Puts each non-null correlation field into the MDC and returns the previous values (fixed order:
   * requestId, database, traceId, spanId) for {@link #restoreCorrelation}. Returns {@code null} when
   * there is no correlation to publish.
   */
  private static String[] pushCorrelation(final LogManager.Correlation correlation) {
    if (correlation == null)
      return null;

    return new String[] {
        swap(MDC_REQUEST_ID, correlation.requestId()),
        swap(MDC_DATABASE, correlation.database()),
        swap(MDC_TRACE_ID, correlation.traceId()),
        swap(MDC_SPAN_ID, correlation.spanId())
    };
  }

  /**
   * Restores only the MDC keys that {@link #pushCorrelation} actually set (those whose correlation
   * field was non-null). Keys for null fields were never touched, so leaving them alone preserves
   * whatever the host application had placed there.
   */
  private static void restoreCorrelation(final LogManager.Correlation correlation, final String[] saved) {
    if (correlation == null || saved == null)
      return;

    if (correlation.requestId() != null)
      restore(MDC_REQUEST_ID, saved[0]);
    if (correlation.database() != null)
      restore(MDC_DATABASE, saved[1]);
    if (correlation.traceId() != null)
      restore(MDC_TRACE_ID, saved[2]);
    if (correlation.spanId() != null)
      restore(MDC_SPAN_ID, saved[3]);
  }

  /** Sets {@code key} to {@code value} when {@code value} is non-null, returning the previous value. */
  private static String swap(final String key, final String value) {
    if (value == null)
      return null;
    final String previous = MDC.get(key);
    MDC.put(key, value);
    return previous;
  }

  /** Puts {@code previous} back under {@code key}, or removes the key when there was no previous value. */
  private static void restore(final String key, final String previous) {
    if (previous != null)
      MDC.put(key, previous);
    else
      MDC.remove(key);
  }
}
