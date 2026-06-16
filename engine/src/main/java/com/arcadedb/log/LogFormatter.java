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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.utility.AnsiCode;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.IllegalFormatException;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.regex.Pattern;

/**
 * Basic Log formatter.
 *
 * @author Luca Garulli
 */

public class LogFormatter extends Formatter {

  protected static final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  /**
   * The end-of-line character for this platform.
   */
  protected static final String EOL = System.getProperty("line.separator");

  /**
   * Whitelist of printf-style conversion characters we accept when falling back from
   * MessageFormat to {@link String#formatted(Object...)}. Restricted to plain value
   * conversions (no width/precision/index modifiers) so that an unexpected template
   * containing line-separator/locale ({@code %n}, {@code %t}) or layout specifiers does
   * not reach {@link String#formatted}.
   */
  private static final Pattern SAFE_PRINTF_PATTERN = Pattern.compile("%[%bBhHsScCdoxXeEfgGaA]");

  @Override
  public String format(final LogRecord record) {
    final String formatted = customFormatMessage(record);

    final Throwable current = record.getThrown();
    if (current == null)
      return formatted;

    final StringBuilder buffer = new StringBuilder(512);
    buffer.append(formatted);
    buffer.append(EOL);

    final StringWriter writer = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(writer);
    current.printStackTrace(printWriter);
    printWriter.flush();
    buffer.append(writer.getBuffer());
    printWriter.close();

    return buffer.toString();
  }

  /**
   * Overrides the JDK default to also support printf-style format strings ({@code %s}, {@code %d},
   * etc.) when {@link Formatter#formatMessage(LogRecord)} would otherwise drop the parameter array
   * (i.e. the message has no {@code MessageFormat} placeholders {@code {0}..{3}}). Behaviour:
   * <ol>
   *   <li>Delegate to the JDK default, which substitutes {@code MessageFormat} placeholders when
   *       present (the path used by gRPC, JBoss, java.net.http, etc.).</li>
   *   <li>If the JDK returned the raw template (no substitution) and parameters were supplied,
   *       fall back to {@link String#formatted(Object...)} for printf-style compatibility.</li>
   * </ol>
   * Pre-formatted records with no parameters (ArcadeDB's {@code DefaultLogger} convention) are
   * returned unchanged.
   */
  @Override
  public String formatMessage(final LogRecord record) {
    final String julFormatted = super.formatMessage(record);
    final Object[] parameters = record.getParameters();
    final String rawMessage = record.getMessage();
    if (parameters != null && parameters.length > 0 && rawMessage != null
        && julFormatted.equals(rawMessage) && SAFE_PRINTF_PATTERN.matcher(rawMessage).find()) {
      try {
        return rawMessage.formatted(parameters);
      } catch (final IllegalFormatException ignore) {
        return rawMessage;
      }
    }
    return julFormatted;
  }

  protected String customFormatMessage(final LogRecord iRecord) {
    final Level level = iRecord.getLevel();
    final String message = AnsiCode.format(formatMessage(iRecord), false);
    final String requester = getSourceClassSimpleName(iRecord.getLoggerName());

    final StringBuilder buffer = new StringBuilder(512);
    buffer.append(EOL);
    buffer.append(dateFormat.format(LocalDateTime.now()));

    buffer.append(" %-5.5s ".formatted(level.getName()));

    if (requester != null) {
      buffer.append("[");
      buffer.append(requester);
      buffer.append("] ");
    }

    buffer.append(message);
    appendTraceTag(buffer);

    return AnsiCode.format(buffer.toString(), false);
  }

  /**
   * Appends {@code [traceId=...]} to {@code buffer} when {@code arcadedb.server.logIncludeTrace} is
   * enabled and a trace is currently active. The configuration is read only when a trace id is
   * present, so the common (no-trace) path does no work and the default text output is byte-identical
   * to before. Shared by {@link LogFormatter} and {@code AnsiLogFormatter}.
   */
  protected void appendTraceTag(final StringBuilder buffer) {
    final String traceId = LogManager.instance().getTraceId();
    if (traceId != null && GlobalConfiguration.SERVER_LOG_INCLUDE_TRACE.getValueAsBoolean()) {
      buffer.append(" [traceId=");
      buffer.append(traceId);
      buffer.append("]");
    }
  }

  protected String getSourceClassSimpleName(final String iSourceClassName) {
    if (iSourceClassName == null)
      return null;
    return iSourceClassName.substring(iSourceClassName.lastIndexOf(".") + 1);
  }
}
