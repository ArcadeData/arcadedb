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
package com.arcadedb;

import com.arcadedb.log.LogFormatter;
import com.arcadedb.utility.AnsiLogFormatter;

import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that JUL LogRecord parameters using MessageFormat-style placeholders ({0}, {1})
 * are correctly substituted by both LogFormatter and AnsiLogFormatter.
 */
class LogFormatterMessageFormatTest {

  /**
   * gRPC ManagedChannelImpl logs via JUL with MessageFormat-style placeholders {0} and {1}.
   * LogFormatter must substitute them rather than emitting the raw template.
   */
  @Test
  void logFormatterSubstitutesMessageFormatPlaceholders() {
    final LogRecord record = new LogRecord(Level.WARNING, "[{0}] Failed to resolve name. status={1}");
    record.setParameters(new Object[]{ "channel-7", "UNAVAILABLE" });
    record.setLoggerName("io.grpc.ManagedChannelImpl");

    final String formatted = new LogFormatter().format(record);

    assertThat(formatted).contains("channel-7");
    assertThat(formatted).contains("UNAVAILABLE");
    assertThat(formatted).doesNotContain("{0}");
    assertThat(formatted).doesNotContain("{1}");
  }

  /**
   * Same coverage for AnsiLogFormatter, which extends LogFormatter and overrides
   * customFormatMessage with ANSI-coloring logic.
   */
  @Test
  void ansiLogFormatterSubstitutesMessageFormatPlaceholders() {
    final LogRecord record = new LogRecord(Level.WARNING, "[{0}] Failed to resolve name. status={1}");
    record.setParameters(new Object[]{ "channel-7", "UNAVAILABLE" });
    record.setLoggerName("io.grpc.ManagedChannelImpl");

    final String formatted = new AnsiLogFormatter().format(record);

    assertThat(formatted).contains("channel-7");
    assertThat(formatted).contains("UNAVAILABLE");
    assertThat(formatted).doesNotContain("{0}");
    assertThat(formatted).doesNotContain("{1}");
  }

  /**
   * ArcadeDB's DefaultLogger pre-formats messages before passing to JUL (no parameters array).
   * Those messages must pass through unchanged.
   */
  @Test
  void preformattedMessagesPassThrough() {
    final LogRecord record = new LogRecord(Level.INFO, "Server started on port 2480");
    record.setLoggerName("com.arcadedb.server.ArcadeDBServer");

    assertThat(new LogFormatter().format(record)).contains("Server started on port 2480");
    assertThat(new AnsiLogFormatter().format(record)).contains("Server started on port 2480");
  }

  /**
   * Multiple MessageFormat arguments across a single template are all substituted.
   */
  @Test
  void multipleMessageFormatArgumentsAreAllSubstituted() {
    final LogRecord record = new LogRecord(Level.SEVERE,
        "Node {0} lost contact with {1} after {2} ms");
    record.setParameters(new Object[]{ "node-1", "node-3", "5000" });
    record.setLoggerName("io.grpc.ManagedChannelImpl");

    final String formatted = new LogFormatter().format(record);

    assertThat(formatted).contains("node-1");
    assertThat(formatted).contains("node-3");
    assertThat(formatted).contains("5000");
    assertThat(formatted).doesNotContain("{0}");
    assertThat(formatted).doesNotContain("{1}");
    assertThat(formatted).doesNotContain("{2}");
  }

  /**
   * Backwards-compatibility for printf-style ({@code %s}, {@code %d}) JUL records:
   * the JDK Formatter.formatMessage only handles MessageFormat-style {@code {0}}, so without the
   * override LogFormatter would drop the parameters array. The override must fall back to
   * {@link String#formatted(Object...)} when the JDK path returns the raw template unchanged.
   */
  @Test
  void printfStylePlaceholdersWithParametersAreSubstituted() {
    final LogRecord record = new LogRecord(Level.INFO,
        "Connected to %s:%d as user %s");
    record.setParameters(new Object[]{ "db-host", 2480, "root" });
    record.setLoggerName("com.example.LegacyJulCaller");

    final String formatted = new LogFormatter().format(record);

    assertThat(formatted).contains("db-host");
    assertThat(formatted).contains("2480");
    assertThat(formatted).contains("root");
    assertThat(formatted).doesNotContain("%s");
    assertThat(formatted).doesNotContain("%d");
  }

  /**
   * AnsiLogFormatter must also keep the printf-style fallback (it inherits formatMessage from
   * LogFormatter).
   */
  @Test
  void ansiFormatterPrintfStylePlaceholdersAreSubstituted() {
    final LogRecord record = new LogRecord(Level.INFO, "value=%s count=%d");
    record.setParameters(new Object[]{ "abc", 42 });
    record.setLoggerName("com.example.LegacyJulCaller");

    final String formatted = new AnsiLogFormatter().format(record);

    assertThat(formatted).contains("abc");
    assertThat(formatted).contains("42");
    assertThat(formatted).doesNotContain("%s");
    assertThat(formatted).doesNotContain("%d");
  }

  /**
   * A malformed printf template (parameter count/type mismatch) must not throw — the formatter
   * should fall back to the raw message rather than propagating IllegalFormatException.
   */
  @Test
  void malformedPrintfTemplateFallsBackToRawMessage() {
    final LogRecord record = new LogRecord(Level.INFO, "wants %d got nothing");
    record.setParameters(new Object[]{ "not-a-number" });
    record.setLoggerName("com.example.LegacyJulCaller");

    final String formatted = new LogFormatter().format(record);

    assertThat(formatted).contains("wants %d got nothing");
  }

  /**
   * Templates containing only non-whitelisted printf conversions (e.g. {@code %n} line separator,
   * {@code %t} date/time, width/precision modifiers) must NOT be passed to
   * {@link String#formatted(Object...)} — the whitelist is the defensive check we use to keep an
   * untrusted exception/library message from reaching the format-string sink.
   */
  @Test
  void unsafePrintfConversionsAreNotApplied() {
    final LogRecord record = new LogRecord(Level.INFO, "line%nseparator and %tY year");
    record.setParameters(new Object[]{ new Date() });
    record.setLoggerName("com.example.LegacyJulCaller");

    final String formatted = new LogFormatter().format(record);

    assertThat(formatted).contains("line%nseparator and %tY year");
  }
}
