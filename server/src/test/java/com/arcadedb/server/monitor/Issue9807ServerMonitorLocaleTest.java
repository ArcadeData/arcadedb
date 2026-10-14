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
package com.arcadedb.server.monitor;

import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9807: {@code ServerMonitor} built its low-disk and heap-pressure warnings with {@code String.format} and no
 * {@link Locale}. The text is the body of a server event that {@code GET /api/v1/server} serves to any REST client, so under
 * a comma-decimal locale the figures carried a comma and under a locale with its own numbering system they were not even
 * ASCII digits: one cluster answered with a different string from each node. Issue #9968: the JVM safepoint warning of the
 * same class and the server start event were left on the default locale, so the claim itself is now checked over every
 * event the server reports.
 * <p>
 * Mutates the JVM default locale for the duration of each test and restores it afterward - tests in this module run
 * sequentially, but this is still process-wide state.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9807ServerMonitorLocaleTest {

  private static final long GB = 1024L * 1024L * 1024L;

  private Locale originalDefault;

  @BeforeEach
  void saveDefaultLocale() {
    originalDefault = Locale.getDefault();
  }

  @AfterEach
  void restoreDefaultLocale() {
    Locale.setDefault(originalDefault);
  }

  @ParameterizedTest
  @ValueSource(strings = { "en-US", "de-DE", "fr-FR", "th-TH-u-nu-thai", "ar-EG-u-nu-arab" })
  void lowDiskWarningIsTheSameStringInEveryLocale(final String languageTag) {
    Locale.setDefault(Locale.forLanguageTag(languageTag));

    assertThat(ServerMonitor.lowDiskMessage(12.5F, (long) (1.2 * GB), 10 * GB, "/data")).isEqualTo(
        "Available space on disk is only 12.5% (1.20 GB free of 10.00 GB total) on '/data'");
  }

  @ParameterizedTest
  @ValueSource(strings = { "en-US", "de-DE", "fr-FR", "th-TH-u-nu-thai", "ar-EG-u-nu-arab" })
  void heapWarningIsTheSameStringInEveryLocale(final String languageTag) {
    Locale.setDefault(Locale.forLanguageTag(languageTag));

    assertThat(ServerMonitor.heapPressureMessage(8.4F, (long) (3.75 * GB), 4 * GB)).isEqualTo(
        "Server overloaded: available heap RAM is only 8.4% (3.75 GB used of 4.00 GB max)");
  }

  /** Issue #9968: the third warning of the class, left on the default locale when the other two were pinned. */
  @ParameterizedTest
  @ValueSource(strings = { "en-US", "de-DE", "fr-FR", "th-TH-u-nu-thai", "ar-EG-u-nu-arab" })
  void safepointWarningIsTheSameStringInEveryLocale(final String languageTag) {
    Locale.setDefault(Locale.forLanguageTag(languageTag));

    assertThat(ServerMonitor.safepointSpikeMessage(42.5F, 1.25F, 3.75F)).isEqualTo(
        "Server overloaded: JVM Safepoint spiked up 42.5% from the last sampling (avg time: 1.25ms -> 3.75ms)");
  }

  /** Issue #9968: the server start event carries the CPU count, which the default locale renders in its own digits. */
  @ParameterizedTest
  @ValueSource(strings = { "en-US", "de-DE", "fr-FR", "th-TH-u-nu-thai", "ar-EG-u-nu-arab" })
  void serverStartedEventIsTheSameStringInEveryLocale(final String languageTag) {
    Locale.setDefault(Locale.forLanguageTag(languageTag));

    assertThat(ArcadeDBServer.startedMessage("production", 16, 4L * GB)).isEqualTo(
        "ArcadeDB Server started in 'production' mode (CPUs=16 MAXRAM=4.00GB)");
  }

  /**
   * The claim rather than its instances (issue #9968): no message reported to the server event log is formatted with the
   * JVM default locale. Every {@code reportEvent(...)} call of the server is read from the sources, together with the
   * initializer of a local variable it passes as the message, and none may format through {@code String.format} without
   * a {@link Locale} or through {@code String.formatted}, which takes none. A fourth warning added on the default locale
   * fails here, whichever class reports it.
   */
  @Test
  void noServerEventMessageIsFormattedWithTheDefaultLocale() throws IOException {
    final Path sources = Path.of("src", "main", "java");
    assertThat(sources).as("run from the server module").isDirectory();

    final List<String> offenders = new ArrayList<>();
    int calls = 0;
    try (final Stream<Path> files = Files.walk(sources)) {
      for (final Path file : files.filter(f -> f.toString().endsWith(".java")).toList()) {
        final String source = Files.readString(file);
        int from = 0;
        while ((from = source.indexOf(REPORT_EVENT_CALL, from)) >= 0) {
          from += REPORT_EVENT_CALL.length();
          final String arguments = balancedArguments(source, from);
          ++calls;

          String checked = arguments;
          final String message = arguments.substring(arguments.lastIndexOf(',') + 1).strip();
          if (IDENTIFIER.matcher(message).matches()) {
            // A message built beforehand in a local: its initializer is what formats it
            final Matcher declaration = Pattern.compile("\\b" + message + "\\s*=([^;]*);").matcher(source.substring(0, from));
            while (declaration.find())
              checked = arguments + declaration.group(1);
          }
          if (DEFAULT_LOCALE_FORMAT.matcher(checked).find() || checked.contains(".formatted("))
            offenders.add(file.getFileName() + ": reportEvent(" + arguments.strip() + ")");
        }
      }
    }

    assertThat(calls).as("the scan found the server's reportEvent calls").isGreaterThanOrEqualTo(8);
    assertThat(offenders).as("event messages formatted with the JVM default locale").isEmpty();
  }

  private static final String  REPORT_EVENT_CALL     = ".reportEvent(";
  private static final Pattern IDENTIFIER            = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  /** {@code String.format(} whose first argument is not a {@code Locale}. */
  private static final Pattern DEFAULT_LOCALE_FORMAT = Pattern.compile("String\\.format\\(\\s*(?!Locale\\.)");

  /** The text between the parenthesis opened just before {@code start} and the one that closes it. */
  private static String balancedArguments(final String source, final int start) {
    int depth = 1;
    for (int i = start; i < source.length(); i++) {
      final char c = source.charAt(i);
      if (c == '(')
        ++depth;
      else if (c == ')' && --depth == 0)
        return source.substring(start, i);
    }
    return source.substring(start);
  }
}
