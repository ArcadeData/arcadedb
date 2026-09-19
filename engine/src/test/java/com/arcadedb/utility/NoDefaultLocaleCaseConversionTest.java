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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bans the no-argument {@code String.toUpperCase()} / {@code toLowerCase()} from the engine's production sources.
 * <p>
 * They fold with the JVM DEFAULT locale, and in the Turkish, Azeri and Lithuanian locales {@code 'i'} upper-cases
 * to the dotted {@code 'İ'} (U+0130) rather than to {@code 'I'}. Every internal use of these methods in this code
 * base is keyword or identifier normalisation - a SQL keyword, an enum constant name, a function name - where that
 * is simply wrong, and the failure is invisible to anyone not running such a locale.
 * <p>
 * It is a SOURCE-level guard because it has to be. Issue #7900 found the family by running the suite under
 * {@code -Duser.language=tr}, and the damage on the worst arm was silent: {@code CREATE INDEX ... COLLATE ci}
 * built a case-SENSITIVE index, the UNIQUE constraint stopped firing, and the wrong collation was PERSISTED, so
 * moving the database to a normal-locale server did not repair it. A behavioural test can only cover the sites
 * somebody thought to write one for; this covers the next one, which is the whole point - the eight-site sweep
 * that fix needed was itself the second pass over this family, after the one in {@code com.arcadedb.function}.
 * <p>
 * The fix at any site this flags is one argument: {@code toUpperCase(Locale.ROOT)}. If a site genuinely wants the
 * user's locale - formatting text for a human to read - it says so with {@code Locale.getDefault()}, which is
 * explicit and passes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class NoDefaultLocaleCaseConversionTest {

  private static final Path MAIN_SOURCES = Path.of("src", "main", "java", "com", "arcadedb");

  /** {@code .toUpperCase()} / {@code .toLowerCase()} with an EMPTY argument list, whatever the receiver. */
  private static final Pattern NO_ARG_CASE_CONVERSION = Pattern.compile("\\.to(?:Upper|Lower)Case\\s*\\(\\s*\\)");

  @Test
  void noProductionSourceFoldsCaseWithTheDefaultLocale() throws IOException {
    assertThat(MAIN_SOURCES).as("the module's own sources, relative to the module directory Surefire runs in")
        .isDirectory();

    final List<String> offenders = new ArrayList<>();
    int scanned = 0;

    try (final Stream<Path> sources = Files.walk(MAIN_SOURCES)) {
      for (final Path source : sources.filter(p -> p.toString().endsWith(".java")).toList()) {
        ++scanned;
        offenders.addAll(offendersIn(source.toString(), Files.readString(source, StandardCharsets.UTF_8)));
      }
    }

    assertThat(scanned).as("the scan found no sources at all, so it is asserting about nothing").isPositive();
    assertThat(offenders)
        .as("the no-argument toUpperCase()/toLowerCase() folds with the JVM default locale, which turns 'i' into "
            + "'İ' on a Turkish, Azeri or Lithuanian server and silently breaks keyword matching - pass "
            + "Locale.ROOT (or Locale.getDefault() if the text really is for a human to read)")
        .isEmpty();
  }

  /**
   * Guards the guard: a scan that has stopped recognising anything passes just as quietly as a clean tree, which
   * is the vacuous pass a source-level test is most exposed to. This asserts the JUDGEMENT still fires, not only
   * that the walk found files.
   */
  @Test
  void theScanStillRecognisesTheConstructItBans() {
    assertThat(offendersIn("Fixture.java", "  final String k = keyword.toUpperCase();")).hasSize(1);
    assertThat(offendersIn("Fixture.java", "  final String k = keyword.toLowerCase( );")).hasSize(1);

    assertThat(offendersIn("Fixture.java", "  final String k = keyword.toUpperCase(Locale.ROOT);")).isEmpty();
    assertThat(offendersIn("Fixture.java", "  // match field.toLowerCase() against a CI index")).isEmpty();
    assertThat(offendersIn("Fixture.java", "   * {@code haystack.toUpperCase().contains(needle)} copies")).isEmpty();
  }

  /**
   * Every offending line of {@code text}, skipping a line whose match is inside a comment - the family is
   * DESCRIBED in a good many javadocs and {@code //} notes in this tree, precisely because it has been fixed
   * twice, and a guard that flagged its own documentation would be turned off rather than obeyed.
   */
  private static List<String> offendersIn(final String fileName, final String text) {
    final List<String> offenders = new ArrayList<>();
    final Matcher matcher = NO_ARG_CASE_CONVERSION.matcher(text);

    while (matcher.find()) {
      final int lineStart = text.lastIndexOf('\n', matcher.start()) + 1;
      final String before = text.substring(lineStart, matcher.start()).strip();
      if (before.startsWith("//") || before.startsWith("*") || before.startsWith("/*"))
        continue;

      final int lineEnd = text.indexOf('\n', matcher.start());
      offenders.add(fileName + ": " + text.substring(lineStart, lineEnd < 0 ? text.length() : lineEnd).strip());
    }
    return offenders;
  }
}
