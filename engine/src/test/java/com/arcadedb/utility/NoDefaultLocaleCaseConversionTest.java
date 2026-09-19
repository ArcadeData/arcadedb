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
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bans the no-argument {@code String.toUpperCase()} / {@code toLowerCase()} from EVERY module's production sources.
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
 * <p>
 * It scans the WHOLE repository rather than only this module, because the hazard is not engine-specific and the
 * wire protocols are where it bites hardest: a Postgres or Bolt keyword scan, an HTTP header name folded for
 * lookup, and {@code DatabaseBackupConfig}'s {@code Type.valueOf(json.getString("type").toUpperCase())} were all
 * live instances of the same defect, found by widening this scan after #7900's engine-only sweep (PR #7942 review).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class NoDefaultLocaleCaseConversionTest {

  /** Modules that must be among those found, or the root was resolved wrongly and the scan is vacuous. */
  private static final List<String> EXPECTED_MODULES = List.of("engine", "network", "server", "grpcw", "postgresw");

  /** {@code .toUpperCase()} / {@code .toLowerCase()} with an EMPTY argument list, whatever the receiver. */
  private static final Pattern NO_ARG_CASE_CONVERSION = Pattern.compile("\\.to(?:Upper|Lower)Case\\s*\\(\\s*\\)");

  @Test
  void noProductionSourceFoldsCaseWithTheDefaultLocale() throws IOException {
    final Path root = repositoryRoot();

    final List<String> offenders = new ArrayList<>();
    final Set<String> modules = new TreeSet<>();
    int scanned = 0;

    for (final Path moduleSources : productionSourceRoots(root)) {
      modules.add(root.relativize(moduleSources).getName(0).toString());
      try (final Stream<Path> sources = Files.walk(moduleSources)) {
        for (final Path source : sources.filter(p -> p.toString().endsWith(".java")).toList()) {
          ++scanned;
          offenders.addAll(offendersIn(root.relativize(source).toString(),
              Files.readString(source, StandardCharsets.UTF_8)));
        }
      }
    }

    assertThat(scanned).as("the scan found no sources at all, so it is asserting about nothing").isPositive();
    assertThat(modules)
        .as("the repository root resolved to %s, where these modules are missing - the scan would silently cover "
            + "less than it claims", root)
        .containsAll(EXPECTED_MODULES);
    assertThat(offenders)
        .as("the no-argument toUpperCase()/toLowerCase() folds with the JVM default locale, which turns 'i' into "
            + "'\u0130' on a Turkish, Azeri or Lithuanian server and silently breaks keyword matching - pass "
            + "Locale.ROOT (or Locale.getDefault() if the text really is for a human to read)")
        .isEmpty();
  }

  /**
   * The repository root, walked up from the module directory Surefire runs in. Verified by the module assertion
   * above rather than trusted: a wrong root is the one way this whole class goes quietly vacuous.
   */
  private static Path repositoryRoot() {
    for (Path candidate = Path.of("").toAbsolutePath(); candidate != null; candidate = candidate.getParent())
      if (Files.isDirectory(candidate.resolve("engine").resolve("src").resolve("main").resolve("java")))
        return candidate;

    throw new IllegalStateException("cannot locate the repository root from " + Path.of("").toAbsolutePath());
  }

  /** Every module's {@code src/main/java/com/arcadedb}, one level below the root. */
  private static List<Path> productionSourceRoots(final Path root) throws IOException {
    try (final Stream<Path> modules = Files.list(root)) {
      return modules.filter(Files::isDirectory)
          .map(m -> m.resolve("src").resolve("main").resolve("java").resolve("com").resolve("arcadedb"))
          .filter(Files::isDirectory)
          .sorted()
          .toList();
    }
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

    // a comment TRAILING real code is still a comment
    assertThat(offendersIn("Fixture.java", "  foo(); // see keyword.toUpperCase() below")).isEmpty();

    // ...but a `//` inside a string literal does not start one, or the guard would walk past a real offender
    assertThat(offendersIn("Fixture.java", "  final String k = uri(\"http://x\").toUpperCase();")).hasSize(1);
  }

  /**
   * Every offending line of {@code text}, skipping a match that is inside a comment - the family is DESCRIBED in
   * a good many javadocs and {@code //} notes in this tree, precisely because it has been fixed twice, and a
   * guard that flagged its own documentation would be turned off rather than obeyed.
   */
  private static List<String> offendersIn(final String fileName, final String text) {
    final List<String> offenders = new ArrayList<>();
    final Matcher matcher = NO_ARG_CASE_CONVERSION.matcher(text);

    while (matcher.find()) {
      final int lineStart = text.lastIndexOf('\n', matcher.start()) + 1;
      final String before = text.substring(lineStart, matcher.start());
      if (before.stripLeading().startsWith("*") || before.stripLeading().startsWith("/*"))
        continue;
      if (commentStart(before) > -1)
        continue;

      final int lineEnd = text.indexOf('\n', matcher.start());
      offenders.add(fileName + ": " + text.substring(lineStart, lineEnd < 0 ? text.length() : lineEnd).strip());
    }
    return offenders;
  }

  /**
   * Where a line-comment begins in {@code beforeTheMatch}, or {@code -1} if the match is in real code.
   * <p>
   * String literals are tracked rather than ignored, and that direction matters: treating any {@code //} as a
   * comment start would let {@code uri("http://x").toUpperCase()} through, and a guard's FALSE NEGATIVE is the
   * expensive kind - it is the one thing this class exists to prevent. Testing only whether the line STARTS with
   * {@code //} had the opposite flaw, flagging a trailing comment after real code (PR #7942 review).
   */
  private static int commentStart(final String beforeTheMatch) {
    boolean inString = false;
    boolean inChar = false;

    for (int i = 0; i < beforeTheMatch.length(); i++) {
      final char c = beforeTheMatch.charAt(i);

      if (c == '\\' && (inString || inChar)) {
        ++i;
        continue;
      }
      if (c == '"' && !inChar)
        inString = !inString;
      else if (c == '\'' && !inString)
        inChar = !inChar;
      else if (c == '/' && !inString && !inChar && i + 1 < beforeTheMatch.length()
          && beforeTheMatch.charAt(i + 1) == '/')
        return i;
    }
    return -1;
  }
}
