/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
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
 * Regression guard for issue #7141: a {@code LogManager.instance().log(...)} call whose format string carries
 * more conversions than the call supplies arguments does not fail - {@link LogManager} pads the missing slots
 * with {@code null}, so the message prints {@code null} exactly where the offending value belonged. Two such
 * calls had been sitting in the tree unnoticed ({@code ArcadeDBServer.loadDefaultDatabases} lost the malformed
 * startup command, {@code ComponentFile.drop} a phantom third placeholder), each costing an operator a
 * debugging round trip.
 * <p>
 * Compilers cannot catch this: the format string is a plain {@code String} and the arguments are varargs. So
 * the check is done here, over the sources of the modules that ship the server. Deliberately conservative -
 * only calls whose message is a single string literal (or a concatenation of literals) are inspected, and the
 * more permissive of the two possible argument counts is used (the fourth parameter of the overload may be a
 * {@code Throwable} slot rather than a format argument). A report from this test is therefore a genuine
 * missing argument, never a parsing artifact.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LogMessageArityTest {

  private static final Pattern CALL_START =
      Pattern.compile("LogManager\\s*\\.\\s*instance\\s*\\(\\s*\\)\\s*\\.\\s*log\\s*\\(");
  /** A message made only of string literals (text blocks included), possibly concatenated. */
  private static final Pattern LITERAL_MESSAGE =
      Pattern.compile("^(?:\"\"\"[\\s\\S]*?\"\"\"|\"(?:[^\"\\\\]|\\\\.)*\")"
          + "(?:\\s*\\+\\s*(?:\"\"\"[\\s\\S]*?\"\"\"|\"(?:[^\"\\\\]|\\\\.)*\"))*$");
  /** A java.util.Formatter conversion. {@code %%} and {@code %n} are stripped before this is applied. */
  private static final Pattern CONVERSION =
      Pattern.compile("%(?:\\d+\\$)?[-#+ 0,(]*\\d*(?:\\.\\d+)?[a-zA-Z]");

  @Test
  void everyLogCallSuppliesAnArgumentForEveryFormatConversion() throws IOException {
    final List<String> offenders = new ArrayList<>();

    for (final Path module : moduleSourceRoots()) {
      try (final Stream<Path> files = Files.walk(module)) {
        files.filter(p -> p.toString().endsWith(".java")).forEach(p -> {
          try {
            inspect(p, Files.readString(p, StandardCharsets.UTF_8), offenders);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        });
      }
    }

    assertThat(offenders)
        .as("log calls whose format string has more conversions than the call supplies arguments (issue #7141)")
        .isEmpty();
  }

  private static List<Path> moduleSourceRoots() throws IOException {
    // The test runs with its own module directory as working directory; every sibling module lives next to
    // it. Modules are discovered rather than listed so a new one is covered without touching this test.
    final Path repoRoot = Path.of("").toAbsolutePath().getParent();
    final List<Path> roots = new ArrayList<>();
    if (repoRoot == null)
      return roots;
    try (final Stream<Path> modules = Files.list(repoRoot)) {
      modules.map(m -> m.resolve("src").resolve("main").resolve("java")).filter(Files::isDirectory).sorted()
          .forEach(roots::add);
    }
    assertThat(roots).as("no module source root found under " + repoRoot).isNotEmpty();
    return roots;
  }

  private static void inspect(final Path file, final String source, final List<String> offenders) {
    final Matcher m = CALL_START.matcher(source);
    while (m.find()) {
      final int argsStart = m.end();
      final int argsEnd = findClosingParenthesis(source, argsStart);
      if (argsEnd < 0)
        continue;

      final List<String> args = splitTopLevel(source.substring(argsStart, argsEnd));
      if (args.size() < 3)
        continue;

      final String message = args.get(2);
      if (!LITERAL_MESSAGE.matcher(message).matches())
        continue;

      final int conversions = countConversions(message);
      if (conversions == 0)
        continue;

      // Everything after the message. A literal `null` in the first of those slots is unambiguously the
      // Throwable of the log(Object, Level, String, Throwable, Object...) overload - javac picks it because
      // Throwable is more specific than Object - so it is not a format argument. For anything else the slot
      // is ambiguous from source alone, and the permissive reading is taken so only a genuine shortfall is
      // reported.
      final int supplied = args.size() - 3 - ("null".equals(args.get(3)) ? 1 : 0);
      if (supplied >= conversions)
        continue;

      final int line = (int) source.substring(0, m.start()).chars().filter(c -> c == '\n').count() + 1;
      offenders.add(file + ":" + line + " expects " + conversions + " argument(s), supplies at most " + supplied
          + ": " + message.lines().findFirst().orElse(message));
    }
  }

  private static int countConversions(final String literal) {
    final String stripped = literal.replace("%%", "").replace("%n", "");
    final Matcher m = CONVERSION.matcher(stripped);
    int count = 0;
    while (m.find())
      count++;
    return count;
  }

  /** Index of the parenthesis closing the argument list that starts at {@code from}, or -1. */
  private static int findClosingParenthesis(final String source, final int from) {
    int depth = 1;
    for (int i = from; i < source.length(); ) {
      final int skipped = skipLiteral(source, i);
      if (skipped > i) {
        i = skipped;
        continue;
      }
      final char c = source.charAt(i);
      if (c == '(')
        depth++;
      else if (c == ')' && --depth == 0)
        return i;
      i++;
    }
    return -1;
  }

  /** Splits an argument list on commas that are not nested inside brackets or string/char literals. */
  private static List<String> splitTopLevel(final String args) {
    final List<String> parts = new ArrayList<>();
    final StringBuilder current = new StringBuilder();
    int depth = 0;
    for (int i = 0; i < args.length(); ) {
      final int skipped = skipLiteral(args, i);
      if (skipped > i) {
        current.append(args, i, skipped);
        i = skipped;
        continue;
      }
      final char c = args.charAt(i);
      if (c == '(' || c == '[' || c == '{')
        depth++;
      else if (c == ')' || c == ']' || c == '}')
        depth--;
      if (c == ',' && depth == 0) {
        parts.add(current.toString().trim());
        current.setLength(0);
      } else
        current.append(c);
      i++;
    }
    parts.add(current.toString().trim());
    return parts;
  }

  /**
   * When {@code i} opens a text block, a string literal or a char literal, returns the index just past its
   * closing delimiter; otherwise returns {@code i}.
   */
  private static int skipLiteral(final String s, final int i) {
    if (s.startsWith("\"\"\"", i)) {
      int j = i + 3;
      while (j < s.length()) {
        if (s.charAt(j) == '\\')
          j += 2;
        else if (s.startsWith("\"\"\"", j))
          return j + 3;
        else
          j++;
      }
      return s.length();
    }
    final char open = s.charAt(i);
    if (open != '"' && open != '\'')
      return i;
    int j = i + 1;
    while (j < s.length()) {
      final char c = s.charAt(j);
      if (c == '\\')
        j += 2;
      else if (c == open)
        return j + 1;
      else
        j++;
    }
    return s.length();
  }
}
