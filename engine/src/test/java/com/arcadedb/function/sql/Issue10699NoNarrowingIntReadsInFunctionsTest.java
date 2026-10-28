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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, see the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.function.sql;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10699: #10611 converted the integer arguments of the five vector functions it enumerated, and its test
 * enumerated the same five - so it could not find {@code vector.sparseCreate}, {@code randomInt} and {@code if}, which
 * still read their argument through {@code Number.intValue()} (a fraction truncated, a value past the int range
 * wrapped). An enumeration of the converted sites cannot find the unconverted ones; this scan can.
 * <p>
 * Every {@code .intValue()} and {@code Integer.parseInt(} in the SQL function sources is an offender unless it is listed
 * below, with the number of occurrences and the reason it is not a narrowing read of an argument. A new occurrence in a
 * listed file changes its count and fails the scan too. An argument that has to be an int is read through
 * {@link SQLFunctionAbstract#requireWholeIntArgument}, {@link SQLFunctionAbstract#requireIntArgument} (saturating, for a
 * configuration value) or the vector family's {@code parseInteger}, all built on {@code NumberUtils}.
 */
class Issue10699NoNarrowingIntReadsInFunctionsTest {
  private static final Pattern NARROWING_READ = Pattern.compile("\\.intValue\\s*\\(\\s*\\)|\\bInteger\\s*\\.\\s*parseInt\\s*\\(");

  private static final String FUNCTIONS = "src/main/java/com/arcadedb/function/sql";

  /** Relative to {@link #FUNCTIONS}: the reviewed occurrences, and why each is not a narrowing read of an argument. */
  private static final Map<String, Integer> REVIEWED = new TreeMap<>(Map.of(
      // the strict helper itself: parses a TEXT argument, and Integer.parseInt refuses what does not fit
      "SQLFunctionAbstract.java", 1,
      // the vector family's strict helper (#10611), same shape
      "vector/SQLFunctionVectorAbstract.java", 1,
      // the shared option-map reader: a text option parsed by Integer.parseInt, which refuses rather than truncates
      "FunctionOptions.java", 1,
      // narrow a COMPUTED result back to the Integer type of the input (pow(int), sqrt(int)), not an argument
      "math/SQLFunctionPow.java", 1, //
      "math/SQLFunctionSquareRoot.java", 1,
      // the astar/dijkstra option bag (a MAP entry, not a positional argument): out of #10699's scope, tracked as #10788
      "graph/SQLFunctionHeuristicPathFinderAbstract.java", 2));

  @Test
  void noSqlFunctionReadsAnIntegerArgumentThroughANarrowingConversion() throws IOException {
    final Path root = Path.of(FUNCTIONS);
    assertThat(root).as("run from the engine module").isDirectory();

    final Map<String, Integer> found = new TreeMap<>();
    final List<String> offenders = new ArrayList<>();
    int scanned = 0;
    try (final Stream<Path> files = Files.walk(root)) {
      for (final Path file : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
        scanned++;
        final String relative = root.relativize(file).toString().replace('\\', '/');
        final List<String> lines = read(file);
        for (int i = 0; i < lines.size(); i++) {
          final String code = stripComment(lines.get(i));
          final Matcher matcher = NARROWING_READ.matcher(code);
          while (matcher.find()) {
            found.merge(relative, 1, Integer::sum);
            if (!REVIEWED.containsKey(relative))
              offenders.add(relative + ":" + (i + 1) + ": " + lines.get(i).trim());
          }
        }
      }
    }

    assertThat(scanned).as("no SQL function source found under " + root.toAbsolutePath()).isGreaterThan(100);
    assertThat(offenders)
        .as("SQL functions reading an int through intValue()/Integer.parseInt, which truncates 2.5 to 2 and wraps 3000000000 "
            + "to a negative int (issue #10699): read the argument through requireWholeIntArgument / requireIntArgument / "
            + "parseInteger instead")
        .isEmpty();
    assertThat(found)
        .as("the reviewed occurrences changed: a new narrowing read in a reviewed file, or one removed (update REVIEWED)")
        .isEqualTo(REVIEWED);
  }

  /** Drops a line comment, and a line that is part of a block or Javadoc comment. */
  private static String stripComment(final String line) {
    final String trimmed = line.trim();
    if (trimmed.startsWith("*") || trimmed.startsWith("/*") || trimmed.startsWith("//"))
      return "";
    final int lineComment = line.indexOf("//");
    return lineComment >= 0 ? line.substring(0, lineComment) : line;
  }

  private static List<String> read(final Path file) {
    try {
      return Files.readAllLines(file, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
