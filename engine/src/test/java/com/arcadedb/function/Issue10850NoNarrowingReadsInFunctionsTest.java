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
package com.arcadedb.function;

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
 * Issue #10850: the #10699 scan ({@code Issue10699NoNarrowingIntReadsInFunctionsTest}) covers {@code function/sql/**}
 * only, so the Cypher-style functions next to it - {@code left}, {@code substring}, {@code coll.*}, {@code agg.nth},
 * {@code round}, the vector constructor - kept reading their INTEGER arguments through {@code Number.intValue()}, and the
 * long-typed ones ({@code duration(2.5, 'hours')}) through {@code longValue()}: a fraction truncated and a value past the
 * range wrapped.
 * <p>
 * This scan covers the whole {@code function/**} tree, plus the Cypher function implementations kept under
 * {@code query/opencypher/executor} ({@code CypherSubstringFunction} is what Cypher resolves {@code substring()} to). Every
 * {@code .intValue()}, {@code Integer.parseInt(} and {@code .longValue()} there is an offender unless its file is listed
 * below with the number of occurrences and the reason it is not a narrowing read of an argument; a new occurrence in a
 * listed file changes its count and fails the scan too. An INTEGER argument is read through
 * {@code CypherFunctionHelper.requireWholeNumberArgument} / {@code requireWholeIntArgument}, or on the SQL side through
 * {@code SQLFunctionAbstract.requireWholeIntArgument} / {@code requireWholeLongArgument}.
 */
class Issue10850NoNarrowingReadsInFunctionsTest {
  private static final Pattern NARROWING_INT_READ = Pattern.compile(
      "\\.intValue\\s*\\(\\s*\\)|\\bInteger\\s*\\.\\s*parseInt\\s*\\(");
  private static final Pattern NARROWING_LONG_READ = Pattern.compile("\\.longValue\\s*\\(\\s*\\)");

  private static final Path   SOURCES          = Path.of("src/main/java/com/arcadedb");
  private static final String FUNCTIONS        = "function";
  private static final String CYPHER_FUNCTIONS = "query/opencypher/executor";

  /** Relative to {@link #SOURCES}: the reviewed int reads, and why each is not a narrowing read of an argument. */
  private static final Map<String, Integer> REVIEWED_INT = new TreeMap<>(Map.of(
      // the strict helpers: parse a TEXT argument, and Integer.parseInt refuses what does not fit (#10699)
      "function/sql/SQLFunctionAbstract.java", 1, //
      "function/sql/vector/SQLFunctionVectorAbstract.java", 1, //
      "function/sql/FunctionOptions.java", 1, //
      // narrow a COMPUTED result back to the Integer type of the input (pow(int), sqrt(int)), not an argument
      "function/sql/math/SQLFunctionPow.java", 1, //
      "function/sql/math/SQLFunctionSquareRoot.java", 1,
      // year/month/day of a temporal value the engine built: components a LocalDate already holds as ints
      "function/temporal/DateConstructorFunction.java", 3));

  /** Relative to {@link #SOURCES}: the reviewed long reads, and why each is not a narrowing read of an argument. */
  private static final Map<String, Integer> REVIEWED_LONG = new TreeMap<>(Map.ofEntries(
      // a value already known to be Long/Integer/Short/Byte: widening, nothing to lose
      Map.entry("function/DistinctNumericKey.java", 1), //
      Map.entry("function/agg/PercentileDiscFunction.java", 1), //
      Map.entry("function/math/AbsFunction.java", 1), //
      Map.entry("function/number/NumberFormat.java", 1), //
      Map.entry("function/sql/math/NumericSum.java", 3),
      // toInteger()/convert.toInteger(): truncating a FLOAT IS the conversion (toInteger(2.9) is 2 in Neo4j too)
      Map.entry("function/convert/ToIntegerFunction.java", 1), //
      Map.entry("function/convert/ConvertToInteger.java", 1),
      // toBoolean(): FLOAT and DOUBLE are refused before it, the integral value is tested against zero
      Map.entry("function/convert/ToBooleanFunction.java", 1),
      // the sign check of a value getInt/getLong already read strictly
      Map.entry("function/sql/FunctionOptions.java", 1),
      // narrow a COMPUTED result back to the Long type of the input (pow(long), sqrt(long)), not an argument
      Map.entry("function/sql/math/SQLFunctionPow.java", 1), //
      Map.entry("function/sql/math/SQLFunctionSquareRoot.java", 1)));

  @Test
  void noFunctionReadsAnIntArgumentThroughANarrowingConversion() throws IOException {
    scan(NARROWING_INT_READ, REVIEWED_INT, "intValue()/Integer.parseInt, which truncates 2.5 to 2 and wraps 4294967297 to 1");
  }

  @Test
  void noFunctionReadsALongArgumentThroughANarrowingConversion() throws IOException {
    scan(NARROWING_LONG_READ, REVIEWED_LONG, "longValue(), which truncates 2.5 to 2");
  }

  private static void scan(final Pattern pattern, final Map<String, Integer> reviewed, final String what) throws IOException {
    assertThat(SOURCES).as("run from the engine module").isDirectory();

    final Map<String, Integer> found = new TreeMap<>();
    final List<String> offenders = new ArrayList<>();
    final List<Path> files = new ArrayList<>(javaFiles(SOURCES.resolve(FUNCTIONS)));
    // only the function implementations of the Cypher executor, not the whole executor
    files.addAll(javaFiles(SOURCES.resolve(CYPHER_FUNCTIONS)).stream()
        .filter(p -> p.getFileName().toString().matches("Cypher\\w+Function\\.java")).toList());

    for (final Path file : files) {
      final String relative = SOURCES.relativize(file).toString().replace('\\', '/');
      final List<String> lines = read(file);
      for (int i = 0; i < lines.size(); i++) {
        final Matcher matcher = pattern.matcher(stripComment(lines.get(i)));
        while (matcher.find()) {
          found.merge(relative, 1, Integer::sum);
          if (!reviewed.containsKey(relative))
            offenders.add(relative + ":" + (i + 1) + ": " + lines.get(i).trim());
        }
      }
    }

    assertThat(files).as("no function source found under " + SOURCES.toAbsolutePath()).hasSizeGreaterThan(300);
    assertThat(files).as("CypherSubstringFunction is no longer where it was: point the scan at it again")
        .anyMatch(p -> p.endsWith("CypherSubstringFunction.java"));
    assertThat(offenders)
        .as("functions reading an argument through " + what + " (issue #10850): read it through "
            + "CypherFunctionHelper.requireWholeNumberArgument / requireWholeIntArgument, or SQLFunctionAbstract."
            + "requireWholeIntArgument / requireWholeLongArgument instead")
        .isEmpty();
    assertThat(found)
        .as("the reviewed occurrences changed: a new narrowing read in a reviewed file, or one removed (update the list)")
        .isEqualTo(reviewed);
  }

  private static List<Path> javaFiles(final Path root) throws IOException {
    try (final Stream<Path> files = Files.walk(root)) {
      return files.filter(p -> p.toString().endsWith(".java")).sorted().toList();
    }
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
