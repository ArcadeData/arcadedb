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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.importer.graph.CsvRowSource;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for issue #7267.
 * <p>
 * {@link CsvRowSource} split each line with {@code line.split(String.valueOf(delimiter), -1)}, whose first
 * argument is a <b>regular expression</b>. A delimiter that is a regex metacharacter therefore did something
 * other than separate fields, in one of two families, neither of which names the delimiter as the culprit:
 * <ul>
 *   <li><b>silently wrong</b> - {@code '|'} is an alternation of two empty branches, so every character became
 *       its own field, separators included; {@code '.'} matched everything and annihilated the row; {@code '$'}
 *       and {@code '^'} are anchors and split nothing at all. A header line cut into single characters means
 *       every {@code record.get(attribute)} misses, so the import produced property-less vertices, or none;</li>
 *   <li><b>a crash about a regex nobody wrote</b> - {@code '*'}, {@code '+'}, {@code '?'}, {@code '('},
 *       {@code ')'}, {@code '['}, {@code '{'} and {@code '\'} threw {@code PatternSyntaxException}.</li>
 * </ul>
 * Each test drives one of the entry points that can carry an operator-chosen delimiter into the class - the JSON
 * configuration, the public constructor and the three-argument factory - because a fix applied at one of them
 * would leave the others exactly as broken while a single-path test went green.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7267CsvRowSourceLiteralDelimiterTest {

  /**
   * The characters a CSV delimiter can be that a regular expression does not read as themselves.
   * <p>
   * {@code '|'}, {@code '.'}, {@code '$'} and {@code '^'} were the silent family; {@code '*'}, {@code '+'},
   * {@code '?'}, {@code '('}, {@code ')'}, {@code '['}, {@code '{'} and {@code '\'} threw. {@code ']'} and
   * {@code '}'} are here for symmetry and worked before this fix as well - they are not in the set
   * {@code String.split} excludes from its regex-free fast path ({@code ".$|()[{^?*+\"}, {@code String.java:3692}
   * on JDK 26) and a lone closing bracket or brace is literal to {@code Pattern} anyway. Nothing here is claimed
   * to have been broken; the loop's claim is that all fourteen are correct now.
   */
  private static final char[] REGEX_METACHARACTERS = { '|', '.', '$', '^', '*', '+', '?', '(', ')', '[', ']', '{', '}', '\\' };

  private static final String DB_PATH = "target/databases/issue7267-csv-literal-delimiter";

  @TempDir
  Path tempDir;

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> database.getSchema().createVertexType("Person"));
  }

  @AfterEach
  void cleanup() {
    if (database != null)
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  // ───────────────────────────────────────────────────────────────────
  //  Entry point 1: the public constructor
  // ───────────────────────────────────────────────────────────────────

  /**
   * The whole defect in one loop: the same three-column file written with each metacharacter as its separator
   * has to read back as the same three columns, whichever character was chosen. Header and data rows go through
   * the same private splitter, so a header shredded into single characters shows up here as missing keys rather
   * than as wrong values - which is why the assertion is on the record's own attributes and not on a field count.
   */
  @Test
  void everyRegexMetacharacterSplitsOnTheLiteralCharacter() throws Exception {
    for (final char delimiter : REGEX_METACHARACTERS) {
      final Path csv = writeCsv("meta-" + (int) delimiter + ".csv", delimiter,
          "id", "firstName", "lastName");

      final List<Map<String, String>> rows = readAll(new CsvRowSource(csv.toString(), delimiter, 0),
          "id", "firstName", "lastName");

      assertThat(rows)
          .as("delimiter '%s' must cut the file into rows, not into characters", delimiter)
          .hasSize(2);
      assertThat(rows.get(0))
          .as("delimiter '%s' must separate fields on the literal character", delimiter)
          .containsExactlyInAnyOrderEntriesOf(Map.of("id", "1", "firstName", "Jay", "lastName", "Miner"));
      assertThat(rows.get(1))
          .as("delimiter '%s' must separate fields on the literal character", delimiter)
          .containsExactlyInAnyOrderEntriesOf(Map.of("id", "2", "firstName", "Ada", "lastName", "Lovelace"));
    }
  }

  /**
   * The crashing half of the defect, asserted as its own claim. {@code containsExactly} on the previous test
   * would also have failed on a {@code PatternSyntaxException}, but it would have failed with a stack trace that
   * says nothing about why - and "does not throw a regex error" is the promise a config file's author is owed.
   */
  @Test
  void aMetacharacterDelimiterNeverRaisesARegexError() {
    for (final char delimiter : REGEX_METACHARACTERS)
      assertThatCode(() -> {
        final Path csv = writeCsv("nothrow-" + (int) delimiter + ".csv", delimiter, "id", "firstName", "lastName");
        readAll(new CsvRowSource(csv.toString(), delimiter, 0), "id");
      })
          .as("delimiter '%s' is a field separator, not a pattern the operator wrote", delimiter)
          .doesNotThrowAnyException();
  }

  /**
   * The pipe is not a hypothetical choice: it is the delimiter every {@code splitEdge} example in the tree uses,
   * so an operator who already has pipe-delimited data reaches for it as the field separator too. This drives it
   * through a whole import rather than through the row source alone, so the assertion is on what actually lands
   * in the database - the shape of the failure the issue describes, vertices with no properties.
   */
  @Test
  void aPipeDelimitedImportThroughTheConstructorLoadsEveryProperty() throws Exception {
    final Path csv = writeCsv("people-ctor.csv", '|', "id", "firstName", "lastName");

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Person", new CsvRowSource(csv.toString(), '|', 0), v -> {
          v.id("id");
          v.intProperty("id", "id");
          v.property("firstName", "firstName");
          v.property("lastName", "lastName");
        })
        .build()) {
      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(2);
    }

    assertPersonsLoaded();
  }

  // ───────────────────────────────────────────────────────────────────
  //  Entry point 2: the three-argument factory
  // ───────────────────────────────────────────────────────────────────

  /**
   * {@code CsvRowSource.from(dir, file, delimiter)} is a second public way in, and it is the one the JSON path
   * does <b>not</b> use - a fix confined to {@code GraphImporter.createRecordSource} would leave it broken.
   */
  @Test
  void theThreeArgFactorySplitsOnTheLiteralCharacter() throws Exception {
    writeCsv("people-factory.csv", '|', "id", "firstName", "lastName");

    final List<Map<String, String>> rows = readAll(
        CsvRowSource.from(tempDir.toString(), "people-factory.csv", '|'),
        "id", "firstName", "lastName");

    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)).containsEntry("firstName", "Jay").containsEntry("lastName", "Miner");
  }

  // ───────────────────────────────────────────────────────────────────
  //  Entry point 3: the JSON configuration
  // ───────────────────────────────────────────────────────────────────

  /**
   * {@code "delimiter": "|"} on a vertex source reaches {@code new CsvRowSource(path, '|', skipLines)} through
   * {@code GraphImporter.createRecordSource}. The single-character length check added by #7263 lets it through,
   * as it should - a pipe is a legitimate field separator.
   */
  @Test
  void aJsonConfiguredPipeDelimiterImportsEveryProperty() throws Exception {
    writeCsv("people-json.csv", '|', "id", "firstName", "lastName");

    final JSONObject config = new JSONObject().put("vertices", new JSONArray().put(new JSONObject()
        .put("type", "Person")
        .put("file", "people-json.csv")
        .put("delimiter", "|")
        .put("id", "id")
        .put("properties", new JSONObject()
            .put("id", "int:id")
            .put("firstName", "firstName")
            .put("lastName", "lastName"))));

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, tempDir.toString())) {
      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(2);
    }

    assertPersonsLoaded();
  }

  // ───────────────────────────────────────────────────────────────────
  //  Behaviour that must NOT change
  // ───────────────────────────────────────────────────────────────────

  /**
   * The default comma, and every other delimiter that already worked, has to keep answering exactly what it
   * answered before - this fix replaces the splitter for all of them, not only for the broken ones.
   */
  @Test
  void theDefaultCommaDelimiterIsUnchanged() throws Exception {
    writeCsv("people-default.csv", ',', "id", "firstName", "lastName");

    for (final CsvRowSource source : new CsvRowSource[] {
        new CsvRowSource(tempDir.resolve("people-default.csv").toString()),
        CsvRowSource.from(tempDir.toString(), "people-default.csv") }) {

      final List<Map<String, String>> rows = readAll(source, "id", "firstName", "lastName");
      assertThat(rows).hasSize(2);
      assertThat(rows.get(0)).containsEntry("id", "1").containsEntry("firstName", "Jay").containsEntry("lastName", "Miner");
      assertThat(rows.get(1)).containsEntry("id", "2").containsEntry("firstName", "Ada").containsEntry("lastName", "Lovelace");
    }
  }

  /**
   * {@code split(literal, -1)} keeps every empty field, interior and trailing alike, and that shape is what the
   * header/value zip in {@code forEach} is written against. Two consequences are observable from outside and are
   * asserted here: an empty interior field does not shift the columns after it, and a trailing empty header name
   * stays a column of its own rather than being dropped along with the value under it.
   */
  @Test
  void emptyAndTrailingFieldsKeepTheirSplitMinusOneShape() throws Exception {
    final Path csv = tempDir.resolve("empties.csv");
    Files.writeString(csv, "id;firstName;lastName;\n1;;Miner;x\n2;Ada;;\n", StandardCharsets.UTF_8);

    final List<Map<String, String>> rows = readAll(new CsvRowSource(csv.toString(), ';', 0),
        "id", "firstName", "lastName", "");

    assertThat(rows).hasSize(2);
    assertThat(rows.get(0))
        .as("an empty interior field must not shift the columns that follow it")
        .containsEntry("id", "1").containsEntry("lastName", "Miner")
        .doesNotContainKey("firstName");
    assertThat(rows.get(0))
        .as("a trailing empty header name is still a column, exactly as split(literal, -1) reports it")
        .containsEntry("", "x");
    assertThat(rows.get(1))
        .as("trailing empty values read back as absent, the way CsvRecordReader folds empty to null")
        .containsEntry("id", "2").containsEntry("firstName", "Ada")
        .doesNotContainKey("lastName").doesNotContainKey("");
  }

  // ───────────────────────────────────────────────────────────────────
  //  Helpers
  // ───────────────────────────────────────────────────────────────────

  /** Writes a two-row file whose header and values are joined by {@code delimiter}, and answers its path. */
  private Path writeCsv(final String fileName, final char delimiter, final String... headers) throws Exception {
    final String sep = String.valueOf(delimiter);
    final String content = String.join(sep, headers) + "\n"
        + "1" + sep + "Jay" + sep + "Miner" + "\n"
        + "2" + sep + "Ada" + sep + "Lovelace" + "\n";
    final Path file = tempDir.resolve(fileName);
    Files.writeString(file, content, StandardCharsets.UTF_8);
    return file;
  }

  /** Drains a row source into one map per record, reading only the attributes named. */
  private static List<Map<String, String>> readAll(final GraphImporter.RecordSource source, final String... attributes)
      throws Exception {
    final List<Map<String, String>> rows = new ArrayList<>();
    source.forEach(record -> {
      final Map<String, String> row = new LinkedHashMap<>();
      for (final String attribute : attributes) {
        final String value = record.get(attribute);
        if (value != null)
          row.put(attribute, value);
      }
      rows.add(row);
    });
    return rows;
  }

  private void assertPersonsLoaded() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Person ORDER BY id")) {
        assertThat(rs.hasNext()).isTrue();
        final Result jay = rs.next();
        assertThat(jay.<String>getProperty("firstName")).isEqualTo("Jay");
        assertThat(jay.<String>getProperty("lastName")).isEqualTo("Miner");

        assertThat(rs.hasNext()).isTrue();
        final Result ada = rs.next();
        assertThat(ada.<String>getProperty("firstName")).isEqualTo("Ada");
        assertThat(ada.<String>getProperty("lastName")).isEqualTo("Lovelace");
      }
    });
  }
}
