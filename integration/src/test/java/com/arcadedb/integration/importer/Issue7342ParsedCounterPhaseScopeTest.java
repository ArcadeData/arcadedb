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
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.importer.format.XMLImporterFormat;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7342: one {@link ImporterContext} serves all four {@code loadFromSource()} phases of an
 * {@link Importer#load()}, so {@code context.parsed} is a running total unless the phase zeroes it. Nine of the
 * ten {@code FormatImporter} implementations zeroed it themselves, one at a time; {@code JSONImporterFormat}
 * never did. A run ending in a
 * JSON phase therefore reported a {@code parsedRecords} that mixed that phase's rows with the previous phase's,
 * while a run ending in any other format reported only the last phase's - two adjacent invocations of the same
 * CLI reporting the same quantity two different ways.
 * <p>
 * The reset now lives in {@link Importer#loadFromSource} - one place every phase passes through - so a format
 * cannot forget it and a format added later inherits it. {@code JSONImporterFormat} is deliberately left with no
 * reset of its own: it is the one format whose phase is zeroed by the hoist alone, which is what makes
 * {@link #theHoistedResetScopesTheCounterToTheJsonPhase()} a test of the hoist rather than of a tenth copy of the
 * same line.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7342">issue #7342</a>
 */
class Issue7342ParsedCounterPhaseScopeTest {

  /** Three records in the first phase, two in the second: five rows, told apart by which phase parsed them. */
  private static final String FIRST_PHASE_JSON  = "{\"Rows\":[{\"id\":\"1\"},{\"id\":\"2\"},{\"id\":\"3\"}]}";
  private static final String SECOND_PHASE_JSON = "{\"Rows\":[{\"id\":\"4\"},{\"id\":\"5\"}]}";
  private static final String MAPPING           = "{\"Rows\":[{\"@cat\":\"d\",\"@type\":\"Row\",\"@id\":\"id\",\"@idType\":\"string\"}]}";

  /**
   * The reported case, on the live CLI path: two {@code loadFromSource()} phases of one {@code Importer.load()},
   * both dispatching to {@code JSONImporterFormat} - the format that never zeroed the counter itself. Before the
   * hoist the second phase inherited the first phase's three rows and the run reported five; the hoist is the only
   * thing that zeroes a JSON phase, so this is the assertion that fails if it is removed.
   */
  @Test
  void theHoistedResetScopesTheCounterToTheJsonPhase() throws Exception {
    final Map<String, Object> report = runTwoPhaseJsonImport("7342-json-phase-scope");

    assertThat(report.get("parsedRecords"))
        .as("the JSON phase's counter starts from zero, so it reports its own two records rather than five")
        .isEqualTo(2L);
  }

  /**
   * Finding 4: neither of the two things {@code parsedRecords} used to mean was "the rows this import parsed".
   * {@code parsedRecords} stays the last phase's count - it is a published key of a public report map, and the
   * two-phase CLI assertions #7313 added read it - and the whole-run figure is published alongside it.
   */
  @Test
  void theImportWideTotalCountsEveryPhase() throws Exception {
    final Map<String, Object> report = runTwoPhaseJsonImport("7342-json-import-wide-total");

    assertThat(report.get("createdDocuments"))
        .as("the import must actually have run: three records from the first phase and two from the second")
        .isEqualTo(5L);
    assertThat(report.get("totalParsedRecords"))
        .as("the import-wide accumulator carries every phase's rows, including the one still running when the "
            + "report is built")
        .isEqualTo(5L);
  }

  /**
   * Finding 3: {@code FormatImporter#printProgress} prints {@code (parsed - lastParsed) / deltaInSecs}.
   * {@code lastParsed} was not reset alongside {@code parsed}, so the first progress line after a phase boundary
   * subtracted the previous phase's total from this phase's handful of rows and printed a negative rate. The
   * counter and its high-water mark are now zeroed together, in one place, so they cannot come apart.
   */
  @Test
  void aPhaseBoundaryNeverPrintsANegativeParseRate() {
    final List<String> lines = new ArrayList<>();
    final ConsoleLogger logger = new ConsoleLogger(2, lines::add);

    final ImporterSettings settings = new ImporterSettings();
    settings.verboseLevel = 2;

    final ImporterContext context = new ImporterContext();
    // Where a phase that parsed a hundred rows and printed a progress line for them leaves the context.
    context.lastLapOn = System.currentTimeMillis() - 2_000;
    context.parsed.set(100);
    context.lastParsed = 100;

    context.beginParsingPhase();

    // Ten rows into the phase that follows, which is when the next progress line falls due.
    context.parsed.set(10);

    // source == null takes printProgress's first branch, the one that needs no Parser position.
    new XMLImporterFormat().printProgress(settings, context, null, null, logger);

    assertThat(lines).as("verboseLevel 2 prints one progress line").hasSize(1);
    assertThat(lines.getFirst())
        .as("the line reports this phase's own ten rows, not the previous phase's hundred")
        .startsWith("- Parsed 10 (");
    // Not an equality on the rate: deltaInSecs is raw wall clock, so a stall between seeding lastLapOn and printing
    // would move an exact expectation. The defect was a rate below zero, and no stall can produce one - a longer
    // window only divides the same ten rows by a bigger number.
    assertThat(parseRateOf(lines.getFirst()))
        .as("the rate is this phase's own rows over the elapsed window, not this phase's rows minus the "
            + "previous phase's total: %s", lines.getFirst())
        .isNotNegative();
  }

  /**
   * The accumulator in isolation, so the arithmetic the two CLI assertions above rest on is pinned where a
   * failure names it: each boundary rolls the phase that just finished into the total and zeroes the phase
   * counter and its high-water mark together, and the total the report publishes includes the phase still
   * running.
   */
  @Test
  void beginParsingPhaseRollsTheFinishedPhaseIntoTheImportWideTotal() {
    final ImporterContext context = new ImporterContext();

    context.beginParsingPhase();
    context.parsed.set(3);
    context.lastParsed = 3;

    context.beginParsingPhase();
    assertThat(context.parsed.get()).as("the phase counter starts the new phase at zero").isZero();
    assertThat(context.lastParsed).as("and so does the high-water mark printProgress subtracts").isZero();
    assertThat(context.totalParsed.get()).as("the finished phase's three rows are now in the total").isEqualTo(3L);

    context.parsed.set(2);
    assertThat(context.totalParsedRecords())
        .as("the published total includes the phase still running, which no boundary has folded in yet")
        .isEqualTo(5L);
    assertThat(context.toMap())
        .containsEntry("parsedRecords", 2L)
        .containsEntry("totalParsedRecords", 5L);
  }

  /**
   * Drives the two-phase CLI route: {@code -url} is the first {@code loadFromSource()} phase and
   * {@code -documents} the second, both JSON, one shared {@code -mapping}.
   */
  private static Map<String, Object> runTwoPhaseJsonImport(final String name) throws Exception {
    final Path firstPhase = Path.of("target", name + "-phase1.json").toAbsolutePath();
    final Path secondPhase = Path.of("target", name + "-phase2.json").toAbsolutePath();
    Files.createDirectories(firstPhase.getParent());
    Files.writeString(firstPhase, FIRST_PHASE_JSON, StandardCharsets.UTF_8);
    Files.writeString(secondPhase, SECOND_PHASE_JSON, StandardCharsets.UTF_8);

    final String databasePath = "target/databases/" + name;
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      return new Importer(new String[] { "-url", "file://" + firstPhase, "-documents", "file://" + secondPhase,
          "-database", databasePath, "-forceDatabaseCreate", "true", "-mapping", MAPPING }).load();
    } finally {
      dropDatabase(databasePath);
      Files.deleteIfExists(firstPhase);
      Files.deleteIfExists(secondPhase);
      TestHelper.checkActiveDatabases();
    }
  }

  /** The first {@code (N/sec)} group of a progress line - the parse rate, which is the one under test. */
  private static long parseRateOf(final String progressLine) {
    final Matcher matcher = Pattern.compile("\\((-?[\\d,]+)/sec").matcher(progressLine);
    assertThat(matcher.find()).as("the progress line carries a parse rate: %s", progressLine).isTrue();
    return Long.parseLong(matcher.group(1).replace(",", ""));
  }

  private static void dropDatabase(final String databasePath) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists()) {
      final Database db = factory.open();
      db.drop();
    }
    FileUtils.deleteRecursively(new File(databasePath));
  }
}
