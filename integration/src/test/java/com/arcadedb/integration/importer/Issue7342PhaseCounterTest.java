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
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7342: {@code ImporterContext} is created once per {@code Importer.load()} and shared by its four
 * {@code loadFromSource()} phases, so {@code context.parsed} is a running total unless the format zeroes it. Nine
 * of the eleven {@code load()} implementations did, one at a time, {@code JSONImporterFormat} did not, and the two
 * that took a decision off the value corrupted a whole phase when they inherited a non-zero one (#7288, #7313).
 * <p>
 * The reset now lives in {@code Importer.loadFromSource()}, where a format cannot forget it and a format added
 * later inherits it, and {@code parsedRecords} answers one question rather than two: it is the rows THIS IMPORT
 * parsed, every phase of it. It used to be "the last phase's count" for ten formats and "the last phase plus
 * whatever preceded it" for {@code JSONImporterFormat}, so two adjacent invocations of the same CLI reported the
 * same quantity two different ways.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7342PhaseCounterTest {

  /**
   * The unit the hoist rests on. {@code lastParsed} goes with the counter: it is what
   * {@code FormatImporter#printProgress} subtracts from {@link ImporterContext#getParsedTotal()} to turn the
   * counter into a rate (issue #7483 moved that subtraction from the per-phase counter to the cumulative total).
   * {@code lastParsed} is therefore rebased to the cumulative total as of the boundary, not zeroed: zeroing it
   * while the thing it is subtracted FROM stayed cumulative would make the very next progress line compute
   * {@code (wholeImportSoFar - 0) / oneSecond} - a rate spike exactly as visible as the negative-rate bug this
   * reset originally fixed, just inflated instead of negative.
   */
  @Test
  void beginPhaseFoldsThePhaseIntoTheTotalAndRebasesLastParsedToIt() {
    final ImporterContext context = new ImporterContext();

    context.parsed.set(7);
    context.lastParsed = 7;

    context.beginPhase();

    assertThat(context.parsed.get()).as("the next phase counts its own rows from zero").isZero();
    assertThat(context.lastParsed)
        .as("rebased to the cumulative total as of this boundary, so the next call's (getParsedTotal() - lastParsed) "
            + "measures only what the NEW phase parses, not the whole import re-divided by one tick's elapsed time")
        .isEqualTo(7);
    assertThat(context.getParsedTotal()).as("and nothing is lost: the phase's rows join the import-wide total")
        .isEqualTo(7);

    context.parsed.set(3);
    assertThat(context.getParsedTotal()).as("the total is the finished phases plus the one still running")
        .isEqualTo(10);
    assertThat(context.getParsedTotal() - context.lastParsed)
        .as("the rate computation a progress reader does: only the 3 rows the new phase has parsed so far, not the "
            + "cumulative 10")
        .isEqualTo(3);
    assertThat(context.toMap()).containsEntry("parsedRecords", 10L);
  }

  /**
   * The reported shape, and the sharpest expression of it: the SAME two sources in either order.
   * <p>
   * {@code JSONImporterFormat} was the one format that never zeroed the counter, so a run ending in its phase
   * already reported the sum while a run ending in any other reported only that phase. Swapping the two sources
   * therefore changed the answer - six one way, four the other - for an import that parsed the same rows.
   */
  @Test
  void parsedRecordsIsTheWholeRunWhicheverPhaseEndsIt() throws Exception {
    final Path csv = Path.of("target", "importer-7342-phase.csv").toAbsolutePath();
    final Path json = Path.of("target", "importer-7342-phase.json").toAbsolutePath();
    Files.writeString(csv, "id,name\n1,a\n2,b\n3,c\n", StandardCharsets.UTF_8);
    Files.writeString(json, "{\"Rows\":[{\"id\":\"4\",\"name\":\"d\"},{\"id\":\"5\",\"name\":\"e\"}]}",
        StandardCharsets.UTF_8);

    try {
      // FOUR LINES OF CSV - THE HEADER IS PARSED TOO, IT IS ONLY NOT TURNED INTO A RECORD - AND TWO JSON OBJECTS
      final Map<String, Object> csvLast = importTwoPhases("json-then-csv", json, csv);
      final Map<String, Object> jsonLast = importTwoPhases("csv-then-json", csv, json);

      assertThat(csvLast).as("the CSV phase's four lines plus the JSON phase's two objects")
          .containsEntry("parsedRecords", 6L);
      assertThat(jsonLast.get("parsedRecords"))
          .as("and the same two sources in the other order are the same import, so they report the same number")
          .isEqualTo(csvLast.get("parsedRecords"));
    } finally {
      Files.deleteIfExists(csv);
      Files.deleteIfExists(json);
    }
  }

  // -----------------------------------------------------------------------------------------------------------

  /**
   * Runs {@code first} as the {@code -url} phase and {@code second} as the {@code -documents} phase of one
   * {@code Importer.load()}, and returns the report.
   */
  private Map<String, Object> importTwoPhases(final String name, final Path first, final Path second) throws Exception {
    final String databasePath = "target/databases/test-import-7342-" + name;
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      return new Importer(("-url file://" + first + " -documents file://" + second + " -database " + databasePath
          + " -documentType Doc -mapping {'*':[]}").split(" ")).load();
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      TestHelper.checkActiveDatabases();
    }
  }
}
