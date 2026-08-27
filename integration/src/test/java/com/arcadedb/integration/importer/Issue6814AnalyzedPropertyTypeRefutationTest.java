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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6814. {@link AnalyzedProperty} used to conflate two unrelated decisions: "stop collecting samples" and
 * "stop looking at values at all". A value longer than the 100-character sampling limit - or simply one value too
 * many - flipped {@code collectingSamples} off and returned <i>before</i> the {@code Long}/{@code Double} probes, so
 * that value never disproved anything and no later value was examined either. A column whose first value happened to
 * look numeric was then declared {@code LONG}, and the import blew up converting the very value the analysis had
 * skipped.
 * <p>
 * Sampling is a memory bound and still stops; type refutation is evidence and must not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6814AnalyzedPropertyTypeRefutationTest {

  /** 120 characters: past the 100-character sampling limit, and the same text the CSV fixture carries. */
  private static final String LONG_TEXT =
      "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua";

  private static AnalyzedProperty newProperty(final long maxValueSampling) {
    return new AnalyzedProperty("description", Type.STRING, maxValueSampling, 0);
  }

  /**
   * The core of the report: "42" makes the column a LONG candidate, the long sentence must refute it. Before the fix
   * the sentence was never probed and the column ended up LONG.
   */
  @Test
  void aValueLongerThanTheSamplingLimitDisprovesLong() {
    final AnalyzedProperty property = newProperty(100);

    property.setLastContent("42");
    property.setLastContent(LONG_TEXT);
    property.endParsing();

    assertThat(property.getType()).isEqualTo(Type.STRING);
  }

  /**
   * The long value stops sample collection but must not stop analysis: every value after it still counts as evidence.
   */
  @Test
  void valuesAfterTheSamplingLimitStillRefuteTheNumericTypes() {
    final AnalyzedProperty property = newProperty(100);

    property.setLastContent("42");
    property.setLastContent(LONG_TEXT);
    assertThat(property.isCollectingSamples()).isFalse();

    // "99" alone would leave the column a LONG candidate: it is the values around it that must still be heard.
    property.setLastContent("99");
    property.setLastContent("not a number");
    property.endParsing();

    assertThat(property.getType()).isEqualTo(Type.STRING);
  }

  /**
   * Refutation is a real probe, not a blanket "long means text": 150 digits overflow a {@code Long} but are a
   * perfectly good {@code Double}, so the column narrows to DOUBLE rather than staying STRING.
   */
  @Test
  void aLongNumericValueNarrowsToDoubleInsteadOfLong() {
    final AnalyzedProperty property = newProperty(100);

    property.setLastContent("1");
    property.setLastContent("1".repeat(150));
    property.endParsing();

    assertThat(property.getType()).isEqualTo(Type.DOUBLE);
  }

  /**
   * The second short-circuit called out in the report: once {@code maxValueSampling} values have been collected,
   * later values used to be ignored too, so a text value arriving after the limit could not undo a LONG verdict.
   */
  @Test
  void valuesArrivingAfterTheSampleCountLimitStillRefuteTheNumericTypes() {
    final AnalyzedProperty property = newProperty(2);

    property.setLastContent("1");
    property.setLastContent("2");
    property.setLastContent("3");
    property.setLastContent("4");
    assertThat(property.isCollectingSamples()).isFalse();

    property.setLastContent("not a number");
    property.endParsing();

    assertThat(property.getType()).isEqualTo(Type.STRING);
  }

  /**
   * Sampling remains bounded: hitting the limit still drops the collected samples and stops adding new ones, so the
   * fix does not trade a type bug for a memory one.
   */
  @Test
  void samplingStillStopsAndDiscardsCollectedValues() {
    final AnalyzedProperty property = newProperty(100);

    property.setLastContent("alpha");
    assertThat(property.getContents()).containsExactly("alpha");

    property.setLastContent(LONG_TEXT);

    assertThat(property.isCollectingSamples()).isFalse();
    assertThat(property.getContents()).isEmpty();

    property.setLastContent("beta");
    assertThat(property.getContents()).isEmpty();
  }

  /** A column made only of long text is still text, and a null value is still no evidence at all. */
  @Test
  void aColumnOfOnlyLongTextIsStringAndNullsAreIgnored() {
    final AnalyzedProperty onlyLongText = newProperty(100);
    onlyLongText.setLastContent(LONG_TEXT);
    onlyLongText.endParsing();
    assertThat(onlyLongText.getType()).isEqualTo(Type.STRING);

    final AnalyzedProperty onlyNulls = newProperty(100);
    onlyNulls.setLastContent(null);
    onlyNulls.endParsing();
    assertThat(onlyNulls.getType()).isEqualTo(Type.STRING);
  }

  /** The happy paths the analysis exists for must be untouched. */
  @Test
  void purelyNumericColumnsAreStillTypedNumeric() {
    final AnalyzedProperty integers = newProperty(100);
    integers.setLastContent("1");
    integers.setLastContent("");
    integers.setLastContent("2");
    integers.endParsing();
    assertThat(integers.getType()).isEqualTo(Type.LONG);

    final AnalyzedProperty decimals = newProperty(100);
    decimals.setLastContent("1");
    decimals.setLastContent("2.5");
    decimals.endParsing();
    assertThat(decimals.getType()).isEqualTo(Type.DOUBLE);
  }

  /**
   * End to end: the CSV fixture is exactly the report's repro - a numeric-looking first value, then a sentence past
   * the sampling limit, then another numeric value. The schema property must be created as STRING and the import must
   * carry every row, instead of failing with "Cannot convert type 'java.lang.String' to 'LONG'".
   */
  @Test
  void csvImportKeepsALongTextColumnAsString() {
    final String databasePath = "target/databases/test-import-6814-long-text";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);

    try {
      final Importer importer = new Importer(
          ("-documents src/test/resources/importer-documents-long-text.csv -database " + databasePath
              + " -documentType Doc -o").split(" "));

      // toMap() only reports non-zero counters, so a clean run carries no "errors" key at all.
      final Map<String, Object> result = importer.load();
      assertThat(result).doesNotContainKey("errors");
      assertThat(result.get("createdDocuments")).isEqualTo(3L);

      try (final Database db = databaseFactory.open()) {
        assertThat(db.getSchema().getType("Doc").getProperty("description").getType()).isEqualTo(Type.STRING);

        final List<String> descriptions = new ArrayList<>();
        try (final ResultSet resultSet = db.query("sql", "select description from Doc")) {
          resultSet.forEachRemaining(row -> descriptions.add(row.getProperty("description")));
        }
        assertThat(descriptions).containsExactlyInAnyOrder("42", LONG_TEXT, "99");
      }
    } finally {
      databaseFactory.open().drop();
    }
  }
}
