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
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6814: {@code AnalyzedProperty.setLastContent()} treated a value longer than 100 characters as "stop sampling"
 * <b>and</b> as "no evidence about the type", returning before the numeric probes and permanently disabling any
 * further analysis of that column. A column whose first value looked numeric was therefore declared {@code LONG}, and
 * the import then died converting the very value that should have disproved it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6814AnalyzedPropertyLongTextTest {

  /** 150 characters, well past the 100-character sampling cut-off. */
  private static final String LONG_TEXT = "a".repeat(150);

  @Test
  void longValueDisprovesTheNumericCandidates() {
    final AnalyzedProperty property = new AnalyzedProperty("description", Type.STRING, 100, 0);

    property.setLastContent("42");
    property.setLastContent(LONG_TEXT);
    property.endParsing();

    assertThat(property.getType()).isEqualTo(Type.STRING);
  }

  @Test
  void valuesAfterTheSamplingCutOffAreStillAnalyzed() {
    final AnalyzedProperty property = new AnalyzedProperty("description", Type.STRING, 100, 0);

    property.setLastContent("42");
    // STOPS SAMPLE COLLECTION: EVERY VALUE AFTER IT USED TO BE A NO-OP
    property.setLastContent(LONG_TEXT);
    property.setLastContent("not a number");
    property.endParsing();

    assertThat(property.getType()).isEqualTo(Type.STRING);
    assertThat(property.isCollectingSamples()).isFalse();
    assertThat(property.getContents()).isEmpty();
  }

  @Test
  void aGenuinelyNumericColumnIsStillTypedLong() {
    final AnalyzedProperty property = new AnalyzedProperty("age", Type.STRING, 100, 0);

    property.setLastContent("42");
    property.setLastContent("43");
    property.endParsing();

    assertThat(property.getType()).isEqualTo(Type.LONG);
  }

  @Test
  void aGenuinelyDecimalColumnIsStillTypedDouble() {
    final AnalyzedProperty property = new AnalyzedProperty("price", Type.STRING, 100, 0);

    property.setLastContent("10.90");
    property.setLastContent("1985");
    property.endParsing();

    assertThat(property.getType()).isEqualTo(Type.DOUBLE);
  }

  /**
   * End to end: the analyzed type is what {@code AbstractImporter.updateDatabaseSchema()} creates the property with,
   * so the wrong analysis aborted the whole import on the long row with
   * "Cannot convert type 'java.lang.String' to 'LONG'".
   */
  @Test
  void csvWithALongTextColumnWhoseFirstValueLooksNumericImportsAsString() throws IOException {
    final String databasePath = "target/databases/test-import-6814";
    final File csvFile = new File("target/importer-6814-long-text.csv");
    csvFile.getParentFile().mkdirs();
    try (final FileWriter writer = new FileWriter(csvFile)) {
      writer.write("id,description\n");
      writer.write("1,42\n");
      writer.write("2,\"" + LONG_TEXT + "\"\n");
      writer.write("3,not a number\n");
    }

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://" + csvFile.getAbsolutePath());

      assertThat(db.getSchema().getType("Document").getProperty("description").getType()).isEqualTo(Type.STRING);
      assertThat(db.countType("Document", true)).isEqualTo(3);
    } finally {
      db.drop();
      csvFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }
}
