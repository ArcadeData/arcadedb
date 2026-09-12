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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7341: {@code XMLImporterFormat} measured {@code -parsingLimitEntries} with a strict {@code >} against a
 * counter incremented once per COMPLETED object, and the check sits at the bottom of the event loop - after the
 * record has already been handed to {@code database.async().createRecord()}. The loop therefore broke only once
 * the count had PASSED the limit, and the object that tripped it was imported too: {@code -parsingLimitEntries N}
 * imported N+1 entries.
 * <p>
 * {@code -parsingLimitEntries} reads as "import at most N entries", and the same CLI flag on the vector route
 * ({@code TextEmbeddingsImporterLSM}, through {@code parser.limit()}) has always stopped AT the limit, so one flag
 * meant N on one route and N+1 on the other.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7341ParsingLimitIsACapTest {

  private static final int OBJECTS = 6;

  /**
   * The issue's own table: four objects, a limit of two, three imported.
   */
  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3, 5 })
  void parsingLimitEntriesImportsExactlyThatManyObjects(final int limit) throws Exception {
    assertThat(importWithLimit("limit-" + limit, "-parsingLimitEntries " + limit))
        .as("-parsingLimitEntries %d is a cap of %d entries, not a threshold the %dth crosses", limit, limit, limit + 1)
        .isEqualTo(limit);
  }

  /**
   * A limit at or above the source's size is not a limit: every object is imported, and the loop ends because the
   * source does. Pins the boundary from the other side, where an over-eager {@code >=} would cost a real object.
   */
  @ParameterizedTest
  @ValueSource(ints = { 6, 7, 100 })
  void aLimitTheSourceNeverReachesImportsEverything(final int limit) throws Exception {
    assertThat(importWithLimit("wide-" + limit, "-parsingLimitEntries " + limit)).isEqualTo(OBJECTS);
  }

  /**
   * And no limit at all still means no limit: zero is the "unset" value the check gates on.
   */
  @Test
  void noLimitImportsEverything() throws Exception {
    assertThat(importWithLimit("unlimited", "")).isEqualTo(OBJECTS);
  }

  /**
   * The sibling check in {@code analyze()}, against {@code -analyzingLimitEntries}, which had the same shape and
   * the same off-by-one - and which no test in the tree exercised at all.
   * <p>
   * Observed through the schema the analysis produces: each object of the source carries a property of its own, so
   * the properties discovered ARE the objects analyzed. A limit of N must discover exactly N of them.
   */
  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3, 5 })
  void analyzingLimitEntriesAnalyzesExactlyThatManyObjects(final int limit) throws Exception {
    final AnalyzedSchema analyzedSchema = new AnalyzedSchema(100);
    final ImporterSettings settings = new ImporterSettings();
    settings.options.put("analyzingLimitEntries", String.valueOf(limit));

    new XMLImporterFormat().analyze(AnalyzedEntity.EntityType.DOCUMENT, parserOf(distinctlyPropertiedXml()), settings,
        analyzedSchema);

    assertThat(analyzedSchema.getEntity("item").getProperties())
        .as("-analyzingLimitEntries %d analyzes %d objects, so it discovers the %d properties they carry between "
            + "them - not the %dth object's as well", limit, limit, limit, limit + 1)
        .hasSize(limit);
  }

  /**
   * And with no limit the analysis still sees the whole source, so the {@code >=} cannot have started cutting one
   * object short of the end.
   */
  @Test
  void noAnalyzingLimitAnalyzesEveryObject() throws Exception {
    final AnalyzedSchema analyzedSchema = new AnalyzedSchema(100);

    new XMLImporterFormat().analyze(AnalyzedEntity.EntityType.DOCUMENT, parserOf(distinctlyPropertiedXml()),
        new ImporterSettings(), analyzedSchema);

    assertThat(analyzedSchema.getEntity("item").getProperties()).hasSize(OBJECTS);
  }

  // -----------------------------------------------------------------------------------------------------------

  /**
   * {@value #OBJECTS} objects, the kth carrying a property {@code p<k>} and no other: the set of properties the
   * analysis ends up with is a direct read-out of how many objects it looked at.
   */
  private static String distinctlyPropertiedXml() {
    final StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<root>\n");
    for (int i = 1; i <= OBJECTS; ++i)
      xml.append("  <item p").append(i).append("=\"v\"/>\n");
    return xml.append("</root>").toString();
  }

  private static Parser parserOf(final String content) throws Exception {
    final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    return new Parser(new Source("test.xml", new ByteArrayInputStream(bytes), bytes.length, false, null, null), 0);
  }

  /**
   * Imports a {@value #OBJECTS}-object XML source through the live CLI path and returns how many records landed.
   */
  private long importWithLimit(final String name, final String extraArgs) throws Exception {
    final StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<root>\n");
    for (int i = 1; i <= OBJECTS; ++i)
      xml.append("  <item id=\"").append(i).append("\"/>\n");
    xml.append("</root>");

    final File file = new File("target/importer-7341-" + name + ".xml");
    Files.writeString(file.toPath(), xml.toString(), StandardCharsets.UTF_8);

    final String databasePath = "target/databases/test-import-7341-" + name;
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      final Map<String, Object> report = new Importer(
          ("-url file://" + file.getAbsolutePath() + " -database " + databasePath + " " + extraArgs).trim().split(" +"))
          .load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        final long imported = db.getSchema().existsType("v_item") ? db.countType("v_item", true) : 0;
        assertThat(report.getOrDefault("parsedRecords", 0L))
            .as("the report and the database agree on how many objects the import took")
            .isEqualTo(imported);
        return imported;
      }
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      file.delete();
      TestHelper.checkActiveDatabases();
    }
  }
}
