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
package com.arcadedb.integration.importer.format;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.Source;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7313, finding 2: {@code XMLImporterFormat.load()} compared {@code context.parsed} against
 * {@code -parsingLimitEntries} without ever resetting the counter, and {@code ImporterContext} is created once
 * per {@code Importer.load()} and shared by its four {@code loadFromSource()} phases. A stale offset {@code k}
 * therefore truncated the phase after {@code parsingLimitEntries - k} objects, and once {@code k} reached the
 * limit the phase imported nothing at all while still reporting success.
 * <p>
 * The reset itself has since moved OUT of the format and into {@code Importer.loadFromSource()}, where a format
 * cannot forget it (issue #7342), so the invariant is asserted through the live CLI route below rather than by
 * handing a format a pre-loaded counter - which no longer expresses anything the production path does.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class XMLImporterFormatStaleParsedCounterTest {

  private static final String DB_PATH = "target/databases/xml-importer-stale-parsed-test";

  /**
   * Four objects, so a limit of two is genuinely reached before the source ends.
   */
  private static final String FOUR_ITEMS = """
      <?xml version="1.0" encoding="UTF-8"?>
      <root>
        <item id="1"/>
        <item id="2"/>
        <item id="3"/>
        <item id="4"/>
      </root>""";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  private static Parser xmlParser(final String content) throws Exception {
    final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    final Source source = new Source("test.xml", new ByteArrayInputStream(bytes), bytes.length, false, null, null);
    return new Parser(source, 0);
  }

  private static ImporterSettings settingsWithParsingLimit(final long parsingLimitEntries) {
    final ImporterSettings settings = new ImporterSettings();
    settings.parsingLimitEntries = parsingLimitEntries;
    return settings;
  }

  /**
   * A phase that imports nothing never auto-creates the type either, so an absent type is "zero objects
   * imported" rather than a test-harness error - reported as the count it is, so the assertion message stays
   * the one that explains the defect.
   */
  private static long countOf(final Database db, final String typeName) {
    return db.getSchema().existsType(typeName) ? db.countType(typeName, true) : 0;
  }

  /**
   * So the fix cannot be "ignore the limit": the limit must still stop the parse, and at exactly N objects.
   * <p>
   * Two for a limit of two, where this used to assert three: the check was a strict {@code parsed > limit} taken
   * after the object had already been handed to {@code database.async().createRecord()}, so the object that tripped
   * the limit was imported as well and {@code -parsingLimitEntries N} imported N+1 (issue #7341).
   */
  @Test
  void theParseLimitStopsTheImportAtExactlyTheLimit() throws Exception {
    final XMLImporterFormat format = new XMLImporterFormat();
    final ImporterContext context = new ImporterContext();

    format.load(null, AnalyzedEntity.EntityType.DOCUMENT, xmlParser(FOUR_ITEMS), (DatabaseInternal) database, context,
        settingsWithParsingLimit(2));

    assertThat(countOf(database, "item"))
        .as("-parsingLimitEntries N is a cap of N entries, not a threshold the N+1th crosses")
        .isEqualTo(2);
  }

  /**
   * The live CLI path: two {@code loadFromSource()} phases of one {@code Importer.load()} with
   * {@code -parsingLimitEntries} set, the second of which is the XML source under test. Before the fix the
   * second phase inherited the first one's count, was already past the limit, and imported nothing at all - a
   * silent, successful no-op.
   */
  @Test
  void theTwoPhaseCliRouteScopesTheLimitToTheXmlPhase() throws Exception {
    final Path firstPhase = Path.of("target", "xml-stale-parsed-phase1.xml").toAbsolutePath();
    final Path secondPhase = Path.of("target", "xml-stale-parsed-phase2.xml").toAbsolutePath();
    Files.createDirectories(firstPhase.getParent());
    Files.writeString(firstPhase, FOUR_ITEMS.replace("item", "first"), StandardCharsets.UTF_8);
    Files.writeString(secondPhase, FOUR_ITEMS.replace("item", "second"), StandardCharsets.UTF_8);

    final String cliDbPath = "target/databases/xml-stale-parsed-cli";
    FileUtils.deleteRecursively(new File(cliDbPath));

    try {
      new Importer(("-url file://" + firstPhase + " -documents file://" + secondPhase + " -database " + cliDbPath
          + " -parsingLimitEntries 2").split(" ")).load();

      try (final Database cliDatabase = new DatabaseFactory(cliDbPath).open()) {
        assertThat(countOf(cliDatabase, "v_first"))
            .as("the first phase imports up to its own limit")
            .isEqualTo(2);
        assertThat(countOf(cliDatabase, "second"))
            .as("the second phase gets its own budget of objects rather than inheriting an already-spent one")
            .isEqualTo(2);
      }
    } finally {
      FileUtils.deleteRecursively(new File(cliDbPath));
      Files.deleteIfExists(firstPhase);
      Files.deleteIfExists(secondPhase);
    }
  }
}
