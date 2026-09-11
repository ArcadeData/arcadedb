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
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.Source;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.schema.DocumentType;
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
 * Issue #7341: both object limits in {@link XMLImporterFormat} were a strict {@code >} evaluated at the bottom of the
 * parse loop, against a counter the same iteration had already incremented. For a limit of N the counter reached N
 * after the Nth object without tripping the test, so the loop ran once more and the (N+1)th object was created - in
 * {@code load()} below {@code database.async().createRecord(...)}, so it was imported - before {@code N+1 > N} finally
 * broke out.
 * <p>
 * {@code -parsingLimitEntries N} reads as "import at most N entries", and the vector route applies the very same
 * setting through {@code Stream.limit(N)} ({@code TextEmbeddingsImporterLSM:252}), which stops <i>at</i> N. These tests
 * pin both XML limits to the same cap semantics.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class XMLImporterFormatParsingLimitTest {

  private static final String DB_PATH = "target/databases/xml-importer-parsing-limit-test";

  /**
   * Four objects, so a limit of two leaves two objects unread: enough to tell "stopped at 2" from "stopped at 3" from
   * "never stopped".
   */
  private static final String FOUR_ITEMS = """
      <?xml version="1.0" encoding="UTF-8"?>
      <root>
        <item id="1"/>
        <item id="2"/>
        <item id="3"/>
        <item id="4"/>
      </root>""";

  /**
   * The third object carries a property the first two do not. Whether the analysed schema ends up with that property is
   * a direct, observable answer to "did the analyser look at the third object?" - which is what
   * {@code -analyzingLimitEntries 2} is supposed to forbid.
   */
  private static final String THIRD_ITEM_HAS_AN_EXTRA_PROPERTY = """
      <?xml version="1.0" encoding="UTF-8"?>
      <root>
        <item id="1"/>
        <item id="2"/>
        <item id="3" onlyInTheThirdObject="x"/>
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

  private static long countOf(final Database db, final String typeName) {
    return db.getSchema().existsType(typeName) ? db.countType(typeName, true) : 0;
  }

  /**
   * The reported case: {@code -parsingLimitEntries 2} on a four-object source must import two objects, not three.
   */
  @Test
  void theParsingLimitIsACapNotAThreshold() throws Exception {
    final ImporterSettings settings = new ImporterSettings();
    settings.parsingLimitEntries = 2;

    final ImporterContext context = new ImporterContext();

    new XMLImporterFormat().load(null, AnalyzedEntity.EntityType.DOCUMENT, xmlParser(FOUR_ITEMS), (DatabaseInternal) database,
        context, settings);

    assertThat(countOf(database, "item"))
        .as("-parsingLimitEntries 2 imports at most 2 objects: the object that reaches the limit is the last one kept, "
            + "not the first one past it")
        .isEqualTo(2);
    assertThat(context.parsed.get())
        .as("the reported parsed count stops at the limit too")
        .isEqualTo(2);
  }

  /**
   * A limit of one is the tightest non-zero cap and the one where an off-by-one doubles the import.
   */
  @Test
  void aLimitOfOneImportsExactlyOneObject() throws Exception {
    final ImporterSettings settings = new ImporterSettings();
    settings.parsingLimitEntries = 1;

    new XMLImporterFormat().load(null, AnalyzedEntity.EntityType.DOCUMENT, xmlParser(FOUR_ITEMS), (DatabaseInternal) database,
        new ImporterContext(), settings);

    assertThat(countOf(database, "item")).isEqualTo(1);
  }

  /**
   * The other direction, so the fix cannot be "stop one object early everywhere": a limit that the source never reaches
   * must not truncate anything, and a limit equal to the object count must keep every object.
   */
  @Test
  void aLimitAtOrAboveTheObjectCountImportsEverything() throws Exception {
    final ImporterSettings exactly = new ImporterSettings();
    exactly.parsingLimitEntries = 4;

    new XMLImporterFormat().load(null, AnalyzedEntity.EntityType.DOCUMENT, xmlParser(FOUR_ITEMS), (DatabaseInternal) database,
        new ImporterContext(), exactly);

    assertThat(countOf(database, "item"))
        .as("a limit equal to the object count is not a reason to drop the last object")
        .isEqualTo(4);

    database.getSchema().dropType("item");

    final ImporterSettings unreached = new ImporterSettings();
    unreached.parsingLimitEntries = 10;

    new XMLImporterFormat().load(null, AnalyzedEntity.EntityType.DOCUMENT, xmlParser(FOUR_ITEMS), (DatabaseInternal) database,
        new ImporterContext(), unreached);

    assertThat(countOf(database, "item"))
        .as("a limit the source never reaches imports the whole source")
        .isEqualTo(4);
  }

  /**
   * {@code -parsingLimitEntries 0} means "no limit": the guard is {@code parsingLimitEntries > 0}, and tightening the
   * comparison must not turn the disabled case into "stop immediately".
   */
  @Test
  void aZeroLimitMeansNoLimit() throws Exception {
    final ImporterSettings settings = new ImporterSettings();
    settings.parsingLimitEntries = 0;

    new XMLImporterFormat().load(null, AnalyzedEntity.EntityType.DOCUMENT, xmlParser(FOUR_ITEMS), (DatabaseInternal) database,
        new ImporterContext(), settings);

    assertThat(countOf(database, "item")).isEqualTo(4);
  }

  /**
   * The analyse half of the pair ({@code XMLImporterFormat.analyze()}), reached in production from
   * {@code SourceDiscovery.getSchema()}. The third object is the first one carrying {@code onlyInTheThirdObject}, so
   * that property appearing in the analysed schema is proof the analyser read a third object under a limit of two.
   */
  @Test
  void theAnalyzingLimitIsACapNotAThreshold() throws Exception {
    final ImporterSettings settings = new ImporterSettings();
    settings.parseParameter("analyzingLimitEntries", "2");

    final AnalyzedSchema analyzedSchema = new AnalyzedSchema(100);

    final SourceSchema sourceSchema = new XMLImporterFormat().analyze(AnalyzedEntity.EntityType.DOCUMENT,
        xmlParser(THIRD_ITEM_HAS_AN_EXTRA_PROPERTY), settings, analyzedSchema);

    assertThat(sourceSchema).isNotNull();

    final AnalyzedEntity entity = analyzedSchema.getEntity("item");
    assertThat(entity).as("the first two objects are analysed").isNotNull();
    assertThat(entity.getProperty("id")).as("the first two objects are analysed").isNotNull();
    assertThat(entity.getProperty("onlyInTheThirdObject"))
        .as("-analyzingLimitEntries 2 stops after the second object, so the third object's properties never reach the "
            + "analysed schema")
        .isNull();
  }

  /**
   * The live CLI route, end to end: {@code Importer} builds the settings, {@code SourceDiscovery} runs
   * {@code analyze()} and {@code Importer.loadFromSource()} runs {@code load()}. Nothing here reaches into
   * {@code XMLImporterFormat} directly, so it is the proof that the changed lines run outside the unit tests.
   */
  @Test
  void theCliRouteAppliesTheLimitAsACap() throws Exception {
    final Path source = Path.of("target", "xml-parsing-limit-cli.xml").toAbsolutePath();
    Files.createDirectories(source.getParent());
    Files.writeString(source, FOUR_ITEMS, StandardCharsets.UTF_8);

    final String cliDbPath = "target/databases/xml-parsing-limit-cli";
    FileUtils.deleteRecursively(new File(cliDbPath));

    try {
      new Importer(("-documents file://" + source + " -database " + cliDbPath + " -parsingLimitEntries 2").split(" ")).load();

      try (final Database cliDatabase = new DatabaseFactory(cliDbPath).open()) {
        assertThat(countOf(cliDatabase, "item"))
            .as("-parsingLimitEntries 2 on the command line imports 2 objects")
            .isEqualTo(2);
      }
    } finally {
      FileUtils.deleteRecursively(new File(cliDbPath));
      Files.deleteIfExists(source);
    }
  }

  /**
   * The analyse limit through its live caller rather than through a direct {@code analyze()} call:
   * {@code Importer.load()} runs {@code SourceDiscovery.getSchema()} -> {@code analyze()} and then feeds the analysed
   * schema to {@code AbstractImporter.updateDatabaseSchema()}, which declares one property per analysed property
   * ({@code type.createProperty(...)}). A declared {@code onlyInTheThirdObject} on the created type is therefore proof
   * that the analyser reached a third object under {@code -analyzingLimitEntries 2} - the schema is the only place
   * that shows it, since the record the property came from carries it whether or not the type declares it.
   */
  @Test
  void theCliRouteAppliesTheAnalyzingLimitAsACap() throws Exception {
    final Path source = Path.of("target", "xml-analyzing-limit-cli.xml").toAbsolutePath();
    Files.createDirectories(source.getParent());
    Files.writeString(source, THIRD_ITEM_HAS_AN_EXTRA_PROPERTY, StandardCharsets.UTF_8);

    final String cliDbPath = "target/databases/xml-analyzing-limit-cli";
    FileUtils.deleteRecursively(new File(cliDbPath));

    try {
      new Importer(("-documents file://" + source + " -database " + cliDbPath + " -analyzingLimitEntries 2").split(" ")).load();

      try (final Database cliDatabase = new DatabaseFactory(cliDbPath).open()) {
        assertThat(cliDatabase.getSchema().existsType("item")).isTrue();

        final DocumentType type = cliDatabase.getSchema().getType("item");
        assertThat(type.existsProperty("id"))
            .as("the objects within the limit are analysed, so their properties are declared")
            .isTrue();
        assertThat(type.existsProperty("onlyInTheThirdObject"))
            .as("-analyzingLimitEntries 2 on the command line stops the analyser after the second object, so the third "
                + "object's property never becomes part of the schema")
            .isFalse();
      }
    } finally {
      FileUtils.deleteRecursively(new File(cliDbPath));
      Files.deleteIfExists(source);
    }
  }
}
