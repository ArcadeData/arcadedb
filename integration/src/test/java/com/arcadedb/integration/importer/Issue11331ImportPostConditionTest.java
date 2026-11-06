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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issues #11331, #11311, #11309: {@code Importer.load()} checked nothing about what it had read, so an import that read nothing
 * answered exactly like an import of an empty source. Every route is pinned here: nothing named, a setting nobody reads, a null
 * schema, an analysis that failed, a source the sniffer misread, a zero-byte source, a page that is not data. What remains
 * legitimately empty answers with explicit zero counters. {@code Issue11311EveryFormatOutcomeTest} (gremlin module) drives the
 * same outcomes through every {@code FormatImporter}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11331ImportPostConditionTest {
  private static final String DB_PATH = "target/databases/test-import-11331";
  private static final String PEOPLE  = "<people><person><name>Ann</name></person><person><name>Bob</name></person></people>";

  private Database database;
  private Path     directory;

  @BeforeEach
  void setup() throws IOException {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    directory = Files.createTempDirectory(Path.of("target"), "importer-11331");
  }

  @AfterEach
  void cleanup() {
    dropDatabase();
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(directory.toFile());
    TestHelper.checkActiveDatabases();
  }

  private void dropDatabase() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
  }

  // ROUTE 1: NOTHING NAMED

  @Test
  void anImportThatNamesNoSourceIsRefused() {
    final Result result = importDatabase("import database with commitEvery = 10");

    assertThat(result.<String>getProperty("result")).isEqualTo("FAIL");
    assertThat(result.<String>getProperty("reason")).contains("url", "documents", "vertices", "edges");
  }

  @Test
  void anImportThatNamesNoSourceCreatesNoDatabase() {
    final String path = DB_PATH + "-never-created";
    FileUtils.deleteRecursively(new File(path));

    assertThatThrownBy(() -> new Importer(new String[] { "-database", path }).load()).isInstanceOf(IllegalArgumentException.class);

    assertThat(new File(path)).doesNotExist();
  }

  @Test
  void aSettingNobodyReadsIsRefusedNamingTheCloseOne() {
    final Result result = importDatabase("import database with vertex = 'x.csv'");

    assertThat(result.<String>getProperty("result")).isEqualTo("FAIL");
    assertThat(result.<String>getProperty("reason")).contains("'vertex'", "vertices");
  }

  @Test
  void aSettingNobodyReadsIsRefusedBesideARealSource() throws IOException {
    final Path csv = write("people.csv", "id,name\n1,Ann\n");

    final Result result = importDatabase("import database file://" + csv + " with edge = 'x.csv'");

    assertThat(result.<String>getProperty("result")).as("the edges the typo meant would be silently missing").isEqualTo("FAIL");
    assertThat(result.<String>getProperty("reason")).contains("'edge'", "edges");
    assertThat(database.getSchema().getTypes()).isEmpty();
  }

  @Test
  void aSettingThatReadsLikeNoOtherIsRefusedListingTheSupportedOnes() {
    final Result result = importDatabase("import database with foo = 'bar'");

    assertThat(result.<String>getProperty("result")).isEqualTo("FAIL");
    assertThat(result.<String>getProperty("reason")).contains("'foo'", "Supported settings", "commitEvery", "vertices");
  }

  @Test
  void theCommandLineRefusesASettingNobodyReadsToo() {
    assertThatThrownBy(() -> new Importer(new String[] { "-url", "file://x.csv", "-vertex", "x.csv" }))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("'vertex'");
  }

  // ROUTE 2: A NULL SCHEMA AND A FAILED ANALYSIS

  @Test
  void aNullSchemaIsRefusedNamingTheSource() throws IOException {
    final Path csv = write("people.csv", "id,name\n1,Ann\n");
    final Importer importer = new Importer(database, "file://" + csv) {
      @Override
      protected SourceDiscovery newSourceDiscovery(final String url) {
        return new SourceDiscovery(url) {
          @Override
          public SourceSchema getSchema(final ImporterSettings settings, final AnalyzedEntity.EntityType entityType,
              final AnalyzedSchema analyzedSchema, final ConsoleLogger logger) {
            return null;
          }
        };
      }
    };

    assertThatThrownBy(importer::load).isInstanceOf(ImportException.class).hasMessageContaining(csv.toString())
        .hasMessageContaining("schema");
  }

  /**
   * The issue's guarantee, with N taken from the source rather than assumed: every fetch of a remote source fails the import, XML
   * and CSV alike. The XML analysis used to turn the failure of its own fetch into a null schema and an empty OK.
   */
  @ParameterizedTest
  @ValueSource(strings = { "people.xml", "people.csv" })
  void aFetchThatFailsAtAnyStepFailsTheImport(final String resource) throws Exception {
    final String body = resource.endsWith(".xml") ?
        "<people>\n<person><name>Ann</name></person>\n<person><name>Bob</name></person>\n</people>\n" :
        "id,name\n1,Ann\n2,Bob\n";

    final Object previous = GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getValue();
    GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(false);

    final AtomicInteger gets = new AtomicInteger();
    final AtomicInteger failingGet = new AtomicInteger(-1);
    final HttpServer origin = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    origin.createContext("/" + resource, exchange -> {
      final boolean fail = gets.incrementAndGet() == failingGet.get();
      final byte[] answer = (fail ? "unavailable" : body).getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(fail ? 503 : 200, answer.length);
      exchange.getResponseBody().write(answer);
      exchange.close();
    });
    origin.start();

    try {
      final String url = "http://127.0.0.1:" + origin.getAddress().getPort() + "/" + resource;

      final Map<String, Object> control = new Importer(database, url).load();
      assertThat(control.get("createdRecords")).as("the source imports when every fetch succeeds").isEqualTo(2L);
      final int fetches = gets.get();
      assertThat(fetches).as("the importer fetches a remote source several times").isGreaterThan(1);

      for (int n = 2; n <= fetches; n++) {
        dropDatabase();
        database = new DatabaseFactory(DB_PATH).create();
        gets.set(0);
        failingGet.set(n);

        final int failing = n;
        assertThatThrownBy(() -> new Importer(database, url).load()).as("fetch %d of %d answers 503", failing, fetches)
            .isInstanceOf(ImportException.class).hasMessageContaining("503");
      }
    } finally {
      origin.stop(0);
      GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(previous);
    }
  }

  // ROUTE 3: A SOURCE THE SNIFFER MISREAD

  @Test
  void aMinifiedXmlDocumentIsXmlWhateverItsExtension() throws IOException {
    assertThat(importFile("people.dat", PEOPLE).<Long>getProperty("createdVertices")).isEqualTo(2L);
  }

  @Test
  void aMinifiedXmlDocumentWithADeclarationIsXmlWhateverItsExtension() throws IOException {
    assertThat(importFile("people.dat", "<?xml version=\"1.0\"?>" + PEOPLE).<Long>getProperty("createdVertices")).isEqualTo(2L);
  }

  @Test
  void aFirstLineOfAMultiLineDocumentIsEnoughToRecogniseXml() throws IOException {
    final String document = "<people><person><name>Ann</name></person>\n<person><name>Bob</name></person></people>\n";

    assertThat(importFile("people.dat", document).<Long>getProperty("createdVertices")).isEqualTo(2L);
  }

  @Test
  void anXmlDocumentOpeningWithAByteOrderMarkIsXmlWhateverItsExtension() throws IOException {
    final String document = "﻿<?xml version=\"1.0\"?>\n<people>\n<person><name>Ann</name></person>\n"
        + "<person><name>Bob</name></person>\n</people>\n";

    final Result result = importFile("people.dat", document);

    assertThat(result.<Long>getProperty("createdVertices")).isEqualTo(2L);
    assertThat(result.<Long>getProperty("createdDocuments")).as("no XML line may become a record of its own").isNull();
  }

  @Test
  void anXmlDocumentOpeningWithWhitespaceIsXmlWhateverItsExtension() throws IOException {
    assertThat(importFile("people.dat", "\n  " + PEOPLE).<Long>getProperty("createdVertices")).isEqualTo(2L);
  }

  /** Markup in the cells of a delimited file: the root closes and text follows, which no XML document does. */
  @Test
  void delimitedTextWithMarkupInItsCellsStaysDelimitedText() throws IOException {
    final Result result = importFile("cells.dat", "<b>id</b>,<i>name</i>,<u>age</u>\n1,Ann,30\n2,Bob,40\n");

    assertThat(result.<Long>getProperty("createdDocuments")).isEqualTo(2L);
  }

  @Test
  void aHeaderOfBracketedColumnNamesStaysDelimitedText() throws IOException {
    final Result result = importFile("columns.dat", "<id>,<name>,<age>,<city>\n1,Ann,30,Rome\n2,Bob,40,Oslo\n");

    assertThat(result.<Long>getProperty("createdDocuments")).isEqualTo(2L);
  }

  /** The boundary of the first-line test, on the shapes either side of it. */
  @Test
  void theFirstLineTestTellsADocumentFromMarkupInCells() {
    assertThat(SourceDiscovery.isXmlFirstLine("<a>x</a>")).isTrue();
    assertThat(SourceDiscovery.isXmlFirstLine("<a><b>x</b><b>y</b></a>")).isTrue();
    assertThat(SourceDiscovery.isXmlFirstLine("<a><b>x</b><b>y</b>")).as("a document that goes on past the line").isTrue();
    assertThat(SourceDiscovery.isXmlFirstLine("<a v=\"1>2\"><b/>x, y</a>")).as("a > inside an attribute value").isTrue();
    assertThat(SourceDiscovery.isXmlFirstLine("<?xml version=\"1.0\"?><a>x, y")).isTrue();

    assertThat(SourceDiscovery.isXmlFirstLine("<a>x</a>,<b>y</b>")).as("text after the root").isFalse();
    assertThat(SourceDiscovery.isXmlFirstLine("<a/><b/>")).as("two roots").isFalse();
    assertThat(SourceDiscovery.isXmlFirstLine("<a>,<b>,<c>")).as("nothing closes").isFalse();
    assertThat(SourceDiscovery.isXmlFirstLine("</a>x")).as("closes what never opened").isFalse();
    assertThat(SourceDiscovery.isXmlFirstLine("<5>x</5>")).as("not an element name").isFalse();
    assertThat(SourceDiscovery.isXmlFirstLine("<>x</>")).as("an empty tag").isFalse();
  }

  // ROUTE 4: AN EMPTY SOURCE IS ONE RULE

  @ParameterizedTest
  @ValueSource(strings = { "empty.csv", "empty.json", "empty.jsonl", "empty.xml", "empty.txt", "empty" })
  void aSourceOfZeroBytesIsRefusedWhateverItsFormat(final String name) throws IOException {
    final Path file = write(name, "");

    assertThatThrownBy(() -> new Importer(database, "file://" + file).load()).isInstanceOf(ImportException.class)
        .hasMessageContaining(file.toString()).hasMessageContaining("is empty");
  }

  @Test
  void aZeroByteSourceIsRefusedForAnExplicitFileTypeToo() throws IOException {
    final Path file = write("empty.dat", "");
    final Importer importer = new Importer(database, "file://" + file);
    importer.setSettings(Map.of("fileType", "csv"));

    assertThatThrownBy(importer::load).isInstanceOf(ImportException.class).hasMessageContaining("is empty");
  }

  /** The url was once mandatory, so scripts name an empty file there and the real ones beside it (issue #2267). */
  @Test
  void aZeroByteSourceBesideARealOneIsSkippedWithAWarning() throws IOException {
    final Path empty = write("empty.csv", "");
    final Path vertices = write("vertices.csv", "id,name\n1,Ann\n");

    final Result result = importDatabase(
        "import database file://" + empty + " with vertices = 'file://" + vertices + "', typeIdProperty = 'id'");

    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<Long>getProperty("createdRecords")).isEqualTo(1L);
    assertThat(result.<List<String>>getProperty("warningMessages")).hasSize(1).first().asString().contains("empty.csv", "is empty");
  }

  // ROUTE 5: A SOURCE THAT IS NOT THE FORMAT IT CLAIMS

  @ParameterizedTest
  @ValueSource(strings = { "<!DOCTYPE html><html><body>Error, try again, later</body></html>\n",
      "<!doctype HTML>\n<html>\n<body>\nError, try again\n</body>\n</html>\n", "<html lang=\"en\"><body>Error, try again</body></html>\n",
      "\n  <html><body>Error, try again</body></html>\n" })
  void anHtmlPageIsNotData(final String page) throws IOException {
    for (final String name : List.of("page.csv", "page.jsonl", "page.dat")) {
      final Path file = write(name, page);

      assertThatThrownBy(() -> new Importer(database, "file://" + file).load()).as(name).isInstanceOf(ImportException.class)
          .hasMessageContaining(file.toString()).hasMessageContaining("HTML");
    }
    assertThat(database.getSchema().getTypes()).isEmpty();
  }

  @Test
  void anXhtmlDocumentAskedForAsXmlIsStillImported() throws IOException {
    final Path file = write("page.xml", "<html>\n<head><title>t</title></head>\n<body><p>hello</p></body>\n</html>\n");

    assertThat(new Importer(database, "file://" + file).load().get("createdRecords")).isEqualTo(2L);
  }

  // A JSON SOURCE HOLDING ONE OBJECT: REFUSED WITH "Transaction not begun", WHICH NAMES NO PROBLEM WITH THE SOURCE

  @Test
  void aSingleTopLevelJsonObjectIsImported() throws IOException {
    final Result result = importFile("record.json", "{\"a\":1}");

    assertThat(result.<Long>getProperty("createdDocuments")).isEqualTo(1L);
    assertThat(database.countType("Document", true)).isEqualTo(1L);
    assertThat(database.isTransactionActive()).as("the transaction of the record is resolved").isFalse();
  }

  @Test
  void aSingleTopLevelJsonObjectJoinsTheTransactionOfTheCaller() throws IOException {
    final Path json = write("record.json", "{\"a\":1}");
    database.command("sql", "CREATE DOCUMENT TYPE Document");

    database.begin();
    new Importer(database, "file://" + json).load();
    assertThat(database.isTransactionActive()).as("the transaction of the caller stays the caller's").isTrue();
    database.rollback();

    assertThat(database.countType("Document", true)).as("and its rollback takes the record back").isZero();
  }

  // AN UNCOMPRESSED ORIENTDB EXPORT: REFUSED WITH A ClassCastException NAMING BufferedInputStream

  @Test
  void anUncompressedOrientDBExportIsRead() throws IOException {
    final Path export = write("export.json", """
        {"info":{"name":"x","default-cluster-id":3,"exporter-version":12,"engine-version":"3.1.13","engine-build":"test",\
        "storage-config-version":23,"schema-version":4,"schemaRecordId":"#0:1","indexMgrRecordId":"#0:2"},\
        "clusters":[{"name":"internal","id":0},{"name":"index","id":1},{"name":"manindex","id":2},{"name":"default","id":3}],\
        "schema":{"version":1,"blob-clusters":[],"classes":[{"name":"Account","default-cluster-id":3,"cluster-ids":[3],\
        "cluster-selection":"round-robin","properties":[]}]},\
        "records":[{"@type":"d","@rid":"#0:1","@version":1},{"@type":"d","@rid":"#0:2","@version":1,"indexes":[]},\
        {"@type":"d","@rid":"#3:0","@class":"Account","@version":1,"code":42}]}""");

    assertThat(new Importer(database, "file://" + export).load().get("createdRecords")).isEqualTo(1L);
    assertThat(database.countType("Account", true)).isEqualTo(1L);
  }

  // WHAT IS LEGITIMATELY EMPTY ANSWERS WITH EXPLICIT ZEROES

  @Test
  void aHeaderOnlyCsvAnswersWithExplicitCounters() throws IOException {
    assertNothingCreated(importFile("header.csv", "id,name\n"), "header.csv", "CSV");
  }

  @Test
  void anEmptyJsonArrayAnswersWithExplicitCounters() throws IOException {
    assertNothingCreated(importFile("records.json", "[]"), "records.json", "JSON");
  }

  @Test
  void aRootOnlyXmlDocumentAnswersWithExplicitCounters() throws IOException {
    assertNothingCreated(importFile("people.xml", "<people/>\n"), "people.xml", "XML");
  }

  @Test
  void onlyTheSourceThatCreatedNothingIsNamed() throws IOException {
    final Path vertices = write("vertices.csv", "id,name\n1,Ann\n");
    final Path edges = write("edges.csv", "from,to\n");

    final Result result = importDatabase(
        "import database with vertices = 'file://" + vertices + "', typeIdProperty = 'id', edges = 'file://" + edges
            + "', edgeFromField = 'from', edgeToField = 'to'");

    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<Long>getProperty("createdRecords")).isEqualTo(1L);
    assertThat(result.<List<String>>getProperty("warningMessages")).hasSize(1).first().asString().contains("edges.csv")
        .doesNotContain("vertices.csv");
  }

  @Test
  void anImportThatCreatedRecordsCarriesTheTotalAndNoWarning() throws IOException {
    final Result result = importFile("people.csv", "id,name\n1,Ann\n2,Bob\n");

    assertThat(result.<Long>getProperty("createdRecords")).isEqualTo(2L);
    assertThat(result.<Long>getProperty("createdDocuments")).isEqualTo(2L);
    assertThat(result.hasProperty("warningMessages")).isFalse();
    assertThat(database.iterateType("Document", true).next().asDocument().getPropertyNames())
        .as("the characters the HTML check read are back for the format: the header is whole").contains("id", "name");
  }

  private void assertNothingCreated(final Result result, final String source, final String format) {
    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<Long>getProperty("createdRecords")).as("the zero is stated, not left to be inferred").isZero();
    assertThat(result.hasProperty("parsedRecords")).isTrue();
    assertThat(result.<Long>getProperty("warnings")).isEqualTo(1L);
    assertThat(result.<List<String>>getProperty("warningMessages")).hasSize(1).first().asString().contains(source, format);
    for (final DocumentType type : database.getSchema().getTypes())
      assertThat(database.countType(type.getName(), true)).isZero();
  }

  private Result importFile(final String name, final String content) throws IOException {
    return importDatabase("import database file://" + write(name, content));
  }

  private Path write(final String name, final String content) throws IOException {
    final Path file = directory.resolve(name).toAbsolutePath();
    Files.writeString(file, content, StandardCharsets.UTF_8);
    return file;
  }

  private Result importDatabase(final String sql) {
    try (final ResultSet resultSet = database.command("sql", sql)) {
      return resultSet.next();
    }
  }
}
