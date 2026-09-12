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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7482: {@code -parsingLimitEntries} was read by {@code ImporterSettings.parseParameter} and echoed back in
 * {@code SourceDiscovery}'s recognition log line, but only {@code XMLImporterFormat} (after issue #7341) and the
 * vector route ({@code TextEmbeddingsImporterLSM}) actually enforced it. Every other {@code FormatImporter} -
 * {@code CSVImporterFormat}, {@code JSONImporterFormat}, {@code JsonlImporterFormat}, {@code RDFImporterFormat} -
 * imported the whole source while logging the limit as if it were in effect.
 * <p>
 * {@code -parsingLimitBytes} was worse: parsed, stored and logged, but enforced by NOTHING - {@code Parser}'s own
 * byte-limit machinery exists but every construction site passed {@code 0}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7482ParsingLimitAppliesToAllFormatsTest {

  private static final int ROWS = 6;

  @Test
  void csvDocumentsHonourParsingLimitEntries() throws Exception {
    final File csv = writeFile("importer-7482-csv.csv", csvOf(ROWS));
    final String databasePath = "target/databases/test-import-7482-csv";
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      // context.parsed COUNTS EVERY ROW read() HANDS BACK, THE HEADER INCLUDED (THE SAME COUNTER RDFImporterFormat's
      // OWN COMMENT DESCRIBES): 1 HEADER ROW + 2 DATA ROWS = A LIMIT OF 3 TO GET EXACTLY 2 DOCUMENTS.
      new Importer(("-url file://" + csv.getAbsolutePath() + " -database " + databasePath + " -parsingLimitEntries 3")
          .split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("Document", true))
            .as("-parsingLimitEntries 3 must cap a CSV document import at 2 data rows (plus the 1 header row it also "
                + "counts), not import the whole file")
            .isEqualTo(2);
      }
    } finally {
      dropDatabase(databasePath);
      csv.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  @Test
  void jsonlHonoursParsingLimitEntries() throws Exception {
    final StringBuilder jsonl = new StringBuilder();
    for (int i = 0; i < ROWS; i++)
      jsonl.append("{\"t\":\"d\",\"c\":{\"t\":\"Doc\",\"r\":\"#1:").append(i).append("\",\"p\":{\"n\":\"v").append(i)
          .append("\"}}}\n");
    final File file = writeFile("importer-7482-jsonl.jsonl", jsonl.toString());
    final String databasePath = "target/databases/test-import-7482-jsonl";
    FileUtils.deleteRecursively(new File(databasePath));

    try (final Database setup = new DatabaseFactory(databasePath).create()) {
      setup.transaction(() -> setup.getSchema().createDocumentType("Doc"));
    }

    try {
      new Importer(
          ("-url file://" + file.getAbsolutePath() + " -database " + databasePath + " -parsingLimitEntries 2").split(" "))
          .load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.query("sql", "select count(*) as c from Doc").next().<Long>getProperty("c"))
            .as("-parsingLimitEntries 2 must cap a jsonl import at 2 records")
            .isEqualTo(2L);
      }
    } finally {
      dropDatabase(databasePath);
      file.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * Jsonl got the identical {@code parsingLimitBytes} guard as CSV/RDF, checked at the top of its {@code while}
   * loop before the {@code -onRowError skip} {@code continue} - but only CSV's own row loop had an end-to-end test
   * exercising the guard. This pins the same claim for Jsonl's.
   */
  @Test
  void jsonlHonoursParsingLimitBytes() throws Exception {
    final int rows = 500;
    final StringBuilder jsonl = new StringBuilder();
    for (int i = 0; i < rows; i++)
      jsonl.append("{\"t\":\"d\",\"c\":{\"t\":\"Doc\",\"r\":\"#1:").append(i).append("\",\"p\":{\"n\":\"v").append(i)
          .append("\"}}}\n");
    final File file = writeFile("importer-7482-jsonl-bytes.jsonl", jsonl.toString());
    final String databasePath = "target/databases/test-import-7482-jsonl-bytes";
    FileUtils.deleteRecursively(new File(databasePath));

    try (final Database setup = new DatabaseFactory(databasePath).create()) {
      setup.transaction(() -> setup.getSchema().createDocumentType("Doc"));
    }

    try {
      new Importer(("-url file://" + file.getAbsolutePath() + " -database " + databasePath + " -parsingLimitBytes 200")
          .split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        final long imported = db.query("sql", "select count(*) as c from Doc").next().<Long>getProperty("c");
        assertThat(imported).as("a 200-byte budget on a source many times larger must stop the import short of the end")
            .isLessThan(rows);
      }
    } finally {
      dropDatabase(databasePath);
      file.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * The array-based JSON format needed the byte limit threaded through two more call levels than the others, and
   * the array has to be fully drained (via {@code JsonReader#skipValue()}) once the limit trips, or
   * {@code reader.endArray()} throws: this pins that the limit stops importing AND that the reader is left in a
   * state {@code endArray()} accepts.
   */
  @Test
  void jsonArrayHonoursParsingLimitEntriesWithoutBreakingTheReader() throws Exception {
    final StringBuilder json = new StringBuilder("{\"Users\":[");
    for (int i = 0; i < ROWS; i++) {
      if (i > 0)
        json.append(',');
      json.append("{\"id\":\"").append(i).append("\"}");
    }
    json.append("]}");
    final File file = writeFile("importer-7482-json.json", json.toString());
    final String databasePath = "target/databases/test-import-7482-json";
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      new Importer(("-url file://" + file.getAbsolutePath() + " -database " + databasePath
          + " -forceDatabaseCreate true -mapping {'Users':[]} -parsingLimitEntries 2").split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("Document", true))
            .as("-parsingLimitEntries 2 must cap a JSON array import at 2 records, and the reader must still close "
                + "cleanly (no unread array elements left for reader.endArray() to choke on)")
            .isEqualTo(2);
      }
    } finally {
      dropDatabase(databasePath);
      file.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * A record with a nested object used to cost the entries budget twice: {@code parseRecord()} increments
   * {@code context.parsed} for every object it recurses into too - a nested object property, or a nested object
   * that is itself an array entry via {@code parseArray()} - so a record with one nested object advanced
   * {@code context.parsed} by 2, not 1, and the JSON array cap (originally checked against {@code context.parsed})
   * tripped after fewer TOP-LEVEL records than requested. The cap now checks {@code recordIndex}, which
   * {@code parseRecordsArray()} increments exactly once per top-level array object.
   */
  @Test
  void jsonArrayParsingLimitCountsTopLevelRecordsNotNestedObjects() throws Exception {
    final StringBuilder json = new StringBuilder("{\"Users\":[");
    for (int i = 0; i < ROWS; i++) {
      if (i > 0)
        json.append(',');
      json.append("{\"id\":\"").append(i).append("\",\"address\":{\"city\":\"city-").append(i).append("\"}}");
    }
    json.append("]}");
    final File file = writeFile("importer-7482-json-nested.json", json.toString());
    final String databasePath = "target/databases/test-import-7482-json-nested";
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      new Importer(("-url file://" + file.getAbsolutePath() + " -database " + databasePath
          + " -forceDatabaseCreate true -mapping {'Users':[]} -parsingLimitEntries 2").split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("Document", true))
            .as("each of the 6 records carries one nested object, so counting nested objects toward the cap would "
                + "trip it after 1 top-level record instead of 2")
            .isEqualTo(2);
      }
    } finally {
      dropDatabase(databasePath);
      file.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * {@code -parsingLimitBytes} used to be pure decoration: parsed, logged, never enforced anywhere. Univocity's own
   * internal buffering means the exact row where the cap fires cannot be pinned without depending on its buffer
   * size, so this only pins that a small byte budget on a source many times its size stops the import well short
   * of the end - which no test before this issue could have failed, because nothing checked the flag at all.
   * <p>
   * Not asserted as "at least one row lands": the byte check now runs before the header-row skip too (see
   * {@link #csvParsingLimitBytesStopsTheImportEvenWhileSkippingRows()}), and Univocity's own internal buffering can
   * legitimately push {@code parser.getPosition()} past a budget this small before the very first row is even
   * handed back, importing zero rows - which is the check doing its job, not a defect.
   */
  @Test
  void csvDocumentsHonourParsingLimitBytes() throws Exception {
    final int rows = 500;
    final File csv = writeFile("importer-7482-csv-bytes.csv", csvOf(rows));
    final String databasePath = "target/databases/test-import-7482-csv-bytes";
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      new Importer(("-url file://" + csv.getAbsolutePath() + " -database " + databasePath + " -parsingLimitBytes 200")
          .split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        final long imported = db.getSchema().existsType("Document") ? db.countType("Document", true) : 0;
        assertThat(imported).as("a 200-byte budget on a source many times larger must stop the import short of the end")
            .isLessThan(rows);
      }
    } finally {
      dropDatabase(databasePath);
      csv.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * The byte-limit check has to run BEFORE the skip-row {@code continue} that {@code -documentsSkipEntries} takes,
   * not only after a row is actually processed: checked only at the bottom of the loop, a long run of skipped rows
   * would keep reading (and re-checking nothing) for as long as skipping continued, so a tiny byte budget entirely
   * inside the skipped block would not stop the import until the skip block ended - importing every row after it
   * regardless of the budget. With the fix, the loop breaks WHILE still skipping, before a single row is imported.
   */
  @Test
  void csvParsingLimitBytesStopsTheImportEvenWhileSkippingRows() throws Exception {
    final int rows = 100;
    final int skipRows = 80;
    final File csv = writeFile("importer-7482-csv-bytes-skip.csv", csvOf(rows));
    final String databasePath = "target/databases/test-import-7482-csv-bytes-skip";
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      // -documentsSkipEntries 80: THE FIRST 80 (OUT OF 101, HEADER INCLUDED) ROWS ARE SKIPPED VIA THE LOOP'S
      // 'continue'. A 50-BYTE BUDGET LANDS WELL INSIDE THAT SKIPPED PREFIX.
      new Importer(("-url file://" + csv.getAbsolutePath() + " -database " + databasePath
          + " -documentsSkipEntries " + skipRows + " -parsingLimitBytes 50").split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.getSchema().existsType("Document") ? db.countType("Document", true) : 0)
            .as("a 50-byte budget entirely inside the 80-row skipped prefix must stop the import before any of the "
                + "20 rows after the skip block are ever reached, not after the whole skipped prefix is read")
            .isZero();
      }
    } finally {
      dropDatabase(databasePath);
      csv.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * RDF got the identical {@code parsingLimitBytes} guard as CSV, but only CSV had an end-to-end test exercising
   * it; this pins the same claim for RDF's own row loop.
   */
  @Test
  void rdfHonoursParsingLimitBytes() throws Exception {
    final int triples = 500;
    final StringBuilder content = new StringBuilder(triples * 40);
    for (int i = 1; i <= triples; ++i)
      content.append("<http://a/s").append(i).append("> <http://a/rel> <http://a/o").append(i).append("> .\n");
    final File file = writeFile("importer-7482-rdf-bytes.nt", content.toString());
    final String databasePath = "target/databases/test-import-7482-rdf-bytes";
    FileUtils.deleteRecursively(new File(databasePath));

    // THE VERTEX TYPE RDFImporterFormat RESOLVES SUBJECTS/OBJECTS AGAINST: AN RDF SOURCE'S OWN ANALYSIS ONLY
    // REGISTERS THE EDGE TYPE, THE SAME SETUP EVERY OTHER RDFImporterFormat CLI TEST USES.
    try (final Database seed = new DatabaseFactory(databasePath).create()) {
      seed.transaction(() -> {
        seed.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
        seed.getSchema().getType("Node").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { "id" });
        seed.getSchema().createEdgeType("Related");
      });
    }

    try {
      new Importer(("-url file://" + file.getAbsolutePath() + " -database " + databasePath + " -edgeType Related"
          + " -parsingLimitBytes 200").split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("Related", true))
            .as("a 200-byte budget on a source many times larger must stop the import short of the end")
            .isLessThan(triples);
      }
    } finally {
      dropDatabase(databasePath);
      file.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * XMLImporterFormat.load() got its own {@code parsingLimitBytes} guard measured via
   * {@code xmlReader.getLocation().getCharacterOffset()} (unlike every other format, to stay precise despite
   * {@code XMLStreamReader}'s own internal buffering - see the comment at the guard). Only {@code analyze()}'s
   * matching guard had an end-to-end test before this; this pins {@code load()}'s.
   */
  @Test
  void xmlLoadHonoursParsingLimitBytes() throws Exception {
    final StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<root>\n");
    for (int i = 1; i <= ROWS; ++i)
      xml.append("  <item id=\"").append(i).append("\"/>\n");
    xml.append("</root>");
    final File file = writeFile("importer-7482-xml-bytes.xml", xml.toString());
    final String databasePath = "target/databases/test-import-7482-xml-bytes";
    FileUtils.deleteRecursively(new File(databasePath));

    // A BUDGET LANDING RIGHT AFTER THE SECOND <item>: xmlReader.getLocation().getCharacterOffset() IS THE PARSER'S
    // OWN LOGICAL POSITION, NOT A BUFFER-SIZE-GRANULAR ONE, SO (UNLIKE THE CSV/RDF/Jsonl BYTE TESTS ABOVE) THE
    // EXACT STOPPING POINT CAN BE PINNED HERE.
    final int budget = xml.indexOf("<item id=\"3");

    try {
      new Importer(
          ("-url file://" + file.getAbsolutePath() + " -database " + databasePath + " -parsingLimitBytes " + budget)
              .split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("v_item", true))
            .as("the budget lands right after the 2nd item's closing tag, so exactly 2 objects should have been "
                + "handed to createRecord() before the guard fired")
            .isEqualTo(2);
      }
    } finally {
      dropDatabase(databasePath);
      file.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  // -----------------------------------------------------------------------------------------------------------

  private static String csvOf(final int rows) {
    final StringBuilder csv = new StringBuilder("id,name\n");
    for (int i = 0; i < rows; i++)
      csv.append(i).append(",name-").append(i).append('\n');
    return csv.toString();
  }

  private static File writeFile(final String fileName, final String content) throws Exception {
    final File file = new File("target/" + fileName);
    file.getParentFile().mkdirs();
    Files.writeString(file.toPath(), content, StandardCharsets.UTF_8);
    return file;
  }

  private static void dropDatabase(final String databasePath) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
  }
}
