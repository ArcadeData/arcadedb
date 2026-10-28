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
import com.arcadedb.database.Document;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10754: a JSON document whose top level is an array was detected as CSV - the sniffer knew only <code>{</code> - so the
 * whole array became one CSV header row and the import created nothing and reported success. The pretty-printed form failed
 * with "If it is a CSV file, please specify the header", and forcing JSON failed with "Expected BEGIN_OBJECT but was
 * BEGIN_ARRAY", because the mapping-less JSON route read only a single top-level object.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10754JsonArrayDetectionTest {
  private static final String ONE_LINE = "[{\"id\":1,\"count\":42,\"bigId\":9007199254740993,\"price\":1.50},"
      + "{\"id\":2,\"count\":7,\"bigId\":9007199254740992,\"price\":2.25}]";

  private static final String PRETTY = """
      [
        {"id": 1, "count": 42, "bigId": 9007199254740993, "price": 1.50},
        {"id": 2, "count": 7, "bigId": 9007199254740992, "price": 2.25}
      ]
      """;

  @Test
  void oneLineArrayIsImportedAsJson() throws Exception {
    assertTwoRecords(importSource("importer-10754-nums.json", ONE_LINE, ""));
  }

  @Test
  void prettyPrintedArrayIsImportedAsJson() throws Exception {
    assertTwoRecords(importSource("importer-10754-pretty.json", PRETTY, ""));
  }

  /** Detection is by content: the extension must not be what saves it. */
  @Test
  void arrayIsRecognisedWithoutTheExtension() throws Exception {
    assertTwoRecords(importSource("importer-10754-nums.txt", ONE_LINE, ""));
  }

  /** A JSON document opening with whitespace was missed by the first-character dispatch, for an object as for an array. */
  @Test
  void leadingWhitespaceBeforeJsonIsSkipped() throws Exception {
    assertTwoRecords(importSource("importer-10754-ws-array.txt", "\n\n  \t" + PRETTY, ""));

    final String databasePath = importSource("importer-10754-ws-object.txt", "\n   {\"id\": 1, \"name\": \"one\"}\n", "");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.countType("Document", true)).isEqualTo(1);
    } finally {
      dropDatabase(databasePath);
    }
  }

  /** A leading comment block, then a blank line: the comments are skipped, then the whitespace, before the dispatch. */
  @Test
  void commentBlockThenBlankLineBeforeArray() throws Exception {
    assertTwoRecords(importSource("importer-10754-commented.json", "# exported numbers\n// second comment\n\n" + PRETTY, ""));
  }

  /** An XML source whose first line carries text between its tags is not placed by the dispatch, but its extension says XML. */
  @Test
  void xmlExtensionWithTextOnTheFirstLineIsXml() throws Exception {
    final String databasePath = importSource("importer-10754-oneline.xml",
        "<places><place id=\"1\">first</place><place id=\"2\">second</place></places>\n", "");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.countType("v_place", true)).isEqualTo(2);
    } finally {
      dropDatabase(databasePath);
    }
  }

  /** An empty array is a JSON source with nothing in it, not a delimited text file the sniffer cannot place. */
  @Test
  void emptyArrayImportsNothingWithoutFailing() throws Exception {
    final String databasePath = importSource("importer-10754-empty.json", "[ ]", "");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.getSchema().existsType("Document")).isFalse();
    } finally {
      dropDatabase(databasePath);
    }
  }

  @Test
  void arrayEntryThatIsNotAnObjectIsRefusedByPosition() {
    assertThatThrownBy(() -> importSource("importer-10754-scalar.json", "[{\"id\":1}, 2]", ""))
        .hasStackTraceContaining("$[1]").hasStackTraceContaining("only an object is a record");
    dropDatabase(databasePath("importer-10754-scalar.json"));
  }

  /** A delimited-text header that merely opens with '[' is still delimited text. */
  @Test
  void csvHeaderOpeningWithBracketIsStillCsv() throws Exception {
    final String databasePath = importSource("importer-10754-brackets.csv.txt", "[id],[name]\n1,one\n2,two\n", "");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.countType("Document", true)).isEqualTo(2);
    } finally {
      dropDatabase(databasePath);
    }
  }

  /**
   * A .json source the sniffer cannot place is refused naming the extension, instead of being declared CSV and imported as one
   * header row. The refusal names the escape hatch, which then works.
   */
  @Test
  void jsonExtensionWithOtherContentIsRefusedAndFileTypeOverrides() throws Exception {
    final String csv = "id,name\n1,one\n2,two\n";
    assertThatThrownBy(() -> importSource("importer-10754-mislabelled.json", csv, ""))
        .hasStackTraceContaining(".json").hasStackTraceContaining("-fileType");
    dropDatabase(databasePath("importer-10754-mislabelled.json"));

    final String databasePath = importSource("importer-10754-mislabelled.json", csv, " -fileType csv");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.countType("Document", true)).isEqualTo(2);
    } finally {
      dropDatabase(databasePath);
    }
  }

  /** Every JSON number used to be read as a double: 42 became 42.0 and 9007199254740993 became 9007199254740992. */
  private static void assertTwoRecords(final String databasePath) {
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.countType("Document", true)).isEqualTo(2);
      try (final ResultSet rs = db.query("sql", "select from Document where id = 1")) {
        final Document first = rs.next().getRecord().get().asDocument();
        assertThat(first.get("count")).isEqualTo(42L);
        assertThat(first.get("bigId")).isEqualTo(9007199254740993L);
        assertThat(first.get("price")).isEqualTo(1.5);
      }
    } finally {
      dropDatabase(databasePath);
    }
  }

  @Test
  void integerBeyondLongIsKeptExact() throws Exception {
    final String databasePath = importSource("importer-10754-huge.json", "[{\"id\":1,\"huge\":123456789012345678901234567890}]", "");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      final Document doc = db.iterateType("Document", true).next().asDocument();
      assertThat(new BigDecimal(doc.get("huge").toString())).isEqualTo(new BigDecimal("123456789012345678901234567890"));
    } finally {
      dropDatabase(databasePath);
    }
  }

  /** An integer read as a LONG still lands as the declared type of a property the schema already holds. */
  @Test
  void integerIntoAnExistingDoublePropertyIsConverted() throws Exception {
    final File source = new File("target/importer-10754-declared.json");
    Files.writeString(source.toPath(), ONE_LINE, StandardCharsets.UTF_8);
    final String databasePath = databasePath("importer-10754-declared.json");
    FileUtils.deleteRecursively(new File(databasePath));
    try (final Database setup = new DatabaseFactory(databasePath).create()) {
      setup.getSchema().createDocumentType("Document").createProperty("count", Type.DOUBLE);
    }

    try {
      new Importer(("-url file://" + source.getAbsolutePath() + " -database " + databasePath).split(" ")).load();
      try (final Database db = new DatabaseFactory(databasePath).open()) {
        try (final ResultSet rs = db.query("sql", "select from Document where id = 1")) {
          final Document first = rs.next().getRecord().get().asDocument();
          assertThat(first.get("count")).isEqualTo(42.0);
          assertThat(first.get("bigId")).isEqualTo(9007199254740993L);
        }
      }
    } finally {
      dropDatabase(databasePath);
      source.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /** A magnitude a double cannot hold is kept exact: 1e309 would be Infinity, 1e-400 would be 0. */
  @Test
  void decimalOutsideTheDoubleRangeIsKeptExact() throws Exception {
    final String databasePath = importSource("importer-10754-overflow.json", "[{\"id\":1,\"big\":1e309,\"tiny\":1e-400,\"zero\":0.0}]",
        "");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      final Document doc = db.iterateType("Document", true).next().asDocument();
      assertThat(new BigDecimal(doc.get("big").toString())).isEqualByComparingTo(new BigDecimal("1e309"));
      assertThat(new BigDecimal(doc.get("tiny").toString())).isEqualByComparingTo(new BigDecimal("1e-400"));
      assertThat(doc.get("zero")).isEqualTo(0.0);
    } finally {
      dropDatabase(databasePath);
    }
  }

  /** An array of scalars is JSON too: it reaches the JSON importer and is refused by position, not read as delimited text. */
  @Test
  void scalarArrayIsJsonAndRefusedByPosition() {
    for (final String content : new String[] { "[1,2]", "[\"a\",\"b\"]", "[true, false]", "[ null ]", "[-1.5]" }) {
      final String fileName = "importer-10754-scalars.txt";
      assertThatThrownBy(() -> importSource(fileName, content, "")).as(content)
          .hasStackTraceContaining("$[0]").hasStackTraceContaining("only an object is a record");
      dropDatabase(databasePath(fileName));
    }
  }

  /** The generic fileType names the format of the url source on the vertex route too, not only on the database one. */
  @Test
  void fileTypeAppliesWhenTheUrlIsImportedAsVertices() throws Exception {
    final String databasePath = importSource("importer-10754-vertices.json", "id,name\n1,one\n2,two\n",
        " -vertexType Person -fileType csv");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.countType("Person", true)).isEqualTo(2);
    } finally {
      dropDatabase(databasePath);
    }
  }

  /** JSON's only non-canonical integer within the long range is "-0": it is the LONG zero, not a DECIMAL. */
  @Test
  void negativeZeroIsTheLongZero() throws Exception {
    final String databasePath = importSource("importer-10754-negzero.json",
        "[{\"id\":1,\"z\":-0,\"min\":" + Long.MIN_VALUE + "}]", "");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      final Document doc = db.iterateType("Document", true).next().asDocument();
      assertThat(doc.get("z")).isEqualTo(0L);
      // the one value parseLong() also answers for "not canonical": still read as the LONG it is
      assertThat(doc.get("min")).isEqualTo(Long.MIN_VALUE);
    } finally {
      dropDatabase(databasePath);
    }
  }

  /** The whitespace between the '[' and the first record is skipped whatever its length. */
  @Test
  void longWhitespaceAfterTheBracketIsSkipped() throws Exception {
    assertTwoRecords(importSource("importer-10754-wide.txt", "[" + " ".repeat(5000) + ONE_LINE.substring(1), ""));
  }

  private static String importSource(final String fileName, final String content, final String extraArgs) throws Exception {
    final File source = new File("target/" + fileName);
    Files.writeString(source.toPath(), content, StandardCharsets.UTF_8);
    final String databasePath = databasePath(fileName);
    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(("-url file://" + source.getAbsolutePath() + " -database " + databasePath + " -forceDatabaseCreate true"
          + extraArgs).split(" ")).load();
    } finally {
      source.delete();
      TestHelper.checkActiveDatabases();
    }
    return databasePath;
  }

  private static String databasePath(final String fileName) {
    return "target/databases/test-import-10754-" + fileName.replace('.', '-');
  }

  private static void dropDatabase(final String databasePath) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    else
      FileUtils.deleteRecursively(new File(databasePath));
  }
}
