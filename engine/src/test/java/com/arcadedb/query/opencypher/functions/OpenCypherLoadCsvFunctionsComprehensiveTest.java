/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for OpenCypher LOAD CSV functions based on Neo4j Cypher documentation.
 * Tests cover: file(), linenumber()
 *
 * These functions are only useful when run on a query that uses LOAD CSV.
 * In all other contexts they always return null.
 */
class OpenCypherLoadCsvFunctionsComprehensiveTest {

  private Database database;
  private Path testCsvFile;

  @BeforeEach
  void setUp() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-cypher-load-csv-functions");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    // Create test CSV file with headers
    testCsvFile = Files.createTempFile("test-load-csv", ".csv");
    try (BufferedWriter writer = Files.newBufferedWriter(testCsvFile)) {
      writer.write("name,age,city");
      writer.newLine();
      writer.write("Alice,30,Paris");
      writer.newLine();
      writer.write("Bob,25,London");
      writer.newLine();
      writer.write("Charlie,35,Berlin");
      writer.newLine();
    }
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
    if (testCsvFile != null) {
      try {
        Files.deleteIfExists(testCsvFile);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
    }
  }

  // ==================== file() Tests ====================

  @Test
  void fileReturnsFileName() {
    final ResultSet result = database.command("opencypher",
        "LOAD CSV FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "RETURN file() AS filename");
    assertThat(result.hasNext()).isTrue();
    final String filename = (String) result.next().getProperty("filename");
    assertThat(filename).isNotNull();
    assertThat(filename).isEqualTo(testCsvFile.toAbsolutePath().toString());
  }

  @Test
  void fileReturnsSameNameForAllRows() {
    final ResultSet result = database.command("opencypher",
        "LOAD CSV FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "RETURN file() AS filename");
    assertThat(result.hasNext()).isTrue();
    final String expectedFilename = testCsvFile.toAbsolutePath().toString();
    while (result.hasNext()) {
      final String filename = (String) result.next().getProperty("filename");
      assertThat(filename).isEqualTo(expectedFilename);
    }
  }

  @Test
  void fileReturnsNullOutsideLoadCsvContext() {
    final ResultSet result = database.command("opencypher", "RETURN file() AS filename");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().getProperty("filename") == null).isTrue();
  }

  @Test
  void fileWithHeaderRow() {
    final ResultSet result = database.command("opencypher",
        "LOAD CSV WITH HEADERS FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "RETURN file() AS filename LIMIT 1");
    assertThat(result.hasNext()).isTrue();
    final String filename = (String) result.next().getProperty("filename");
    assertThat(filename).isNotNull();
    assertThat(filename).isEqualTo(testCsvFile.toAbsolutePath().toString());
  }

  @Test
  void fileWithoutHeaderRow() {
    final ResultSet result = database.command("opencypher",
        "LOAD CSV FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "RETURN file() AS filename LIMIT 1");
    assertThat(result.hasNext()).isTrue();
    final String filename = (String) result.next().getProperty("filename");
    assertThat(filename).isNotNull();
    assertThat(filename).isEqualTo(testCsvFile.toAbsolutePath().toString());
  }

  // ==================== linenumber() Tests ====================

  @Test
  void linenumberReturnsLineNumber() {
    final ResultSet result = database.command("opencypher",
        "LOAD CSV FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "RETURN linenumber() AS line");
    assertThat(result.hasNext()).isTrue();
    int expectedLine = 1;
    while (result.hasNext()) {
      final Integer line = ((Number) result.next().getProperty("line")).intValue();
      assertThat(line).isEqualTo(expectedLine);
      expectedLine++;
    }
  }

  @Test
  void linenumberWithHeadersStartsAt1() {
    // When CSV has headers, the header row is line 1, first data row is line 2
    final ResultSet result = database.command("opencypher",
        "LOAD CSV WITH HEADERS FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "RETURN linenumber() AS line");
    assertThat(result.hasNext()).isTrue();
    int expectedLine = 2; // First data row is line 2 (line 1 is header)
    while (result.hasNext()) {
      final Integer line = ((Number) result.next().getProperty("line")).intValue();
      assertThat(line).isEqualTo(expectedLine);
      expectedLine++;
    }
  }

  @Test
  void linenumberWithoutHeadersStartsAt1() {
    // When CSV has no headers, first row is line 1
    final ResultSet result = database.command("opencypher",
        "LOAD CSV FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "RETURN linenumber() AS line");
    assertThat(result.hasNext()).isTrue();
    int expectedLine = 1; // First row is line 1
    while (result.hasNext()) {
      final Integer line = ((Number) result.next().getProperty("line")).intValue();
      assertThat(line).isEqualTo(expectedLine);
      expectedLine++;
    }
  }

  @Test
  void linenumberReturnsNullOutsideLoadCsvContext() {
    final ResultSet result = database.command("opencypher", "RETURN linenumber() AS line");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().getProperty("line") == null).isTrue();
  }

  @Test
  void linenumberIncrementsPerRow() {
    final ResultSet result = database.command("opencypher",
        "LOAD CSV FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "RETURN linenumber() AS line ORDER BY line");
    assertThat(result.hasNext()).isTrue();
    int previousLine = 0;
    while (result.hasNext()) {
      final Integer line = ((Number) result.next().getProperty("line")).intValue();
      assertThat(line).isGreaterThan(previousLine);
      previousLine = line;
    }
  }

  @Test
  void linenumberWithFileCombined() {
    final ResultSet result = database.command("opencypher",
        "LOAD CSV FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "RETURN file() AS filename, linenumber() AS line");
    assertThat(result.hasNext()).isTrue();
    final String expectedFilename = testCsvFile.toAbsolutePath().toString();
    int expectedLine = 1;
    while (result.hasNext()) {
      final var row = result.next();
      final String filename = (String) row.getProperty("filename");
      final Integer line = ((Number) row.getProperty("line")).intValue();
      assertThat(filename).isEqualTo(expectedFilename);
      assertThat(line).isEqualTo(expectedLine);
      expectedLine++;
    }
  }

  @Test
  void linenumberWithSelectClause() {
    final ResultSet result = database.command("opencypher",
        "LOAD CSV FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "WITH row, linenumber() AS line\n" +
        "WHERE line > 1\n" +
        "RETURN line");
    assertThat(result.hasNext()).isTrue();
    while (result.hasNext()) {
      final Integer line = ((Number) result.next().getProperty("line")).intValue();
      assertThat(line).isGreaterThan(1);
    }
  }

  @Test
  void linenumberWithCreate() {
    database.getSchema().createVertexType("Person");
    final ResultSet result = database.command("opencypher",
        "LOAD CSV WITH HEADERS FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "CREATE (p:Person {name: row.name, age: toInteger(row.age), lineNumber: linenumber()})\n" +
        "RETURN count(p) AS count");
    assertThat(result.hasNext()).isTrue();
    final Integer count = ((Number) result.next().getProperty("count")).intValue();
    assertThat(count).isEqualTo(3); // 3 data rows (excluding header)

    // Verify line numbers were created correctly
    final ResultSet verifyResult = database.command("opencypher",
        "MATCH (p:Person) RETURN p.lineNumber AS line ORDER BY line");
    assertThat(verifyResult.hasNext()).isTrue();
    int expectedLine = 2; // First data row is line 2 (line 1 is header)
    while (verifyResult.hasNext()) {
      final Integer line = ((Number) verifyResult.next().getProperty("line")).intValue();
      assertThat(line).isEqualTo(expectedLine);
      expectedLine++;
    }
  }

  @Test
  void fileAndLinenumberWithMultipleLoadCsv() throws Exception {
    // Create a second CSV file
    Path secondCsvFile = null;
    try {
      secondCsvFile = Files.createTempFile("test-load-csv-2", ".csv");
      try (BufferedWriter writer = Files.newBufferedWriter(secondCsvFile)) {
        writer.write("value");
        writer.newLine();
        writer.write("100");
        writer.newLine();
        writer.write("200");
        writer.newLine();
      }

      // When there are multiple LOAD CSV clauses, functions return info about
      // the most recently executed LOAD CSV clause
      final ResultSet result = database.command("opencypher",
          "LOAD CSV FROM '" + testCsvFile.toAbsolutePath() + "' AS row1\n" +
          "LOAD CSV FROM '" + secondCsvFile.toAbsolutePath() + "' AS row2\n" +
          "RETURN file() AS filename, linenumber() AS line");
      assertThat(result.hasNext()).isTrue();
      while (result.hasNext()) {
        final var row = result.next();
        final String filename = (String) row.getProperty("filename");
        assertThat(filename).isEqualTo(secondCsvFile.toAbsolutePath().toString());
      }
    } finally {
      if (secondCsvFile != null) {
        Files.deleteIfExists(secondCsvFile);
      }
    }
  }

  @Test
  void linenumberWithSkipRows() {
    final ResultSet result = database.command("opencypher",
        "LOAD CSV FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "SKIP 1\n" +
        "RETURN linenumber() AS line");
    assertThat(result.hasNext()).isTrue();
    // Even with SKIP, linenumber should reflect the actual line number in the file
    int expectedLine = 2; // First row after skip is line 2
    while (result.hasNext()) {
      final Integer line = ((Number) result.next().getProperty("line")).intValue();
      assertThat(line).isEqualTo(expectedLine);
      expectedLine++;
    }
  }

  @Test
  void linenumberWithLimit() {
    final ResultSet result = database.command("opencypher",
        "LOAD CSV FROM '" + testCsvFile.toAbsolutePath() + "' AS row\n" +
        "LIMIT 2\n" +
        "RETURN linenumber() AS line");
    assertThat(result.hasNext()).isTrue();
    int count = 0;
    while (result.hasNext()) {
      final Integer line = ((Number) result.next().getProperty("line")).intValue();
      assertThat(line).isEqualTo(count + 1);
      count++;
    }
    assertThat(count).isEqualTo(2);
  }
}
