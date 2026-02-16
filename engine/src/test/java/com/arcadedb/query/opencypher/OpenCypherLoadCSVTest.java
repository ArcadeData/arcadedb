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
package com.arcadedb.query.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for LOAD CSV Cypher clause.
 */
public class OpenCypherLoadCSVTest {
  private Database database;
  private Path testDataDir;

  @BeforeEach
  void setUp() throws IOException {
    database = new DatabaseFactory("./target/databases/testloadcsv").create();
    testDataDir = Path.of("./target/test-data/loadcsv");
    Files.createDirectories(testDataDir);
  }

  @AfterEach
  void tearDown() throws IOException {
    // Reset configuration changes
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LOAD_CSV_ALLOW_FILE_URLS, true);
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LOAD_CSV_IMPORT_DIRECTORY, "");

    if (database != null) {
      database.drop();
      database = null;
    }
    // Clean up test CSV files
    if (Files.exists(testDataDir)) {
      Files.walk(testDataDir)
          .sorted(java.util.Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  @Test
  void loadCSVWithoutHeaders() throws IOException {
    final Path csvFile = testDataDir.resolve("basic.csv");
    Files.writeString(csvFile, "Alice,30\nBob,25\nCharlie,35\n");

    final String url = csvFile.toAbsolutePath().toUri().toString();
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV FROM '" + url + "' AS row RETURN row")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(3);
    @SuppressWarnings("unchecked") final List<String> firstRow = results.get(0).getProperty("row");
    assertThat(firstRow).containsExactly("Alice", "30");
    @SuppressWarnings("unchecked") final List<String> secondRow = results.get(1).getProperty("row");
    assertThat(secondRow).containsExactly("Bob", "25");
    @SuppressWarnings("unchecked") final List<String> thirdRow = results.get(2).getProperty("row");
    assertThat(thirdRow).containsExactly("Charlie", "35");
  }

  @Test
  void loadCSVWithHeaders() throws IOException {
    final Path csvFile = testDataDir.resolve("headers.csv");
    Files.writeString(csvFile, "name,age\nAlice,30\nBob,25\n");

    final String url = csvFile.toAbsolutePath().toUri().toString();
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV WITH HEADERS FROM '" + url + "' AS row RETURN row.name AS name, row.age AS age")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(2);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Alice");
    assertThat(results.get(0).<String>getProperty("age")).isEqualTo("30");
    assertThat(results.get(1).<String>getProperty("name")).isEqualTo("Bob");
    assertThat(results.get(1).<String>getProperty("age")).isEqualTo("25");
  }

  @Test
  void loadCSVCustomFieldTerminator() throws IOException {
    final Path csvFile = testDataDir.resolve("semicolon.csv");
    Files.writeString(csvFile, "name;age\nAlice;30\nBob;25\n");

    final String url = csvFile.toAbsolutePath().toUri().toString();
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV WITH HEADERS FROM '" + url + "' AS row FIELDTERMINATOR ';' RETURN row.name AS name, row.age AS age")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(2);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Alice");
    assertThat(results.get(0).<String>getProperty("age")).isEqualTo("30");
  }

  @Test
  void loadCSVCreateNodes() throws IOException {
    final Path csvFile = testDataDir.resolve("people.csv");
    Files.writeString(csvFile, "name,age\nAlice,30\nBob,25\n");

    database.getSchema().createVertexType("Person");

    final String url = csvFile.toAbsolutePath().toUri().toString();
    database.transaction(() -> {
      database.command("opencypher",
          "LOAD CSV WITH HEADERS FROM '" + url + "' AS row CREATE (n:Person {name: row.name, age: toInteger(row.age)})");
    });

    // Verify the nodes were created
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (n:Person) RETURN n.name AS name, n.age AS age ORDER BY n.name")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(2);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Alice");
    assertThat(results.get(0).<Object>getProperty("age")).isEqualTo(30L);
    assertThat(results.get(1).<String>getProperty("name")).isEqualTo("Bob");
    assertThat(results.get(1).<Object>getProperty("age")).isEqualTo(25L);
  }

  @Test
  void loadCSVFileAndLineNumberFunctions() throws IOException {
    final Path csvFile = testDataDir.resolve("lineno.csv");
    Files.writeString(csvFile, "a,b\nc,d\ne,f\n");

    final String url = csvFile.toAbsolutePath().toUri().toString();
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV FROM '" + url + "' AS row RETURN file() AS f, linenumber() AS ln, row")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(3);
    // file() should return the URL
    assertThat(results.get(0).<String>getProperty("f")).isEqualTo(url);
    // linenumber() should be 1-based line numbers
    assertThat(results.get(0).<Object>getProperty("ln")).isEqualTo(1);
    assertThat(results.get(1).<Object>getProperty("ln")).isEqualTo(2);
    assertThat(results.get(2).<Object>getProperty("ln")).isEqualTo(3);
  }

  @Test
  void loadCSVQuotedFieldsWithComma() throws IOException {
    final Path csvFile = testDataDir.resolve("quoted.csv");
    Files.writeString(csvFile, "name,title\n\"Smith, Jr.\",Manager\nAlice,Developer\n");

    final String url = csvFile.toAbsolutePath().toUri().toString();
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV WITH HEADERS FROM '" + url + "' AS row RETURN row.name AS name, row.title AS title")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(2);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Smith, Jr.");
    assertThat(results.get(0).<String>getProperty("title")).isEqualTo("Manager");
    assertThat(results.get(1).<String>getProperty("name")).isEqualTo("Alice");
  }

  @Test
  void loadCSVEmptyFile() throws IOException {
    final Path csvFile = testDataDir.resolve("empty.csv");
    Files.writeString(csvFile, "");

    final String url = csvFile.toAbsolutePath().toUri().toString();
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV FROM '" + url + "' AS row RETURN row")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).isEmpty();
  }

  @Test
  void loadCSVWithParameterUrl() throws IOException {
    final Path csvFile = testDataDir.resolve("param.csv");
    Files.writeString(csvFile, "hello,world\n");

    final String url = csvFile.toAbsolutePath().toUri().toString();
    final Map<String, Object> params = new HashMap<>();
    params.put("url", url);
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV FROM $url AS row RETURN row", params)) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(1);
    @SuppressWarnings("unchecked") final List<String> row = results.get(0).getProperty("row");
    assertThat(row).containsExactly("hello", "world");
  }

  @Test
  void loadCSVWithHeadersEmptyFileReturnsNoRows() throws IOException {
    final Path csvFile = testDataDir.resolve("headers_empty.csv");
    Files.writeString(csvFile, "name,age\n");

    final String url = csvFile.toAbsolutePath().toUri().toString();
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV WITH HEADERS FROM '" + url + "' AS row RETURN row.name AS name")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).isEmpty();
  }

  @Test
  void loadCSVBareFilePath() throws IOException {
    final Path csvFile = testDataDir.resolve("bare.csv");
    Files.writeString(csvFile, "hello,world\n");

    // Use bare file path (no file:/// prefix)
    final String path = csvFile.toAbsolutePath().toString();
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV FROM '" + path + "' AS row RETURN row")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(1);
    @SuppressWarnings("unchecked") final List<String> row = results.get(0).getProperty("row");
    assertThat(row).containsExactly("hello", "world");
  }

  // ===== Security: File URLs disabled =====

  @Test
  void loadCSVFileUrlsDisabled() throws IOException {
    final Path csvFile = testDataDir.resolve("security.csv");
    Files.writeString(csvFile, "a,b\n");

    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LOAD_CSV_ALLOW_FILE_URLS, false);

    final String url = csvFile.toAbsolutePath().toUri().toString();
    assertThatThrownBy(() -> database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV FROM '" + url + "' AS row RETURN row")) {
        while (rs.hasNext())
          rs.next();
      }
    })).isInstanceOf(SecurityException.class)
        .hasMessageContaining("file:/// URLs are disabled");
  }

  @Test
  void loadCSVBarePathDisabledWhenFileUrlsOff() throws IOException {
    final Path csvFile = testDataDir.resolve("security2.csv");
    Files.writeString(csvFile, "a,b\n");

    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LOAD_CSV_ALLOW_FILE_URLS, false);

    final String path = csvFile.toAbsolutePath().toString();
    assertThatThrownBy(() -> database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV FROM '" + path + "' AS row RETURN row")) {
        while (rs.hasNext())
          rs.next();
      }
    })).isInstanceOf(SecurityException.class);
  }

  // ===== Security: Import directory restriction =====

  @Test
  void loadCSVImportDirectoryAllowsFilesInside() throws IOException {
    final Path csvFile = testDataDir.resolve("inside.csv");
    Files.writeString(csvFile, "hello,world\n");

    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LOAD_CSV_IMPORT_DIRECTORY,
        testDataDir.toAbsolutePath().toString());

    // Use just the filename — should resolve relative to the import directory
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV FROM 'inside.csv' AS row RETURN row")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(1);
    @SuppressWarnings("unchecked") final List<String> row = results.get(0).getProperty("row");
    assertThat(row).containsExactly("hello", "world");
  }

  @Test
  void loadCSVImportDirectoryBlocksPathTraversal() throws IOException {
    final Path csvFile = testDataDir.resolve("traversal.csv");
    Files.writeString(csvFile, "a,b\n");

    // Set import directory to a subdirectory
    final Path subDir = testDataDir.resolve("allowed");
    Files.createDirectories(subDir);
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LOAD_CSV_IMPORT_DIRECTORY,
        subDir.toAbsolutePath().toString());

    // Try to access parent directory with ../
    assertThatThrownBy(() -> database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV FROM '../traversal.csv' AS row RETURN row")) {
        while (rs.hasNext())
          rs.next();
      }
    })).isInstanceOf(SecurityException.class)
        .hasMessageContaining("path traversal blocked");
  }

  // ===== Gzip compression =====

  @Test
  void loadCSVGzipCompressed() throws IOException {
    final Path gzFile = testDataDir.resolve("data.csv.gz");
    try (final GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(gzFile.toFile()))) {
      gos.write("name,age\nAlice,30\nBob,25\n".getBytes());
    }

    final String url = gzFile.toAbsolutePath().toUri().toString();
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV WITH HEADERS FROM '" + url + "' AS row RETURN row.name AS name, row.age AS age")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(2);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Alice");
    assertThat(results.get(0).<String>getProperty("age")).isEqualTo("30");
    assertThat(results.get(1).<String>getProperty("name")).isEqualTo("Bob");
    assertThat(results.get(1).<String>getProperty("age")).isEqualTo("25");
  }

  // ===== ZIP compression =====

  @Test
  void loadCSVZipCompressed() throws IOException {
    final Path zipFile = testDataDir.resolve("data.csv.zip");
    try (final ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile.toFile()))) {
      zos.putNextEntry(new ZipEntry("data.csv"));
      zos.write("name,age\nCharlie,40\nDiana,35\n".getBytes());
      zos.closeEntry();
    }

    final String url = zipFile.toAbsolutePath().toUri().toString();
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV WITH HEADERS FROM '" + url + "' AS row RETURN row.name AS name, row.age AS age")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(2);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Charlie");
    assertThat(results.get(0).<String>getProperty("age")).isEqualTo("40");
    assertThat(results.get(1).<String>getProperty("name")).isEqualTo("Diana");
    assertThat(results.get(1).<String>getProperty("age")).isEqualTo("35");
  }

  // ===== Backslash quote escaping =====

  @Test
  void loadCSVBackslashEscaping() throws IOException {
    final Path csvFile = testDataDir.resolve("backslash.csv");
    // Use backslash escaping: \"
    Files.writeString(csvFile, "name,quote\n\"Alice\",\"She said \\\"hello\\\"\"\nBob,world\n");

    final String url = csvFile.toAbsolutePath().toUri().toString();
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "LOAD CSV WITH HEADERS FROM '" + url + "' AS row RETURN row.name AS name, row.quote AS quote")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(2);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Alice");
    assertThat(results.get(0).<String>getProperty("quote")).isEqualTo("She said \"hello\"");
    assertThat(results.get(1).<String>getProperty("name")).isEqualTo("Bob");
    assertThat(results.get(1).<String>getProperty("quote")).isEqualTo("world");
  }

  // ===== CALL {} IN TRANSACTIONS =====

  @Test
  void loadCSVCallInTransactionsWithBatchSize() throws IOException {
    final Path csvFile = testDataDir.resolve("batch.csv");
    final StringBuilder csv = new StringBuilder("name\n");
    for (int i = 0; i < 100; i++)
      csv.append("Person").append(i).append("\n");
    Files.writeString(csvFile, csv.toString());

    database.getSchema().createVertexType("BatchPerson");

    // IN TRANSACTIONS manages its own transactions
    database.setAutoTransaction(true);
    final String url = csvFile.toAbsolutePath().toUri().toString();
    try (final ResultSet rs = database.command("opencypher",
        "LOAD CSV WITH HEADERS FROM '" + url + "' AS row " +
            "CALL { WITH row CREATE (n:BatchPerson {name: row.name}) } IN TRANSACTIONS OF 25 ROWS")) {
      while (rs.hasNext())
        rs.next();
    }

    // Verify all 100 nodes were created
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (n:BatchPerson) RETURN count(n) AS cnt")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(1);
    assertThat(results.get(0).<Long>getProperty("cnt")).isEqualTo(100L);
  }

  @Test
  void loadCSVCallInTransactionsDefaultBatch() throws IOException {
    final Path csvFile = testDataDir.resolve("defaultbatch.csv");
    final StringBuilder csv = new StringBuilder("name\n");
    for (int i = 0; i < 50; i++)
      csv.append("Item").append(i).append("\n");
    Files.writeString(csvFile, csv.toString());

    database.getSchema().createVertexType("DefaultBatchItem");

    // IN TRANSACTIONS manages its own transactions
    database.setAutoTransaction(true);
    final String url = csvFile.toAbsolutePath().toUri().toString();
    try (final ResultSet rs = database.command("opencypher",
        "LOAD CSV WITH HEADERS FROM '" + url + "' AS row " +
            "CALL { WITH row CREATE (n:DefaultBatchItem {name: row.name}) } IN TRANSACTIONS")) {
      while (rs.hasNext())
        rs.next();
    }

    // Verify all 50 nodes were created
    final List<Result> results = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (n:DefaultBatchItem) RETURN count(n) AS cnt")) {
        while (rs.hasNext())
          results.add(rs.next());
      }
    });

    assertThat(results).hasSize(1);
    assertThat(results.get(0).<Long>getProperty("cnt")).isEqualTo(50L);
  }
}
