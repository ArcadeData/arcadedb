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
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.integration.importer.graph.XmlRowSource;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the generic {@link GraphImporter} with {@link XmlRowSource} in element mode
 * (reading child elements as fields) using the existing importer-simple.xml test data.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphImporterXMLTest {

  private static final String DB_PATH = "target/databases/graph-importer-xml-test";
  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void cleanup() {
    if (database != null)
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void importBooksFromXML() throws Exception {
    final String resourceDir = new File("src/test/resources").getAbsolutePath();

    database.transaction(() -> database.getSchema().createVertexType("Book"));

    // XmlRowSource with element name "book" and readChildElements=true
    // reads both attributes (id, isbn) and child elements (title, author, year)
    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Book", new XmlRowSource(resourceDir + "/importer-simple.xml", "book", true), v -> {
          v.id("id");
          v.intProperty("bookId", "id");
          v.property("isbn", "isbn");
          v.property("title", "title");
          v.property("author", "author");
          v.property("year", "year");
        })
        .build()) {

      importer.run();

      assertThat(importer.getVertexCount()).isEqualTo(2);
    }

    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT count(*) as c FROM Book")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2);
      }
    });

    // Verify properties
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Book WHERE bookId = 1")) {
        assertThat(rs.hasNext()).isTrue();
        final var book = rs.next();
        assertThat((String) book.getProperty("title")).isEqualTo("The Great Book");
        assertThat((String) book.getProperty("author")).isEqualTo("John Doe");
        assertThat((String) book.getProperty("isbn")).isEqualTo("978-1234567890");
        assertThat((String) book.getProperty("year")).isEqualTo("2023");
      }
    });
  }
}
