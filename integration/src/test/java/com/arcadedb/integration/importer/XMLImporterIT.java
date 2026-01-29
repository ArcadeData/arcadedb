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
import com.arcadedb.database.Record;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for XML import functionality.
 * <p>
 * Issue #1144: XML import fails with namespaced elements because:
 * 1. Full namespace URI is used in type names instead of local name
 * 2. Vertex types are not auto-created during import
 */
class XMLImporterIT {

  @BeforeEach
  @AfterEach
  void beforeTests() {
    TestHelper.checkActiveDatabases();
  }

  /**
   * Test importing a simple XML file without namespaces.
   * Verifies that vertex types are auto-created and records are imported.
   */
  @Test
  void importSimpleXML() throws Exception {
    final String databasePath = "target/databases/test-import-xml-simple";

    Importer importer = new Importer(
        ("-url file://src/test/resources/importer-simple.xml -database " + databasePath
            + " -forceDatabaseCreate true").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      // Verify vertex type was auto-created with correct name (v_book, not v_{...}book)
      assertThat(db.getSchema().existsType("v_book")).isTrue();
      assertThat(db.countType("v_book", true)).isEqualTo(2);

      // Verify the records have the expected properties
      final Iterator<Vertex> books = (Iterator<Vertex>) (Iterator<?>) db.iterateType("v_book", true);
      while (books.hasNext()) {
        final Vertex book = books.next();
        assertThat(book.has("id")).isTrue();
        assertThat(book.has("isbn")).isTrue();
        assertThat(book.has("title")).isTrue();
        assertThat(book.has("author")).isTrue();
        assertThat(book.has("year")).isTrue();
      }
    }

    TestHelper.checkActiveDatabases();
  }

  /**
   * Test importing XML file with default namespace (xmlns="...").
   * Issue #1144: This should use local element names, not namespace URIs.
   */
  @Test
  void importXMLWithNamespace() throws Exception {
    final String databasePath = "target/databases/test-import-xml-namespace";

    Importer importer = new Importer(
        ("-url file://src/test/resources/importer-namespace.xml -database " + databasePath
            + " -forceDatabaseCreate true").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      // Issue #1144: The type should be v_record, NOT v_{http://www.loc.gov/MARC21/slim}record
      assertThat(db.getSchema().existsType("v_record")).isTrue();

      // Verify the namespace URI type was NOT created (the bug)
      assertThat(db.getSchema().existsType("v_{http://www.loc.gov/MARC21/slim}record")).isFalse();

      assertThat(db.countType("v_record", true)).isEqualTo(1);

      // Verify the record was created with at least one property from nested elements
      // With objectNestLevel=1 (default), <datafield> sub-elements become properties
      final Vertex record = db.iterateType("v_record", true).next().asVertex();
      // The datafield element name becomes a property key, subfield content becomes value
      assertThat(record.has("datafield")).isTrue();
      // Issue #1144: datafield value should NOT be null or empty
      // With multiple <datafield> elements, the last one's subfield content is kept
      // The last <datafield> contains "Chastain, Joel W.,"
      final Object datafieldValue = record.get("datafield");
      assertThat(datafieldValue).isNotNull();
      assertThat(datafieldValue.toString()).isNotEmpty();
      assertThat(datafieldValue.toString()).contains("Chastain");
    }

    TestHelper.checkActiveDatabases();
  }

  /**
   * Test for issue #2758: XMLImporterFormat should respect EntityType parameter.
   * When using -documents parameter, the importer should create documents, not vertices.
   * Type names are derived from XML tags (e.g., <book> becomes "book" for documents).
   */
  @Test
  void importXMLAsDocuments() throws Exception {
    final String databasePath = "target/databases/test-import-xml-documents";

    // Import XML as documents using -documents parameter
    Importer importer = new Importer(
        ("-documents file://src/test/resources/importer-simple.xml -database " + databasePath
            + " -forceDatabaseCreate true -objectNestLevel 1").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      // Should create document type named "book" (from <book> XML tag, no "v_" prefix)
      assertThat(db.getSchema().existsType("book")).isTrue();

      // Verify it's a DocumentType but NOT a VertexType
      final DocumentType bookType = db.getSchema().getType("book");
      assertThat(bookType).isInstanceOf(DocumentType.class);
      assertThat(bookType).isNotInstanceOf(VertexType.class);

      assertThat(db.countType("book", true)).isEqualTo(2);

      // Verify we can iterate as documents
      final Iterator<Record> iterator = db.iterateType("book", true);
      assertThat(iterator.hasNext()).isTrue();

      final Document doc = iterator.next().asDocument();
      assertThat(doc.has("id")).isTrue();
      assertThat(doc.has("isbn")).isTrue();
      assertThat(doc.has("title")).isTrue();
    }

    TestHelper.checkActiveDatabases();
  }

  /**
   * Test for issue #2758: XMLImporterFormat should respect EntityType parameter.
   * When using -vertices parameter, the importer should create vertices.
   * Type names are derived from XML tags (e.g., <book> becomes "v_book" for vertices).
   */
  @Test
  void importXMLAsVerticesWithExplicitType() throws Exception {
    final String databasePath = "target/databases/test-import-xml-vertices-explicit";

    // Import XML as vertices using -vertices parameter
    Importer importer = new Importer(
        ("-vertices file://src/test/resources/importer-simple.xml -database " + databasePath
            + " -forceDatabaseCreate true -objectNestLevel 1").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      // Should create vertex type named "v_book" (from <book> XML tag with "v_" prefix)
      assertThat(db.getSchema().existsType("v_book")).isTrue();

      // Verify it's a VertexType
      final DocumentType vBookType = db.getSchema().getType("v_book");
      assertThat(vBookType).isInstanceOf(VertexType.class);

      assertThat(db.countType("v_book", true)).isEqualTo(2);

      // Verify we can iterate as vertices
      final Iterator<Record> iterator = db.iterateType("v_book", true);
      assertThat(iterator.hasNext()).isTrue();

      final Vertex vertex = iterator.next().asVertex();
      assertThat(vertex.has("id")).isTrue();
      assertThat(vertex.has("isbn")).isTrue();
      assertThat(vertex.has("title")).isTrue();
    }

    TestHelper.checkActiveDatabases();
  }

  /**
   * Test for issue #1278: RDF file import problems.
   * RDF files have rdf:Description elements that should be imported as vertices.
   * Namespace prefixes should be preserved in type names and property names.
   * - Type: v_rdf_Description (with namespace prefix)
   * - Attributes: rdf_about (namespace prefix + local name)
   * - Properties: cd_artist, cd_country, etc. (namespace prefix + local name)
   */
  @Test
  void importRDFFile() throws Exception {
    final String databasePath = "target/databases/test-import-rdf";

    // Import RDF file using default settings
    Importer importer = new Importer(
        ("-url file://src/test/resources/importer-rdf.xml -database " + databasePath
            + " -forceDatabaseCreate true").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      // Issue #1278: The type should be v_rdf_Description, with namespace prefix preserved
      // NOT v_{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description (full namespace URI)
      // NOT v_Description (missing namespace prefix)
      assertThat(db.getSchema().existsType("v_rdf_Description")).isTrue();

      // Verify the namespace URI type was NOT created (the original bug)
      assertThat(db.getSchema().existsType("v_{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description")).isFalse();

      // Should have 2 Description elements (two CDs)
      assertThat(db.countType("v_rdf_Description", true)).isEqualTo(2);

      // Verify the records were created with properties from nested elements
      // Properties from cd namespace should preserve the namespace prefix
      final Iterator<Vertex> descriptions = (Iterator<Vertex>) (Iterator<?>) db.iterateType("v_rdf_Description", true);
      boolean foundBobDylan = false;
      boolean foundBonnieTyler = false;

      while (descriptions.hasNext()) {
        final Vertex desc = descriptions.next();
        // The 'about' attribute from rdf:about should be captured with namespace prefix
        assertThat(desc.has("rdf_about")).isTrue();
        // Child elements like cd:artist should have namespace prefix in property keys
        assertThat(desc.has("cd_artist")).isTrue();
        assertThat(desc.has("cd_country")).isTrue();
        assertThat(desc.has("cd_company")).isTrue();
        assertThat(desc.has("cd_price")).isTrue();
        assertThat(desc.has("cd_year")).isTrue();

        String artist = desc.getString("cd_artist");
        if ("Bob Dylan".equals(artist)) {
          foundBobDylan = true;
          assertThat(desc.getString("cd_country")).isEqualTo("USA");
          assertThat(desc.getString("cd_year")).isEqualTo("1985");
        } else if ("Bonnie Tyler".equals(artist)) {
          foundBonnieTyler = true;
          assertThat(desc.getString("cd_country")).isEqualTo("UK");
          assertThat(desc.getString("cd_year")).isEqualTo("1988");
        }
      }

      assertThat(foundBobDylan).isTrue();
      assertThat(foundBonnieTyler).isTrue();
    }

    TestHelper.checkActiveDatabases();
  }

  /**
   * Test that XML files without namespace prefixes still work correctly.
   * When elements have no namespace prefix, only the local name should be used.
   */
  @Test
  void importXMLWithoutNamespacePrefix() throws Exception {
    final String databasePath = "target/databases/test-import-xml-no-prefix";

    Importer importer = new Importer(
        ("-url file://src/test/resources/importer-simple.xml -database " + databasePath
            + " -forceDatabaseCreate true").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      // Without namespace prefix, type should be v_book (no extra prefix)
      assertThat(db.getSchema().existsType("v_book")).isTrue();
      assertThat(db.countType("v_book", true)).isEqualTo(2);

      // Properties should have no prefix
      final Vertex book = db.iterateType("v_book", true).next().asVertex();
      assertThat(book.has("id")).isTrue();
      assertThat(book.has("isbn")).isTrue();
      assertThat(book.has("title")).isTrue();
      assertThat(book.has("author")).isTrue();
      assertThat(book.has("year")).isTrue();
    }

    TestHelper.checkActiveDatabases();
  }
}
