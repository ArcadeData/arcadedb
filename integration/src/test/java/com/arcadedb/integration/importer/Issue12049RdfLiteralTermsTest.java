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
import com.arcadedb.database.RID;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12049: the RDF importer read N-Triples as delimited text. An empty literal {@code ""} was an empty cell, so the
 * terminator {@code .} became the object; a plain literal lost its quotes, so {@code "http://ex.org/o1"} became the same vertex
 * as {@code <http://ex.org/o1>}, while a tagged or typed literal kept them; no escape was decoded; and a database without the
 * {@code Node} type failed the import. Subjects and objects are now keyed by their term: an IRI by its text, a literal by its
 * canonical N-Triples form; a literal's vertex also carries what a user reads, its decoded lexical form in {@code value} and its
 * language tag and datatype in {@code lang} and {@code datatype}.
 */
class Issue12049RdfLiteralTermsTest {
  private static final String DB_PATH = "target/databases/issue12049";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
  }

  @Test
  void literalObjectsAreKeyedByTheirCanonicalForm() throws Exception {
    final File source = new File("target/issue12049.nt");
    Files.writeString(source.toPath(), """
        <http://ex.org/s1> <http://ex.org/p> <http://ex.org/o1> .
        # a comment line and a blank line are not statements

        <http://ex.org/s1> <http://ex.org/homepage> "http://ex.org/o1" .
        <http://ex.org/s1> <http://ex.org/name> "hello world" .
        <http://ex.org/s1> <http://ex.org/nameTyped> "hello world"^^<http://www.w3.org/2001/XMLSchema#string> .
        <http://ex.org/s1> <http://ex.org/label> "bonjour"@fr .
        <http://ex.org/s1> <http://ex.org/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
        <http://ex.org/s1> <http://ex.org/note> "caf\\u00E9\\nline2" .
        <http://ex.org/s1> <http://ex.org/quote> "say \\"hi\\"" .
        <http://ex.org/s1> <http://ex.org/empty> "" .
        _:b1 <http://ex.org/see> <http://ex.org/caf\\u00E9> .
        """, StandardCharsets.UTF_8);

    try (final Database db = new DatabaseFactory(DB_PATH).create()) {
      try (final ResultSet rs = db.command("sql", "IMPORT DATABASE file://" + source.getAbsolutePath())) {
        final Result result = rs.next();
        assertThat(result.<String>getProperty("result")).isEqualTo("OK");
        assertThat(result.<Long>getProperty("createdEdges")).isEqualTo(10L);
      }

      final Map<String, String> objectByPredicate = new HashMap<>();
      final Map<String, RID> objectRidByPredicate = new HashMap<>();
      final Map<String, Result> objectRowByPredicate = new HashMap<>();
      try (final ResultSet rs = db.query("sql",
          "SELECT label, @in.id AS o, @in AS oRid, @in.value AS value, @in.lang AS lang, @in.datatype AS datatype FROM Relationship")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final String predicate = row.getProperty("label");
          objectByPredicate.put(predicate, row.getProperty("o"));
          objectRidByPredicate.put(predicate, row.getProperty("oRid"));
          objectRowByPredicate.put(predicate, row);
        }
      }

      assertThat(objectByPredicate.get("http://ex.org/p")).isEqualTo("http://ex.org/o1");
      assertThat(objectByPredicate.get("http://ex.org/homepage")).isEqualTo("\"http://ex.org/o1\"");
      assertThat(objectRidByPredicate.get("http://ex.org/homepage"))
          .as("a literal is never the vertex of the IRI it spells").isNotEqualTo(objectRidByPredicate.get("http://ex.org/p"));
      assertThat(objectByPredicate.get("http://ex.org/name")).isEqualTo("\"hello world\"");
      assertThat(objectRidByPredicate.get("http://ex.org/nameTyped"))
          .as("an xsd:string literal is the plain literal").isEqualTo(objectRidByPredicate.get("http://ex.org/name"));
      assertThat(objectByPredicate.get("http://ex.org/label")).isEqualTo("\"bonjour\"@fr");
      assertThat(objectByPredicate.get("http://ex.org/age")).isEqualTo("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>");
      assertThat(objectByPredicate.get("http://ex.org/note")).isEqualTo("\"café\\nline2\"");
      assertThat(objectByPredicate.get("http://ex.org/quote")).isEqualTo("\"say \\\"hi\\\"\"");
      assertThat(objectByPredicate.get("http://ex.org/empty")).as("the empty literal, not the terminator").isEqualTo("\"\"");
      assertThat(objectByPredicate.get("http://ex.org/see")).isEqualTo("http://ex.org/café");

      // WHAT A USER READS: THE DECODED LEXICAL FORM, NO QUOTES, AND THE TAG AND DATATYPE IN PROPERTIES OF THEIR OWN
      assertLiteral(objectRowByPredicate.get("http://ex.org/name"), "hello world", null, null);
      assertLiteral(objectRowByPredicate.get("http://ex.org/homepage"), "http://ex.org/o1", null, null);
      assertLiteral(objectRowByPredicate.get("http://ex.org/label"), "bonjour", "fr", null);
      assertLiteral(objectRowByPredicate.get("http://ex.org/age"), "42", null, "http://www.w3.org/2001/XMLSchema#integer");
      assertLiteral(objectRowByPredicate.get("http://ex.org/note"), "café\nline2", null, null);
      assertLiteral(objectRowByPredicate.get("http://ex.org/quote"), "say \"hi\"", null, null);
      assertLiteral(objectRowByPredicate.get("http://ex.org/empty"), "", null, null);
      assertThat(objectRowByPredicate.get("http://ex.org/p").<String>getProperty("value")).as("an IRI has no literal value").isNull();

      try (final ResultSet rs = db.query("sql", "SELECT @out.id AS s FROM Relationship WHERE label = 'http://ex.org/see'")) {
        assertThat(rs.next().<String>getProperty("s")).isEqualTo("_:b1");
      }
    } finally {
      source.delete();
    }
  }

  /**
   * The escapes of an IRI are decoded wherever the IRI is, and the sniff of the first statement accepts them: a file that OPENS with
   * one is recognised as N-Triples, where it was handed to the delimited-text reading. No {@code .nt} extension, so the content decides.
   */
  @Test
  void aFileWhoseFirstStatementHasAnEscapedIriIsImportedAsRdf() throws Exception {
    final File source = new File("target/issue12049-escaped-first.txt");
    Files.writeString(source.toPath(), """
        <http://ex.org/caf\\u00E9> <http://ex.org/p> <http://ex.org/o1> .
        <http://ex.org/s2> <http://ex.org/p> "x" .
        """, StandardCharsets.UTF_8);

    try (final Database db = new DatabaseFactory(DB_PATH).create()) {
      try (final ResultSet rs = db.command("sql", "IMPORT DATABASE file://" + source.getAbsolutePath())) {
        final Result result = rs.next();
        assertThat(result.<String>getProperty("result")).isEqualTo("OK");
        assertThat(result.<Long>getProperty("createdEdges")).isEqualTo(2L);
      }

      try (final ResultSet rs = db.query("sql", "SELECT count() AS c FROM Node WHERE id = ?", "http://ex.org/café")) {
        assertThat(rs.next().<Long>getProperty("c")).as("the subject keyed by its decoded IRI").isEqualTo(1L);
      }
      try (final ResultSet rs = db.query("sql", "SELECT count() AS c FROM Node")) {
        assertThat(rs.next().<Long>getProperty("c")).as("café, o1, s2 and the literal").isEqualTo(4L);
      }
    } finally {
      source.delete();
    }
  }

  private static void assertLiteral(final Result row, final String value, final String lang, final String datatype) {
    assertThat(row.<String>getProperty("value")).isEqualTo(value);
    assertThat(row.<String>getProperty("lang")).isEqualTo(lang);
    assertThat(row.<String>getProperty("datatype")).isEqualTo(datatype);
  }
}
