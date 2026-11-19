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
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.Source;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Review of issue #12049 (N-Triples read as statements): where a statement's terms begin and end. A blank-node label stops
 * before the statement terminator, a delimiter of more than one character separates terms, and a line longer than any
 * statement within {@code maxPropertySize} can be is refused as a row error without being kept whole in memory.
 */
class RDFImporterFormatStatementBoundariesTest {
  private static final String DB_PATH = "target/databases/rdf-importer-statement-boundaries";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    // THE EDGE TYPE THE FORMAT'S analyze() REGISTERS ON THE IMPORT ROUTE: load() IS DRIVEN DIRECTLY HERE
    database.transaction(() -> database.getSchema().createEdgeType("Related"));
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

  /** {@code _:b1.} is the label {@code b1} and the terminator: the same vertex a later {@code _:b1} names. */
  @Test
  void blankNodeLabelStopsBeforeTheTerminator() throws Exception {
    load(new RDFImporterFormat(" "), settings(), """
        <http://a/s> <http://a/p> _:b1.
        _:b1 <http://a/q> <http://a/o> .
        _:x.y <http://a/r> <http://a/o>.
        """);

    assertThat(keys()).containsExactlyInAnyOrder("http://a/s", "_:b1", "http://a/o", "_:x.y");
    assertThat(edges()).containsEntry("http://a/p", "http://a/s>_:b1").containsEntry("http://a/q", "_:b1>http://a/o")
        .containsEntry("http://a/r", "_:x.y>http://a/o");
  }

  @Test
  void multiCharacterDelimiterSeparatesTheTerms() throws Exception {
    load(new RDFImporterFormat(";;"), settings(), """
        <http://a/s1> ;; <http://a/rel> ;; <http://a/o1> .
        <http://a/s2>;;<http://a/lit>;;"x;y" .
        """);

    assertThat(edges()).containsOnly(Map.entry("http://a/rel", "http://a/s1>http://a/o1"),
        Map.entry("http://a/lit", "http://a/s2>\"x;y\""));
  }

  /** Refused as a row error, so -onRowError skip goes on with the next statement. */
  @Test
  void overlongLineIsARowError() throws Exception {
    final String overlong = "<http://a/s> <http://a/p> \"" + "x".repeat(1_000_000) + "\" .\n";
    final String content = overlong + "<http://a/s> <http://a/p> <http://a/o> .\n";

    final ImporterSettings settings = settings();
    settings.options.put("maxPropertySize", 100);
    assertThatThrownBy(() -> load(new RDFImporterFormat(" "), settings, content)).isInstanceOf(ImportException.class)
        .hasMessageContaining("maxPropertySize=100");

    settings.onRowError = "skip";
    final ImporterContext context = load(new RDFImporterFormat(" "), settings, content);
    assertThat(context.errors.get()).isEqualTo(1);
    assertThat(edges()).containsOnly(Map.entry("http://a/p", "http://a/s>http://a/o"));
  }

  /** What the overlong line costs: the reader keeps no more of it than a statement within the limit can be. */
  @Test
  void overlongLineIsNotKeptWhole() throws Exception {
    final NTriplesStatement statement = new NTriplesStatement(" ", 100);
    final String overlong = "<http://a/s> <http://a/p> \"" + "x".repeat(1_000_000) + "\" .";
    assertThat(statement.readLine(new StringReader(overlong + "\r\n<http://a/s> <http://a/p> <http://a/o> .\n"))).isTrue();
    assertThat(statement.lineLength()).isEqualTo(overlong.length());
    assertThat(statement.lineTooLong()).isTrue();
    assertThat(statement.line().length()).isLessThanOrEqualTo(4 * 100 + 4096);

    assertThat(statement.readLine(new StringReader(""))).isFalse();
  }

  /** Without maxPropertySize a term is bounded the way the delimited-text reading bounded a cell: 4,096 characters. */
  @Test
  void termLongerThanTheDefaultLimitIsRefused() {
    final String content = "<http://a/s> <http://a/p> \"" + "x".repeat(5_000) + "\" .\n";
    assertThatThrownBy(() -> load(new RDFImporterFormat(" "), settings(), content)).isInstanceOf(ImportException.class)
        .hasMessageContaining("maxPropertySize=4096");
  }

  /**
   * A term over the limit is refused while it is scanned: a 2,000-character literal within the line bound of a 1,024-character
   * limit leaves no more than the limit of its value built.
   */
  @Test
  void overlongTermIsRefusedBeforeItsValueIsBuilt() {
    final NTriplesStatement statement = new NTriplesStatement(" ", 1024);
    final String text = "<http://a/s> <http://a/p> \"" + "x".repeat(2_000) + "\" .";
    assertThatThrownBy(() -> statement.parse(text, new String[3])).isInstanceOf(ImportException.class)
        .hasMessageContaining("Term 3 is longer than maxPropertySize=1024 characters");
    assertThat(statement.builtTermLength()).isLessThanOrEqualTo(1024);
  }

  /** A byte-order mark at the start of the file is not part of the first subject. */
  @Test
  void leadingByteOrderMarkIsSkipped() throws Exception {
    load(new RDFImporterFormat(" "), settings(), """
        \uFEFF<http://a/s> <http://a/p> <http://a/o> .
        <http://a/s> <http://a/q> "\uFEFFx" .
        """);

    assertThat(keys()).containsExactlyInAnyOrder("http://a/s", "http://a/o", "\"\uFEFFx\"");
    assertThat(edges()).containsEntry("http://a/p", "http://a/s>http://a/o");
  }

  /**
   * A comma-delimited triple file that relied on CSV quoting: a quoted cell is an N-Triples literal now, kept with its quotes
   * in the key (it was the unquoted {@code a, b}), its unquoted text in {@code value}. The comma inside the quotes stays in it.
   */
  @Test
  void quotedCommaDelimitedCellIsALiteral() throws Exception {
    load(new RDFImporterFormat(","), settings(), """
        <http://a/s>,<http://a/p>,"a, b"
        """);

    assertThat(edges()).containsOnly(Map.entry("http://a/p", "http://a/s>\"a, b\""));
    assertThat(valueOf("\"a, b\"")).isEqualTo("a, b");
  }

  /**
   * A CSV doubled quote ({@code ""}) is not an N-Triples escape: the literal ends at the first quote. In the object the literal
   * is cut there and the rest of the line ignored, as anything after the third term is; in the subject the row is left short
   * and refused. A valid N-Triples file escapes the quote ({@code \"}), which {@code Issue12049RdfLiteralTermsTest} covers.
   */
  @Test
  void csvDoubledQuoteEndsTheLiteral() throws Exception {
    load(new RDFImporterFormat(","), settings(), "<http://a/s>,<http://a/p>,\"say \"\"hi\"\"\"\n");
    assertThat(edges()).containsOnly(Map.entry("http://a/p", "http://a/s>\"say \""));
    assertThat(valueOf("\"say \"")).isEqualTo("say ");

    assertThatThrownBy(() -> load(new RDFImporterFormat(","), settings(), """
        "a""b",<http://a/p>,<http://a/o>
        """)).isInstanceOf(ImportException.class).hasMessageContaining("has 1 term(s)");
  }

  /**
   * An error names the statement by its 0-based index, blank and comment lines not counted (the number the CSV parser gave a
   * row), and says so: the physical line is not what it counts.
   */
  @Test
  void errorNamesTheStatementIndexAndSaysSo() {
    assertThatThrownBy(() -> load(new RDFImporterFormat(" "), settings(), """
        # a comment
        <http://a/s> <http://a/p> <http://a/o> .
        # another comment

        <http://a/s> <http://a/p>
        """)).isInstanceOf(ImportException.class)
        .hasMessageContaining("Row at line 1 (0-based, blank and comment lines not counted) has 2 term(s)");
  }

  /** A language tag is ASCII letters, digits and '-' (BCP 47): a non-ASCII letter in it refuses the row. */
  @Test
  void nonAsciiLanguageTagIsRefused() throws Exception {
    load(new RDFImporterFormat(" "), settings(), """
        <http://a/s> <http://a/p> "colour"@en-GB .
        """);
    assertThat(keys()).containsExactlyInAnyOrder("http://a/s", "\"colour\"@en-GB");

    assertThatThrownBy(() -> load(new RDFImporterFormat(" "), settings(), """
        <http://a/s> <http://a/p> "chat"@fr\u00E7 .
        """)).isInstanceOf(ImportException.class).hasMessageContaining("Invalid language tag");
  }

  /** The graph term of an N-Quads statement is ignored, as it was: the statement is its first three terms. */
  @Test
  void nQuadsGraphTermIsIgnored() throws Exception {
    load(new RDFImporterFormat(" "), settings(), """
        <http://a/s> <http://a/p> <http://a/o> <http://a/g> .
        <http://a/s> <http://a/q> "x"@en <http://a/g> .
        """);

    assertThat(keys()).containsExactlyInAnyOrder("http://a/s", "http://a/o", "\"x\"@en");
    assertThat(edges()).containsOnly(Map.entry("http://a/p", "http://a/s>http://a/o"), Map.entry("http://a/q", "http://a/s>\"x\"@en"));
  }

  /**
   * An empty database and a transaction the caller holds: the vertex type and its unique index are created inside it, the
   * edges are left staged in it, and the caller's commit makes them durable.
   */
  @Test
  void callerOwnedTransactionOnAnEmptyDatabase() throws Exception {
    assertThat(database.getSchema().existsType("Node")).isFalse();

    database.begin();
    load(new RDFImporterFormat(" "), settings(), """
        <http://a/s> <http://a/p> "v" .
        <http://a/s> <http://a/q> <http://a/o> .
        """, true);
    assertThat(database.isTransactionActive()).as("the caller's transaction is left open").isTrue();
    database.commit();

    assertThat(database.getSchema().getType("Node").getPolymorphicIndexByProperties("id").isUnique()).isTrue();
    assertThat(edges()).containsOnly(Map.entry("http://a/p", "http://a/s>\"v\""), Map.entry("http://a/q", "http://a/s>http://a/o"));
    assertThat(valueOf("\"v\"")).isEqualTo("v");
  }

  /**
   * A schema change is not transactional: the type and index created for the import stay, empty, when the caller rolls its
   * transaction back and when the import fails in a transaction of its own.
   */
  @Test
  void typeAndIndexSurviveARolledBackImport() throws Exception {
    database.begin();
    load(new RDFImporterFormat(" "), settings(), """
        <http://a/s> <http://a/p> <http://a/o> .
        """, true);
    database.rollback();

    assertThat(database.getSchema().getType("Node").getPolymorphicIndexByProperties("id").isUnique()).isTrue();
    assertThat(keys()).isEmpty();

    database.getSchema().dropType("Node");
    assertThatThrownBy(() -> load(new RDFImporterFormat(" "), settings(), """
        <http://a/s> <http://a/p> <http://a/o> .
        <http://a/s> <http://a/p>
        """)).isInstanceOf(ImportException.class);

    assertThat(database.getSchema().getType("Node").getPolymorphicIndexByProperties("id").isUnique()).isTrue();
    assertThat(keys()).isEmpty();
    assertThat(edges()).isEmpty();
  }

  private ImporterContext load(final RDFImporterFormat format, final ImporterSettings settings, final String content)
      throws Exception {
    return load(format, settings, content, false);
  }

  private ImporterContext load(final RDFImporterFormat format, final ImporterSettings settings, final String content,
      final boolean callerTransaction) throws Exception {
    final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    final Parser parser = new Parser(new Source("test.nt", new ByteArrayInputStream(bytes), bytes.length, false, null, null), 0);
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = callerTransaction;
    format.load(null, null, parser, (DatabaseInternal) database, context, settings);
    return context;
  }

  private String valueOf(final String key) {
    try (final ResultSet rs = database.query("sql", "SELECT value FROM Node WHERE id = ?", key)) {
      return rs.next().getProperty("value");
    }
  }

  private static ImporterSettings settings() {
    final ImporterSettings settings = new ImporterSettings();
    settings.vertexTypeName = "Node";
    settings.edgeTypeName = "Related";
    settings.typeIdProperty = "id";
    return settings;
  }

  private List<String> keys() {
    final List<String> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM Node")) {
      while (rs.hasNext())
        keys.add(rs.next().getProperty("id"));
    }
    return keys;
  }

  /** Edge label to "out key>in key". */
  private Map<String, String> edges() {
    final Map<String, String> edges = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT label, @out.id AS s, @in.id AS o FROM Related")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        edges.put(row.getProperty("label"), row.getProperty("s") + ">" + row.getProperty("o"));
      }
    }
    return edges;
  }
}
