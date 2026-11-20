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
import java.io.IOException;
import java.io.Reader;
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
    final StringReader reader = new StringReader(overlong + "\r\n<http://a/s> <http://a/p> <http://a/o> .\n");
    assertThat(statement.readLine(reader)).isTrue();
    assertThat(statement.lineLength()).isEqualTo(overlong.length());
    assertThat(statement.lineTooLong()).isTrue();
    assertThat(statement.line().length()).isEqualTo(4 * 100 + 4096);

    assertThat(statement.readLine(reader)).as("the line after the overlong one is read whole").isTrue();
    assertThat(statement.lineTooLong()).isFalse();
    assertThat(statement.line()).isEqualTo("<http://a/s> <http://a/p> <http://a/o> .");

    assertThat(statement.readLine(reader)).isFalse();
  }

  /** Lines end at LF, CR and CRLF; an empty line is a line, and the end of the source is not one. */
  @Test
  void linesEndAtLfCrAndCrLf() throws Exception {
    assertThat(lines(new StringReader("a\nb\r\nc\rd\n\ne"))).containsExactly("a", "b", "c", "d", "", "e");
    assertThat(lines(new StringReader("a\n"))).containsExactly("a");
    assertThat(lines(new StringReader("a\r"))).containsExactly("a");
    assertThat(lines(new StringReader("a\r\n"))).containsExactly("a");
    assertThat(lines(new StringReader("a\n\n"))).containsExactly("a", "");
    assertThat(lines(new StringReader("\n"))).containsExactly("");
    assertThat(lines(new StringReader(""))).isEmpty();
  }

  /**
   * The source is read a chunk at a time: a CRLF split between two chunks is one line end, a line longer than a chunk is one line,
   * and a reader that hands out one or three characters per call, as a network stream can, gives the same lines.
   */
  @Test
  void linesAreTheSameWhereverTheReaderSplitsTheSource() throws Exception {
    for (int shift = -2; shift <= 2; shift++) {
      final String first = "a".repeat(NTriplesStatement.CHUNK_SIZE + shift);
      // THE CR IS THE LAST CHARACTER OF A CHUNK FOR shift == -1, AND THE LF THE FIRST OF THE NEXT
      final String source = first + "\r\n" + "b\n" + "\r" + "c".repeat(2 * NTriplesStatement.CHUNK_SIZE) + "\r\n";
      final List<String> expected = List.of(first, "b", "", "c".repeat(2 * NTriplesStatement.CHUNK_SIZE));

      assertThat(lines(new StringReader(source))).as("whole chunks, shift %d", shift).isEqualTo(expected);
      assertThat(lines(new TrickleReader(source, 1))).as("one character per read, shift %d", shift).isEqualTo(expected);
      assertThat(lines(new TrickleReader(source, 3))).as("three characters per read, shift %d", shift).isEqualTo(expected);
    }
  }

  /** The byte-order mark is skipped at the start of the source only, and a source of nothing else has no line. */
  @Test
  void byteOrderMarkIsSkippedAtTheStartOfTheSourceOnly() throws Exception {
    assertThat(lines(new StringReader("﻿a\n﻿b"))).containsExactly("a", "﻿b");
    assertThat(lines(new TrickleReader("﻿a\n", 1))).containsExactly("a");
    assertThat(lines(new StringReader("﻿"))).isEmpty();
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
   * A CSV doubled quote ({@code ""}) is not an N-Triples escape: the literal ends at the first quote, and the text after it is
   * refused as a row error, in the object as in the subject. It was cut off there with the rest of the row ignored, so
   * {@code "say ""hi"""} was imported as the literal {@code "say "} with a result of OK. A valid N-Triples file escapes the quote
   * ({@code \"}), which {@code Issue12049RdfLiteralTermsTest} covers.
   */
  @Test
  void csvDoubledQuoteIsRefusedNotCutShort() throws Exception {
    assertThatThrownBy(() -> load(new RDFImporterFormat(","), settings(), "<http://a/s>,<http://a/p>,\"say \"\"hi\"\"\"\n"))
        .isInstanceOf(ImportException.class).hasMessageContaining("Unexpected text after the literal");
    assertThat(edges()).isEmpty();

    assertThatThrownBy(() -> load(new RDFImporterFormat(","), settings(), """
        "a""b",<http://a/p>,<http://a/o>
        """)).isInstanceOf(ImportException.class).hasMessageContaining("Unexpected text after the literal");

    // -onRowError skip GOES ON WITH THE NEXT STATEMENT
    final ImporterSettings skip = settings();
    skip.onRowError = "skip";
    final ImporterContext context = load(new RDFImporterFormat(","), skip,
        "<http://a/s>,<http://a/p>,\"say \"\"hi\"\"\"\n<http://a/s>,<http://a/q>,\"fine\"\n");
    assertThat(context.errors.get()).isEqualTo(1);
    assertThat(edges()).containsOnly(Map.entry("http://a/q", "http://a/s>\"fine\""));
  }

  /** A literal is followed by the end of the line, whitespace, the delimiter, the terminator, a comment or the next term. */
  @Test
  void whatMayFollowALiteral() {
    final NTriplesStatement statement = new NTriplesStatement(" ", 4096);
    final String[] terms = new String[3];
    for (final String object : new String[] { "\"a\".", "\"a\" .", "\"a\"#comment", "\"a\"<http://a/g> .", "\"a\"_:g .", "\"a\"" }) {
      assertThat(statement.parse("<http://a/s> <http://a/p> " + object, terms)).as(object).isEqualTo(3);
      assertThat(terms[2]).as(object).isEqualTo("\"a\"");
    }
    assertThat(statement.parse("<http://a/s> <http://a/p> \"a\"@en.", terms)).isEqualTo(3);
    assertThat(terms[2]).isEqualTo("\"a\"@en");
    assertThat(statement.parse("<http://a/s> <http://a/p> \"a\"^^<http://a/t>.", terms)).isEqualTo(3);
    assertThat(terms[2]).isEqualTo("\"a\"^^<http://a/t>");

    final NTriplesStatement comma = new NTriplesStatement(",", 4096);
    assertThat(comma.parse("<http://a/s>,<http://a/p>,\"a\",<http://a/g>", terms)).isEqualTo(3);
    assertThat(terms[2]).isEqualTo("\"a\"");

    for (final String object : new String[] { "\"a\"b", "\"a\"\"b\"", "\"a\"@en@fr", "\"a\"^b", "\"a\"@en\"b\"", "\"a\"^^<http://a/t>x" })
      assertThatThrownBy(() -> statement.parse("<http://a/s> <http://a/p> " + object, terms)).as(object)
          .isInstanceOf(ImportException.class).hasMessageContaining("Unexpected text after the literal");
  }

  /** {@code ^^} is followed by an IRI between angle brackets: a prefixed name is Turtle, and it used to lose the datatype silently. */
  @Test
  void prefixedDatatypeIsRefused() throws Exception {
    final NTriplesStatement statement = new NTriplesStatement(" ", 4096);
    assertThatThrownBy(() -> statement.parse("<http://a/s> <http://a/p> \"42\"^^xsd:integer .", new String[3]))
        .isInstanceOf(ImportException.class).hasMessageContaining("Invalid datatype at column 31");

    final ImporterSettings skip = settings();
    skip.onRowError = "skip";
    final ImporterContext context = load(new RDFImporterFormat(" "), skip, """
        <http://a/s> <http://a/p> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
        <http://a/s> <http://a/q> "42"^^xsd:integer .
        <http://a/s> <http://a/r> "3" .
        """);
    assertThat(context.errors.get()).isEqualTo(1);
    assertThat(edges()).containsOnlyKeys("http://a/p", "http://a/r");
  }

  /** An escape is {@code \}{@code u} and four, or {@code \}{@code U} and eight, ASCII hexadecimal digits: no sign, no other digit. */
  @Test
  void escapesTakeHexadecimalDigitsOnly() {
    final NTriplesStatement statement = new NTriplesStatement(" ", 4096);
    final String[] terms = new String[3];
    assertThat(statement.parse("<http://a/\\u0041> <http://a/p> \"\\u00e9\\U0001F600\\U0010FFFF\" .", terms)).isEqualTo(3);
    assertThat(terms[0]).isEqualTo("http://a/A");
    assertThat(terms[2]).isEqualTo("\"\u00e9\uD83D\uDE00\uDBFF\uDFFF\"");

    for (final String escape : new String[] { "\\u+041", "\\u-041", "\\u00G1", "\\u004", "\\u\uFF10\uFF10\uFF10\uFF11", "\\U0000+041",
        "\\U00110000", "\\U0000004" })
      assertThatThrownBy(() -> statement.parse("<http://a/s> <http://a/p> \"" + escape + "\" .", terms)).as(escape)
          .isInstanceOf(ImportException.class);
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

  /** Every line {@code reader} holds, read the way the importer reads them. */
  private static List<String> lines(final Reader reader) throws IOException {
    final NTriplesStatement statement = new NTriplesStatement(" ", 0);
    final List<String> lines = new ArrayList<>();
    while (statement.readLine(reader))
      lines.add(statement.line());
    return lines;
  }

  /** A reader that gives at most {@code step} characters per read, as a stream that delivers in small pieces does. */
  private static final class TrickleReader extends Reader {
    private final String source;
    private final int    step;
    private       int    position;

    TrickleReader(final String source, final int step) {
      this.source = source;
      this.step = step;
    }

    @Override
    public int read(final char[] buffer, final int offset, final int length) {
      if (position >= source.length())
        return -1;
      final int n = Math.min(Math.min(step, length), source.length() - position);
      source.getChars(position, position + n, buffer, offset);
      position += n;
      return n;
    }

    @Override
    public void close() {
      // NOTHING TO RELEASE
    }
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
