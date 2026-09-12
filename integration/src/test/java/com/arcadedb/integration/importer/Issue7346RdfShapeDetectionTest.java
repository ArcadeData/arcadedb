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
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7346: RDF detection rejected two canonical N-Triples shapes, so an ordinary RDF file was imported as CSV
 * and died with a {@code NumberFormatException} about an IRI.
 * <p>
 * The old test collected every character that fell OUTSIDE {@code <...>} and required them all to be equal, with a
 * loop bound of {@code size() - 1} that tolerated exactly one trailing character - the {@code .} of a canonical
 * triple. Both failures follow from that single tolerated character:
 * <ul>
 * <li>a <b>CRLF</b> line ending adds a second one, so the loop reaches the {@code .} and gives up;</li>
 * <li>a <b>literal object</b> is not inside {@code <...>}, so every character of its text is collected and the
 * comparison disagrees at the first one.</li>
 * </ul>
 * The second is the common case: an RDF file whose objects are literals rather than IRIs. The replacement tests
 * the SHAPE of the statement - {@code subject predicate object [.]} - which is stricter than "every character
 * outside the brackets is identical", not looser: it requires two IRI-shaped terms where the old test required
 * only matching bracket counts.
 * <p>
 * Every case here runs the live {@code Importer.load()} with NO {@code -delimiter}, which is the whole point:
 * passing one is the workaround the failure gave the user no way to find.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7346RdfShapeDetectionTest {

  /** The shape that already worked, kept so a regression in it is not read as a regression in the new ones. */
  @Test
  void lineFeedWithIriObjects() throws Exception {
    assertImportsAsRdf("lf-iri", """
        <http://a/s1> <http://a/rel> <http://a/o1> .
        <http://a/s2> <http://a/rel> <http://a/o2> .
        <http://a/s3> <http://a/rel> <http://a/o3> .
        """);
  }

  /** A CRLF line ending is canonical N-Triples and used to leave a second trailing character on the line. */
  @Test
  void carriageReturnLineFeedWithIriObjects() throws Exception {
    assertImportsAsRdf("crlf-iri",
        "<http://a/s1> <http://a/rel> <http://a/o1> .\r\n"
            + "<http://a/s2> <http://a/rel> <http://a/o2> .\r\n"
            + "<http://a/s3> <http://a/rel> <http://a/o3> .\r\n");
  }

  /** The common case: the objects are literals, so every character of them fell outside the angle brackets. */
  @Test
  void lineFeedWithLiteralObjects() throws Exception {
    assertImportsAsRdf("lf-literal", """
        <http://a/s1> <http://a/rel> "hello world" .
        <http://a/s2> <http://a/rel> "second value" .
        <http://a/s3> <http://a/rel> "third value" .
        """);
  }

  /** Both at once, which is what an RDF file exported on Windows actually looks like. */
  @Test
  void carriageReturnLineFeedWithLiteralObjects() throws Exception {
    assertImportsAsRdf("crlf-literal",
        "<http://a/s1> <http://a/rel> \"hello world\" .\r\n"
            + "<http://a/s2> <http://a/rel> \"second value\" .\r\n"
            + "<http://a/s3> <http://a/rel> \"third value\" .\r\n");
  }

  /** A tab-separated file is N-Triples too, and the separator has to reach the parser as a tab. */
  @Test
  void tabSeparatedTermsAreRecognisedAndParsedAsTabs() throws Exception {
    assertImportsAsRdf("tab-iri",
        "<http://a/s1>\t<http://a/rel>\t<http://a/o1>\t.\n"
            + "<http://a/s2>\t<http://a/rel>\t<http://a/o2>\t.\n"
            + "<http://a/s3>\t<http://a/rel>\t<http://a/o3>\t.\n");
  }

  /** A blank-node subject does not even begin with '<', so the dispatch never used to look at the line at all. */
  @Test
  void blankNodeSubjectsAreRecognised() throws Exception {
    assertImportsAsRdf("blank-subject", """
        _:b1 <http://a/rel> <http://a/o1> .
        _:b2 <http://a/rel> <http://a/o2> .
        _:b3 <http://a/rel> <http://a/o3> .
        """);
  }

  /** A typed literal carries an IRI after the closing quote, and a tagged one a language. */
  @Test
  void typedAndLanguageTaggedLiteralsAreRecognised() throws Exception {
    assertImportsAsRdf("typed-literal", """
        <http://a/s1> <http://a/rel> "12"^^<http://www.w3.org/2001/XMLSchema#integer> .
        <http://a/s2> <http://a/rel> "bonjour"@fr .
        <http://a/s3> <http://a/rel> "hello"@en-GB .
        """);
  }

  /**
   * The dispatch now looks at a line starting with {@code _} as well, because a blank-node subject does not begin
   * with {@code <}. A delimited file whose first column is named {@code _id} starts with the same character and
   * must still be sniffed as delimited text: reading the first line to decide is not the same as consuming it.
   */
  @Test
  void aDelimitedFileWhoseFirstColumnStartsWithUnderscoreIsStillDelimitedText() throws Exception {
    final String databasePath = "target/databases/test-import-7346-underscore-header";
    final File file = new File("target/importer-7346-underscore-header.txt");
    Files.writeString(file.toPath(), """
        _id;name;score
        1;first;10
        2;second;20
        """, StandardCharsets.UTF_8);

    try {
      new Importer(new String[] { "-url", "file://" + file.getAbsolutePath(), "-database", databasePath,
          "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("Document", true)).isEqualTo(2);
        assertThat(db.iterateType("Document", true).next().asDocument(true).getPropertyNames())
            .as("the header is still read from the first line, and split on the sniffed separator")
            .contains("_id", "name", "score");
      }
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      file.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The detector is a hand-rolled index-chasing scan, and it runs on the first line of whatever file a user
   * points the importer at. What must hold for EVERY input, not only the shapes above, is that it terminates and
   * returns rather than walking off the end of the buffer: an {@code ArrayIndexOutOfBoundsException} out of format
   * sniffing would be reported as a broken file rather than as the bug it is.
   * <p>
   * Two generators. The first mutates a valid triple one character at a time - delete, duplicate, truncate,
   * substitute - which is where an off-by-one in a four-method recursive descent actually lives. The second is
   * random text over the alphabet that matters ({@code < > " _ : . \ } separators, letters), which reaches the
   * shapes a mutation cannot.
   * <p>
   * The invariant asserted on top of "it returned" is the one worth having and cheap to check independently: a
   * line the detector ACCEPTS carries at least two {@code <...>} terms, because subject and predicate are the two
   * places the grammar admits nothing else. A regression that started accepting delimited text would break it.
   */
  @Test
  @Timeout(30)
  void theDetectorTerminatesAndStaysStrictOnEveryNearMiss() {
    final String valid = "<http://a/s1> <http://a/rel> \"hello world\"@en .";
    final char[] alphabet = { '<', '>', '"', '_', ':', '.', ' ', '\t', ',', ';', '|', '\r', '\\', '@', '^', 'a', '1' };
    final Random random = new Random(7346);

    final List<String> candidates = new ArrayList<>();
    for (int i = 0; i < valid.length(); i++) {
      candidates.add(valid.substring(0, i) + valid.substring(i + 1));                    // delete
      candidates.add(valid.substring(0, i) + valid.charAt(i) + valid.substring(i));      // duplicate
      candidates.add(valid.substring(0, i));                                             // truncate
      for (final char c : alphabet)
        candidates.add(valid.substring(0, i) + c + valid.substring(i + 1));              // substitute
    }
    for (int i = 0; i < 5_000; i++) {
      final StringBuilder line = new StringBuilder();
      for (int j = random.nextInt(24); j > 0; --j)
        line.append(alphabet[random.nextInt(alphabet.length)]);
      candidates.add(line.toString());
    }

    int accepted = 0;
    for (final String candidate : candidates) {
      final char separator = SourceDiscovery.nTriplesSeparator(candidate);
      if (separator == 0)
        continue;
      ++accepted;
      assertThat(iriTermCount(candidate))
          .as("accepted as a triple, so subject and predicate are both IRIs: <%s>", candidate)
          .isGreaterThanOrEqualTo(2);
      assertThat(SourceDiscovery.nTriplesSeparator(candidate))
          .as("the same line answers the same way twice: <%s>", candidate)
          .isEqualTo(separator);
    }
    // Without this the whole loop is vacuous: a detector that rejected everything would satisfy every assertion
    // above. 458 of the 5,940 candidates are accepted as this is written, and the single-character mutations of a
    // valid triple are most of them, which is the population the invariant is worth checking on.
    assertThat(accepted).as("the near misses have to include some the detector accepts").isGreaterThan(50);
  }

  /** The number of non-empty {@code <...>} runs in the line, counted without the detector's own helpers. */
  private static int iriTermCount(final String line) {
    int count = 0;
    for (int open = line.indexOf('<'); open >= 0; open = line.indexOf('<', open + 1)) {
      final int close = line.indexOf('>', open + 1);
      if (close > open + 1) {
        ++count;
        open = close;
      }
    }
    return count;
  }

  // -----------------------------------------------------------------------------------------------------------
  // What must NOT be read as RDF. The shape test is stricter than the old uniqueness one, and these pin that.
  // -----------------------------------------------------------------------------------------------------------

  /** An XML document opens with '<' and matching brackets, and is still not a triple. */
  @Test
  void anXmlDocumentIsStillDetectedAsXml() {
    assertThat(SourceDiscovery.nTriplesSeparator("<row id=\"1\" name=\"first\"/>")).isEqualTo((char) 0);
    assertThat(SourceDiscovery.nTriplesSeparator("<a>text</a>")).isEqualTo((char) 0);
    assertThat(SourceDiscovery.nTriplesSeparator("<?xml version=\"1.0\"?>")).isEqualTo((char) 0);
  }

  /** Three terms are needed, and the whole line has to be consumed by them. */
  @Test
  void aLineThatIsNotAWholeStatementIsRejected() {
    assertThat(SourceDiscovery.nTriplesSeparator("<http://a/s> <http://a/p>")).as("no object").isEqualTo((char) 0);
    assertThat(SourceDiscovery.nTriplesSeparator("<http://a/s> <http://a/p> <http://a/o> . trailing"))
        .as("something after the terminator").isEqualTo((char) 0);
    assertThat(SourceDiscovery.nTriplesSeparator("<http://a/s> <http://a/p>,<http://a/o> ."))
        .as("one separator per file, not a different one per gap").isEqualTo((char) 0);
    assertThat(SourceDiscovery.nTriplesSeparator("<http://a/s>-<http://a/p>-<http://a/o>"))
        .as("'-' belongs inside a language tag, so it is not offered as a separator").isEqualTo((char) 0);
    assertThat(SourceDiscovery.nTriplesSeparator("\"a\" \"b\" \"c\" ."))
        .as("a literal is a term only in the object position").isEqualTo((char) 0);
    assertThat(SourceDiscovery.nTriplesSeparator("")).isEqualTo((char) 0);
  }

  /** The separator that comes back is the source's field delimiter, so it has to be the one actually used. */
  @Test
  void theSeparatorReturnedIsTheOneBetweenTheTerms() {
    assertThat(SourceDiscovery.nTriplesSeparator("<http://a/s> <http://a/p> <http://a/o> .")).isEqualTo(' ');
    assertThat(SourceDiscovery.nTriplesSeparator("<http://a/s>\t<http://a/p>\t<http://a/o>\t.")).isEqualTo('\t');
    assertThat(SourceDiscovery.nTriplesSeparator("<http://a/s> <http://a/p> <http://a/o> .\r"))
        .as("the \\r of a CRLF line is trailing whitespace, not a fourth term").isEqualTo(' ');
    assertThat(SourceDiscovery.nTriplesSeparator("<http://a/s>,<http://a/p>,<http://a/o>"))
        .as("a comma-separated triple file was accepted before this change and still is").isEqualTo(',');
    assertThat(SourceDiscovery.nTriplesSeparator("<http://a/s>   <http://a/p>   <http://a/o> ."))
        .as("a run of the separator is one gap, not three empty terms").isEqualTo(' ');
  }

  // -----------------------------------------------------------------------------------------------------------

  /**
   * Imports {@code content} through the live CLI path and asserts the three triples became three edges.
   * <p>
   * Three and no longer two: the RDF format used to skip its first line as a header, which is what the issue's own
   * repro table recorded ({@code parsedRecords=3, createdEdges=2}) and what #7345 has since fixed - an RDF source
   * has no header row, so the first triple is data like every other line.
   */
  private void assertImportsAsRdf(final String name, final String content) throws Exception {
    final String databasePath = "target/databases/test-import-7346-" + name;
    // No .nt / .rdf extension: the format has to be decided by sniffing the content, which is what is under test
    final File file = new File("target/importer-7346-" + name + ".txt");
    Files.writeString(file.toPath(), content, StandardCharsets.UTF_8);

    // The vertex type the RDF format resolves its subjects and objects against. Created up front rather than left
    // to the import, because an RDF source's analysis registers only the EDGE type - the same setup every other
    // RDFImporterFormat test does, and nothing this issue is about.
    FileUtils.deleteRecursively(new File(databasePath));
    try (final Database seed = new DatabaseFactory(databasePath).create()) {
      seed.transaction(() -> {
        seed.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
        seed.getSchema().getType("Node").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { "id" });
        seed.getSchema().createEdgeType("Related");
      });
    }

    try {
      final Map<String, Object> result = new Importer(new String[] { "-url", "file://" + file.getAbsolutePath(),
          "-database", databasePath, "-edgeType", "Related" }).load();

      assertThat(result).as("every line reached the parser and every one of them became an edge (#7345)")
          .containsEntry("parsedRecords", 3L).containsEntry("createdEdges", 3L);

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("Related", true)).isEqualTo(3);
      }
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      file.delete();
    }
    TestHelper.checkActiveDatabases();
  }
}
