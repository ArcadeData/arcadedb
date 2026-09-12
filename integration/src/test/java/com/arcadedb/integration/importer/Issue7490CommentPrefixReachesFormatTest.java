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
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7490: content sniffing skipped the leading comment block and then handed it, intact, to the format that
 * decision picked.
 * <p>
 * Only some formats could cope. {@code #} happens to be univocity's own comment character, so the two delimited-text
 * formats ate it; {@code XMLImporterFormat} died with {@code "Error on importing from source ..."} and
 * {@code JSONImporterFormat} with {@code MalformedJsonException ... at line 1 column 2}, and nothing anywhere
 * skipped {@code //}, which broke delimited text too - {@code CSVImporterFormat.analyze()} read the {@code // c}
 * line as the header, so {@code fieldNames} was one column wide and the real header shifted into the data. Comment
 * support was half-built, and the half that existed was the half nobody could see.
 * <p>
 * The fix is the one place the issue asks for: {@link Parser} drops the block once, so
 * {@code getInputStream()}/{@code getReader()} - which is how EVERY format reads the source - start at the first
 * data line. {@code Issue7347CommentLinesSniffingTest} could assert the end-to-end import only for {@code #} on the
 * delimited-text formats, and said so; this asserts it for the rest.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7490CommentPrefixReachesFormatTest {

  private static final String DATA = "id,name\n1,Jay\n2,Elon\n";

  static Stream<Arguments> commentBlocks() {
    return Stream.of(//
        Arguments.of("none", ""), //
        Arguments.of("hash", "# exported on some date\n"), //
        Arguments.of("slash", "// exported on some date\n"), //
        Arguments.of("hash-block", "# exported on some date\n# by someone\n"), //
        Arguments.of("slash-block", "// exported on some date\n// by someone\n"), //
        Arguments.of("mixed", "# exported on some date\n// by someone\n"));
  }

  /**
   * The defect at the boundary it lives on, for every format at once: whatever a format reads the source through -
   * {@code getInputStream()} for XML, JSONL and the vector formats, {@code getReader()} for JSON, an
   * {@code InputStreamReader} over the stream for delimited text - it has to begin at the first data line.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("commentBlocks")
  void theStreamAFormatReadsBeginsAtTheFirstDataLine(final String name, final String comments) throws IOException {
    final Parser parser = parserOver(comments + DATA);

    assertThat(new String(parser.getInputStream().readAllBytes(), StandardCharsets.UTF_8))
        .as("the byte stream every format but JSON reads")
        .isEqualTo(DATA);

    final Parser second = parserOver(comments + DATA);
    final char[] buffer = new char[DATA.length() * 2];
    final int read = second.getReader().read(buffer);
    assertThat(new String(buffer, 0, Math.max(read, 0)))
        .as("and the character stream JSONImporterFormat reads")
        .isEqualTo(DATA);
  }

  /**
   * A {@code //} in the MIDDLE of a source is data, not a comment: only the LEADING block is dropped, which is
   * exactly what the sniffer skips. A protocol-relative URL in a value keeps its row.
   */
  @Test
  void onlyTheLeadingBlockIsDropped() throws IOException {
    final String content = "id,url\n1,//cdn.example.com/a\n";
    assertThat(new String(parserOver(content).getInputStream().readAllBytes(), StandardCharsets.UTF_8))
        .as("nothing to strip: the source does not open with a comment")
        .isEqualTo(content);

    assertThat(new String(parserOver("# c\n" + content).getInputStream().readAllBytes(), StandardCharsets.UTF_8))
        .as("the comment goes, the '//' value stays")
        .isEqualTo(content);
  }

  /**
   * A leading {@code /} followed by a NUL is data, and the NUL has to come back. The second byte is read only to
   * tell {@code //} from a data line opening with a single {@code /}, and its "was not read" sentinel has to be
   * distinct from {@code 0} - which is what {@code read()} answers for a real NUL byte - or the NUL is read and
   * never given back, which is the silent data loss this whole change is about.
   */
  @Test
  void aSlashFollowedByANulKeepsTheNul() throws IOException {
    final String content = "/\u0000id,name\n1,Jay\n";
    assertThat(new String(parserOver(content).getInputStream().readAllBytes(), StandardCharsets.UTF_8))
        .as("nothing is a comment here, so nothing is dropped")
        .isEqualTo(content);
  }

  /**
   * {@code available()} must never count the comment block as readable: those bytes are not readable from this
   * stream at all, and answering with them would tell a format that has not read yet there is more data waiting
   * than there is.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("commentBlocks")
  void availableNeverCountsTheCommentBlock(final String name, final String comments) throws IOException {
    final InputStream in = parserOver(comments + DATA).getInputStream();

    assertThat(in.available()).as("nothing is readable without blocking until the block has been dropped").isZero();

    assertThat(in.read()).as("the first byte of the data, not of the comment").isEqualTo(DATA.charAt(0));
    assertThat(in.available()).as("and once it has, the estimate is the data's").isEqualTo(DATA.length() - 1);
  }

  /**
   * The block is dropped on the BYTE stream, which is only the same operation as dropping it on the decoded
   * characters for a charset that encodes {@code #}, {@code /} and {@code \n} as the one ASCII byte each of them
   * is. For UTF-16 and UTF-32 a byte-oriented scan would match the LOW BYTE of an unrelated character and leave the
   * stream misaligned by one, so the guard has to answer false there and the block is left in place instead.
   * <p>
   * Asserted on the predicate rather than through an import, because
   * {@code DatabaseFactory.getDefaultCharset()} answers UTF-8 and only UTF-8: the false arm cannot be reached from
   * the outside today, which is the whole reason it is worth pinning down.
   */
  @Test
  void theByteLevelStripIsOnlyAppliedToAnAsciiCompatibleCharset() {
    assertThat(Parser.isCommentStrippableCharset(StandardCharsets.UTF_8)).isTrue();
    assertThat(Parser.isCommentStrippableCharset(StandardCharsets.US_ASCII)).isTrue();
    assertThat(Parser.isCommentStrippableCharset(StandardCharsets.ISO_8859_1)).isTrue();
    assertThat(Parser.isCommentStrippableCharset(Charset.forName("windows-1252"))).isTrue();

    assertThat(Parser.isCommentStrippableCharset(StandardCharsets.UTF_16))
        .as("'#' is 0x00 0x23 here, and its low byte would be matched at the wrong offset")
        .isFalse();
    assertThat(Parser.isCommentStrippableCharset(StandardCharsets.UTF_16LE)).isFalse();
    assertThat(Parser.isCommentStrippableCharset(StandardCharsets.UTF_16BE)).isFalse();
    assertThat(Parser.isCommentStrippableCharset(Charset.forName("UTF-32"))).isFalse();
  }

  /**
   * Content sniffing reads the source WHOLE - it walks the comment block itself, counting the lines so it can rewind
   * over them - so the stripping must not be applied under it.
   */
  @Test
  void contentSniffingStillSeesTheSourceWhole() throws IOException {
    final String content = "# c\n" + DATA;
    final Parser sniffing = new Parser(source(content), 0, false);

    final StringBuilder read = new StringBuilder();
    while (sniffing.isAvailable())
      read.append(sniffing.nextChar());

    assertThat(read.toString()).isEqualTo(content);
  }

  /**
   * XML end-to-end: the block reached the {@code XMLStreamReader}, which cannot begin a document on a {@code #} or a
   * {@code /}, so the whole import failed with {@code ImportException} - for BOTH comment markers.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("commentBlocks")
  void anXmlSourceImportsThroughItsLeadingCommentBlock(final String name, final String comments) throws Exception {
    final String databasePath = "target/databases/test-import-7490-xml-" + name;
    final File file = new File("target/importer-7490-xml-" + name + ".xml");
    Files.writeString(file.toPath(), comments + "<root><row id=\"1\" name=\"Jay\"/><row id=\"2\" name=\"Elon\"/></root>\n",
        StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(new String[] { "-documents", "file://" + file.getAbsolutePath(), "-database", databasePath,
          "-documentType", "Doc", "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        // THE XML FORMAT NAMES THE TYPE AFTER THE ELEMENT, NOT AFTER -documentType
        assertThat(db.countType("row", true))
            .as("a commented source imports exactly like the identical source without the comments")
            .isEqualTo(2);
      }
    } finally {
      dropAndDelete(databasePath, file);
    }
  }

  /**
   * JSON end-to-end: {@code JSONImporterFormat} reads through {@code parser.getReader()}, which is the same stream
   * by another accessor and was equally unfiltered - Gson reported a malformed document at line 1 column 2.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("commentBlocks")
  void aJsonSourceImportsThroughItsLeadingCommentBlock(final String name, final String comments) throws Exception {
    final String databasePath = "target/databases/test-import-7490-json-" + name;
    final File file = new File("target/importer-7490-json-" + name + ".json");
    Files.writeString(file.toPath(), comments + "{\n  \"id\": 1,\n  \"name\": \"Jay\"\n}\n", StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(new String[] { "-documents", "file://" + file.getAbsolutePath(), "-database", databasePath,
          "-documentType", "Doc", "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("Doc", true)).isEqualTo(1);
        assertThat(db.query("sql", "select from Doc").nextIfAvailable().<String>getProperty("name")).isEqualTo("Jay");
      }
    } finally {
      dropAndDelete(databasePath, file);
    }
  }

  /**
   * Delimited text, which the move out of {@code CSVImporterFormat} must not regress: {@code #} worked by accident
   * of univocity's default comment character, {@code //} because #7347 had already taught the format to drop it.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("commentBlocks")
  void aCsvSourceImportsThroughItsLeadingCommentBlock(final String name, final String comments) throws Exception {
    final String databasePath = "target/databases/test-import-7490-csv-" + name;
    final File file = new File("target/importer-7490-csv-" + name + ".csv");
    Files.writeString(file.toPath(), comments + DATA, StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(new String[] { "-documents", "file://" + file.getAbsolutePath(), "-database", databasePath,
          "-documentType", "Doc", "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("Doc", true)).isEqualTo(2);
        assertThat(db.getSchema().getType("Doc").getPropertyNames())
            .as("the properties come from the real header row, not from the words of the comment")
            .contains("id", "name");
      }
    } finally {
      dropAndDelete(databasePath, file);
    }
  }

  // -----------------------------------------------------------------------------------------------------------

  private static Source source(final String content) {
    final byte[] raw = content.getBytes(StandardCharsets.UTF_8);
    final InputStream in = new ByteArrayInputStream(raw);
    return new Source("test", in, raw.length, false, null, null);
  }

  private static Parser parserOver(final String content) throws IOException {
    return new Parser(source(content), 0);
  }

  private void dropAndDelete(final String databasePath, final File file) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    file.delete();
    TestHelper.checkActiveDatabases();
  }
}
