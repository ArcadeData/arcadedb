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
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7501: {@code Parser.getCurrentChar()} answered {@code 0} both for "nothing has been read yet" - the state
 * {@code reset()} leaves - and for a NUL the source really carries, and {@code SourceDiscovery.readLine()} read the
 * value as the former.
 * <p>
 * A genuine NUL has {@code isEndOfStream() == false} and {@code getCurrentChar() == 0}, so it took the "start from
 * the first character of the source" branch and was dropped: the line handed to the separator scan and to
 * {@code analyzeChar()} was one character shorter than the source. It is the same sentinel collision that
 * {@code CSVImporterFormat.sourceReader()} had and that #7497 closed there with a distinct {@code NOT_READ = -2};
 * this is the copy of it that loop could not reach.
 * <p>
 * A sentinel {@code char} cannot fix it - every {@code char} value is a legal character - so the parser answers the
 * question itself, with {@link Parser#isBeforeFirstChar()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7501ParserNulIsNotNothingReadTest {

  private static final char NUL = (char) 0;

  /**
   * The collision itself: after reading a real NUL the parser must NOT look like one that has read nothing.
   */
  @Test
  void aNulThatWasReadIsNotTheSameAsHavingReadNothing() throws IOException {
    final Parser parser = parserOver(NUL + "id,name\n");

    assertThat(parser.isBeforeFirstChar()).as("nothing read yet").isTrue();
    assertThat(parser.getCurrentChar()).as("and the placeholder is indistinguishable from a NUL").isEqualTo(NUL);

    assertThat(parser.nextChar()).as("the source's first character is a NUL").isEqualTo(NUL);
    assertThat(parser.isBeforeFirstChar()).as("which the parser must not report as 'nothing read yet'").isFalse();
    assertThat(parser.isEndOfStream()).as("and it is a character, not the end of the source").isFalse();

    parser.reset();
    assertThat(parser.isBeforeFirstChar()).as("reset puts it back").isTrue();
    assertThat(parser.getCurrentChar()).isEqualTo(NUL);
  }

  /**
   * The consequence at the reader that had it: a line the parser is already positioned on keeps its leading NUL.
   * That is exactly what the comment-block walk in {@code analyzeText} produces - {@code skipLine()} leaves the
   * parser on the first character of the NEXT line, and that character is the one that used to disappear.
   */
  @Test
  void aLineTheParserIsAlreadyOnKeepsItsLeadingNul() throws IOException {
    final Parser parser = parserOver("# exported\n" + NUL + "id,name\n1,Jay\n");

    // Step over the comment line the way analyzeText does; the parser lands on the NUL that opens line 2.
    assertThat(SourceDiscovery.readLine(parser)).isEqualTo("# exported");
    assertThat(parser.getCurrentChar()).isEqualTo(NUL);
    assertThat(parser.isBeforeFirstChar()).isFalse();

    assertThat(SourceDiscovery.readLine(parser))
        .as("the sniffed line is what the source says, NUL included")
        .isEqualTo(NUL + "id,name");
    assertThat(SourceDiscovery.readLine(parser)).isEqualTo("1,Jay");
  }

  /**
   * The other half of the same statement: a parser that genuinely has read nothing still starts from the first
   * character of the source rather than prepending the placeholder, and a NUL first character is read exactly once.
   */
  @Test
  void aParserThatHasReadNothingStillStartsAtTheFirstCharacter() throws IOException {
    assertThat(SourceDiscovery.readLine(parserOver("id,name\n1,Jay\n")))
        .as("no character is prepended and none is lost")
        .isEqualTo("id,name");

    assertThat(SourceDiscovery.readLine(parserOver(NUL + "id,name\n")))
        .as("a NUL first character is read once, not zero times and not twice")
        .isEqualTo(NUL + "id,name");
  }

  /**
   * An empty source: {@code readLine()} must not turn the end-of-stream marker into a character, which is the
   * defence the {@code isEndOfStream()} conjunct is there for (issue #7494). A parser positioned past the end of
   * the source is not "before the first character" either, so the two states stay distinct at both ends.
   */
  @Test
  void anEmptySourceYieldsAnEmptyLine() throws IOException {
    assertThat(SourceDiscovery.readLine(parserOver(""))).isEmpty();

    final Parser exhausted = parserOver("a");
    exhausted.nextChar();
    exhausted.nextChar();
    assertThat(exhausted.isEndOfStream()).isTrue();
    assertThat(exhausted.isBeforeFirstChar()).as("the end of the source is not the start of it").isFalse();
    assertThat(SourceDiscovery.readLine(exhausted)).isEmpty();
  }

  /**
   * End to end: a NUL-leading delimited-text source is still recognised as delimited text and imported whole,
   * comment block or not. The NUL is not a separator candidate, so keeping it changes no answer the sniffer gives -
   * which is the point: the fix restores the character the sniffer works on without moving the sniffing.
   */
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "none", "hash", "slash" })
  void aNulLeadingSourceStillImportsWhole(final String commentBlock) throws Exception {
    final String comments = switch (commentBlock) {
      case "hash" -> "# exported\n";
      case "slash" -> "// exported\n";
      default -> "";
    };

    final String databasePath = "target/databases/test-import-7501-" + commentBlock;
    final File file = new File("target/importer-7501-" + commentBlock + ".txt");
    Files.writeString(file.toPath(), comments + NUL + "id;name\n1;Jay\n2;Ann\n", StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(new String[] { "-documents", "file://" + file.getAbsolutePath(), "-database", databasePath,
          "-documentType", "Doc", "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("Doc", true)).as("both data rows arrive").isEqualTo(2);
        // The header's FIRST column carries the NUL, because the source says it does. The second does not, and is
        // what pins that the delimiter was still read as ';'.
        assertThat(db.iterateType("Doc", true).next().asDocument().getString("name")).isIn("Jay", "Ann");
      }
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      file.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  private static Parser parserOver(final String content) throws IOException {
    final byte[] raw = content.getBytes(StandardCharsets.UTF_8);
    final InputStream in = new ByteArrayInputStream(raw);
    // skipLeadingComments=false: this is the shape content sniffing builds, which walks the comment block itself.
    return new Parser(new Source("test", in, raw.length, false, null, null), 0, false);
  }
}
