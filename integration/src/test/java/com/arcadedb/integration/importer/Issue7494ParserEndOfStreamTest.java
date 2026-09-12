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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7494: {@code Parser.isAvailable()} meant "readable right now without blocking", not "not at the end of the
 * source".
 * <p>
 * It was {@code reader.ready() || is.available() > 0}. Neither answers "is the stream exhausted":
 * {@code BufferedInputStream.available()} is {@code count - pos + in.available()}, and for a socket-backed stream
 * the underlying {@code available()} is the number of bytes that have ALREADY ARRIVED - zero between two packets of
 * a connection that is perfectly healthy and has more to send. So on a slow or chunked remote source
 * ({@code -url http://...}) the method answered {@code false} in the MIDDLE of the stream, and every caller - all of
 * them in {@code SourceDiscovery} - read that as end-of-input: the separator scan sniffed a partial first line, the
 * comment loop gave up part-way through the block, the 1024-character JSON probe stopped early. A local file's
 * {@code available()} is its remaining length, which is why no test had ever seen it.
 * <p>
 * {@link Trickle} below is a source that behaves exactly like that socket: it hands over one byte per read and
 * always reports zero bytes available. Nothing about it is pathological - it is what a well-behaved
 * {@code InputStream} is allowed to do, and what one over a network does routinely.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7494ParserEndOfStreamTest {

  private static final String CONTENT = "# a comment line\n<http://a/s1> <http://a/rel> <http://a/o1> .\n";

  /**
   * The defect itself, at its smallest: every character of the source has to be reachable through
   * {@code isAvailable()}, however grudgingly the stream hands its bytes over.
   */
  @Test
  void isAvailableWalksTheWholeSourceWhenNoByteIsEverReportedAsAvailable() throws IOException {
    final Parser parser = trickleParser(CONTENT);

    final StringBuilder read = new StringBuilder();
    while (parser.isAvailable())
      read.append(parser.nextChar());

    assertThat(read.toString())
        .as("isAvailable() answers 'not at the end', not 'a byte has already arrived'")
        .isEqualTo(CONTENT);
  }

  /**
   * And it must still stop: a peek that finds the end of the stream is not "available", and asking twice does not
   * consume anything.
   */
  @Test
  void isAvailableIsFalseAtTheEndAndStaysFalse() throws IOException {
    final Parser parser = trickleParser("ab");

    assertThat(parser.isAvailable()).isTrue();
    assertThat(parser.nextChar()).isEqualTo('a');
    assertThat(parser.isAvailable()).isTrue();
    assertThat(parser.nextChar()).isEqualTo('b');

    assertThat(parser.isAvailable()).as("the source is exhausted").isFalse();
    assertThat(parser.isAvailable()).as("and asking again neither consumes nor changes the answer").isFalse();
  }

  /**
   * The peeked character is the one {@code nextChar()} returns next: {@code isAvailable()} reads ahead to answer,
   * and a character read to answer a question must not be lost.
   */
  @Test
  void aCharacterReadToAnswerIsAvailableIsNotLost() throws IOException {
    final Parser parser = trickleParser("xyz");

    assertThat(parser.isAvailable()).isTrue();
    assertThat(parser.peekChar()).as("peeking does not advance").isEqualTo('x');
    assertThat(parser.peekChar()).isEqualTo('x');
    assertThat(parser.nextChar()).isEqualTo('x');
    assertThat(parser.getCurrentChar()).isEqualTo('x');
    assertThat(parser.nextChar()).isEqualTo('y');
  }

  /**
   * {@code nextChar()} narrows {@code Reader.read()}'s {@code -1} to {@code (char) 0xFFFF}, so the end of the source
   * arrives as a character value and a caller that did not guard the call cannot tell the two apart. The flag can.
   */
  @Test
  void theEndOfTheSourceIsReportedAsSuchAndNotOnlyAsACharacter() throws IOException {
    final Parser parser = trickleParser("a");

    assertThat(parser.nextChar()).isEqualTo('a');
    assertThat(parser.isEndOfStream()).isFalse();

    assertThat(parser.nextChar()).as("the historic value, kept so callers that test for it still work")
        .isEqualTo(Parser.END_OF_STREAM);
    assertThat(parser.isEndOfStream()).as("and the answer that does not depend on 0xFFFF being absent from the data")
        .isTrue();
  }

  /**
   * The whole point of the fix, one level up: content sniffing over a trickling source has to reach the same verdict
   * it reaches over the same bytes in a file. With the old {@code isAvailable()} the comment loop stopped inside the
   * comment block and the first data line was never sniffed.
   */
  @Test
  void contentSniffingReachesTheSameVerdictOverATricklingSource() throws IOException {
    final SourceSchema fromTrickle = analyze(new Trickle(bytes(CONTENT)));
    final SourceSchema fromMemory = analyze(new ByteArrayInputStream(bytes(CONTENT)));

    assertThat(fromTrickle.getContentImporter().getFormat())
        .as("the same bytes, only handed over one at a time")
        .isEqualTo(fromMemory.getContentImporter().getFormat())
        .isEqualTo("RDF");
  }

  // -----------------------------------------------------------------------------------------------------------

  private static byte[] bytes(final String content) {
    return content.getBytes(StandardCharsets.UTF_8);
  }

  private static Parser trickleParser(final String content) throws IOException {
    final byte[] raw = bytes(content);
    // skipLeadingComments=false: this test drives the parser's own character API, which is what content sniffing
    // uses and which sees the source whole
    return new Parser(new Source("trickle", new Trickle(raw), raw.length, false, null, null), 0, false);
  }

  private static SourceSchema analyze(final InputStream in) throws IOException {
    final ImporterSettings settings = new ImporterSettings();
    // AN EXTENSION-LESS URL: THE FORMAT HAS TO BE DECIDED BY SNIFFING THE CONTENT, WHICH IS THE ROUTE UNDER TEST
    settings.url = "memory://test";
    settings.edges = "memory://test";
    settings.documentTypeName = "Doc";
    settings.vertexTypeName = "Node";
    settings.edgeTypeName = "Related";

    final SourceDiscovery discovery = new SourceDiscovery("memory://test") {
      @Override
      public Source getSource() {
        return new Source("memory://test", in, -1, false, null, null);
      }
    };

    return discovery.getSchema(settings, AnalyzedEntity.EntityType.EDGE, new AnalyzedSchema(100), new ConsoleLogger(0));
  }

  /**
   * A source that never reports a byte as available and hands over one byte per read - which is what a socket does
   * between two packets, and what the fixed {@code isAvailable()} has to see through.
   */
  private static final class Trickle extends FilterInputStream {
    private Trickle(final byte[] content) {
      super(new ByteArrayInputStream(content));
    }

    @Override
    public int available() {
      return 0;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      if (len == 0)
        return 0;
      final int one = in.read();
      if (one < 0)
        return -1;
      b[off] = (byte) one;
      return 1;
    }
  }
}
