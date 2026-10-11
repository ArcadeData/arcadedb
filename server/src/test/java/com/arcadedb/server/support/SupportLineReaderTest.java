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
package com.arcadedb.server.support;

import org.junit.jupiter.api.Test;

import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The strict line reader the AI Assistant reads its peers' streams with (issue #9627). */
class SupportLineReaderTest {

  /** A peer that sends bytes forever and never a newline; counts what was read. */
  private static final class EndlessLine extends Reader {
    long read;

    @Override
    public int read(final char[] buffer, final int offset, final int length) {
      Arrays.fill(buffer, offset, offset + length, 'a');
      read += length;
      return length;
    }

    @Override
    public void close() {
    }
  }

  @Test
  void aStrictReaderStopsAtTheCapOfALineThatNeverEnds() {
    final EndlessLine peer = new EndlessLine();
    final SupportLineReader reader = SupportLineReader.failingOver(peer, 1_000);

    assertThatThrownBy(reader::readLine).isInstanceOf(SupportLineReader.LineTooLongException.class)
        .hasMessageContaining("1000 characters");
    // It gave up within one chunk of the cap rather than reading (and dropping) the rest of the line
    assertThat(peer.read).isLessThanOrEqualTo(1_000 + 2 * 65_536);
  }

  @Test
  void aStrictReaderReadsLinesUpToTheCapUnchanged() throws Exception {
    final String atCap = "x".repeat(16);
    final SupportLineReader reader = SupportLineReader.failingOver(new StringReader("a\r\n" + atCap + "\n\nlast"), 16);

    assertThat(reader.readLine()).isEqualTo("a");
    assertThat(reader.readLine()).isEqualTo(atCap);
    assertThat(reader.readLine()).isEmpty();
    assertThat(reader.readLine()).isEqualTo("last");
    assertThat(reader.readLine()).isNull();
  }

  @Test
  void aStrictReaderFailsOnALineOneOverTheCapSpreadAcrossChunks() throws Exception {
    // Two reads of the underlying reader make up the line: the cap is on the line, not on one chunk
    final String line = "y".repeat(65_536 + 10);
    final SupportLineReader reader = SupportLineReader.failingOver(new StringReader("ok\n" + line + "\n"), 65_536 + 9);

    assertThat(reader.readLine()).isEqualTo("ok");
    assertThatThrownBy(reader::readLine).isInstanceOf(SupportLineReader.LineTooLongException.class);
  }

  @Test
  void theLenientReaderStillCutsAndCarriesOn() throws Exception {
    final int[] truncated = new int[1];
    final SupportLineReader reader = new SupportLineReader(new StringReader("x".repeat(20) + "\nnext"), 10, truncated);

    assertThat(reader.readLine()).isEqualTo("x".repeat(10) + " ...[10 characters cut]");
    assertThat(reader.readLine()).isEqualTo("next");
    assertThat(truncated[0]).isEqualTo(1);
  }
}
