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
package com.arcadedb.server.ha.raft;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8686: a TX_ENTRY carries the Raft log index its originator had applied when it prepared the transaction, as
 * a framed extension section, so the leader can tell a transaction prepared before a schema change from one prepared
 * after it. The section is optional: an entry without it (a peer that predates it, or a node that could not state the
 * index) reads as "unknown" and is never refused for it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8686TxPreparedAtIndexCodecTest {

  private static final byte[] WAL = { 1, 2, 3, 4 };

  @Test
  void thePreparedAtIndexRoundTrips() {
    final ByteString entry = RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), 4711L);

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.TX_ENTRY);
    assertThat(decoded.txPreparedAtIndex()).isEqualTo(4711L);
    assertThat(decoded.walData()).isEqualTo(WAL);
  }

  @Test
  void anEntryWithoutTheSectionReadsAsUnknown() {
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap()));

    assertThat(decoded.txPreparedAtIndex()).isEqualTo(-1L);
  }

  @Test
  void anUnknownIndexWritesNoSectionAtAll() {
    // Byte for byte the entry an older build writes, so an unknown index costs nothing on the wire.
    assertThat(RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), -1L))
        .isEqualTo(RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap()));
  }

  /** The section is framed, so a decoder that predates it skips it: what makes emitting it safe on a mixed cluster. */
  @Test
  void theSectionIsAFramedExtensionThatAnOlderDecoderSkips() {
    final ByteString withSection = RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), 9L);
    final ByteString plain = RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap());

    assertThat(withSection.size()).isGreaterThan(plain.size());
    assertThat(withSection.substring(0, plain.size())).as("the fields an older build reads are untouched").isEqualTo(plain);
    assertThat(withSection.substring(plain.size(), plain.size() + 4).toByteArray())
        .as("and the tail opens with the extension magic").containsExactly(0x41, 0x44, 0x42, 0x58);
  }

  @Test
  void aSectionOfAnotherNameIsSkippedAndTheIndexStaysUnknown() {
    final ByteString entry = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap()), new byte[] { 0, 3, 'x', 'y', 'z' });

    assertThat(RaftLogEntryCodec.decode(entry).txPreparedAtIndex()).isEqualTo(-1L);
  }

  @Test
  void aTruncatedSectionIsRefusedAsCorruptionRatherThanReadAsUnknown() {
    // Named like ours but carrying no long: reading it as "unknown" would silently switch the check off.
    final ByteString entry = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap()),
        new byte[] { 0, 20, 't', 'x', '-', 'p', 'r', 'e', 'p', 'a', 'r', 'e', 'd', '-', 'a', 't', '-', 'i', 'n', 'd', 'e', 'x', 1, 2 });

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> RaftLogEntryCodec.decode(entry))
        .isInstanceOf(RaftLogEntryDecodeException.class);
  }
}
