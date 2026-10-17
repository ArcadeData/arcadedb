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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.index.fulltext.FullTextCorpusDeltas.CorpusDelta;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10051 (#10018): the BM25 corpus counters of a full-text index are not part of the pages a replicated transaction
 * writes, so a follower never moved them. The transaction now carries what it did to them in a framed extension section of its
 * {@code TX_ENTRY}, written only once every peer can read it, and the node that originated the transaction applies the same
 * deltas by committing it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10051FullTextCorpusDeltaReplicationTest {
  private static final String TYPE  = "Doc10051";
  private static final String INDEX = TYPE + "[content]";
  private static final byte[] WAL   = { 1, 2, 3, 4 };

  @TempDir
  Path tempDir;

  private LocalDatabase             proxied;
  private FakeRaftHAServer          raftServer;
  private FakeRaftTransactionBroker broker;
  private RaftReplicatedDatabase    database;

  @BeforeEach
  void setUp() {
    // Schema in place BEFORE the wrapper exists, so no DDL commit takes the Raft path
    proxied = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue10051").toString()).create();
    proxied.transaction(() -> {
      proxied.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
      proxied.command("sql", "CREATE PROPERTY " + TYPE + ".content STRING");
      proxied.command("sql", "CREATE INDEX ON " + TYPE + " (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      proxied.command("sql", "CREATE DOCUMENT TYPE Plain10051");
      proxied.command("sql", "CREATE PROPERTY Plain10051.name STRING");
    });
    proxied.transaction(() -> {
      for (int i = 0; i < 4; i++)
        proxied.command("sql", "INSERT INTO " + TYPE + " SET content = 'alpha beta gamma'");
      // Every property name in the dictionary BEFORE the wrapper exists: a dictionary commit would take the Raft path itself
      proxied.command("sql", "INSERT INTO Plain10051 SET name = 'seed'");
    });

    broker = new FakeRaftTransactionBroker();
    broker.returns("replicateTransaction", 7L);
    raftServer = FakeRaftHAServer.detached();
    raftServer.transactionBroker(broker);
    raftServer.quorumTimeout(1_000L);
    raftServer.stateMachine(new ArcadeStateMachine());
    raftServer.shutdownRequested(false);
    raftServer.txPreparedAtCapable(false);
    raftServer.leader(false);

    database = new RaftReplicatedDatabase(null, proxied, raftServer);
  }

  @AfterEach
  void tearDown() {
    if (proxied != null && proxied.isOpen()) {
      proxied.rollbackAllNested();
      proxied.drop();
    }
  }

  private FullTextIndexMetadata metadata() {
    return LSMTreeFullTextIndex.metadataOf(proxied.getSchema().getIndexByName(INDEX));
  }

  private void insertAndCommit(final int count) {
    database.begin();
    for (int i = 0; i < count; i++)
      proxied.newDocument(TYPE).set("content", "alpha beta gamma delta").save();
    database.commit();
  }

  @SuppressWarnings("unchecked")
  private List<CorpusDelta> carriedDeltas() {
    final List<List<Object>> calls = broker.calls("replicateTransaction");
    assertThat(calls).hasSize(1);
    return (List<CorpusDelta>) calls.getFirst().get(4);
  }

  // ---- the wire format

  @Test
  void theDeltasRoundTripWithThePreparedAtIndex() {
    final List<CorpusDelta> deltas = List.of(new CorpusDelta(INDEX, 3, 12), new CorpusDelta("Other[text]", -1, -7));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), 4711L, deltas));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.TX_ENTRY);
    assertThat(decoded.corpusDeltas()).containsExactlyElementsOf(deltas);
    assertThat(decoded.txPreparedAtIndex()).as("one section never swallows the other").isEqualTo(4711L);
    assertThat(decoded.walData()).isEqualTo(WAL);
  }

  @Test
  void theDeltasRoundTripWithoutAPreparedAtIndex() {
    final List<CorpusDelta> deltas = List.of(new CorpusDelta(INDEX, 3, 12));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), -1L, deltas));

    assertThat(decoded.corpusDeltas()).containsExactlyElementsOf(deltas);
    assertThat(decoded.txPreparedAtIndex()).isEqualTo(-1L);
  }

  @Test
  void noDeltasWritesNoSectionAtAll() {
    // Byte for byte the entry an older build writes, so a transaction that wrote no full-text index costs nothing on the wire
    assertThat(RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), 9L, Collections.emptyList()))
        .isEqualTo(RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), 9L));
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap())).corpusDeltas())
        .isEmpty();
  }

  /** The section is framed, so a decoder that predates it skips it: what makes emitting it safe on a mixed cluster. */
  @Test
  void theSectionIsAFramedExtensionThatAnOlderDecoderSkips() {
    final ByteString plain = RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap());
    final ByteString withSection = RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), -1L,
        List.of(new CorpusDelta(INDEX, 1, 4)));

    assertThat(withSection.size()).isGreaterThan(plain.size());
    assertThat(withSection.substring(0, plain.size())).as("the fields an older build reads are untouched").isEqualTo(plain);
    assertThat(withSection.substring(plain.size(), plain.size() + 4).toByteArray())
        .as("and the tail opens with the extension magic").containsExactly(0x41, 0x44, 0x42, 0x58);
  }

  @Test
  void aSectionOfAnotherNameIsSkipped() {
    final ByteString entry = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap()), new byte[] { 0, 3, 'x', 'y', 'z' });

    assertThat(RaftLogEntryCodec.decode(entry).corpusDeltas()).isEmpty();
  }

  /** A section a later build adds after ours is skipped, and ours is still read. */
  @Test
  void anUnknownSectionAfterTheDeltasIsSkippedAndTheDeltasAreStillRead() {
    final List<CorpusDelta> deltas = List.of(new CorpusDelta(INDEX, 3, 12));
    final ByteString entry = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), 4711L, deltas), new byte[] { 0, 3, 'x', 'y', 'z' });

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.corpusDeltas()).containsExactlyElementsOf(deltas);
    assertThat(decoded.txPreparedAtIndex()).isEqualTo(4711L);
  }

  @Test
  void aTruncatedSectionIsRefusedAsCorruptionRatherThanAppliedWithoutDeltas() {
    // Named like ours but cut short after the count: applying the entry without them would leave this node's counters behind
    final byte[] name = RaftLogEntryCodec.TX_FULLTEXT_CORPUS_SECTION.getBytes(StandardCharsets.UTF_8);
    final byte[] section = new byte[2 + name.length + 4 + 1];
    section[1] = (byte) name.length;
    System.arraycopy(name, 0, section, 2, name.length);
    section[2 + name.length + 3] = 1; // one delta announced, none present

    final ByteString entry = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap()), section);

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(entry)).isInstanceOf(RaftLogEntryDecodeException.class);
  }

  @Test
  void thisBuildAdvertisesTheCapability() {
    assertThat(PeerCapabilities.LOCAL).contains(PeerCapabilities.TX_FULLTEXT_CORPUS_DELTAS);
  }

  // ---- the originating node

  @Test
  void aCommitCarriesTheNetEffectOnTheCountersOnceEveryPeerCanReadIt() {
    raftServer.fullTextCorpusDeltaCapable(true);

    insertAndCommit(3);

    assertThat(carriedDeltas()).containsExactly(new CorpusDelta(INDEX, 3, 12));
  }

  @Test
  void aCommitCarriesNothingWhileAPeerCannotReadTheSection() {
    raftServer.fullTextCorpusDeltaCapable(false);

    insertAndCommit(3);

    assertThat(carriedDeltas()).as("an older build halts on trailing bytes it does not know").isEmpty();
  }

  @Test
  void aTransactionThatWroteNoFullTextIndexCarriesNothing() {
    raftServer.fullTextCorpusDeltaCapable(true);

    database.begin();
    proxied.newDocument("Plain10051").set("name", "x").save();
    database.commit();

    assertThat(carriedDeltas()).isEmpty();
  }

  /** A removal and an insert of the same transaction travel as their net effect, which is what every node applies. */
  @Test
  void anUpdateTravelsAsItsNetEffect() {
    raftServer.fullTextCorpusDeltaCapable(true);

    database.begin();
    proxied.iterateType(TYPE, false).next().asDocument().modify().set("content", "alpha beta gamma delta epsilon").save();
    database.commit();

    assertThat(carriedDeltas()).containsExactly(new CorpusDelta(INDEX, 0, 2));
  }

  /**
   * A rewrite that keeps the number of tokens nets to {@code (0, 0)}, and still ships it (issue #10132): the node applying the
   * entry must learn that the counters were touched, or a corpus scan overlapping the apply calls itself exact.
   */
  @Test
  void anUpdateThatKeepsItsTokenLengthShipsAZeroPair() {
    raftServer.fullTextCorpusDeltaCapable(true);

    database.begin();
    proxied.iterateType(TYPE, false).next().asDocument().modify().set("content", "delta epsilon zeta").save();
    database.commit();

    assertThat(carriedDeltas()).containsExactly(new CorpusDelta(INDEX, 0, 0));
  }

  /** Issue #10132: a delete and an insert of the same length in one transaction ship their index with a zero pair. */
  @Test
  void aDeleteAndAnInsertOfTheSameLengthShipAZeroPair() {
    raftServer.fullTextCorpusDeltaCapable(true);

    database.begin();
    proxied.iterateType(TYPE, false).next().asDocument().delete();
    proxied.newDocument(TYPE).set("content", "delta epsilon zeta").save();
    database.commit();

    assertThat(carriedDeltas()).containsExactly(new CorpusDelta(INDEX, 0, 0));
  }

  /** A zero pair is a real section on the wire, not an absent one, and it decodes back as such. */
  @Test
  void aZeroPairRoundTrips() {
    final List<CorpusDelta> deltas = List.of(new CorpusDelta(INDEX, 0, 0));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), -1L, deltas));

    assertThat(decoded.corpusDeltas()).containsExactlyElementsOf(deltas);
  }

  @Test
  void aCommitRefusedByTheClusterNeverTouchesTheCounters() {
    raftServer.fullTextCorpusDeltaCapable(true);
    broker.fails("replicateTransaction", new ConcurrentModificationException("Concurrent modification on page 3/0"));
    final long docsBefore = metadata().getTotalDocs();
    final long lengthBefore = metadata().getSumDocLength();

    database.begin();
    proxied.newDocument(TYPE).set("content", "alpha beta gamma delta").save();
    assertThatThrownBy(database::commit).isInstanceOf(ConcurrentModificationException.class);

    assertThat(metadata().getTotalDocs()).isEqualTo(docsBefore);
    assertThat(metadata().getSumDocLength()).isEqualTo(lengthBefore);
  }
}
