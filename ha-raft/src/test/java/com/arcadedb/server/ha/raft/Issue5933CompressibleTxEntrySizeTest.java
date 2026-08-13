/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.compression.CompressionFactory;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5933: a bulk insert into a 4-node Raft cluster committed a {@code TX_ENTRY}
 * that no node could ever apply, and every node crash-looped on the same Raft log index forever.
 * <p>
 * Two independent, mismatched size gates guarded transaction replication:
 * <ul>
 *   <li>at SUBMIT time {@code RaftGroupCommitter.submitAndWait} bounds the ENCODED entry, whose WAL the
 *       codec has already LZ4-compressed, against {@code min(appendBufferSize, grpcMessageSizeMax)};</li>
 *   <li>at APPLY time {@code RaftLogEntryCodec.decode} bounded the UNCOMPRESSED WAL length against a
 *       hardcoded 64MB constant unrelated to either setting.</li>
 * </ul>
 * A migration-shaped transaction (repetitive, text/JSON-heavy) only has to compress by ~2.4x to pass the
 * first gate and fail the second: the reporter's entry was 77,158,147 bytes uncompressed. By the time the
 * decode fails the entry is durably committed at a fixed log index on a majority of the cluster, so
 * {@code ArcadeStateMachine} halts the node to avoid divergence and replay deterministically reproduces the
 * same failure on every restart, on every node.
 * <p>
 * The fix makes the two gates agree by construction: the ENCODER refuses to build an entry the decoder
 * could not accept, so an unappliable entry can never reach the log; and the decoder's ceiling is raised
 * well above the producer's, so a cluster already holding such an entry applies it and recovers instead of
 * crash-looping.
 */
class Issue5933CompressibleTxEntrySizeTest {

  /** The reporter's payload: 77,158,147 bytes of uncompressed WAL, well-compressing migration data. */
  private static final int REPORTED_WAL_SIZE = 77_158_147;

  /** Shared across the tests so the 77MB allocation is paid once rather than once per test. */
  private static byte[] compressibleWal;

  @BeforeAll
  static void buildCompressiblePayload() {
    // Repetitive, text-shaped content: exactly the profile of a bulk migration WAL, and what makes the
    // compressed entry sail under the submit gate while the raw WAL is above the old decode ceiling.
    compressibleWal = new byte[REPORTED_WAL_SIZE];
    final byte[] pattern = "{\"@type\":\"Customer\",\"name\":\"ACME\",\"city\":\"Rome\"}".getBytes();
    for (int i = 0; i < compressibleWal.length; i++)
      compressibleWal[i] = pattern[i % pattern.length];
  }

  @AfterAll
  static void releasePayload() {
    compressibleWal = null;
  }

  /**
   * The gap itself: the reporter's transaction compresses far below the submit gate while its raw WAL sits
   * above the decode ceiling. Asserted directly so the test still means something if the constants move.
   */
  @Test
  void theReportedTransactionPassesTheSubmitGateOnItsCompressedSize() {
    final int compressedSize = CompressionFactory.getDefault().compress(compressibleWal).length;
    final long submitGate = GlobalConfiguration.maxReplicatedRaftEntrySize(new ContextConfiguration());

    assertThat(submitGate).as("stock configuration: the appender byte limit binds").isEqualTo(32L * 1024 * 1024);
    assertThat((long) compressedSize).as("the compressed entry is what the submit gate measures, and it fits")
        .isLessThan(submitGate);
    assertThat(compressibleWal.length).as("the raw WAL is what the applier has to materialize, and it did not fit")
        .isGreaterThan(RaftLogEntryCodec.MAX_ENTRY_BYTES);
  }

  /**
   * The fix: the producer refuses the transaction instead of committing an entry no node can apply. The
   * failure has to be non-retryable, or the caller would resubmit the same doomed entry forever.
   */
  @Test
  void anUnappliableTransactionIsRejectedBeforeItCanBeProposed() {
    assertThatThrownBy(() -> RaftLogEntryCodec.encodeTxEntry("heimdall", compressibleWal, Collections.emptyMap()))
        .isInstanceOf(ReplicatedEntryTooLargeException.class)
        .isNotInstanceOf(NeedRetryException.class)
        .hasMessageContaining("heimdall")
        .hasMessageContaining(String.valueOf(REPORTED_WAL_SIZE))
        .hasMessageContaining("uncompressed");
  }

  /**
   * Same gap, same fix, for the WAL embedded in a {@code SCHEMA_ENTRY}: {@code splitSchemaEntry} groups WAL
   * entries by raw size but only re-checks the COMPRESSED chunk against the cap, so one indivisible,
   * well-compressing WAL entry crosses the same mismatch.
   */
  @Test
  void anUnappliableSchemaWalEntryIsRejectedBeforeItCanBeProposed() {
    assertThatThrownBy(() -> RaftLogEntryCodec.encodeSchemaEntry("heimdall", "{}", Collections.emptyMap(),
        Collections.emptyMap(), List.of(compressibleWal), List.of(Collections.emptyMap())))
        .isInstanceOf(ReplicatedEntryTooLargeException.class)
        .isNotInstanceOf(NeedRetryException.class)
        .hasMessageContaining("uncompressed");
  }

  /**
   * Recovery for a cluster that is ALREADY bricked. The entry at index 45275 is durably committed; the only
   * way those nodes ever start again is for the upgraded decoder to accept it. So the decoder's ceiling sits
   * well above the producer's, and an entry written by the old encoder decodes intact.
   */
  @Test
  void anAlreadyCommittedOversizedTransactionDecodesInsteadOfHaltingTheNode() {
    final ByteString legacyEntry = encodeTxEntryWithoutProducerGuard("heimdall", compressibleWal);

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(legacyEntry);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.TX_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("heimdall");
    assertThat(decoded.walData()).hasSize(REPORTED_WAL_SIZE);
    assertThat(decoded.walData()).isEqualTo(compressibleWal);
  }

  /**
   * The decode ceiling is what a node ACCEPTS and the encode ceiling is what it PRODUCES; the first must stay
   * strictly above the second or the mismatch this issue is about comes straight back. It is also a wire-format
   * constant rather than a configurable: a node whose ceiling differed from its peers' would apply an entry
   * they reject, which is the divergence the state machine halts to prevent.
   */
  @Test
  void theDecoderAcceptsStrictlyMoreThanTheEncoderProduces() {
    assertThat(RaftLogEntryCodec.MAX_DECODED_ENTRY_BYTES).isGreaterThan(RaftLogEntryCodec.MAX_ENTRY_BYTES);

    // And the producer ceiling must not sit BELOW the submit gate, or an INCOMPRESSIBLE transaction that
    // replicates fine today (raw size ~= compressed size) would start being rejected by the new check.
    assertThat((long) RaftLogEntryCodec.MAX_ENTRY_BYTES)
        .isGreaterThanOrEqualTo(GlobalConfiguration.maxReplicatedRaftEntrySize(new ContextConfiguration()));
  }

  /**
   * Raising the decode ceiling must not turn the decoder into an allocation amplifier: a corrupt or hostile
   * length field claiming hundreds of MB behind a handful of compressed bytes is refused on the LZ4 format's
   * own expansion bound, before any array is allocated.
   */
  @Test
  void aCorruptUncompressedLengthIsRefusedWithoutAllocatingIt() {
    final byte[] tinyPayload = CompressionFactory.getDefault().compress("a short transaction".getBytes());
    final ByteString forged = forgeTxEntry("heimdall", 400 * 1024 * 1024, tinyPayload);

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(forged))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("TX_ENTRY uncompressed WAL")
        .hasMessageContaining("expansion ratio");
  }

  /**
   * And the absolute ceiling still stands for a claim large enough to clear the ratio bound: a length above it
   * is refused rather than allocated.
   */
  @Test
  void aLengthAboveTheDecodeCeilingIsStillRefused() {
    // Sized so the ratio bound alone would let this through - the absolute ceiling is what must catch it.
    final int claimed = RaftLogEntryCodec.MAX_DECODED_ENTRY_BYTES + 1;
    final byte[] payload = new byte[claimed / 255 + 1024];
    final ByteString forged = forgeTxEntry("heimdall", claimed, payload);

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(forged))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("TX_ENTRY uncompressed WAL");
  }

  /**
   * A length field for a section read straight off the stream is bounded by the bytes the entry ACTUALLY
   * carries, which is exact and far tighter than any constant - so raising the absolute ceiling cannot make a
   * forged length a bigger allocation than it was before. Both the compressed WAL of a TX_ENTRY and the
   * (uncompressed) schema JSON of a SCHEMA_ENTRY go through it.
   */
  @Test
  void aLengthLongerThanTheEntryIsRefusedBeforeItIsAllocated() {
    final ByteString forgedTx = forgeTxEntry("heimdall", 64, 300 * 1024 * 1024,
        CompressionFactory.getDefault().compress("a short transaction".getBytes()));
    assertThatThrownBy(() -> RaftLogEntryCodec.decode(forgedTx))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("TX_ENTRY compressed WAL")
        .hasMessageContaining("bytes remain in the entry");

    final ByteString forgedSchema = forgeSchemaEntryWithSchemaLength("heimdall", 300 * 1024 * 1024);
    assertThatThrownBy(() -> RaftLogEntryCodec.decode(forgedSchema))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("SCHEMA_ENTRY schemaJson")
        .hasMessageContaining("bytes remain in the entry");
  }

  /**
   * Nothing that replicated before may stop replicating now: ordinary transactions, compressible or not, must
   * still round-trip byte for byte.
   */
  @Test
  void ordinaryTransactionsStillRoundTripUnchanged() {
    final byte[] incompressible = new byte[1024 * 1024];
    for (int i = 0; i < incompressible.length; i++)
      incompressible[i] = (byte) ((i * 2654435761L) >>> 13);

    for (final byte[] wal : List.of(incompressible, new byte[1024 * 1024])) {
      final Map<Integer, Integer> deltas = Map.of(7, 3, 9, -1);
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
          RaftLogEntryCodec.encodeTxEntry("graph", wal, deltas));
      assertThat(decoded.walData()).isEqualTo(wal);
      assertThat(decoded.bucketRecordDelta()).isEqualTo(deltas);
    }
  }

  /**
   * Boundary: the producer check is "greater than", so a transaction of exactly the ceiling still replicates.
   */
  @Test
  void aTransactionExactlyAtTheProducerCeilingIsAccepted() {
    final byte[] atTheLimit = new byte[RaftLogEntryCodec.MAX_ENTRY_BYTES];
    assertThatNoException()
        .isThrownBy(() -> RaftLogEntryCodec.encodeTxEntry("graph", atTheLimit, Collections.emptyMap()));
  }

  /**
   * The complement of the fix on the configuration side: the decode ceiling is a wire-format constant, so an
   * entry cap configured ABOVE it would recreate exactly this issue for entries between the two. Fail at
   * startup, where an operator can act on it, rather than at apply time on a committed entry.
   */
  @Test
  void anEntryCapAboveTheDecodeCeilingIsRefusedAtStartup() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "1GB");
    cfg.setValue(GlobalConfiguration.HA_WRITE_BUFFER_SIZE, "1025MB");
    cfg.setValue(GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX, 1024L * 1024 * 1024);

    assertThatThrownBy(() -> RaftPropertiesBuilder.build(cfg))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("arcadedb.ha.appendBufferSize");

    // The stock defaults must of course still build.
    assertThatNoException().isThrownBy(() -> RaftPropertiesBuilder.build(new ContextConfiguration()));
  }

  /**
   * Reproduces what the pre-fix encoder wrote: the TX_ENTRY frame with no producer-side bound on the
   * uncompressed WAL length. This is what sits at index 45275 in the reporter's Raft log.
   */
  private static ByteString encodeTxEntryWithoutProducerGuard(final String databaseName, final byte[] walData) {
    return forgeTxEntry(databaseName, walData.length, CompressionFactory.getDefault().compress(walData));
  }

  private static ByteString forgeTxEntry(final String databaseName, final int uncompressedLength,
      final byte[] compressed) {
    return forgeTxEntry(databaseName, uncompressedLength, compressed.length, compressed);
  }

  /**
   * @param declaredCompressedLength written into the frame instead of the payload's real length, so a forged
   *                                 length field can be exercised without hand-writing the whole frame twice
   */
  private static ByteString forgeTxEntry(final String databaseName, final int uncompressedLength,
      final int declaredCompressedLength, final byte[] compressed) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);
      dos.writeByte(RaftLogEntryType.TX_ENTRY.getId());
      dos.writeUTF(databaseName);
      dos.writeInt(uncompressedLength);
      dos.writeInt(declaredCompressedLength);
      dos.write(compressed);
      dos.writeInt(0); // no bucket deltas
      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to forge TX entry", e);
    }
  }

  private static ByteString forgeSchemaEntryWithSchemaLength(final String databaseName, final int schemaLength) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);
      dos.writeByte(RaftLogEntryType.SCHEMA_ENTRY.getId());
      dos.writeUTF(databaseName);
      dos.writeInt(schemaLength);
      dos.write("{}".getBytes());
      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to forge SCHEMA entry", e);
    }
  }
}
