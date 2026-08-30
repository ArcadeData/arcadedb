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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedBlob;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedChunk;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #4416: a TimeSeries shard used to stop sealing FOREVER once its sealed store outgrew what one Raft entry
 * could carry. The store only grows, so the projected size never came back under the cap, and the shard's samples
 * stayed in the uncompressed mutable bucket for good - correct and fully replicated, but never compressed, never
 * retained, never downsampled.
 * <p>
 * The ceiling is lifted by SLICING: a store above one entry is shipped as an ordered sequence of entries the
 * follower stages and reassembles. This class pins the leader-side half of that - the arithmetic a follower's
 * reassembly depends on, the wire section that carries it, and the configuration that decides where the remaining
 * ceiling sits - without a cluster, because none of it needs one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4416SlicedSealedStoreTest {

  private static final String DB   = "graph";
  private static final String TYPE = "weather";
  private static final String FILE = "weather_shard_0.ts.sealed";

  // ---- the slicer ------------------------------------------------------------------------------------------

  /**
   * A store that fits one entry keeps shipping the way it always did: as a whole-file blob. The empty list is the
   * signal for that, and it is what keeps every cluster whose stores fit inline byte-for-byte on the old path.
   */
  @Test
  void aStoreThatFitsOneEntryIsNotSliced() {
    final TsSealedBlob blob = new TsSealedBlob(TYPE, 0, FILE, new byte[4096]);

    assertThat(RaftReplicatedDatabase.sliceSealedBlob(blob, 4096, DB)).isEmpty();
    assertThat(RaftReplicatedDatabase.sliceSealedBlob(blob, 8192, DB)).isEmpty();
  }

  /**
   * A budget of zero is what a cap too small to hold even one slice's framing produces. Slicing is impossible
   * there, and saying so with an empty list is what leaves {@code TimeSeriesShard}'s guard free to skip the shard
   * exactly as it did before slicing existed - rather than emitting a sequence of entries that carry no payload.
   */
  @Test
  void aNonPositiveBudgetDisablesSlicing() {
    final TsSealedBlob blob = new TsSealedBlob(TYPE, 0, FILE, new byte[4096]);

    assertThat(RaftReplicatedDatabase.sliceSealedBlob(blob, 0, DB)).isEmpty();
    assertThat(RaftReplicatedDatabase.sliceSealedBlob(blob, -1, DB)).isEmpty();
  }

  /**
   * The invariants a follower's reassembly is built on, asserted together because they only mean anything
   * together: contiguous offsets starting at zero, no slice above the budget, exactly one slice flagged
   * {@code last}, one whole-file length and CRC every slice agrees on, and bytes that concatenate back to the
   * original image.
   */
  @Test
  void slicesReassembleIntoTheOriginalImage() throws Exception {
    final byte[] sealed = randomBytes(10_000);
    final long budget = 3_000;

    final List<TsSealedChunk> slices = RaftReplicatedDatabase.sliceSealedBlob(
        new TsSealedBlob(TYPE, 3, FILE, sealed), budget, DB);

    assertThat(slices).as("10000 bytes at 3000 per slice").hasSize(4);

    final CRC32 expectedCrc = new CRC32();
    expectedCrc.update(sealed);

    long nextOffset = 0;
    final ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
    for (int i = 0; i < slices.size(); i++) {
      final TsSealedChunk slice = slices.get(i);
      assertThat(slice.typeName()).isEqualTo(TYPE);
      assertThat(slice.shardIndex()).isEqualTo(3);
      assertThat(slice.fileName()).isEqualTo(FILE);
      assertThat(slice.offset()).as("slice %d must start where slice %d ended", i, i - 1).isEqualTo(nextOffset);
      assertThat(slice.bytes().length).as("no slice may exceed the budget").isLessThanOrEqualTo((int) budget);
      assertThat(slice.fileLength()).as("every slice describes the WHOLE file").isEqualTo(sealed.length);
      assertThat(slice.fileCrc()).as("every slice carries the WHOLE file's CRC").isEqualTo(expectedCrc.getValue());
      assertThat(slice.last()).as("only the final slice publishes").isEqualTo(i == slices.size() - 1);
      reassembled.write(slice.bytes());
      nextOffset += slice.bytes().length;
    }

    assertThat(reassembled.toByteArray()).isEqualTo(sealed);
  }

  /** A file that divides exactly by the budget must not produce a trailing empty slice. */
  @Test
  void anExactMultipleOfTheBudgetProducesNoEmptyTrailingSlice() {
    final List<TsSealedChunk> slices = RaftReplicatedDatabase.sliceSealedBlob(
        new TsSealedBlob(TYPE, 0, FILE, randomBytes(9_000)), 3_000, DB);

    assertThat(slices).hasSize(3);
    assertThat(slices).allSatisfy(slice -> assertThat(slice.bytes()).isNotEmpty());
    assertThat(slices.getLast().last()).isTrue();
  }

  // ---- the wire format -------------------------------------------------------------------------------------

  /** Round trip of the slice section, including the flag that has to be readable BEFORE it. */
  @Test
  void schemaEntryCarriesSlicesThroughAnEncodeDecodeRoundTrip() {
    final byte[] payload = randomBytes(2_048);
    final TsSealedChunk slice = new TsSealedChunk(TYPE, 2, FILE, 9_999L, 1234567L, 4_096L, payload, true);

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry(DB, "{\"x\":1}", Map.of(7, "a.dat"),
        Collections.emptyMap(), List.of("wal".getBytes()), List.of(Map.of(1, 2)), Collections.emptyList(), false,
        List.of(slice));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.moreChunksFollow()).as("the flag before the new section must still decode").isFalse();
    assertThat(decoded.schemaJson()).isEqualTo("{\"x\":1}");
    assertThat(decoded.filesToAdd()).containsEntry(7, "a.dat");
    assertThat(decoded.walEntries()).hasSize(1);
    assertThat(decoded.sealedFileBlobs()).isEmpty();
    assertThat(decoded.sealedFileChunks()).hasSize(1);

    final TsSealedChunk back = decoded.sealedFileChunks().getFirst();
    assertThat(back.typeName()).isEqualTo(TYPE);
    assertThat(back.shardIndex()).isEqualTo(2);
    assertThat(back.fileName()).isEqualTo(FILE);
    assertThat(back.fileLength()).isEqualTo(9_999L);
    assertThat(back.fileCrc()).isEqualTo(1234567L);
    assertThat(back.offset()).isEqualTo(4_096L);
    assertThat(back.last()).isTrue();
    assertThat(back.bytes()).isEqualTo(payload);
  }

  /**
   * A delivery-only slice entry - the shape every slice but the last travels in - must decode with the
   * continuation flag SET, because that flag is what stops the follower reloading its schema over a state the
   * sequence has not finished delivering (#5443).
   */
  @Test
  void aDeliveryOnlySliceEntryKeepsTheContinuationFlag() {
    final TsSealedChunk slice = new TsSealedChunk(TYPE, 0, FILE, 8_192L, 42L, 0L, randomBytes(4_096), false);

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), true, List.of(slice)));

    assertThat(decoded.moreChunksFollow()).isTrue();
    assertThat(decoded.sealedFileChunks()).hasSize(1);
    assertThat(decoded.sealedFileChunks().getFirst().last()).isFalse();
    assertThat(decoded.schemaJson()).isEmpty();
    assertThat(decoded.walEntries()).isEmpty();
  }

  /**
   * The compatibility claim, asserted on the BYTES rather than on the decode: an entry with nothing to slice must
   * be byte-identical to what the previous codec produced, or the new trailing section is not trailing at all and
   * every entry a mixed-version cluster exchanges changes shape.
   */
  @Test
  void anEntryWithNoSlicesIsByteIdenticalToThePreviousFormat() {
    final List<byte[]> wal = List.of("wal-one".getBytes(), "wal-two".getBytes());
    final List<TsSealedBlob> blobs = List.of(new TsSealedBlob(TYPE, 0, FILE, randomBytes(512)));

    final ByteString withoutTheParameter = RaftLogEntryCodec.encodeSchemaEntry(DB, "{}", Map.of(1, "f.dat"),
        Map.of(2, "g.dat"), wal, List.of(Map.of(3, 4), Map.of(5, 6)), blobs);
    final ByteString withAnEmptySection = RaftLogEntryCodec.encodeSchemaEntry(DB, "{}", Map.of(1, "f.dat"),
        Map.of(2, "g.dat"), wal, List.of(Map.of(3, 4), Map.of(5, 6)), blobs, false, Collections.emptyList());

    assertThat(withAnEmptySection.toByteArray()).isEqualTo(withoutTheParameter.toByteArray());
    assertThat(RaftLogEntryCodec.decode(withoutTheParameter).sealedFileChunks()).isEmpty();
  }

  /** A slice corrupted on the wire is refused, not installed: the decoder checks a per-slice CRC. */
  @Test
  void aCorruptedSliceFailsItsCrc() {
    final TsSealedChunk slice = new TsSealedChunk(TYPE, 0, FILE, 1_024L, 7L, 0L, randomBytes(1_024), true);
    final byte[] encoded = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false,
        List.of(slice)).toByteArray();

    // Flip a byte inside the compressed slice payload, which is the tail of the entry.
    encoded[encoded.length - 5] ^= 0x7F;

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(ByteString.copyFrom(encoded)))
        .isInstanceOf(IllegalStateException.class);
  }

  // ---- the splitter ----------------------------------------------------------------------------------------

  /**
   * A publishing entry that ALSO has to be split by WAL volume must keep its final slices on the LAST chunk, next
   * to the schema JSON and {@code filesToRemove}: the slices publish, and publishing early is what #5443 showed to
   * be sticky rather than merely wasteful.
   */
  @Test
  void theFinalSlicesRideTheLastChunkOfASplitSchemaEntry() {
    final List<byte[]> wal = List.of(new byte[3_000], new byte[3_000], new byte[3_000]);
    final List<TsSealedChunk> finalSlices = List.of(
        new TsSealedChunk(TYPE, 0, FILE, 12_000L, 99L, 9_000L, randomBytes(3_000), true));

    final List<ByteString> chunks = RaftTransactionBroker.splitSchemaEntry(DB, "{\"schema\":1}",
        Collections.emptyMap(), Collections.emptyMap(), wal, List.of(Map.of(), Map.of(), Map.of()),
        Collections.emptyList(), finalSlices, 8_000, 20_000, false);

    assertThat(chunks).as("three 3000-byte WAL entries cannot share one 8000-byte entry").hasSizeGreaterThan(1);

    for (int i = 0; i < chunks.size(); i++) {
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(chunks.get(i));
      final boolean last = i == chunks.size() - 1;
      assertThat(decoded.sealedFileChunks().isEmpty())
          .as("only the publishing chunk may carry sealed slices (chunk %d/%d)", i + 1, chunks.size())
          .isEqualTo(!last);
      assertThat(decoded.moreChunksFollow()).isEqualTo(!last);
    }
  }

  // ---- where the ceiling now sits --------------------------------------------------------------------------

  /**
   * The ceiling with stock settings: the 4MB appender limit binds per ENTRY (#4743), and 512 slices of it put the
   * per-shard sealed store far above the 48MB the inline cap used to impose. The comparison is against the
   * configured inline cap itself, because that number IS what used to stop the shard sealing.
   */
  @Test
  void theDefaultCeilingIsFarAboveTheInlineCap() {
    final ContextConfiguration configuration = new ContextConfiguration();

    final long ceiling = GlobalConfiguration.maxReplicatedSealedStoreSize(configuration);

    assertThat(ceiling).isGreaterThan(configuration.getValueAsLong(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE));
    assertThat(ceiling).isGreaterThan(GlobalConfiguration.maxReplicatedRaftEntrySize(configuration));
    assertThat(ceiling).as("the leader still materializes the file as one array before slicing it")
        .isLessThanOrEqualTo(Integer.MAX_VALUE);
  }

  /**
   * A cap so small that a slice cannot even carry its own framing leaves the ceiling AT the per-entry cap, which
   * is what keeps the pre-existing behaviour intact: such a shard is still skipped and its samples still stay in
   * the replicated mutable bucket, rather than being shipped as an unbounded run of payload-free entries.
   */
  /**
   * The reserve has to grow WITH the slice, because what it absorbs does: a slice of already-compressed sealed
   * blocks can come off the LZ4 encoder larger than it went in, by up to {@code n/255 + 16} bytes. A fixed reserve
   * is under water by the time the entry cap reaches a few megabytes, and an entry over the cap is not a slow
   * path - Ratis rejects it and the leader steps down, repeatedly (#4743).
   */
  @Test
  void theBudgetReservesEnoughForASliceThatCompressionMakesBIGGER() {
    final ContextConfiguration configuration = new ContextConfiguration();

    for (final long entryCap : new long[] { 64L * 1024, 1024L * 1024, 4L * 1024 * 1024, 32L * 1024 * 1024 }) {
      configuration.setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, entryCap);
      configuration.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, Long.toString(entryCap));

      final long budget = GlobalConfiguration.replicatedSealedChunkBudget(configuration);
      final long worstCaseEncoded = budget + budget / 255 + 16;

      assertThat(worstCaseEncoded).as("an incompressible %d-byte slice must still fit a %d-byte entry", budget,
          entryCap).isLessThan(entryCap);
    }
  }

  @Test
  void aCapTooSmallToSliceLeavesTheCeilingAtOneEntry() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, 10L);

    assertThat(GlobalConfiguration.replicatedSealedChunkBudget(configuration)).isZero();
    assertThat(GlobalConfiguration.maxReplicatedSealedStoreSize(configuration))
        .as("a store of 11 bytes must still be refused, exactly as before").isEqualTo(10L);
  }

  /** The inline cap never wins over the transport: #4743's rule survives the new arithmetic. */
  @Test
  void theTransportStillClampsTheConfiguredCap() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, 512L * 1024 * 1024);
    configuration.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "1048576");

    assertThat(GlobalConfiguration.maxReplicatedSealedEntrySize(configuration)).isEqualTo(1024L * 1024);
    assertThat(GlobalConfiguration.replicatedSealedChunkBudget(configuration))
        .isEqualTo(1024L * 1024 - GlobalConfiguration.REPLICATED_SEALED_CHUNK_FRAMING_BYTES - (1024L * 1024) / 128);
  }

  private static byte[] randomBytes(final int length) {
    final byte[] bytes = new byte[length];
    new Random(length).nextBytes(bytes);
    return bytes;
  }
}
