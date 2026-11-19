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
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedBlob;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedChunk;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Leader-side shipping of sealed TimeSeries stores that are too large for one Raft entry: slice arithmetic, the
 * publishing entry and the wire sections, without a cluster.
 */
class SealedStoreSliceShippingTest {

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
   */
  @Nested
  class SliceArithmetic {

    private static final String DB   = "graph";
    private static final String TYPE = "weather";
    private static final String FILE = "weather_shard_0.ts.sealed";

    // ---- the slicer ------------------------------------------------------------------------------------------

    /**
     * A store that fits one entry keeps shipping the way it always did: as a whole-file blob. The empty list is the
     * signal for that, and it is what keeps every cluster whose stores fit inline byte-for-byte on the old path.
     */
    // Regression for #4416: a store that fits one entry is not sliced
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
    // Regression for #4416: a non positive budget disables slicing
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
    // Regression for #4416: slices reassemble into the original image
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
    // Regression for #4416: an exact multiple of the budget produces no empty trailing slice
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
    // Regression for #4416: schema entry carries slices through an encode decode round trip
    @Test
    void schemaEntryCarriesSlicesThroughAnEncodeDecodeRoundTrip() {
      final byte[] payload = randomBytes(2_048);
      // Geometrically real, because the decoder now insists on it: a final slice ends exactly at the file length.
      final TsSealedChunk slice = new TsSealedChunk(TYPE, 2, FILE, 6_144L, 1234567L, 4_096L, payload, true);

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
      assertThat(back.fileLength()).isEqualTo(6_144L);
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
    // Regression for #4416: a delivery only slice entry keeps the continuation flag
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
    // Regression for #4416: an entry with no slices is byte identical to the previous format
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
    // Regression for #4416: a corrupted slice fails its crc
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

    /**
     * Offsets and lengths that cannot describe a real file are refused by the DECODER, which is what keeps the
     * applier's "a sequence that does not line up asks for a resync, it never crashes" rule true for a malformed
     * field as well as for a missing slice: a negative offset reaches {@code RandomAccessFile.seek} and comes back
     * as a raw IOException the apply path can only report as an unexpected error.
     */
    // Regression for #4416: a slice whose geometry cannot describe a file is refused
    @Test
    void aSliceWhoseGeometryCannotDescribeAFileIsRefused() {
      final byte[] payload = randomBytes(512);

      final List<TsSealedChunk> impossible = List.of(
          new TsSealedChunk(TYPE, 0, FILE, 4_096L, 7L, -1L, payload, false),          // negative offset
          new TsSealedChunk(TYPE, 0, FILE, -1L, 7L, 0L, payload, false),              // negative file length
          new TsSealedChunk(TYPE, 0, FILE, 256L, 7L, 0L, payload, false),             // slice overruns the file
          new TsSealedChunk(TYPE, 0, FILE, 4_096L, 7L, 4_000L, payload, false),       // slice overruns the end
          new TsSealedChunk(TYPE, 0, FILE, 4_096L, 7L, 0L, payload, true));           // final slice stops short

      for (final TsSealedChunk chunk : impossible) {
        final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), true,
            List.of(chunk));

        assertThatThrownBy(() -> RaftLogEntryCodec.decode(encoded))
            .as("offset %d + %d bytes of a %d-byte file, last=%s", chunk.offset(), chunk.bytes().length,
                chunk.fileLength(), chunk.last())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Invalid SCHEMA_ENTRY sealed slice");
      }
    }

    /** The geometry the producer actually emits passes, so the guard above cannot be refusing everything. */
    // Regression for #4416: every slice the slicer produces passes the geometry check
    @Test
    void everySliceTheSlicerProducesPassesTheGeometryCheck() {
      for (final TsSealedChunk chunk : RaftReplicatedDatabase.sliceSealedBlob(
          new TsSealedBlob(TYPE, 0, FILE, randomBytes(10_000)), 3_000, DB)) {
        final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), true,
            List.of(chunk));

        assertThat(RaftLogEntryCodec.decode(encoded).sealedFileChunks()).hasSize(1);
      }
    }

    // ---- the splitter ----------------------------------------------------------------------------------------

    /**
     * A publishing entry that ALSO has to be split by WAL volume must keep its final slices on the LAST chunk, next
     * to the schema JSON and {@code filesToRemove}: the slices publish, and publishing early is what #5443 showed to
     * be sticky rather than merely wasteful.
     */
    // Regression for #4416: the final slices ride the last chunk of a split schema entry
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
     * The ceiling with stock settings: the 32MB append-buffer limit binds per ENTRY (#4743) rather than the 48MB
     * inline cap, and slicing puts the per-shard sealed store far above the 48MB that cap used to impose. The
     * comparison is against the configured inline cap itself, because that number IS what used to stop the shard
     * sealing. Which of the two bounds actually SETS the ceiling is pinned separately by
     * {@link #atStockSettingsTheCeilingComesFromTheClampNotFromTheSliceCount} - it is the Integer.MAX_VALUE clamp,
     * not 512 times the budget.
     */
    // Regression for #4416: the default ceiling is far above the inline cap
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
     * The reserve has to grow WITH the slice, because what it absorbs does: a slice of already-compressed sealed
     * blocks can come off the LZ4 encoder larger than it went in, by up to {@code n/255 + 16} bytes. A fixed reserve
     * is under water by the time the entry cap reaches a few megabytes, and an entry over the cap is not a slow
     * path - Ratis rejects it and the leader steps down, repeatedly (#4743).
     */
    // Regression for #4416: the budget reserves enough for a slice that compression makes bigger
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

    /**
     * A cap so small that a slice cannot even carry its own framing leaves the ceiling AT the per-entry cap, which
     * is what keeps the pre-existing behaviour intact: such a shard is still skipped and its samples still stay in
     * the replicated mutable bucket, rather than being shipped as an unbounded run of payload-free entries.
     */
    // Regression for #4416: a cap too small to slice leaves the ceiling at one entry
    @Test
    void aCapTooSmallToSliceLeavesTheCeilingAtOneEntry() {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, 10L);

      assertThat(GlobalConfiguration.replicatedSealedChunkBudget(configuration)).isZero();
      assertThat(GlobalConfiguration.maxReplicatedSealedStoreSize(configuration))
          .as("a store of 11 bytes must still be refused, exactly as before").isEqualTo(10L);
    }

    /** The inline cap never wins over the transport: #4743's rule survives the new arithmetic. */
    // Regression for #4416: the transport still clamps the configured cap
    @Test
    void theTransportStillClampsTheConfiguredCap() {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, 512L * 1024 * 1024);
      configuration.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "1048576");

      assertThat(GlobalConfiguration.maxReplicatedSealedEntrySize(configuration)).isEqualTo(1024L * 1024);
      assertThat(GlobalConfiguration.replicatedSealedChunkBudget(configuration))
          .isEqualTo(1024L * 1024 - GlobalConfiguration.REPLICATED_SEALED_CHUNK_FRAMING_BYTES - (1024L * 1024) / 128);
    }

    /**
     * The branch nothing else pins: a store needing MORE than {@link GlobalConfiguration#MAX_REPLICATED_SEALED_CHUNKS}
     * slices is still shipped, whole and intact, rather than refused.
     * <p>
     * Refusing here is the tempting alternative and it is WRONG: the leader has already rewritten and swapped its
     * sealed file by the time the slicer runs, so withholding it leaves every follower on an image no node holds any
     * more. That is divergence traded for a latency spike. This test is what stops a future "surely we should cap
     * this" edit from making that trade silently - it fails the moment the slicer starts returning a truncated list
     * or an empty one.
     * <p>
     * Only the maintenance path (retention, downsampling) can reach here: {@code TimeSeriesShard.compactInternal}
     * has a pre-check that keeps compaction under the ceiling, and {@code runSealedMaintenanceReplicated}
     * deliberately has none, for the same divergence reason.
     */
    // Regression for #4416: a store needing more slices than expected is shipped anyway rather than refused
    @Test
    void aStoreNeedingMoreSlicesThanExpectedIsShippedANYWAYRatherThanRefused() {
      final int overTheBound = GlobalConfiguration.MAX_REPLICATED_SEALED_CHUNKS + 137;
      final byte[] sealed = randomBytes(overTheBound);

      final List<TsSealedChunk> slices = RaftReplicatedDatabase.sliceSealedBlob(
          new TsSealedBlob(TYPE, 0, FILE, sealed), 1, DB);

      assertThat(slices).as("every byte must still ship - a short list here is silent follower divergence")
          .hasSize(overTheBound);
      assertThat(slices.stream().filter(TsSealedChunk::last).count()).as("exactly one slice publishes").isEqualTo(1);
      assertThat(slices.getLast().last()).isTrue();

      final ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
      long nextOffset = 0;
      for (final TsSealedChunk slice : slices) {
        assertThat(slice.offset()).isEqualTo(nextOffset);
        assertThat(slice.fileLength()).isEqualTo(sealed.length);
        reassembled.writeBytes(slice.bytes());
        nextOffset += slice.bytes().length;
      }
      assertThat(reassembled.toByteArray()).as("an oversized sequence still reassembles exactly").isEqualTo(sealed);
    }

    /**
     * The regime that made the skip warning's old "%d slices of %d bytes each" sentence arithmetically FALSE: at the
     * stock settings the {@link Integer#MAX_VALUE} clamp - not
     * {@link GlobalConfiguration#MAX_REPLICATED_SEALED_CHUNKS} times the budget - is what sets the ceiling, and the
     * two differ by an order of magnitude. An operator who multiplies the two numbers printed in that message and
     * does not get the ceiling printed beside them stops believing the message.
     */
    // Regression for #4416: at stock settings the ceiling comes from the clamp not from the slice count
    @Test
    void atStockSettingsTheCeilingComesFromTheClampNotFromTheSliceCount() {
      final ContextConfiguration configuration = new ContextConfiguration();

      final long budget = GlobalConfiguration.replicatedSealedChunkBudget(configuration);
      final long ceiling = GlobalConfiguration.maxReplicatedSealedStoreSize(configuration);

      assertThat(ceiling).as("the clamp is what binds").isEqualTo(Integer.MAX_VALUE);
      assertThat(budget * GlobalConfiguration.MAX_REPLICATED_SEALED_CHUNKS)
          .as("so the product is NOT the ceiling, and a message asserting it is would be wrong")
          .isGreaterThan(ceiling);
      assertThat(ceiling / budget).as("the stock ceiling is reached in far fewer than %d slices",
          GlobalConfiguration.MAX_REPLICATED_SEALED_CHUNKS)
          .isLessThan(GlobalConfiguration.MAX_REPLICATED_SEALED_CHUNKS);
    }

    private static byte[] randomBytes(final int length) {
      final byte[] bytes = new byte[length];
      new Random(length).nextBytes(bytes);
      return bytes;
    }
  }

  /**
   * Issue #6933: the sliced-sealed-store path added by #6917 sized every slice as if its store were the only sealed
   * payload of the session, and then folded the FINAL slice of EVERY store into one publishing entry.
   * <p>
   * {@code TimeSeriesEngine.runSealedMaintenanceReplicated} runs retention and downsampling for every shard of a
   * type inside ONE {@code runWithCompactionReplication} session, so N shards over the per-entry budget produce N
   * final slices of up to a full budget each on a single entry - which {@code splitSchemaEntry} cannot split,
   * because the payload that blows the cap is the header rather than the WAL. It throws, and it throws AFTER every
   * earlier slice of every store has already been committed to the Raft log.
   * <p>
   * This class pins the leader-side arithmetic that makes the publishing entry fit by construction, without a
   * cluster, because none of it needs one.
   */
  @Nested
  class MultiStorePublishingEntry {

    private static final String DB   = "graph";
    private static final String TYPE = "weather";

    /** The transport cap, and the sealed payload one entry may carry under it. */
    private static final long ENTRY_CAP     = 64L * 1024;
    private static final long SEALED_BUDGET =
        ENTRY_CAP - GlobalConfiguration.REPLICATED_SEALED_CHUNK_FRAMING_BYTES - ENTRY_CAP / 128;

    /** A schema JSON of a realistic size for a database with a handful of types. */
    private static final String SCHEMA_JSON = "{\"schema\":\"" + "x".repeat(4_000) + "\"}";

    // ---- the defect ------------------------------------------------------------------------------------------

    /**
     * The state of the world before the fix, kept as the proof that the guard below is guarding something: three
     * stores sliced as if each owned the whole entry hand {@code splitSchemaEntry} three full-budget final slices,
     * and the splitter refuses them - after the delivery-only slices have already been committed.
     */
    // Regression for #6933: three full budget final slices cannot share one publishing entry
    @Test
    void threeFullBudgetFinalSlicesCannotShareOnePublishingEntry() {
      final List<TsSealedChunk> finalSlices = new ArrayList<>();
      for (int shard = 0; shard < 3; shard++)
        finalSlices.add(RaftReplicatedDatabase.sliceSealedBlob(storeOf(shard, 3), SEALED_BUDGET, DB).getLast());

      assertThatThrownBy(() -> RaftTransactionBroker.splitSchemaEntry(DB, SCHEMA_JSON, Collections.emptyMap(),
          Collections.emptyMap(), List.of(new byte[512]), List.of(Map.of()), Collections.emptyList(), finalSlices,
          ENTRY_CAP, Integer.MAX_VALUE, false))
          .as("three slices of ~%d bytes each cannot ride one %d-byte entry", SEALED_BUDGET, ENTRY_CAP)
          .isInstanceOf(ReplicatedEntryTooLargeException.class);
    }

    // ---- the session-wide plan -------------------------------------------------------------------------------

    /**
     * The fix, asserted where it matters: whatever the session's shape, everything that has to ride the PUBLISHING
     * entry - the whole blobs plus every store's final slice - fits it, together with the schema JSON and the file
     * maps that publish beside them.
     */
    // Regression for #6933: every stores final slice together fits one publishing entry
    @Test
    void everyStoresFinalSliceTogetherFitsOnePublishingEntry() {
      final List<TsSealedBlob> stores = List.of(storeOf(0, 3), storeOf(1, 5), storeOf(2, 1));

      assertThat(encodedPublishingEntrySize(stores)).isLessThanOrEqualTo(ENTRY_CAP);
    }

    /** The same, with more shards than a slice can be usefully divided among on a small entry cap. */
    // Regression for #6933: a session with many shards still fits one publishing entry
    @Test
    void aSessionWithManyShardsStillFitsOnePublishingEntry() {
      final List<TsSealedBlob> stores = new ArrayList<>();
      for (int shard = 0; shard < 8; shard++)
        stores.add(storeOf(shard, 2 + shard));

      assertThat(encodedPublishingEntrySize(stores)).isLessThanOrEqualTo(ENTRY_CAP);
    }

    /**
     * A store small enough to ride whole still does - it becomes a {@code TsSealedBlob}, not a one-slice sequence -
     * and it is charged against the SAME publishing budget as the sliced stores beside it, which is what stops a
     * mixed session overflowing where an all-sliced one does not.
     */
    // Regression for #6933: a store that fits its share still rides whole and is charged against the same budget
    @Test
    void aStoreThatFitsItsShareStillRidesWholeAndIsChargedAgainstTheSameBudget() {
      final TsSealedBlob small = new TsSealedBlob(TYPE, 0, fileNameOf(0), randomBytes(600));
      final TsSealedBlob big = storeOf(1, 4);

      final List<RaftReplicatedDatabase.SealedSlicePlan> plans = RaftReplicatedDatabase.planSealedShipping(
          List.of(small, big), SEALED_BUDGET, publishingCapacity(), DB);

      assertThat(plans.getFirst().sliced()).as("a 600-byte store needs no slicing").isFalse();
      assertThat(plans.getLast().sliced()).isTrue();
      assertThat(encodedPublishingEntrySize(List.of(small, big))).isLessThanOrEqualTo(ENTRY_CAP);
    }

    /**
     * The publishing entry has to fit BEFORE the first delivery-only slice is committed to the Raft log, because
     * nothing rolls those back: a leader that discovers the entry is unbuildable at the end has already left every
     * follower staging a sealed store it will never install.
     */
    // Regression for #6933: a publishing entry that cannot be built is refused before anything is shipped
    @Test
    void aPublishingEntryThatCannotBeBuiltIsRefusedBeforeAnythingIsShipped() {
      assertThatThrownBy(() -> RaftReplicatedDatabase.planSealedShipping(List.of(storeOf(0, 3), storeOf(1, 3)),
          SEALED_BUDGET, RaftReplicatedDatabase.publishingSealedCapacity(SEALED_BUDGET, ENTRY_CAP, (int) ENTRY_CAP), DB))
          .as("a header that eats the whole entry leaves nothing for the final slices")
          .isInstanceOf(ReplicatedEntryTooLargeException.class);
    }

    /**
     * A cap too small to carry even one slice's framing disables slicing, and that has to keep meaning "ship whole,
     * exactly as before #4416" rather than "refuse the session": {@code TimeSeriesShard}'s own guard is what keeps
     * such a shard from sealing at all, and it is unchanged.
     */
    // Regression for #6933: a budget of zero leaves every store shipping whole
    @Test
    void aBudgetOfZeroLeavesEveryStoreShippingWhole() {
      final List<RaftReplicatedDatabase.SealedSlicePlan> plans = RaftReplicatedDatabase.planSealedShipping(
          List.of(storeOf(0, 3), storeOf(1, 3)), 0, publishingCapacity(), DB);

      assertThat(plans).hasSize(2);
      assertThat(plans).allSatisfy(plan -> assertThat(plan.sliced()).isFalse());
    }

    /**
     * The framing a slice costs besides its payload is not a constant: the type name and the file name derived from
     * it are both written on EVERY slice, and nothing caps a type name's length. A session of stores whose names run
     * to the hundreds of bytes must still build a publishing entry that fits, which it only does because the margin
     * is measured rather than assumed.
     */
    // Regression for #6933: stores with very long names still fit one publishing entry
    @Test
    void storesWithVeryLongNamesStillFitOnePublishingEntry() {
      final String longType = "t".repeat(400);
      final List<TsSealedBlob> stores = new ArrayList<>();
      // Enough shards that the names' bulk outgrows the single-slice reserve the budget already holds back: with a
      // flat 256-byte allowance per extra store this entry comes out over the cap.
      for (int shard = 0; shard < 16; shard++)
        stores.add(new TsSealedBlob(longType, shard, longType + "_shard_" + shard + ".ts.sealed",
            randomBytes((int) (SEALED_BUDGET * 2))));

      assertThat(encodedPublishingEntrySize(stores)).isLessThanOrEqualTo(ENTRY_CAP);
    }

    /**
     * The last unmeasured assumption, on the store the flat reserve is supposed to cover: a LONE store whose names
     * cost more per slice than {@link GlobalConfiguration#REPLICATED_SEALED_CHUNK_FRAMING_BYTES} still has to build
     * an entry that fits, so the first store is charged for whatever it costs above that reserve rather than
     * exempted from measurement.
     */
    // Regression for #6933: a lone store whose names outgrow the flat reserve still fits one publishing entry
    @Test
    void aLoneStoreWhoseNamesOutgrowTheFlatReserveStillFitsOnePublishingEntry() {
      final String hugeType = "t".repeat(6_000);
      final TsSealedBlob store = new TsSealedBlob(hugeType, 0, hugeType + "_shard_0.ts.sealed",
          randomBytes((int) (SEALED_BUDGET * 2)));

      assertThat(encodedPublishingEntrySize(List.of(store))).isLessThanOrEqualTo(ENTRY_CAP);
    }

    // ---- the geometry a follower reassembles from ------------------------------------------------------------

    /**
     * The invariants a follower's reassembly is built on, asserted for a plan whose final slice is deliberately
     * SMALLER than its body slices - the shape the session-wide budget produces and the uniform slicer never did:
     * contiguous offsets from zero, no body slice above the entry budget, a final slice within the share this store
     * was given, exactly one slice flagged {@code last}, and bytes that concatenate back to the original image.
     */
    // Regression for #6933: slices with a small final slice still reassemble into the original image
    @Test
    void slicesWithASmallFinalSliceStillReassembleIntoTheOriginalImage() throws Exception {
      final TsSealedBlob blob = storeOf(2, 4);
      final long tailBudget = 1_500;

      final RaftReplicatedDatabase.SealedSlicePlan plan =
          RaftReplicatedDatabase.planSealedSlices(blob, SEALED_BUDGET, tailBudget, DB);

      assertThat(plan.sliced()).isTrue();

      long nextOffset = 0;
      final ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
      for (int i = 0; i < plan.count(); i++) {
        final TsSealedChunk slice = plan.slice(blob, i);
        final boolean last = i == plan.count() - 1;

        assertThat(slice.offset()).as("slice %d must start where slice %d ended", i, i - 1).isEqualTo(nextOffset);
        assertThat(slice.bytes()).as("no slice may be empty").isNotEmpty();
        assertThat(slice.bytes().length).as("no slice may exceed the entry budget")
            .isLessThanOrEqualTo((int) SEALED_BUDGET);
        if (last)
          assertThat(slice.bytes().length).as("the publishing slice may not exceed this store's share")
              .isLessThanOrEqualTo((int) tailBudget);
        assertThat(slice.fileLength()).isEqualTo(blob.bytes().length);
        assertThat(slice.last()).as("only the final slice publishes").isEqualTo(last);

        reassembled.write(slice.bytes());
        nextOffset += slice.bytes().length;
      }

      assertThat(reassembled.toByteArray()).isEqualTo(blob.bytes());
    }

    /** Every slice the planner emits must still pass the decoder's geometry check. */
    // Regression for #6933: every slice planned with a small tail passes the geometry check
    @Test
    void everySlicePlannedWithASmallTailPassesTheGeometryCheck() {
      final TsSealedBlob blob = storeOf(0, 3);
      final RaftReplicatedDatabase.SealedSlicePlan plan =
          RaftReplicatedDatabase.planSealedSlices(blob, SEALED_BUDGET, 900, DB);

      for (int i = 0; i < plan.count(); i++) {
        final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), true,
            List.of(plan.slice(blob, i)));

        assertThat(RaftLogEntryCodec.decode(encoded).sealedFileChunks()).hasSize(1);
      }
    }

    /**
     * When one store owns the whole budget the plan must reproduce the uniform slicer byte for byte, or the
     * arithmetic #4416 pinned has quietly changed for every single-shard cluster.
     */
    // Regression for #6933: a store that owns the whole budget is sliced exactly as before
    @Test
    void aStoreThatOwnsTheWholeBudgetIsSlicedExactlyAsBefore() {
      final TsSealedBlob blob = storeOf(0, 4);

      final List<TsSealedChunk> uniform = RaftReplicatedDatabase.sliceSealedBlob(blob, SEALED_BUDGET, DB);
      final RaftReplicatedDatabase.SealedSlicePlan plan =
          RaftReplicatedDatabase.planSealedSlices(blob, SEALED_BUDGET, SEALED_BUDGET, DB);

      assertThat(plan.count()).isEqualTo(uniform.size());
      for (int i = 0; i < uniform.size(); i++) {
        assertThat(plan.slice(blob, i).offset()).isEqualTo(uniform.get(i).offset());
        assertThat(plan.slice(blob, i).bytes()).isEqualTo(uniform.get(i).bytes());
        assertThat(plan.slice(blob, i).last()).isEqualTo(uniform.get(i).last());
      }
    }

    // ---- the heap contract -----------------------------------------------------------------------------------

    /**
     * The plan is METADATA. It must not hold the store's bytes, nor any slice cut from them: the leader already
     * carries one whole-file image per sealed store, and the eager slice list #6917 built doubled that for stores
     * the same commit allowed to reach ~2GB. A component of array type here is that doubling coming back.
     */
    // Regression for #6933: a plan holds no bytes
    @Test
    void aPlanHoldsNoBytes() {
      assertThat(RaftReplicatedDatabase.SealedSlicePlan.class.getRecordComponents())
          .as("a plan describes slices, it does not hold them")
          .allSatisfy(component -> assertThat(component.getType().isArray()).isFalse());
    }

    /** And a slice is cut on demand, so shipping one leaves nothing of it behind. */
    // Regression for #6933: each slice is cut fresh on demand
    @Test
    void eachSliceIsCutFreshOnDemand() {
      final TsSealedBlob blob = storeOf(0, 3);
      final RaftReplicatedDatabase.SealedSlicePlan plan =
          RaftReplicatedDatabase.planSealedSlices(blob, SEALED_BUDGET, SEALED_BUDGET, DB);

      assertThat(plan.slice(blob, 0).bytes())
          .isEqualTo(plan.slice(blob, 0).bytes())
          .isNotSameAs(plan.slice(blob, 0).bytes());
    }


    /**
     * The two bounds the capacity has to respect are different bounds, and only the transport one knows about the
     * schema JSON: a deliberately small {@code tsMaxSealedInlineSize} beside a stock append buffer leaves the
     * POLICY budget binding, and charging the header against THAT would stop a shard that used to seal.
     */
    // Regression for #6933: a small inline cap is not charged for the schema json
    @Test
    void aSmallInlineCapIsNotChargedForTheSchemaJson() {
      final long tinyPolicyBudget = 222;

      assertThat(RaftReplicatedDatabase.publishingSealedCapacity(tinyPolicyBudget, 32L * 1024 * 1024, 1_867))
          .as("the header comes off the transport cap, which has room to spare")
          .isEqualTo(tinyPolicyBudget);
    }

    /** And when the two caps coincide the header does come off, because then it really is competing for the entry. */
    // Regression for #6933: a stock cap is charged for the schema json
    @Test
    void aStockCapIsChargedForTheSchemaJson() {
      assertThat(RaftReplicatedDatabase.publishingSealedCapacity(SEALED_BUDGET, ENTRY_CAP, publishingHeaderSize()))
          .isLessThan(SEALED_BUDGET);
    }

    // ---- helpers ---------------------------------------------------------------------------------------------

    /** The publishing entry a session over {@code stores} would build, encoded exactly as the broker encodes it. */
    private long encodedPublishingEntrySize(final List<TsSealedBlob> stores) {
      final List<RaftReplicatedDatabase.SealedSlicePlan> plans =
          RaftReplicatedDatabase.planSealedShipping(stores, SEALED_BUDGET, publishingCapacity(), DB);

      final List<TsSealedBlob> wholeBlobs = new ArrayList<>();
      final List<TsSealedChunk> finalSlices = new ArrayList<>();
      for (int i = 0; i < stores.size(); i++) {
        final RaftReplicatedDatabase.SealedSlicePlan plan = plans.get(i);
        if (plan.sliced())
          finalSlices.add(plan.slice(stores.get(i), plan.count() - 1));
        else
          wholeBlobs.add(stores.get(i));
      }

      return RaftLogEntryCodec.encodeSchemaEntry(DB, SCHEMA_JSON, Collections.emptyMap(), filesToRemove(),
          Collections.emptyList(), Collections.emptyList(), wholeBlobs, false, finalSlices).size();
    }

    private static long publishingCapacity() {
      return RaftReplicatedDatabase.publishingSealedCapacity(SEALED_BUDGET, ENTRY_CAP, publishingHeaderSize());
    }

    private static int publishingHeaderSize() {
      return RaftLogEntryCodec.encodeSchemaEntry(DB, SCHEMA_JSON, Collections.emptyMap(), filesToRemove(),
          Collections.emptyList(), Collections.emptyList(), Collections.emptyList()).size();
    }

    private static Map<Integer, String> filesToRemove() {
      return Map.of(11, "weather_shard_0.ts.sealed.old", 12, "weather_shard_1.ts.sealed.old");
    }

    /**
     * A sealed store of {@code bodySlices} full budgets plus an almost-full remainder, which is what makes the
     * uniform slicer hand the publishing entry a nearly full-budget final slice - the shape that overflows.
     */
    private static TsSealedBlob storeOf(final int shard, final int bodySlices) {
      return new TsSealedBlob(TYPE, shard, fileNameOf(shard),
          randomBytes((int) (SEALED_BUDGET * bodySlices + SEALED_BUDGET - 137L * (shard + 1))));
    }

    private static String fileNameOf(final int shard) {
      return TYPE + "_shard_" + shard + ".ts.sealed";
    }

    private static byte[] randomBytes(final int length) {
      final byte[] bytes = new byte[length];
      new Random(length).nextBytes(bytes);
      return bytes;
    }
  }
}
