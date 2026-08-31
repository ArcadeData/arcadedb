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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedBlob;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedChunk;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6933MultiStoreSealedShippingTest {

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
  @Test
  void everyStoresFinalSliceTogetherFitsOnePublishingEntry() {
    final List<TsSealedBlob> stores = List.of(storeOf(0, 3), storeOf(1, 5), storeOf(2, 1));

    assertThat(encodedPublishingEntrySize(stores)).isLessThanOrEqualTo(ENTRY_CAP);
  }

  /** The same, with more shards than a slice can be usefully divided among on a small entry cap. */
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
  @Test
  void aBudgetOfZeroLeavesEveryStoreShippingWhole() {
    final List<RaftReplicatedDatabase.SealedSlicePlan> plans = RaftReplicatedDatabase.planSealedShipping(
        List.of(storeOf(0, 3), storeOf(1, 3)), 0, publishingCapacity(), DB);

    assertThat(plans).hasSize(2);
    assertThat(plans).allSatisfy(plan -> assertThat(plan.sliced()).isFalse());
  }

  // ---- the geometry a follower reassembles from ------------------------------------------------------------

  /**
   * The invariants a follower's reassembly is built on, asserted for a plan whose final slice is deliberately
   * SMALLER than its body slices - the shape the session-wide budget produces and the uniform slicer never did:
   * contiguous offsets from zero, no body slice above the entry budget, a final slice within the share this store
   * was given, exactly one slice flagged {@code last}, and bytes that concatenate back to the original image.
   */
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
  @Test
  void aPlanHoldsNoBytes() {
    assertThat(RaftReplicatedDatabase.SealedSlicePlan.class.getRecordComponents())
        .as("a plan describes slices, it does not hold them")
        .allSatisfy(component -> assertThat(component.getType().isArray()).isFalse());
  }

  /** And a slice is cut on demand, so shipping one leaves nothing of it behind. */
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
  @Test
  void aSmallInlineCapIsNotChargedForTheSchemaJson() {
    final long tinyPolicyBudget = 222;

    assertThat(RaftReplicatedDatabase.publishingSealedCapacity(tinyPolicyBudget, 32L * 1024 * 1024, 1_867))
        .as("the header comes off the transport cap, which has room to spare")
        .isEqualTo(tinyPolicyBudget);
  }

  /** And when the two caps coincide the header does come off, because then it really is competing for the entry. */
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
