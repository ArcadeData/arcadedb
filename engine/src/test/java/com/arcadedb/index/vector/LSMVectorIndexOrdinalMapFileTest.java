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
package com.arcadedb.index.vector;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The ordinal map sidecar on its own (issue #7842). What this file says is what an ordinal of a persisted graph
 * MEANS, so anything it cannot read whole has to read as "no map" - never as a map that happens to agree, which
 * would pair a graph with the wrong records rather than merely cost a rebuild.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexOrdinalMapFileTest {

  @TempDir
  Path directory;

  @Test
  void aWrittenMapReadsBackAsItself() {
    final LSMVectorIndexOrdinalMapFile map = mapFile();
    final int[] vectorIds = { 0, 1, 4, 9, 100_000 };

    map.write(vectorIds, id -> new RID(3, id * 7L + 1));

    assertThat(map.exists()).isTrue();
    final LSMVectorIndexOrdinalMapFile.Content content = map.read();
    assertThat(content).isNotNull();
    assertThat(content.size()).isEqualTo(vectorIds.length);
    assertThat(content.vectorIds()).containsExactly(vectorIds);
    for (int ordinal = 0; ordinal < vectorIds.length; ordinal++) {
      assertThat(content.hasRid(ordinal)).isTrue();
      assertThat(content.bucketIds()[ordinal]).isEqualTo(3);
      assertThat(content.positions()[ordinal]).isEqualTo(vectorIds[ordinal] * 7L + 1);
    }
  }

  /**
   * A RID position is a long, and the whole point of recording the RID is to detect an id space that was reissued
   * underneath the graph - a comparison that is only as good as the round trip.
   */
  @Test
  void aRidPositionUsingTheFullLongRangeSurvives() {
    final LSMVectorIndexOrdinalMapFile map = mapFile();
    final long awkward = 0x0001_2345_6789_ABCDL;

    map.write(new int[] { 42 }, id -> new RID(0x7FFF, awkward));

    final LSMVectorIndexOrdinalMapFile.Content content = map.read();
    assertThat(content.bucketIds()[0]).isEqualTo(0x7FFF);
    assertThat(content.positions()[0]).isEqualTo(awkward);
  }

  /**
   * An ordinal whose vector had already lost its location when the graph was persisted carries no RID. It must read
   * back as "none recorded" rather than as bucket 0 position 0, which is a real RID some record holds.
   */
  @Test
  void anOrdinalWithNoRidIsDistinguishableFromOneAtBucketZero() {
    final LSMVectorIndexOrdinalMapFile map = mapFile();

    map.write(new int[] { 7, 8 }, id -> id == 7 ? null : new RID(0, 0L));

    final LSMVectorIndexOrdinalMapFile.Content content = map.read();
    assertThat(content.hasRid(0)).as("no location was recorded for vector 7").isFalse();
    assertThat(content.hasRid(1)).as("bucket 0 / position 0 is a real RID").isTrue();
    assertThat(content.bucketIds()[1]).isZero();
    assertThat(content.positions()[1]).isZero();
  }

  /**
   * A negative bucket id is the engine's own marker for an absent or unsaved RID, so one cannot be written as a real
   * location without becoming indistinguishable from "no RID recorded" on the way back in.
   */
  @Test
  void aRidWithTheSentinelBucketIsRecordedAsNoRidRatherThanAsALocation() {
    final LSMVectorIndexOrdinalMapFile map = mapFile();

    map.write(new int[] { 5 }, id -> new RID(-1, 99L));

    final LSMVectorIndexOrdinalMapFile.Content content = map.read();
    assertThat(content.hasRid(0)).as("it must read back as absent, never as a location at bucket -1").isFalse();
  }

  @Test
  void anEmptyMapReadsBackEmptyRatherThanAbsent() {
    final LSMVectorIndexOrdinalMapFile map = mapFile();

    map.write(new int[0], id -> null);

    final LSMVectorIndexOrdinalMapFile.Content content = map.read();
    assertThat(content).isNotNull();
    assertThat(content.size()).isZero();
  }

  /**
   * The encoder drains a fixed buffer rather than building the whole payload, so the entry that straddles a drain
   * boundary - and the hash across those boundaries - is the thing that would break if the chunking were wrong.
   * 40,000 entries is several drains at any plausible buffer size.
   */
  @Test
  void aMapSpanningManyWriteChunksRoundTrips() {
    final LSMVectorIndexOrdinalMapFile map = mapFile();
    final int[] vectorIds = new int[40_000];
    for (int i = 0; i < vectorIds.length; i++)
      vectorIds[i] = i * 3;

    map.write(vectorIds, id -> new RID(id % 17, id * 1_000_003L));

    final LSMVectorIndexOrdinalMapFile.Content content = map.read();
    assertThat(content).as("a payload spanning several write chunks must still hash and parse whole").isNotNull();
    assertThat(content.vectorIds()).containsExactly(vectorIds);
    for (int ordinal = 0; ordinal < vectorIds.length; ordinal++) {
      assertThat(content.bucketIds()[ordinal]).isEqualTo(vectorIds[ordinal] % 17);
      assertThat(content.positions()[ordinal]).isEqualTo(vectorIds[ordinal] * 1_000_003L);
    }
  }

  @Test
  void aMissingMapReadsAsAbsent() {
    assertThat(mapFile().read()).isNull();
  }

  @Test
  void aTruncatedMapReadsAsAbsent() throws Exception {
    final LSMVectorIndexOrdinalMapFile map = mapFile();
    map.write(new int[] { 0, 1, 2, 3 }, id -> new RID(1, id));

    final byte[] whole = Files.readAllBytes(mapPath());
    Files.write(mapPath(), Arrays.copyOf(whole, whole.length - 5));

    assertThat(map.read()).as("a short read must never be trusted as a whole map").isNull();
  }

  @Test
  void aCorruptedMapReadsAsAbsent() throws Exception {
    final LSMVectorIndexOrdinalMapFile map = mapFile();
    map.write(new int[] { 0, 1, 2, 3 }, id -> new RID(1, id));

    final byte[] whole = Files.readAllBytes(mapPath());
    // One flipped payload byte, with the trailing hash left intact: exactly what the hash is there to catch.
    whole[whole.length - 12] ^= 0x40;
    Files.write(mapPath(), whole);

    assertThat(map.read()).as("a payload that does not match its own hash must read as absent").isNull();
  }

  /**
   * The format is versioned so it can evolve, and the check that enforces that sits BEHIND the hash check - so a
   * test that only flips the version byte exercises the hash branch instead. This one repairs the hash afterwards,
   * which is exactly what a future build writing version 2 would produce.
   */
  @Test
  void aMapWrittenByAnUnknownFormatVersionReadsAsAbsent() throws Exception {
    final LSMVectorIndexOrdinalMapFile map = mapFile();
    map.write(new int[] { 0, 1, 2 }, id -> new RID(1, id));

    final byte[] whole = Files.readAllBytes(mapPath());
    assertThat(whole[0]).as("the version is the leading varint, one byte while it is small")
        .isEqualTo((byte) LSMVectorIndexOrdinalMapFile.FORMAT_VERSION);
    whole[0] = (byte) (LSMVectorIndexOrdinalMapFile.FORMAT_VERSION + 1);

    final int payloadLength = whole.length - 8;
    long hash = LSMVectorIndexOrdinalMapFile.hashOf(whole, payloadLength);
    for (int i = whole.length - 1; i >= payloadLength; i--) {
      whole[i] = (byte) hash;
      hash >>>= 8;
    }
    Files.write(mapPath(), whole);

    assertThat(map.read()).as("a layout this build does not know must not be read as one it does").isNull();
  }

  @Test
  void theFingerprintReturnedByAWriteIsTheOneTheManifestWouldHaveComputed() {
    final int[] vectorIds = { 0, 3, 11 };
    final IntFunction<RID> rids = id -> id == 3 ? null : new RID(2, id * 5L);

    final long returned = mapFile().write(vectorIds, rids);

    assertThat(returned)
        .as("the write accumulates the fingerprint from the RIDs it already resolved, so it must agree with the "
            + "one-shot form the map-absent fallback path still uses")
        .isEqualTo(LSMVectorIndexGraphManifest.fingerprintOf(vectorIds, rids));
  }

  /**
   * A count is read back before the three arrays it sizes are allocated, so a file whose header claims far more
   * entries than its payload can hold would allocate from that claim - and an OutOfMemoryError is an Error, which
   * {@code read()}'s catch would not turn into the "no usable map" answer every other unreadable file gets.
   */
  @Test
  void aMapClaimingMoreEntriesThanItCanHoldReadsAsAbsent() throws Exception {
    final Binary payload = new Binary(16);
    payload.putUnsignedNumber(LSMVectorIndexOrdinalMapFile.FORMAT_VERSION);
    payload.putUnsignedNumber(Integer.MAX_VALUE);

    final byte[] bytes = payload.toByteArray();
    final byte[] file = Arrays.copyOf(bytes, bytes.length + 8);
    long hash = LSMVectorIndexOrdinalMapFile.hashOf(bytes, bytes.length);
    for (int i = file.length - 1; i >= bytes.length; i--) {
      file[i] = (byte) hash;
      hash >>>= 8;
    }
    Files.write(mapPath(), file);

    assertThat(mapFile().read())
        .as("the header hashes correctly and still describes nothing this file holds: it must be refused, not "
            + "allocated from")
        .isNull();
  }

  /**
   * A caller that will never read a map back - a PRODUCT-quantized index, whose load path refuses to consult one -
   * must not merely skip writing it: a map left over from before that decision would pair this generation's pages
   * with the previous generation's ordinals.
   */
  @Test
  void aManifestWrittenWithoutRecordingAMapDropsWhateverMapWasThere() {
    final LSMVectorIndexGraphManifest manifest = new LSMVectorIndexGraphManifest(graphPath());
    final int[] vectorIds = { 0, 1 };
    final IntFunction<RID> rids = id -> new RID(1, id);
    manifest.write(vectorIds, rids, true, null, 0L);
    assertThat(manifest.readOrdinalMap()).isNotNull();

    manifest.write(vectorIds, rids, false, null, 0L);

    assertThat(manifest.readOrdinalMap()).isNull();
    assertThat(manifest.read().fingerprint())
        .as("the certificate itself is unaffected: the same one walk still produces it")
        .isEqualTo(LSMVectorIndexGraphManifest.fingerprintOf(vectorIds, rids));
  }

  @Test
  void anInvalidatedMapIsGone() {
    final LSMVectorIndexOrdinalMapFile map = mapFile();
    map.write(new int[] { 0 }, id -> new RID(1, 1L));

    map.invalidate();

    assertThat(map.exists()).isFalse();
    assertThat(map.read()).isNull();
  }

  /** The manifest owns the map's lifecycle, so the two can never describe different generations of the pages. */
  @Test
  void writingTheManifestWithoutAnArrayDropsTheMap() {
    final LSMVectorIndexGraphManifest manifest = new LSMVectorIndexGraphManifest(graphPath());
    manifest.write(new int[] { 0, 1 }, id -> new RID(1, id), true, null, 0L);
    assertThat(manifest.readOrdinalMap()).isNotNull();

    manifest.write(2, 1234L, LSMVectorIndexGraphManifest.NO_UNREACHABLE_ORDINALS, 0L);

    assertThat(manifest.readOrdinalMap())
        .as("a manifest written from counts alone cannot vouch for an array, so it must not leave one behind")
        .isNull();
  }

  @Test
  void markingTheManifestUnusableDropsTheMap() {
    final LSMVectorIndexGraphManifest manifest = new LSMVectorIndexGraphManifest(graphPath());
    manifest.write(new int[] { 0, 1 }, id -> new RID(1, id), true, null, 0L);

    manifest.markUnusable("simulated persist failure");

    assertThat(manifest.readOrdinalMap()).isNull();
  }

  @Test
  void invalidatingTheManifestDropsTheMap() {
    final LSMVectorIndexGraphManifest manifest = new LSMVectorIndexGraphManifest(graphPath());
    manifest.write(new int[] { 0, 1 }, id -> new RID(1, id), true, null, 0L);

    manifest.invalidate();

    assertThat(manifest.readOrdinalMap()).isNull();
  }

  /** Noting a deferred close says nothing about the pages, so it must leave their map alone (issue #6657). */
  @Test
  void markingTheCloseDeferredKeepsTheMap() {
    final LSMVectorIndexGraphManifest manifest = new LSMVectorIndexGraphManifest(graphPath());
    manifest.write(new int[] { 0, 1 }, id -> new RID(1, id), true, null, 0L);

    manifest.markCloseDeferred();

    assertThat(manifest.readOrdinalMap()).isNotNull();
    assertThat(manifest.read().closeDeferredRebuild()).isTrue();
  }

  @Test
  void aWriteLeavesNoTemporaryBehind() throws Exception {
    final LSMVectorIndexOrdinalMapFile map = mapFile();
    map.write(new int[] { 0, 1 }, id -> new RID(1, id));
    map.write(new int[] { 0, 1, 2 }, id -> new RID(1, id));

    try (final var entries = Files.list(directory)) {
      assertThat(entries.filter(p -> p.getFileName().toString().endsWith(".tmp")))
          .as("a temporary nothing reads and nothing would ever remove must not survive the write").isEmpty();
    }
  }

  private String graphPath() {
    return directory.resolve("graph.vecgraph").toString();
  }

  private LSMVectorIndexOrdinalMapFile mapFile() {
    return new LSMVectorIndexOrdinalMapFile(graphPath());
  }

  private Path mapPath() {
    return directory.resolve("graph.vecgraph." + LSMVectorIndexOrdinalMapFile.FILE_EXT);
  }
}
