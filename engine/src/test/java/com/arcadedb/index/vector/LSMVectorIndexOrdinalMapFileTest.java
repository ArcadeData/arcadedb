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

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

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

  @Test
  void anEmptyMapReadsBackEmptyRatherThanAbsent() {
    final LSMVectorIndexOrdinalMapFile map = mapFile();

    map.write(new int[0], id -> null);

    final LSMVectorIndexOrdinalMapFile.Content content = map.read();
    assertThat(content).isNotNull();
    assertThat(content.size()).isZero();
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
    Files.write(mapPath(), java.util.Arrays.copyOf(whole, whole.length - 5));

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
    manifest.write(new int[] { 0, 1 }, id -> new RID(1, id), null, 0L);
    assertThat(manifest.readOrdinalMap()).isNotNull();

    manifest.write(2, 1234L, LSMVectorIndexGraphManifest.NO_UNREACHABLE_ORDINALS, 0L);

    assertThat(manifest.readOrdinalMap())
        .as("a manifest written from counts alone cannot vouch for an array, so it must not leave one behind")
        .isNull();
  }

  @Test
  void markingTheManifestUnusableDropsTheMap() {
    final LSMVectorIndexGraphManifest manifest = new LSMVectorIndexGraphManifest(graphPath());
    manifest.write(new int[] { 0, 1 }, id -> new RID(1, id), null, 0L);

    manifest.markUnusable("simulated persist failure");

    assertThat(manifest.readOrdinalMap()).isNull();
  }

  @Test
  void invalidatingTheManifestDropsTheMap() {
    final LSMVectorIndexGraphManifest manifest = new LSMVectorIndexGraphManifest(graphPath());
    manifest.write(new int[] { 0, 1 }, id -> new RID(1, id), null, 0L);

    manifest.invalidate();

    assertThat(manifest.readOrdinalMap()).isNull();
  }

  /** Noting a deferred close says nothing about the pages, so it must leave their map alone (issue #6657). */
  @Test
  void markingTheCloseDeferredKeepsTheMap() {
    final LSMVectorIndexGraphManifest manifest = new LSMVectorIndexGraphManifest(graphPath());
    manifest.write(new int[] { 0, 1 }, id -> new RID(1, id), null, 0L);

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
