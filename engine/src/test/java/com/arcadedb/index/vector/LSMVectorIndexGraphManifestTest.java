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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The sidecar of {@link LSMVectorIndexGraphFile}, on its own. Every one of these behaviours decides whether a
 * persisted graph is reused or rebuilt (issue #6106), and each is cheaper to pin here than through a database:
 * anything this class cannot read has to read as "no manifest", never as a manifest that happens to agree.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class LSMVectorIndexGraphManifestTest {

  @TempDir
  Path directory;

  @Test
  void aWrittenManifestReadsBackAsItself() {
    final LSMVectorIndexGraphManifest manifest = manifest();
    final long fingerprint = LSMVectorIndexGraphManifest.fingerprintOf(new int[] { 0, 1, 2 },
        id -> new RID(3, id * 10L));

    manifest.write(3, fingerprint, LSMVectorIndexGraphManifest.NO_UNREACHABLE_ORDINALS, 0L);

    assertThat(manifest.exists()).isTrue();
    final LSMVectorIndexGraphManifest.Content content = manifest.read();
    assertThat(content).isNotNull();
    assertThat(content.vectorCount()).isEqualTo(3);
    assertThat(content.fingerprint()).as("a 64-bit fingerprint must survive the round trip whole")
        .isEqualTo(fingerprint);
  }

  /**
   * The fingerprint is stored as a string precisely because a JSON number decodes through a double, which cannot
   * hold 64 significant bits. A value that exercises the low bits is the one that catches a regression here.
   */
  @Test
  void aFingerprintUsingTheFullSixtyFourBitsSurvives() {
    final LSMVectorIndexGraphManifest manifest = manifest();
    final long awkward = -6_148_914_691_236_517_206L; // 0xAAAA...AAAA

    manifest.write(7, awkward, LSMVectorIndexGraphManifest.NO_UNREACHABLE_ORDINALS, 0L);

    assertThat(manifest.read().fingerprint()).isEqualTo(awkward);
  }

  /** No live set can have a negative size, which is what makes an unusable manifest impossible to match. */
  @Test
  void anUnusableManifestCannotBeMatchedByAnyLiveSet() {
    final LSMVectorIndexGraphManifest manifest = manifest();
    manifest.write(12, 1234L, LSMVectorIndexGraphManifest.NO_UNREACHABLE_ORDINALS, 0L);

    manifest.markUnusable("simulated persist failure");

    final LSMVectorIndexGraphManifest.Content content = manifest.read();
    assertThat(content).as("it is present - absence would read as 'older version' and fall back to the node count")
        .isNotNull();
    assertThat(content.vectorCount()).isNegative();
  }

  @Test
  void aTruncatedOrCorruptedManifestReadsAsAbsent() throws Exception {
    final LSMVectorIndexGraphManifest manifest = manifest();
    manifest.write(5, 99L, LSMVectorIndexGraphManifest.NO_UNREACHABLE_ORDINALS, 0L);

    Files.writeString(manifestPath(), "{\"formatVersion\": 1, \"vectorCou", StandardCharsets.UTF_8);

    assertThat(manifest.read()).as("unparseable must never be read as a manifest that agrees").isNull();
  }

  @Test
  void aManifestFromAnotherLayoutReadsAsAbsent() throws Exception {
    final JSONObject fromTheFuture = new JSONObject();
    fromTheFuture.put("formatVersion", LSMVectorIndexGraphManifest.FORMAT_VERSION + 1);
    fromTheFuture.put("vectorCount", 5);
    fromTheFuture.put("fingerprint", "99");
    Files.writeString(manifestPath(), fromTheFuture.toString(), StandardCharsets.UTF_8);

    assertThat(manifest().read()).as("fields this build does not know may mean anything: rebuild instead").isNull();
  }

  @Test
  void readingAndInvalidatingAMissingManifestAreSilentNoOps() {
    final LSMVectorIndexGraphManifest manifest = manifest();

    assertThat(manifest.exists()).isFalse();
    assertThat(manifest.read()).isNull();
    manifest.invalidate();
    assertThat(manifest.exists()).isFalse();
  }

  @Test
  void invalidateRemovesTheManifestSoNothingVouchesForThePages() {
    final LSMVectorIndexGraphManifest manifest = manifest();
    manifest.write(4, 7L, LSMVectorIndexGraphManifest.NO_UNREACHABLE_ORDINALS, 0L);

    manifest.invalidate();

    assertThat(manifest.exists()).isFalse();
    assertThat(manifest.read()).isNull();
  }

  /**
   * A process killed between the temporary write and the atomic move leaves the temporary behind for good: nothing
   * else in the engine knows the file exists. The next write sweeps it.
   */
  @Test
  void aLeftoverTemporaryIsSweptByTheNextWrite() throws Exception {
    final Path leftover = directory.resolve("graph.vecgraph." + LSMVectorIndexGraphManifest.FILE_EXT + ".dead.tmp");
    final Path unrelated = directory.resolve("graph.vecgraph");
    Files.writeString(leftover, "half written", StandardCharsets.UTF_8);
    Files.writeString(unrelated, "the graph itself", StandardCharsets.UTF_8);

    manifest().write(1, 1L, LSMVectorIndexGraphManifest.NO_UNREACHABLE_ORDINALS, 0L);

    assertThat(leftover).as("the sweep must remove an abandoned temporary of this manifest").doesNotExist();
    assertThat(unrelated).as("and must match by name, so it cannot reach anything else").exists();
    try (final Stream<Path> files = Files.list(directory)) {
      assertThat(files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".tmp")).toList())
          .as("and the write must leave no temporary of its own behind").isEqualTo(List.of());
    }
  }

  /**
   * The ordinals a build could not link are part of what the manifest has to say about the pages next to it: the
   * session that built the graph serves those vectors from the delta scan, and a reopened session can only do the
   * same if it is told which ones they are (issue #7190).
   */
  @Test
  void unreachableOrdinalsSurviveTheRoundTrip() {
    final LSMVectorIndexGraphManifest manifest = manifest();

    manifest.write(50_000, 42L, new int[] { 7, 39_896, 49_999 }, 0L);

    assertThat(manifest.read().unreachableOrdinals()).containsExactly(7, 39_896, 49_999);
  }

  @Test
  void aManifestWithoutUnreachableOrdinalsReadsAsNoneRatherThanNull() {
    final LSMVectorIndexGraphManifest manifest = manifest();

    manifest.write(10, 5L, LSMVectorIndexGraphManifest.NO_UNREACHABLE_ORDINALS, 0L);

    assertThat(manifest.read().unreachableOrdinals())
        .as("callers walk this array; an absent entry must read as empty, never as null").isEmpty();
  }

  /**
   * The set is a new optional key inside the SAME format version, deliberately: bumping the version would make
   * every manifest written by an older build read as absent, and every existing index rebuild its graph on the
   * first open after an upgrade. A manifest from before issue #7190 therefore has to stay perfectly usable.
   */
  @Test
  void aManifestWrittenBeforeThisFieldExistedIsStillUsable() throws Exception {
    final JSONObject beforeIssue7190 = new JSONObject();
    beforeIssue7190.put("formatVersion", LSMVectorIndexGraphManifest.FORMAT_VERSION);
    beforeIssue7190.put("vectorCount", 1_500);
    beforeIssue7190.put("fingerprint", "-6148914691236517206");
    beforeIssue7190.put("closeDeferredRebuild", false);
    Files.writeString(manifestPath(), beforeIssue7190.toString(), StandardCharsets.UTF_8);

    final LSMVectorIndexGraphManifest.Content content = manifest().read();
    assertThat(content).as("an older manifest must not be refused over a key it could not have written").isNotNull();
    assertThat(content.vectorCount()).isEqualTo(1_500);
    assertThat(content.fingerprint()).isEqualTo(-6_148_914_691_236_517_206L);
    assertThat(content.unreachableOrdinals()).isEmpty();
  }

  /**
   * A malformed set costs the set, not the manifest: ignoring it puts the index back at the pre-#7190 behaviour for
   * those nodes, while refusing the manifest would force a full rebuild of a graph that is otherwise fine.
   */
  @Test
  void anUnreadableUnreachableOrdinalsEntryIsIgnoredRatherThanFailingTheManifest() throws Exception {
    final JSONObject damaged = new JSONObject();
    damaged.put("formatVersion", LSMVectorIndexGraphManifest.FORMAT_VERSION);
    damaged.put("vectorCount", 20);
    damaged.put("fingerprint", "77");
    damaged.put("unreachableOrdinals", new JSONArray(new Object[] { "not-a-number" }));
    Files.writeString(manifestPath(), damaged.toString(), StandardCharsets.UTF_8);

    final LSMVectorIndexGraphManifest.Content content = manifest().read();
    assertThat(content).isNotNull();
    assertThat(content.vectorCount()).isEqualTo(20);
    assertThat(content.unreachableOrdinals()).isEmpty();
  }

  /**
   * {@code markCloseDeferred()} is a note about pages that have NOT changed, so what those pages leave unreachable
   * has not changed either. Losing the set there would make the very next open miss those vectors again.
   */
  @Test
  void markingACloseAsDeferredKeepsTheUnreachableOrdinals() {
    final LSMVectorIndexGraphManifest manifest = manifest();
    manifest.write(1_000, 11L, new int[] { 3, 4 }, 3_148_792_924L);

    manifest.markCloseDeferred();

    final LSMVectorIndexGraphManifest.Content content = manifest.read();
    assertThat(content.closeDeferredRebuild()).isTrue();
    assertThat(content.vectorCount()).isEqualTo(1_000);
    assertThat(content.unreachableOrdinals()).containsExactly(3, 4);
    assertThat(content.graphBytes())
        .as("the pages have not changed, so neither has their length (issue #7362)").isEqualTo(3_148_792_924L);
  }

  /** A completed build supersedes whatever the previous one orphaned - a Vamana build orphans a fresh set. */
  @Test
  void aLaterWriteReplacesThePreviousUnreachableOrdinals() {
    final LSMVectorIndexGraphManifest manifest = manifest();
    manifest.write(1_000, 11L, new int[] { 3, 4 }, 0L);

    manifest.write(1_000, 12L, new int[] { 9 }, 0L);

    assertThat(manifest.read().unreachableOrdinals()).containsExactly(9);
  }

  /**
   * The fingerprint is the proof that a persisted graph still describes the records it was built over (issue #6106): a graph is
   * reused when the fingerprint of the live ordinal map equals the recorded one, so every part of the ordinal to (vector id, RID)
   * correspondence has to move it. Written and read back through the same function, a fingerprint that ignored a byte of an id, a
   * bucket or a position would pass every other test here and certify a graph built over other records.
   */
  @Test
  void theFingerprintMovesWithEveryPartOfTheCorrespondence() {
    final int[] ids = { 0, 1, 2, 3 };
    final long base = LSMVectorIndexGraphManifest.fingerprintOf(ids, id -> new RID(3, id * 10L));

    assertThat(base).isNotZero();
    assertThat(LSMVectorIndexGraphManifest.fingerprintOf(ids, id -> new RID(3, id * 10L))).as("deterministic").isEqualTo(base);
    assertThat(LSMVectorIndexGraphManifest.fingerprintOf(new int[] { 0, 1, 2 }, id -> new RID(3, id * 10L))).as("one ordinal fewer")
        .isNotEqualTo(base);
    assertThat(LSMVectorIndexGraphManifest.fingerprintOf(new int[] { 0, 1, 3, 2 }, id -> new RID(3, id * 10L))).as("two ids swapped")
        .isNotEqualTo(base);
    for (final int shift : new int[] { 0, 8, 16, 24 }) {
      final int movedId = 2 ^ (1 << shift);
      assertThat(LSMVectorIndexGraphManifest.fingerprintOf(new int[] { 0, 1, movedId, 3 }, id -> new RID(3, id * 10L)))
          .as("byte of the vector id touched by 1 << %d", shift).isNotEqualTo(base);
      assertThat(LSMVectorIndexGraphManifest.fingerprintOf(ids, id -> new RID(id == 2 ? 3 ^ (1 << shift) : 3, id * 10L)))
          .as("byte of the bucket id touched by 1 << %d", shift).isNotEqualTo(base);
    }
    for (final int shift : new int[] { 0, 8, 16, 24, 32, 40, 48, 56 })
      assertThat(LSMVectorIndexGraphManifest.fingerprintOf(ids, id -> new RID(3, id == 2 ? (id * 10L) ^ (1L << shift) : id * 10L)))
          .as("byte of the position touched by 1L << %d", shift).isNotEqualTo(base);
    assertThat(LSMVectorIndexGraphManifest.fingerprintOf(ids, id -> id == 2 ? null : new RID(3, id * 10L)))
        .as("an unresolvable RID is not skipped").isNotEqualTo(base);
    assertThat(LSMVectorIndexGraphManifest.fingerprintOf(ids, id -> id == 2 ? null : new RID(3, id * 10L)))
        .as("an unresolvable RID is not a location at bucket 0, position 0")
        .isNotEqualTo(LSMVectorIndexGraphManifest.fingerprintOf(ids, id -> id == 2 ? new RID(0, 0L) : new RID(3, id * 10L)));
  }

  /**
   * The fingerprint is persisted in every manifest, so its construction is a format: changing it makes every graph on disk read as
   * built over other records and costs each index a rebuild at the first open after an upgrade. Pinned against an independent
   * FNV-1a over the documented byte sequence (the count, then per ordinal the id, the bucket id and the position, each big-endian).
   */
  @Test
  void theFingerprintIsFnv1aOverTheCountAndEveryIdBucketAndPosition() {
    final int[] ids = { 0, 7, 300, 70_000, Integer.MAX_VALUE };
    final RID[] rids = { new RID(0, 0L), new RID(5, 1L << 40), new RID(2, 123_456_789L), null, new RID(1_000_000, Long.MAX_VALUE) };

    long expected = 0xcbf29ce484222325L;
    expected = fnv1a(expected, ids.length, 4);
    for (int i = 0; i < ids.length; i++) {
      expected = fnv1a(expected, ids[i], 4);
      expected = fnv1a(expected, rids[i] == null ? -1 : rids[i].getBucketId(), 4);
      expected = fnv1a(expected, rids[i] == null ? -1L : rids[i].getPosition(), 8);
    }

    final long actual = LSMVectorIndexGraphManifest.fingerprintOf(ids, id -> {
      for (int i = 0; i < ids.length; i++)
        if (ids[i] == id)
          return rids[i];
      return null;
    });

    assertThat(actual).isEqualTo(expected);
    assertThat(LSMVectorIndexGraphManifest.fingerprintSeed(5)).isEqualTo(fnv1a(0xcbf29ce484222325L, 5, 4));
  }

  /** FNV-1a over the {@code width} big-endian bytes of {@code value}. */
  private static long fnv1a(final long seed, final long value, final int width) {
    long hash = seed;
    for (int i = width - 1; i >= 0; i--) {
      hash ^= (value >>> (8 * i)) & 0xFFL;
      hash *= 0x100000001b3L;
    }
    return hash;
  }

  private LSMVectorIndexGraphManifest manifest() {
    return new LSMVectorIndexGraphManifest(directory.resolve("graph.vecgraph").toString());
  }

  private Path manifestPath() {
    return directory.resolve("graph.vecgraph." + LSMVectorIndexGraphManifest.FILE_EXT);
  }
}
