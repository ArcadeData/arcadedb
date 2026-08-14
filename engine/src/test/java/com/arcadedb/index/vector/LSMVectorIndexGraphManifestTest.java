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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
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

    manifest.write(3, fingerprint);

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

    manifest.write(7, awkward);

    assertThat(manifest.read().fingerprint()).isEqualTo(awkward);
  }

  /** No live set can have a negative size, which is what makes an unusable manifest impossible to match. */
  @Test
  void anUnusableManifestCannotBeMatchedByAnyLiveSet() {
    final LSMVectorIndexGraphManifest manifest = manifest();
    manifest.write(12, 1234L);

    manifest.markUnusable("simulated persist failure");

    final LSMVectorIndexGraphManifest.Content content = manifest.read();
    assertThat(content).as("it is present - absence would read as 'older version' and fall back to the node count")
        .isNotNull();
    assertThat(content.vectorCount()).isNegative();
  }

  @Test
  void aTruncatedOrCorruptedManifestReadsAsAbsent() throws IOException {
    final LSMVectorIndexGraphManifest manifest = manifest();
    manifest.write(5, 99L);

    Files.writeString(manifestPath(), "{\"formatVersion\": 1, \"vectorCou", StandardCharsets.UTF_8);

    assertThat(manifest.read()).as("unparseable must never be read as a manifest that agrees").isNull();
  }

  @Test
  void aManifestFromAnotherLayoutReadsAsAbsent() throws IOException {
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
    manifest.write(4, 7L);

    manifest.invalidate();

    assertThat(manifest.exists()).isFalse();
    assertThat(manifest.read()).isNull();
  }

  /**
   * A process killed between the temporary write and the atomic move leaves the temporary behind for good: nothing
   * else in the engine knows the file exists. The next write sweeps it.
   */
  @Test
  void aLeftoverTemporaryIsSweptByTheNextWrite() throws IOException {
    final Path leftover = directory.resolve("graph.vecgraph." + LSMVectorIndexGraphManifest.FILE_EXT + ".dead.tmp");
    final Path unrelated = directory.resolve("graph.vecgraph");
    Files.writeString(leftover, "half written", StandardCharsets.UTF_8);
    Files.writeString(unrelated, "the graph itself", StandardCharsets.UTF_8);

    manifest().write(1, 1L);

    assertThat(leftover).as("the sweep must remove an abandoned temporary of this manifest").doesNotExist();
    assertThat(unrelated).as("and must match by name, so it cannot reach anything else").exists();
    try (final Stream<Path> files = Files.list(directory)) {
      assertThat(files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".tmp")).toList())
          .as("and the write must leave no temporary of its own behind").isEqualTo(List.of());
    }
  }

  private LSMVectorIndexGraphManifest manifest() {
    return new LSMVectorIndexGraphManifest(directory.resolve("graph.vecgraph").toString());
  }

  private Path manifestPath() {
    return directory.resolve("graph.vecgraph." + LSMVectorIndexGraphManifest.FILE_EXT);
  }
}
