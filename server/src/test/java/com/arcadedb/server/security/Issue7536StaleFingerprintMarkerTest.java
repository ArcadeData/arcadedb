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
package com.arcadedb.server.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Issue #7536: the fingerprint recording which replicated security document a node installed is written to
 * {@code server-security-cluster.json}, and the document itself to a different file by a different call. When the
 * marker write fails and the document's own write did not, what stayed on disk was the PREVIOUS marker - so a
 * restart read a document from one point in the cluster's history and a marker from an earlier one.
 * <p>
 * <b>Why a stale marker is the worst of the three outcomes.</b> {@code ServerSecurity.isSuperseded} takes the
 * recorded fingerprint as the baseline every node compares the entry's precondition against. With a marker one
 * change behind its peers', this node refuses the very next entry they accept - no race needed, no second
 * coincidence - and it keeps refusing, because nothing it refuses can move its baseline forward. That is a
 * permanent split of the cluster's security state reached from a single lost write.
 * <p>
 * With NO marker the node answers "I cannot judge" and installs, which is what every node does before the first
 * replicated document of that kind lands. It rejoins the cluster's baseline on the next entry that applies. It is
 * not free - if the first entry after the restart is itself a superseded one, this node installs what its peers
 * refuse, which is the residual issue #7752 tracks and closes by carrying the baseline in the replicated protocol
 * - but it needs that extra coincidence, where the stale marker needs none.
 * <p>
 * So a marker write that fails must leave NO marker behind rather than the previous one. These tests pin that,
 * for each of the three document kinds, plus the retry that a failed write must not suppress.
 *
 * @see ReplicatedSecurityFingerprintRepositoryTest for the load-side fail-open branches
 */
class Issue7536StaleFingerprintMarkerTest {

  @TempDir
  private Path configDir;

  /**
   * The defect, on the users document. The first record lands; the second cannot be written. Before the fix the
   * file still held the FIRST fingerprint, and a restarted node judged its peers' entries against a baseline one
   * change out of date.
   */
  @Test
  void aFailedUsersMarkerWriteLeavesNoMarkerRatherThanTheStaleOne() {
    assertNoStaleMarkerSurvives(ReplicatedSecurityFingerprintRepository.USERS);
  }

  /** The same for the group document, which shares the file and the write. */
  @Test
  void aFailedGroupsMarkerWriteLeavesNoMarkerRatherThanTheStaleOne() {
    assertNoStaleMarkerSurvives(ReplicatedSecurityFingerprintRepository.GROUPS);
  }

  /** And for the API-token document. */
  @Test
  void aFailedApiTokensMarkerWriteLeavesNoMarkerRatherThanTheStaleOne() {
    assertNoStaleMarkerSurvives(ReplicatedSecurityFingerprintRepository.API_TOKENS);
  }

  /**
   * The three kinds share one file, so a write that fails cannot discard only the kind it was called for: what is
   * on disk is a single document and the only operation still available on a volume that cannot be written is
   * removing it. Every kind therefore goes back to "cannot judge" together, which is the conservative direction
   * and is self-closing - the next security change that persists records all three again.
   */
  @Test
  void aFailedWriteDiscardsEveryKindBecauseTheyShareOneFile() {
    final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");
    repository.record(ReplicatedSecurityFingerprintRepository.GROUPS, "fp-groups-1");

    repository.failWrites = true;
    repository.record(ReplicatedSecurityFingerprintRepository.API_TOKENS, "fp-tokens-1");

    final ReplicatedSecurityFingerprintRepository reopened =
        new ReplicatedSecurityFingerprintRepository(configDir.toString());
    assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.USERS))
        .as("a marker that can no longer be kept in step with the others is not a marker to judge from").isNull();
    assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.GROUPS)).isNull();
    assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.API_TOKENS)).isNull();
  }

  /**
   * The running node is unaffected: it applied the document, so it holds the cluster's baseline in memory and goes
   * on judging with it. Only the restart loses it, which is what the discard is for.
   */
  @Test
  void theValueStaysInForceInMemoryAfterAFailedWrite() {
    final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");

    repository.failWrites = true;
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-2");

    assertThat(repository.get(ReplicatedSecurityFingerprintRepository.USERS))
        .as("this node installed the document, so it judges from it until it restarts").isEqualTo("fp-users-2");
  }

  /**
   * The re-record that used to be skipped. {@code record()} writes nothing when the fingerprint has not moved,
   * which keeps the Ratis replay of already-applied entries off the fsync path. But after a write that FAILED,
   * "has not moved" is true of memory and false of the disk, so the skip made the loss permanent: the identical
   * document could never repair the file, and only a change to a DIFFERENT one would. The skip now tracks what
   * reached the disk, not what is in memory.
   */
  @Test
  void aRepeatOfTheSameFingerprintIsRetriedAfterAFailedWrite() {
    final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());

    repository.failWrites = true;
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");

    repository.failWrites = false;
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");

    assertThat(new ReplicatedSecurityFingerprintRepository(configDir.toString())
        .get(ReplicatedSecurityFingerprintRepository.USERS))
        .as("the volume came back and the very next apply of the same document had to re-record it")
        .isEqualTo("fp-users-1");
  }

  /**
   * The residual branch: the write failed AND the stale record could not be removed either. Nothing more can be
   * done on the filesystem at that point, so all that is required is that the apply the record came from is not
   * failed by it - {@code record()} runs on the Raft apply thread, and a throw there is the crash loop issue
   * #7137 exists to prevent.
   */
  @Test
  void aDiscardThatCannotHappenEitherIsStillNotAllowedToFailTheApply() throws IOException {
    // A non-empty directory where the marker file belongs: the rename over it fails, and so does deleting it.
    final Path occupied = configDir.resolve(ReplicatedSecurityFingerprintRepository.FILE_NAME);
    Files.createDirectories(occupied);
    Files.writeString(occupied.resolve("keep-me"), "x", StandardCharsets.UTF_8);

    final ReplicatedSecurityFingerprintRepository repository =
        new ReplicatedSecurityFingerprintRepository(configDir.toString());

    assertThatCode(() -> repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users"))
        .as("a marker that can neither be written nor removed must still never fail a committed entry")
        .doesNotThrowAnyException();
    assertThat(repository.get(ReplicatedSecurityFingerprintRepository.USERS)).isEqualTo("fp-users");

    // The branch this test names is the one where BOTH the write and the discard fail, so both have to have
    // been attempted and both have to have failed: the directory is still a directory, and still holds its file.
    assertThat(Files.isDirectory(occupied))
        .as("the rename could not replace it, which is the write failure this test needs").isTrue();
    assertThat(Files.exists(occupied.resolve("keep-me")))
        .as("and the discard could not remove it either, which is the second failure").isTrue();
    try (final Stream<Path> entries = Files.list(configDir)) {
      assertThat(entries.map(entry -> entry.getFileName().toString()).filter(name -> name.endsWith(".tmp")))
          .as("and the temp file it wrote on the way out is not left behind either").isEmpty();
    }
  }

  /** Records one fingerprint successfully, then fails the next write, and asserts nothing stale survives. */
  private void assertNoStaleMarkerSurvives(final String documentKind) {
    final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());
    repository.record(documentKind, "fp-1");

    assertThat(new ReplicatedSecurityFingerprintRepository(configDir.toString()).get(documentKind))
        .as("the fixture's own premise: the first record did reach the disk").isEqualTo("fp-1");

    repository.failWrites = true;
    repository.record(documentKind, "fp-2");

    assertThat(new ReplicatedSecurityFingerprintRepository(configDir.toString()).get(documentKind))
        .as("'fp-1' here is the divergence of issue #7536: this node would refuse the entry its peers accept")
        .isNull();
  }

  /**
   * The repository with its one atomic write made injectable. A real object rather than a mock: only the write
   * step is replaced, and everything this test is about - the discard, the retry bookkeeping, the log - is the
   * production code.
   */
  private static final class FailableFingerprintRepository extends ReplicatedSecurityFingerprintRepository {
    private boolean failWrites;

    private FailableFingerprintRepository(final String securityConfPath) {
      super(securityConfPath);
    }

    @Override
    void writeAtomically(final Path target, final byte[] bytes) throws IOException {
      if (failWrites)
        throw new IOException("No space left on device");
      super.writeAtomically(target, bytes);
    }
  }
}
