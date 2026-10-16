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
 * Issue #7536, as issue #7752 reshaped it: what a failed write of {@code server-security-cluster.json} leaves on disk,
 * and what it reports.
 * <p>
 * #7536 found that a failed write left the PREVIOUS record behind, and that a restart then judged its peers' entries
 * against a baseline one change out of date - refusing, forever, the very entry they accepted, because the entry that
 * moved the baseline forward could already have been compacted out of the Raft log. It answered by REMOVING the file.
 * <p>
 * Issue #7752 moved the guarantee into the Raft log instead: {@link ReplicatedSecurityFingerprintRepository#isDurable}
 * reports the failed write, and the HA layer keeps the entry that installed the document out of every snapshot
 * checkpoint until it is durable, so a restart REPLAYS it. Against a replay the previous record is exactly the right
 * baseline - the one every peer judged that entry against - while no record at all is the harmful one: the file holds
 * all three kinds, and a replay judged with no baseline installs the entries its peers refused. So a failed write now
 * leaves the previous record in place and says so; these tests pin that for each kind, plus the retry.
 *
 * @see ReplicatedSecurityFingerprintRepositoryTest for the load-side fail-open branches
 */
class Issue7536StaleFingerprintMarkerTest {

  @TempDir
  private Path configDir;

  @Test
  void aFailedUsersRecordWriteKeepsThePreviousRecordAndReportsItNotDurable() {
    assertPreviousRecordSurvives(ReplicatedSecurityFingerprintRepository.USERS);
  }

  @Test
  void aFailedGroupsRecordWriteKeepsThePreviousRecordAndReportsItNotDurable() {
    assertPreviousRecordSurvives(ReplicatedSecurityFingerprintRepository.GROUPS);
  }

  @Test
  void aFailedApiTokensRecordWriteKeepsThePreviousRecordAndReportsItNotDurable() {
    assertPreviousRecordSurvives(ReplicatedSecurityFingerprintRepository.API_TOKENS);
  }

  /**
   * The three kinds share one file, so a failed write of one must not cost the others their baseline: a replay judged
   * without one would install what the peers refused for a kind whose own write never failed.
   */
  @Test
  void aFailedWriteOfOneKindKeepsTheOtherKindsDurable() {
    final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");
    repository.record(ReplicatedSecurityFingerprintRepository.GROUPS, "fp-groups-1");

    repository.failWrites = true;
    repository.record(ReplicatedSecurityFingerprintRepository.API_TOKENS, "fp-tokens-1");

    assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.API_TOKENS)).isFalse();
    assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.USERS)).isTrue();
    assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.GROUPS)).isTrue();

    final ReplicatedSecurityFingerprintRepository reopened =
        new ReplicatedSecurityFingerprintRepository(configDir.toString());
    assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.USERS)).isEqualTo("fp-users-1");
    assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.GROUPS)).isEqualTo("fp-groups-1");
    assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.API_TOKENS))
        .as("never recorded on disk, so a replay of its entry installs it like every peer did").isNull();
  }

  /**
   * The running node is unaffected: it applied the document, so it holds the cluster's baseline in memory and goes
   * on judging with it.
   */
  @Test
  void theValueStaysInForceInMemoryAfterAFailedWrite() {
    final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");

    repository.failWrites = true;
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-2");

    assertThat(repository.get(ReplicatedSecurityFingerprintRepository.USERS))
        .as("this node installed the document, so it judges from it").isEqualTo("fp-users-2");
  }

  /**
   * The re-record that used to be skipped. {@code record()} writes nothing when the fingerprint has not moved, which
   * keeps the Ratis replay of already-applied entries off the fsync path. After a write that FAILED, "has not moved" is
   * true of memory and false of the disk, so the skip tracks what reached the disk, not what is in memory.
   */
  @Test
  void aRepeatOfTheSameFingerprintIsRetriedAfterAFailedWrite() {
    final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());

    repository.failWrites = true;
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");

    repository.failWrites = false;
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");

    assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.USERS)).isTrue();
    assertThat(new ReplicatedSecurityFingerprintRepository(configDir.toString())
        .get(ReplicatedSecurityFingerprintRepository.USERS))
        .as("the volume came back and the very next apply of the same document had to re-record it")
        .isEqualTo("fp-users-1");
  }

  /**
   * {@link ReplicatedSecurityFingerprintRepository#flush()} is the retry issue #7752 runs while the Raft log is pinned:
   * it writes what is in force without waiting for another entry, and is a no-op once everything is on disk.
   */
  @Test
  void flushWritesWhatIsInForceOnceTheVolumeIsWritableAgain() {
    final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-1");

    repository.failWrites = true;
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users-2");
    assertThat(repository.flush()).as("the volume is still failing").isFalse();
    assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.USERS)).isFalse();

    repository.failWrites = false;
    assertThat(repository.flush()).isTrue();
    assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.USERS)).isTrue();
    assertThat(new ReplicatedSecurityFingerprintRepository(configDir.toString())
        .get(ReplicatedSecurityFingerprintRepository.USERS)).isEqualTo("fp-users-2");

    repository.writes = 0;
    assertThat(repository.flush()).isTrue();
    assertThat(repository.writes).as("nothing left to write, so nothing is written").isZero();
  }

  /**
   * A volume on which the record can be neither rewritten nor removed - the read-only sub-case recorded on issue #7752.
   * Nothing more can be done on the filesystem, so all that is required is that the apply the record came from is not
   * failed by it ({@code record()} runs on the Raft apply thread, and a throw there is the crash loop of issue #7137),
   * and that the failure is reported so the HA layer keeps the entry replayable.
   */
  @Test
  void aRecordThatCannotBeWrittenNeverFailsTheApplyAndIsReportedNotDurable() throws IOException {
    // A non-empty directory where the marker file belongs: the rename over it fails.
    final Path occupied = configDir.resolve(ReplicatedSecurityFingerprintRepository.FILE_NAME);
    Files.createDirectories(occupied);
    Files.writeString(occupied.resolve("keep-me"), "x", StandardCharsets.UTF_8);

    final ReplicatedSecurityFingerprintRepository repository =
        new ReplicatedSecurityFingerprintRepository(configDir.toString());

    assertThatCode(() -> repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users"))
        .as("a record that cannot be written must still never fail a committed entry")
        .doesNotThrowAnyException();
    assertThat(repository.get(ReplicatedSecurityFingerprintRepository.USERS)).isEqualTo("fp-users");
    assertThat(repository.isDurable(ReplicatedSecurityFingerprintRepository.USERS)).isFalse();

    assertThat(Files.isDirectory(occupied)).as("the rename could not replace it").isTrue();
    assertThat(Files.exists(occupied.resolve("keep-me"))).as("and nothing tried to remove it").isTrue();
    try (final Stream<Path> entries = Files.list(configDir)) {
      assertThat(entries.map(entry -> entry.getFileName().toString()).filter(name -> name.endsWith(".tmp")))
          .as("and the temp file it wrote on the way out is not left behind either").isEmpty();
    }
  }

  /** Records one fingerprint successfully, then fails the next write, and asserts the first one is what survives. */
  private void assertPreviousRecordSurvives(final String documentKind) {
    final FailableFingerprintRepository repository = new FailableFingerprintRepository(configDir.toString());
    repository.record(documentKind, "fp-1");
    assertThat(repository.isDurable(documentKind)).isTrue();

    repository.failWrites = true;
    repository.record(documentKind, "fp-2");

    assertThat(repository.isDurable(documentKind))
        .as("what is in force is not what a restart would read back, which is what pins the Raft log").isFalse();
    assertThat(new ReplicatedSecurityFingerprintRepository(configDir.toString()).get(documentKind))
        .as("the baseline a replay of the 'fp-2' entry must be judged against, as every peer judged it").isEqualTo("fp-1");
  }

  /**
   * The repository with its one atomic write made injectable. A real object rather than a mock: only the write step
   * is replaced, and everything this test is about - the failure policy, the retry bookkeeping - is the production code.
   */
  private static final class FailableFingerprintRepository extends ReplicatedSecurityFingerprintRepository {
    private boolean failWrites;
    private int     writes;

    private FailableFingerprintRepository(final String securityConfPath) {
      super(securityConfPath);
    }

    @Override
    void writeAtomically(final Path target, final byte[] bytes) throws IOException {
      ++writes;
      if (failWrites)
        throw new IOException("No space left on device");
      super.writeAtomically(target, bytes);
    }
  }
}
