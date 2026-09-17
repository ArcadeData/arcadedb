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
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The fail-open branches of {@link ReplicatedSecurityFingerprintRepository}, which
 * {@code Issue7693FirstReplicatedSecurityEntryTest} exercises only on the happy path (claude-review on PR #7748).
 * <p>
 * Both of them are log-and-swallow, so nothing downstream would notice if they stopped behaving: a load that
 * threw would take the server down over a marker file, and a save that threw would fail a committed security
 * entry - the crash loop issue #7137 exists to prevent. What they have to do instead is degrade in ONE direction,
 * "this node cannot judge", because a node that judges from a value it could not read is the divergence the
 * marker exists to prevent.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ReplicatedSecurityFingerprintRepositoryTest {

  @TempDir
  private Path configDir;

  @Test
  void aRecordedFingerprintIsReadBackByANewInstance() {
    new ReplicatedSecurityFingerprintRepository(configDir.toString())
        .record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users");

    final ReplicatedSecurityFingerprintRepository reopened =
        new ReplicatedSecurityFingerprintRepository(configDir.toString());

    assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.USERS))
        .as("the whole point: it has to survive the restart it is there for").isEqualTo("fp-users");
    assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.GROUPS))
        .as("a document never recorded answers null, which is what makes the node install unconditionally")
        .isNull();
  }

  /** The three kinds are independent: recording one must not make another judgeable. */
  @Test
  void eachDocumentKindKeepsItsOwnFingerprint() {
    final ReplicatedSecurityFingerprintRepository repository =
        new ReplicatedSecurityFingerprintRepository(configDir.toString());
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users");
    repository.record(ReplicatedSecurityFingerprintRepository.GROUPS, "fp-groups");
    repository.record(ReplicatedSecurityFingerprintRepository.API_TOKENS, "fp-tokens");

    final ReplicatedSecurityFingerprintRepository reopened =
        new ReplicatedSecurityFingerprintRepository(configDir.toString());

    assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.USERS)).isEqualTo("fp-users");
    assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.GROUPS)).isEqualTo("fp-groups");
    assertThat(reopened.get(ReplicatedSecurityFingerprintRepository.API_TOKENS)).isEqualTo("fp-tokens");
  }

  /**
   * A file this node cannot parse is treated as absent rather than as a failure. Refusing to start over a marker
   * whose whole purpose is to make a soft failure rarer would be the wrong trade, and answering "cannot judge" is
   * the direction that cannot refuse an entry a peer accepts.
   */
  @Test
  void aCorruptFileReadsAsNoFingerprintAtAll() throws IOException {
    Files.writeString(configDir.resolve(ReplicatedSecurityFingerprintRepository.FILE_NAME),
        "{ this is not json", StandardCharsets.UTF_8);

    final ReplicatedSecurityFingerprintRepository repository =
        new ReplicatedSecurityFingerprintRepository(configDir.toString());

    assertThat(repository.get(ReplicatedSecurityFingerprintRepository.USERS)).isNull();
  }

  /** A truncated or empty value is not a fingerprint either, and must not be handed back as one. */
  @Test
  void anEmptyValueIsNotAFingerprint() throws IOException {
    Files.writeString(configDir.resolve(ReplicatedSecurityFingerprintRepository.FILE_NAME),
        "{\"users\":\"\",\"groups\":\"fp-groups\"}", StandardCharsets.UTF_8);

    final ReplicatedSecurityFingerprintRepository repository =
        new ReplicatedSecurityFingerprintRepository(configDir.toString());

    assertThat(repository.get(ReplicatedSecurityFingerprintRepository.USERS)).isNull();
    assertThat(repository.get(ReplicatedSecurityFingerprintRepository.GROUPS)).isEqualTo("fp-groups");
  }

  /**
   * A write that cannot happen must not reach the caller: {@code record()} runs on the Raft apply thread, and a
   * throw there would fail a committed security entry over a marker file. The value stays in force in memory, so
   * this node keeps judging correctly until it restarts - which is the window the class javadoc documents and
   * issue #7752 is filed to close.
   */
  @Test
  void aWriteThatCannotHappenIsSwallowedAndTheValueStaysInMemory() throws IOException {
    // A regular file where the configuration directory should be: mkdirs cannot create it and the temp file
    // cannot be created beside it.
    final Path notADirectory = configDir.resolve("occupied");
    Files.writeString(notADirectory, "x", StandardCharsets.UTF_8);

    final ReplicatedSecurityFingerprintRepository repository =
        new ReplicatedSecurityFingerprintRepository(notADirectory.resolve("config").toString());

    assertThatCode(() -> repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users"))
        .as("a marker that cannot be written must never fail the apply that produced it")
        .doesNotThrowAnyException();
    assertThat(repository.get(ReplicatedSecurityFingerprintRepository.USERS))
        .as("and the node goes on judging correctly from memory until it restarts").isEqualTo("fp-users");
  }

  /**
   * The file lands in the same configuration directory as {@code server-users.jsonl} and the API-token document,
   * both of which are published owner-only on purpose. A convention that holds for three files in a directory and
   * not the fourth is one nobody can rely on (claude-review on PR #7748).
   */
  @Test
  void theFileIsPublishedOwnerOnly() throws IOException {
    new ReplicatedSecurityFingerprintRepository(configDir.toString())
        .record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users");

    final Path file = configDir.resolve(ReplicatedSecurityFingerprintRepository.FILE_NAME);
    final PosixFileAttributeView posix = Files.getFileAttributeView(file, PosixFileAttributeView.class);
    assumeTrue(posix != null, "POSIX permissions are not a thing on this filesystem");

    assertThat(posix.readAttributes().permissions())
        .containsExactlyInAnyOrder(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
  }

  /** The fsync skip: re-recording a value that has not moved must not touch the file at all. */
  @Test
  void reRecordingAnUnchangedFingerprintWritesNothing() throws IOException {
    final ReplicatedSecurityFingerprintRepository repository =
        new ReplicatedSecurityFingerprintRepository(configDir.toString());
    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users");

    final Path file = configDir.resolve(ReplicatedSecurityFingerprintRepository.FILE_NAME);
    final String written = Files.readString(file, StandardCharsets.UTF_8);
    Files.writeString(file, written + "\n// touched", StandardCharsets.UTF_8);

    repository.record(ReplicatedSecurityFingerprintRepository.USERS, "fp-users");

    assertThat(Files.readString(file, StandardCharsets.UTF_8))
        .as("an unchanged fingerprint is the every-replay case, and it must not cost a write")
        .endsWith("// touched");
  }
}
