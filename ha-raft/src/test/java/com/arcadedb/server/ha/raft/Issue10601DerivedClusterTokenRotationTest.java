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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10601: a cluster token derived from the file at {@code arcadedb.server.rootPasswordPath} is derived again at
 * every start and never stored, so rewriting that file in place (rotating a mounted Kubernetes Secret) moves each node
 * to a new token at its next restart, one pod at a time, and the halves of a rolling restart answer each other 401.
 * <p>
 * Each test drives one of the three fixes the issue asks for: a startup warning that says what actually happens, a
 * refusal whose log line names where this node's token came from and whether its file has been rewritten, and a
 * startup check that holds a file-derived token to the one the node last ran with.
 */
class Issue10601DerivedClusterTokenRotationTest {

  // ---- 1. the startup warning must be true

  @Test
  void theDerivedTokenWarningSaysThatChangingThePasswordChangesTheToken() {
    final String fileWarning = ClusterTokenProvider.derivedTokenWarning(ClusterTokenProvider.Source.DERIVED_FILE);
    assertThat(fileWarning).doesNotContain("does NOT rotate");
    assertThat(fileWarning).contains("arcadedb.server.rootPasswordPath");
    assertThat(fileWarning).contains("rewriting the root password file in place");
    assertThat(fileWarning).contains("changes the token at the next start of each node");
    assertThat(fileWarning).contains("arcadedb.ha.clusterTokenPath");

    final String inlineWarning = ClusterTokenProvider.derivedTokenWarning(ClusterTokenProvider.Source.DERIVED_INLINE);
    assertThat(inlineWarning).doesNotContain("does NOT rotate");
    assertThat(inlineWarning).contains("(arcadedb.server.rootPassword)");
  }

  // ---- 2. the refusal must be legible

  @Test
  void eachSourceIsDescribedWithoutRevealingTheSecret(@TempDir final Path tmp) throws Exception {
    final Path tokenFile = tmp.resolve("clusterToken");
    Files.writeString(tokenFile, "file-token-secret\n");
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "Secret-v1!\n");

    final ContextConfiguration explicit = new ContextConfiguration();
    explicit.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "inline-token-secret");
    final ClusterTokenProvider explicitProvider = new ClusterTokenProvider(explicit);
    explicitProvider.initClusterToken();
    assertThat(explicitProvider.getSource()).isEqualTo(ClusterTokenProvider.Source.EXPLICIT);
    assertThat(explicitProvider.describeSource()).contains("arcadedb.ha.clusterToken").doesNotContain("inline-token-secret");

    final ContextConfiguration explicitFile = new ContextConfiguration();
    explicitFile.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, tokenFile.toString());
    final ClusterTokenProvider explicitFileProvider = new ClusterTokenProvider(explicitFile);
    explicitFileProvider.initClusterToken();
    assertThat(explicitFileProvider.getSource()).isEqualTo(ClusterTokenProvider.Source.EXPLICIT_FILE);
    assertThat(explicitFileProvider.describeSource()).contains(tokenFile.toString()).doesNotContain("file-token-secret");

    final ContextConfiguration inline = new ContextConfiguration();
    inline.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "c10601");
    inline.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "Secret-inline!");
    final ClusterTokenProvider inlineProvider = new ClusterTokenProvider(inline);
    inlineProvider.initClusterToken();
    assertThat(inlineProvider.getSource()).isEqualTo(ClusterTokenProvider.Source.DERIVED_INLINE);
    assertThat(inlineProvider.describeSource()).contains("'c10601'").contains("arcadedb.server.rootPassword")
        .doesNotContain("Secret-inline!");

    final ClusterTokenProvider fileProvider = fileDerived(passwordFile);
    assertThat(fileProvider.getSource()).isEqualTo(ClusterTokenProvider.Source.DERIVED_FILE);
    assertThat(fileProvider.describeSource()).contains("'c10601'").contains(passwordFile.toString())
        .doesNotContain("Secret-v1!");
  }

  @Test
  void aRefusalOnANodeWhosePasswordFileWasRewrittenNamesTheRewrite(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "Secret-v1!\n");
    final ClusterTokenProvider provider = fileDerived(passwordFile);

    assertThat(provider.rootPasswordFileChangedSinceDerivation()).isFalse();
    assertThat(provider.clusterTokenRefusalReport("10.0.0.2:41234"))
        .contains("10.0.0.2:41234")
        .contains("does not match this node's")
        .contains(passwordFile.toString())
        .doesNotContain("has been rewritten");

    Files.writeString(passwordFile, "Secret-v2!\n"); // the operator rotates the Secret; nothing else changes

    assertThat(provider.rootPasswordFileChangedSinceDerivation()).isTrue();
    assertThat(provider.clusterTokenRefusalReport("10.0.0.2:41234"))
        .contains("has been rewritten since this node started")
        .doesNotContain("Secret-v1!")
        .doesNotContain("Secret-v2!");
  }

  @Test
  void aPasswordFileThatCanNoLongerBeReadCountsAsChanged(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "Secret-v1!\n");
    final ClusterTokenProvider provider = fileDerived(passwordFile);
    Files.delete(passwordFile);
    assertThat(provider.rootPasswordFileChangedSinceDerivation()).isTrue();
    // Reported as unreadable, not as rewritten: the file may be only temporarily unreadable.
    assertThat(provider.clusterTokenRefusalReport("peer")).contains("can no longer be read").doesNotContain("has been rewritten");
  }

  @Test
  void refusalReportsAreRateLimited(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "Secret-v1!\n");
    final ClusterTokenProvider provider = fileDerived(passwordFile);

    final long t0 = 1_000_000L;
    assertThat(provider.reportClusterTokenRefusal("peer", t0)).isTrue();
    assertThat(provider.reportClusterTokenRefusal("peer", t0 + 1)).isFalse();
    assertThat(provider.reportClusterTokenRefusal("peer", t0 + ClusterTokenProvider.REFUSAL_REPORT_INTERVAL_MS - 1)).isFalse();
    assertThat(provider.reportClusterTokenRefusal("peer", t0 + ClusterTokenProvider.REFUSAL_REPORT_INTERVAL_MS)).isTrue();
  }

  // ---- 3. a file-derived token is held to the one the node last ran with

  @Test
  void aRewrittenPasswordFileRefusesTheNextStart(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    // The directory the record sits in does not exist before the first start formats the Raft storage.
    final File fingerprint = RaftHAServer.clusterTokenFingerprintFile(tmp.resolve("data/.raft-storage/raft-storage-x").toFile());
    Files.writeString(passwordFile, "Secret-v1!\n");

    // First start records the token it runs with; a restart on the same file passes.
    final ClusterTokenProvider first = fileDerived(passwordFile);
    first.verifyDerivedTokenFingerprint(fingerprint, false);
    assertThat(fingerprint).exists();
    final String recorded = Files.readString(fingerprint.toPath());
    assertThat(recorded).isEqualTo(ClusterTokenProvider.tokenFingerprint(first.getClusterToken()));
    assertThat(recorded).isNotEqualTo(first.getClusterToken());
    fileDerived(passwordFile).verifyDerivedTokenFingerprint(fingerprint, false);

    // The Secret is rotated in place: the next start refuses, instead of joining with a token its peers do not hold.
    Files.writeString(passwordFile, "Secret-v2!\n");
    final ClusterTokenProvider afterRotation = fileDerived(passwordFile);
    assertThat(afterRotation.getClusterToken()).isNotEqualTo(first.getClusterToken());
    assertThatThrownBy(() -> afterRotation.verifyDerivedTokenFingerprint(fingerprint, false))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Refusing to start")
        .hasMessageContaining(passwordFile.toString())
        .hasMessageContaining("arcadedb.ha.clusterTokenAllowRotation");
    assertThat(Files.readString(fingerprint.toPath())).as("a refused start must not overwrite the record").isEqualTo(recorded);
  }

  @Test
  void anExplicitRotationStartsWithTheNewTokenAndRecordsIt(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    final File fingerprint = RaftHAServer.clusterTokenFingerprintFile(tmp.resolve("raft-storage").toFile());
    Files.writeString(passwordFile, "Secret-v1!\n");
    fileDerived(passwordFile).verifyDerivedTokenFingerprint(fingerprint, false);

    Files.writeString(passwordFile, "Secret-v2!\n");
    final ClusterTokenProvider rotated = fileDerived(passwordFile);
    rotated.verifyDerivedTokenFingerprint(fingerprint, true);
    assertThat(Files.readString(fingerprint.toPath())).isEqualTo(ClusterTokenProvider.tokenFingerprint(rotated.getClusterToken()));

    // Once rotated, the new token is the one held: the flag is no longer needed.
    fileDerived(passwordFile).verifyDerivedTokenFingerprint(fingerprint, false);
  }

  @Test
  void anEmptyRecordIsTreatedAsAbsentNotAsARotation(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    final File fingerprint = RaftHAServer.clusterTokenFingerprintFile(tmp.resolve("raft-storage").toFile());
    Files.writeString(passwordFile, "Secret-v1!\n");
    Files.writeString(fingerprint.toPath(), "  \n"); // what a crash between truncate and write leaves

    final ClusterTokenProvider provider = fileDerived(passwordFile);
    provider.verifyDerivedTokenFingerprint(fingerprint, false);

    assertThat(Files.readString(fingerprint.toPath())).isEqualTo(ClusterTokenProvider.tokenFingerprint(provider.getClusterToken()));
    assertThat(fingerprint.toPath().resolveSibling(fingerprint.getName() + ".tmp")).doesNotExist();
  }

  @Test
  void theRecordIsOwnerOnlyAndTheRotationFlagIsNotMaskedAsASecret(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    final File fingerprint = RaftHAServer.clusterTokenFingerprintFile(tmp.resolve("raft-storage").toFile());
    Files.writeString(passwordFile, "Secret-v1!\n");
    fileDerived(passwordFile).verifyDerivedTokenFingerprint(fingerprint, false);
    if (fingerprint.toPath().getFileSystem().supportedFileAttributeViews().contains("posix"))
      assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(fingerprint.toPath()))).isEqualTo("rw-------");

    // An operator must be able to see in the settings report that rotation is left on.
    assertThat(GlobalConfiguration.HA_CLUSTER_TOKEN_ALLOW_ROTATION.isHidden()).isFalse();
    assertThat(GlobalConfiguration.HA_CLUSTER_TOKEN.isHidden()).isTrue();
    assertThat(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH.isHidden()).isTrue();
  }

  @Test
  void aTokenNotDerivedFromAFileClearsTheRecord(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    final File fingerprint = RaftHAServer.clusterTokenFingerprintFile(tmp.resolve("raft-storage").toFile());
    Files.writeString(passwordFile, "Secret-v1!\n");
    fileDerived(passwordFile).verifyDerivedTokenFingerprint(fingerprint, false);
    assertThat(fingerprint).exists();

    final ContextConfiguration explicit = new ContextConfiguration();
    explicit.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "explicit-token");
    final ClusterTokenProvider explicitProvider = new ClusterTokenProvider(explicit);
    explicitProvider.initClusterToken();
    explicitProvider.verifyDerivedTokenFingerprint(fingerprint, false);
    assertThat(fingerprint).doesNotExist();

    // Switching back to a (rotated) file starts from that file, not from a stale record.
    Files.writeString(passwordFile, "Secret-v2!\n");
    fileDerived(passwordFile).verifyDerivedTokenFingerprint(fingerprint, false);
    assertThat(fingerprint).exists();
  }

  @Test
  void aNodeThatDiscardsItsRaftStorageDiscardsTheRecordToo(@TempDir final Path tmp) throws Exception {
    final File storage = tmp.resolve("raft-storage").toFile();
    assertThat(storage.mkdirs()).isTrue();
    final File fingerprint = RaftHAServer.clusterTokenFingerprintFile(storage);
    Files.writeString(fingerprint.toPath(), "stale");

    RaftHAServer.discardNonPersistentRaftStorage(storage);

    assertThat(fingerprint).doesNotExist();
  }

  private static ClusterTokenProvider fileDerived(final Path passwordFile) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "c10601");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());
    final ClusterTokenProvider provider = new ClusterTokenProvider(config);
    provider.initClusterToken();
    return provider;
  }
}
