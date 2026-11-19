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
 * Unit tests for {@link ClusterTokenProvider} (derivation, files, rotation) and for the token retained by the plugin across stop.
 *
 * <p>Regression coverage for #10601:
 * Issue #10601: a cluster token derived from the file at {@code arcadedb.server.rootPasswordPath} is derived again at
 * every start and never stored, so rewriting that file in place (rotating a mounted Kubernetes Secret) moves each node
 * to a new token at its next restart, one pod at a time, and the halves of a rolling restart answer each other 401.
 * <p>
 * Each test drives one of the three fixes the issue asks for: a startup warning that says what actually happens, a
 * refusal whose log line names where this node's token came from and whether its file has been rewritten, and a
 * startup check that holds a file-derived token to the one the node last ran with.
 *
 * <p>Regression coverage for #9554:
 * Issue #9554: {@code ArcadeDBServer.stopInternal()} stops the HA plugin before the HTTP listener, and
 * {@code RaftHAPlugin.stopService()} dropped its Raft server - the only place it read the cluster token from. For the
 * rest of the shutdown every peer's forward that still reached the node, the node it believed to be the leader, was
 * answered 401 "Invalid cluster token" rather than the retryable refusal a node that no longer leads gives.
 */
class ClusterTokenProviderTest {

  @Test
  void consistentTokenDerivation() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    final ClusterTokenProvider p1 = new ClusterTokenProvider(config);
    final ClusterTokenProvider p2 = new ClusterTokenProvider(config);
    assertThat(p1.getClusterToken()).isEqualTo(p2.getClusterToken());
  }

  @Test
  void differentClusterNamesDifferentTokens() {
    final ContextConfiguration c1 = new ContextConfiguration();
    c1.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "cluster-a");
    c1.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "same-password");
    final ContextConfiguration c2 = new ContextConfiguration();
    c2.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "cluster-b");
    c2.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "same-password");
    assertThat(new ClusterTokenProvider(c1).getClusterToken())
        .isNotEqualTo(new ClusterTokenProvider(c2).getClusterToken());
  }

  @Test
  void missingClusterNameThrows() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void missingRootPasswordThrows() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void explicitTokenTakesPrecedence() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "explicit-token");
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThat(new ClusterTokenProvider(config).getClusterToken()).isEqualTo("explicit-token");
  }

  @Test
  void tokenReadFromFile(@TempDir final Path tmp) throws Exception {
    final Path tokenFile = tmp.resolve("cluster.token");
    Files.writeString(tokenFile, "file-token");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, tokenFile.toString());
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThat(new ClusterTokenProvider(config).getClusterToken()).isEqualTo("file-token");
  }

  @Test
  void tokenFromFileIsTrimmed(@TempDir final Path tmp) throws Exception {
    // Most tooling (echo, editors, k8s secrets) appends a trailing newline that must not leak into the token.
    final Path tokenFile = tmp.resolve("cluster.token");
    Files.writeString(tokenFile, "  file-token\n");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, tokenFile.toString());
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThat(new ClusterTokenProvider(config).getClusterToken()).isEqualTo("file-token");
  }

  @Test
  void explicitTokenTakesPrecedenceOverFile(@TempDir final Path tmp) throws Exception {
    final Path tokenFile = tmp.resolve("cluster.token");
    Files.writeString(tokenFile, "file-token");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "explicit-token");
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, tokenFile.toString());
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThat(new ClusterTokenProvider(config).getClusterToken()).isEqualTo("explicit-token");
  }

  @Test
  void unreadableTokenFileThrows() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, "/non/existent/cluster.token");
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void emptyTokenFileThrows(@TempDir final Path tmp) throws Exception {
    final Path tokenFile = tmp.resolve("cluster.token");
    Files.writeString(tokenFile, "  \n");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, tokenFile.toString());
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class);
  }

  // Issue #10462: a server given its root password as a file (the shipped StatefulSet mounts a Secret and passes only
  // arcadedb.server.rootPasswordPath) has a root password, so HA must derive its cluster token from that file too.

  @Test
  void tokenDerivedFromRootPasswordFile(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "test-password");
    final ContextConfiguration fromFile = new ContextConfiguration();
    fromFile.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    fromFile.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());

    final ContextConfiguration inline = new ContextConfiguration();
    inline.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    inline.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");

    assertThat(new ClusterTokenProvider(fromFile).getClusterToken())
        .isEqualTo(new ClusterTokenProvider(inline).getClusterToken());
  }

  @Test
  void rootPasswordFileDropsOnlyTheTrailingNewlineLikeServerSecurity(@TempDir final Path tmp) throws Exception {
    // `kubectl create secret --from-file`, a here-doc or an editor append a newline that is not part of the password:
    // the token must match the one a node given the same password inline derives. Trailing spaces are part of the
    // password, exactly as ServerSecurity reads the same file into the root user.
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "test-password \r\n");
    final ContextConfiguration fromFile = new ContextConfiguration();
    fromFile.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    fromFile.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());

    final ContextConfiguration inline = new ContextConfiguration();
    inline.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    inline.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password ");

    assertThat(new ClusterTokenProvider(fromFile).getClusterToken())
        .isEqualTo(new ClusterTokenProvider(inline).getClusterToken());
  }

  @Test
  void inlineRootPasswordTakesPrecedenceOverRootPasswordFile(@TempDir final Path tmp) throws Exception {
    // Same precedence as ServerSecurity.askForRootPassword(): the file is read only when no inline password is set.
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "file-password");
    final ContextConfiguration both = new ContextConfiguration();
    both.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    both.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "inline-password");
    both.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());

    final ContextConfiguration inline = new ContextConfiguration();
    inline.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    inline.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "inline-password");

    assertThat(new ClusterTokenProvider(both).getClusterToken())
        .isEqualTo(new ClusterTokenProvider(inline).getClusterToken());
  }

  @Test
  void explicitTokenPathTakesPrecedenceOverRootPasswordFile(@TempDir final Path tmp) throws Exception {
    final Path tokenFile = tmp.resolve("clusterToken");
    Files.writeString(tokenFile, "file-token\n");
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "file-password");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH, tokenFile.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());
    assertThat(new ClusterTokenProvider(config).getClusterToken()).isEqualTo("file-token");
  }

  @Test
  void unreadableRootPasswordFileThrowsNamingThePath() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, "/non/existent/rootPassword");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class).hasMessageContaining("/non/existent/rootPassword");
  }

  @Test
  void emptyRootPasswordFileThrows(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "\n");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class).hasMessageContaining("empty");
  }

  @Test
  void missingRootPasswordErrorNamesThePasswordFileSetting() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    assertThatThrownBy(() -> new ClusterTokenProvider(config).getClusterToken())
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH.getKey())
        .hasMessageContaining(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH.getKey());
  }

  @Test
  void testHelperDerivesFromRootPasswordFile(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    Files.writeString(passwordFile, "test-password\n");
    final ContextConfiguration fromFile = new ContextConfiguration();
    fromFile.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    fromFile.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());
    ClusterTokenProvider.initClusterTokenForTest(fromFile);

    final ContextConfiguration inline = new ContextConfiguration();
    inline.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    inline.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");

    assertThat(fromFile.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN))
        .isEqualTo(new ClusterTokenProvider(inline).getClusterToken());
  }

  // ---- 1. the startup warning must be true

  // Regression for #10601: the derived token warning says that changing the password changes the token
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

  // Regression for #10601: each source is described without revealing the secret
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

  // Regression for #10601: a refusal on a node whose password file was rewritten names the rewrite
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

  // Regression for #10601: a password file that can no longer be read counts as changed
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

  // Regression for #10601: refusal reports are rate limited
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

  // Regression for #10601: a rewritten password file refuses the next start
  @Test
  void aRewrittenPasswordFileRefusesTheNextStart(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    // The directory the record sits in does not exist before the first start formats the Raft storage.
    final File fingerprint = RaftStorageFiles.clusterTokenFingerprintFile(tmp.resolve("data/.raft-storage/raft-storage-x").toFile());
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

  // Regression for #10601: an explicit rotation starts with the new token and records it
  @Test
  void anExplicitRotationStartsWithTheNewTokenAndRecordsIt(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    final File fingerprint = RaftStorageFiles.clusterTokenFingerprintFile(tmp.resolve("raft-storage").toFile());
    Files.writeString(passwordFile, "Secret-v1!\n");
    fileDerived(passwordFile).verifyDerivedTokenFingerprint(fingerprint, false);

    Files.writeString(passwordFile, "Secret-v2!\n");
    final ClusterTokenProvider rotated = fileDerived(passwordFile);
    rotated.verifyDerivedTokenFingerprint(fingerprint, true);
    assertThat(Files.readString(fingerprint.toPath())).isEqualTo(ClusterTokenProvider.tokenFingerprint(rotated.getClusterToken()));

    // Once rotated, the new token is the one held: the flag is no longer needed.
    fileDerived(passwordFile).verifyDerivedTokenFingerprint(fingerprint, false);
  }

  // Regression for #10601: an empty record is treated as absent not as a rotation
  @Test
  void anEmptyRecordIsTreatedAsAbsentNotAsARotation(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    final File fingerprint = RaftStorageFiles.clusterTokenFingerprintFile(tmp.resolve("raft-storage").toFile());
    Files.writeString(passwordFile, "Secret-v1!\n");
    Files.writeString(fingerprint.toPath(), "  \n"); // what a crash between truncate and write leaves

    final ClusterTokenProvider provider = fileDerived(passwordFile);
    provider.verifyDerivedTokenFingerprint(fingerprint, false);

    assertThat(Files.readString(fingerprint.toPath())).isEqualTo(ClusterTokenProvider.tokenFingerprint(provider.getClusterToken()));
    assertThat(fingerprint.toPath().resolveSibling(fingerprint.getName() + ".tmp")).doesNotExist();
  }

  // Regression for #10601: the record is owner only and the rotation flag is not masked as a secret
  @Test
  void theRecordIsOwnerOnlyAndTheRotationFlagIsNotMaskedAsASecret(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    final File fingerprint = RaftStorageFiles.clusterTokenFingerprintFile(tmp.resolve("raft-storage").toFile());
    Files.writeString(passwordFile, "Secret-v1!\n");
    fileDerived(passwordFile).verifyDerivedTokenFingerprint(fingerprint, false);
    if (fingerprint.toPath().getFileSystem().supportedFileAttributeViews().contains("posix"))
      assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(fingerprint.toPath()))).isEqualTo("rw-------");

    // An operator must be able to see in the settings report that rotation is left on.
    assertThat(GlobalConfiguration.HA_CLUSTER_TOKEN_ALLOW_ROTATION.isHidden()).isFalse();
    assertThat(GlobalConfiguration.HA_CLUSTER_TOKEN.isHidden()).isTrue();
    assertThat(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH.isHidden()).isTrue();
  }

  // Regression for #10601: a token not derived from a file clears the record
  @Test
  void aTokenNotDerivedFromAFileClearsTheRecord(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    final File fingerprint = RaftStorageFiles.clusterTokenFingerprintFile(tmp.resolve("raft-storage").toFile());
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

  // Regression for #10601: a node that discards its raft storage discards the record too
  @Test
  void aNodeThatDiscardsItsRaftStorageDiscardsTheRecordToo(@TempDir final Path tmp) throws Exception {
    final File storage = tmp.resolve("raft-storage").toFile();
    assertThat(storage.mkdirs()).isTrue();
    final File fingerprint = RaftStorageFiles.clusterTokenFingerprintFile(storage);
    Files.writeString(fingerprint.toPath(), "stale");

    RaftStorageFiles.discardNonPersistentRaftStorage(storage);

    assertThat(fingerprint).doesNotExist();
  }

  // ---- 4. issue #10924: the inline source is held to the token too

  // Regression for #10601: a rotated inline root password refuses the next start
  @Test
  void aRotatedInlineRootPasswordRefusesTheNextStart(@TempDir final Path tmp) throws Exception {
    final File fingerprint = RaftStorageFiles.clusterTokenFingerprintFile(tmp.resolve("data/.raft-storage/raft-storage-x").toFile());

    final ClusterTokenProvider first = inlineDerived("Secret-v1!");
    first.verifyDerivedTokenFingerprint(fingerprint, false);
    assertThat(fingerprint).exists();
    final String recorded = Files.readString(fingerprint.toPath());
    assertThat(recorded).isEqualTo(ClusterTokenProvider.tokenFingerprint(first.getClusterToken()));
    inlineDerived("Secret-v1!").verifyDerivedTokenFingerprint(fingerprint, false);

    final ClusterTokenProvider afterRotation = inlineDerived("Secret-v2!");
    assertThat(afterRotation.getClusterToken()).isNotEqualTo(first.getClusterToken());
    assertThatThrownBy(() -> afterRotation.verifyDerivedTokenFingerprint(fingerprint, false))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Refusing to start")
        .hasMessageContaining("arcadedb.server.rootPassword")
        .hasMessageContaining("arcadedb.ha.clusterTokenAllowRotation")
        .hasMessageNotContaining("Secret-v1!")
        .hasMessageNotContaining("Secret-v2!");
    assertThat(Files.readString(fingerprint.toPath())).as("a refused start must not overwrite the record").isEqualTo(recorded);
  }

  // Regression for #10601: an explicit rotation of an inline root password starts and records the new token
  @Test
  void anExplicitRotationOfAnInlineRootPasswordStartsAndRecordsTheNewToken(@TempDir final Path tmp) throws Exception {
    final File fingerprint = RaftStorageFiles.clusterTokenFingerprintFile(tmp.resolve("raft-storage").toFile());
    inlineDerived("Secret-v1!").verifyDerivedTokenFingerprint(fingerprint, false);

    final ClusterTokenProvider rotated = inlineDerived("Secret-v2!");
    rotated.verifyDerivedTokenFingerprint(fingerprint, true);
    assertThat(Files.readString(fingerprint.toPath())).isEqualTo(ClusterTokenProvider.tokenFingerprint(rotated.getClusterToken()));
    inlineDerived("Secret-v2!").verifyDerivedTokenFingerprint(fingerprint, false);
  }

  // Regression for #10601: switching from the file source to the inline source keeps the record
  @Test
  void switchingFromTheFileSourceToTheInlineSourceKeepsTheRecord(@TempDir final Path tmp) throws Exception {
    final Path passwordFile = tmp.resolve("rootPassword");
    final File fingerprint = RaftStorageFiles.clusterTokenFingerprintFile(tmp.resolve("raft-storage").toFile());
    Files.writeString(passwordFile, "Secret-v1!\n");
    fileDerived(passwordFile).verifyDerivedTokenFingerprint(fingerprint, false);
    final String recorded = Files.readString(fingerprint.toPath());

    // Same password and cluster name, so the same token: the record survives and the start passes.
    inlineDerived("Secret-v1!").verifyDerivedTokenFingerprint(fingerprint, false);
    assertThat(Files.readString(fingerprint.toPath())).isEqualTo(recorded);

    // A different inline password is a different token, and is refused against the file source's record.
    final ClusterTokenProvider other = inlineDerived("Secret-v2!");
    assertThatThrownBy(() -> other.verifyDerivedTokenFingerprint(fingerprint, false)).isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Refusing to start");
  }

  private static ClusterTokenProvider inlineDerived(final String password) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "c10601");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, password);
    final ClusterTokenProvider provider = new ClusterTokenProvider(config);
    provider.initClusterToken();
    return provider;
  }

  private static ClusterTokenProvider fileDerived(final Path passwordFile) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "c10601");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH, passwordFile.toString());
    final ClusterTokenProvider provider = new ClusterTokenProvider(config);
    provider.initClusterToken();
    return provider;
  }

  // Regression for #9554: the cluster token outlives the plugins raft server
  @Test
  void theClusterTokenOutlivesThePluginsRaftServer() {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.setRaftHAServer(FakeRaftHAServer.detached().clusterToken("issue9554-token"));
    assertThat(plugin.getClusterToken()).isEqualTo("issue9554-token");

    plugin.stopService();

    assertThat(plugin.getRaftHAServer()).as("the Raft server is gone once the plugin stops").isNull();
    assertThat(plugin.getClusterToken())
        .as("the token peers authenticate with does not change when this node stops, so neither may the answer")
        .isEqualTo("issue9554-token");

    // stopService() runs twice on every HA shutdown (issue #5890): the second call must not lose it either.
    plugin.stopService();
    assertThat(plugin.getClusterToken()).isEqualTo("issue9554-token");
  }

  // Regression for #9554: a plugin that never started has no token
  @Test
  void aPluginThatNeverStartedHasNoToken() {
    assertThat(new RaftHAPlugin().getClusterToken()).isNull();
  }
}
