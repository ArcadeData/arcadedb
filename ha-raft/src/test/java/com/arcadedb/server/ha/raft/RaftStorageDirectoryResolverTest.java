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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RaftHAServer#resolveRaftStorageDir(String, String, String, String)} covering
 * the default location fix from issue #5272: the Raft storage directory must default to a path
 * <b>under</b> the database directory so it is persisted together with the databases, instead of the
 * pre-#5272 default (the server root path, a sibling of the database directory that lands on
 * ephemeral disk on Kubernetes).
 */
class RaftStorageDirectoryResolverTest {

  private static final String PEER = "localhost_2434";

  @Test
  void explicitConfigurationIsAlwaysHonored(@TempDir final File tmp) {
    final File dbDir = new File(tmp, "databases");
    final File custom = new File(tmp, "custom-raft");

    final File resolved = RaftHAServer.resolveRaftStorageDir(custom.getAbsolutePath(), dbDir.getAbsolutePath(),
        tmp.getAbsolutePath(), PEER);

    assertThat(resolved).isEqualTo(new File(custom, "raft-storage-" + PEER));
  }

  @Test
  void defaultLandsUnderDatabaseDirectory(@TempDir final File tmp) {
    final File dbDir = new File(tmp, "databases");
    final String rootPath = tmp.getAbsolutePath();

    final File resolved = RaftHAServer.resolveRaftStorageDir("", dbDir.getAbsolutePath(), rootPath, PEER);

    // Must be under <databaseDirectory>/.raft-storage, NOT a sibling of the database directory.
    assertThat(resolved).isEqualTo(new File(new File(dbDir, ".raft-storage"), "raft-storage-" + PEER));
    // Regression guard for the original bug: not directly under the server root path.
    assertThat(resolved).isNotEqualTo(new File(tmp, "raft-storage-" + PEER));
  }

  @Test
  void blankDatabaseDirectoryFallsBackToRootPath(@TempDir final File tmp) {
    final File resolved = RaftHAServer.resolveRaftStorageDir("", "", tmp.getAbsolutePath(), PEER);
    assertThat(resolved).isEqualTo(new File(tmp, "raft-storage-" + PEER));
  }

  @Test
  void legacyLocationIsReusedWhenNewDefaultAbsent(@TempDir final File tmp) throws Exception {
    final File dbDir = new File(tmp, "databases");
    final String rootPath = tmp.getAbsolutePath();

    // Simulate a pre-#5272 deployment that persisted the server root path: the legacy Raft storage
    // directory already exists there, while the new default location does not.
    final File legacy = new File(tmp, "raft-storage-" + PEER);
    assertThat(legacy.mkdirs()).isTrue();

    final File resolved = RaftHAServer.resolveRaftStorageDir("", dbDir.getAbsolutePath(), rootPath, PEER);

    assertThat(resolved).isEqualTo(legacy);
  }

  @Test
  void newDefaultIsPreferredWhenBothExist(@TempDir final File tmp) {
    final File dbDir = new File(tmp, "databases");
    final String rootPath = tmp.getAbsolutePath();

    final File legacy = new File(tmp, "raft-storage-" + PEER);
    final File newDefault = new File(new File(dbDir, ".raft-storage"), "raft-storage-" + PEER);
    assertThat(legacy.mkdirs()).isTrue();
    assertThat(newDefault.mkdirs()).isTrue();

    final File resolved = RaftHAServer.resolveRaftStorageDir("", dbDir.getAbsolutePath(), rootPath, PEER);

    assertThat(resolved).isEqualTo(newDefault);
  }
}
