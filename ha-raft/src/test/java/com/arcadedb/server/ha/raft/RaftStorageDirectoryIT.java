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
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@code HA_RAFT_STORAGE_DIRECTORY} correctly redirects the
 * Raft storage sub-folders to a user-configured parent directory.
 */
@Tag("slow")
class RaftStorageDirectoryIT extends BaseRaftHATest {

  private static final String CUSTOM_RAFT_DIR = "./target/custom-raft-storage";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY, CUSTOM_RAFT_DIR);
  }

  @Override
  protected void deleteDatabaseFolders() {
    super.deleteDatabaseFolders();
    FileUtils.deleteRecursively(new File(CUSTOM_RAFT_DIR));
  }

  @Test
  void raftStorageCreatedInConfiguredDirectory() {
    // Each server's raft-storage-<peerId> sub-folder must be created under the
    // configured parent directory, not under the server root path.
    for (int i = 0; i < getServerCount(); i++) {
      final File expectedDir = new File(CUSTOM_RAFT_DIR, "raft-storage-" + peerIdForIndex(i));
      assertThat(expectedDir)
          .as("raft-storage for server %d must exist under configured raftStorageDirectory", i)
          .exists();

      final File unexpectedDir = new File(getServer(i).getRootPath(), "raft-storage-" + peerIdForIndex(i));
      assertThat(unexpectedDir)
          .as("raft-storage for server %d must NOT be created under the default server root when raftStorageDirectory is configured", i)
          .doesNotExist();
    }
  }
}
