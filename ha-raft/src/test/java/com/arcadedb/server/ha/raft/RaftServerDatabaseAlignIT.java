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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "align database" command in Raft HA mode.
 * <p>
 * In Raft HA, page-level alignment (legacy HAServer feature) is not supported -
 * consistency is guaranteed by the Raft consensus protocol instead.
 * These tests verify that the command is rejected with an appropriate error.
 */
class RaftServerDatabaseAlignIT extends BaseRaftHATest {

  RaftServerDatabaseAlignIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
  }

  @Test
  void alignNotSupportedOnLeader() {
    // In Raft HA mode, page-level alignment is not supported - Raft consensus
    // ensures consistency without manual alignment.
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("Expected a Raft leader to be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServer(leaderIndex).getDatabase(getDatabaseName());
    assertThatThrownBy(() -> database.command("sql", "align database"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Align Database not supported");
  }

  @Test
  void raftConsistencyAfterDml() {
    // Verify that DML writes via the Raft leader are consistent across all replicas.
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("Expected a Raft leader to be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServer(leaderIndex).getDatabase(getDatabaseName());
    database.transaction(() -> database.iterateType(EDGE2_TYPE_NAME, true).forEachRemaining(record -> {
      // Just iterate - confirm the edge type is visible and readable on the leader
    }));

    assertClusterConsistency();
  }
}
