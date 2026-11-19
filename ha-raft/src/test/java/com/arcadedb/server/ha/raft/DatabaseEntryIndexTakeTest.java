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
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The index a create or drop of a database committed at is kept only until the answer to that command reads it (review of
 * #11739): the map behind it must not keep one entry per database name ever created or dropped on the node.
 */
class DatabaseEntryIndexTakeTest {

  private static RaftHAServer newDetachedRaftHAServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
    return new RaftHAServer(TestServerHelper.unstartedServer("localhost"), config);
  }

  @Test
  void theAnswerTakesTheIndexOutSoNoNameIsKeptAfterIt() {
    final RaftHAServer raft = newDetachedRaftHAServer();

    raft.recordDatabaseEntryIndex("db", 42L);

    assertThat(raft.takeDatabaseEntryIndex("db")).isEqualTo(42L);
    assertThat(raft.takeDatabaseEntryIndex("db")).as("taken by the answer that read it").isEqualTo(-1L);
  }

  @Test
  void theHighestIndexRecordedBeforeTheAnswerIsTaken() {
    final RaftHAServer raft = newDetachedRaftHAServer();

    raft.recordDatabaseEntryIndex("db", 42L);
    raft.recordDatabaseEntryIndex("db", 40L);
    raft.recordDatabaseEntryIndex("db", -1L);

    assertThat(raft.takeDatabaseEntryIndex("db")).isEqualTo(42L);
  }

  @Test
  void aNameNeverRecordedHasNoIndex() {
    assertThat(newDetachedRaftHAServer().takeDatabaseEntryIndex("never")).isEqualTo(-1L);
  }
}
