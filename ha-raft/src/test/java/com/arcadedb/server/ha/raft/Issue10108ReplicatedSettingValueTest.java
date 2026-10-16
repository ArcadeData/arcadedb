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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10108: the configuration entry {@code RaftReplicatedDatabase} submits carried the setting its
 * statement named but the value the in-memory configuration held when the entry was built. Two statements on the SAME setting
 * on one node therefore still shipped a value the first one did not set: A stored 5, B stored 7, A reached its save and
 * submitted 7, B's own submit was then refused and its caller told that nothing had changed anywhere in the cluster.
 * <p>
 * The wrapper is real and wraps a real database; only the broker is a recording one, so what the wrapper submits is read off the
 * call it makes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10108ReplicatedSettingValueTest {
  private static final String KEY = GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey();

  @TempDir
  Path tempDir;

  private LocalDatabase          local;
  private RaftReplicatedDatabase database;
  private final List<String>     submitted = new ArrayList<>();

  @BeforeEach
  void setUp() {
    local = (LocalDatabase) new DatabaseFactory(tempDir.resolve("db").toString()).create();
    final RaftTransactionBroker broker = new RaftTransactionBroker(null, Quorum.MAJORITY, 5_000) {
      @Override
      public long replicateDatabaseConfiguration(final String dbName, final String changesJson) {
        submitted.add(changesJson);
        return submitted.size();
      }
    };
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(true);
    raft.transactionBroker(broker);
    database = new RaftReplicatedDatabase(null, local, raft);
  }

  @AfterEach
  void tearDown() {
    if (local != null && local.isOpen())
      local.drop();
  }

  private LocalDatabase.SettingChange set(final Object value) {
    return local.changeSetting(KEY, () -> local.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, value));
  }

  private int submittedBuckets(final int entry) {
    return new JSONObject(submitted.get(entry)).getJSONObject("configuration").getInt("typeDefaultBuckets");
  }

  @Test
  void anEntryCarriesTheValueItsStatementSetWhenAnotherStoredTheSameSettingMeanwhile() throws IOException {
    final LocalDatabase.SettingChange a = set(5);
    final LocalDatabase.SettingChange b = set(7);

    database.saveConfiguration(a);

    assertThat(submitted).hasSize(1);
    assertThat(submittedBuckets(0)).as("A's entry carries A's value, not the 7 that B stored before A reached its save").isEqualTo(5);

    // B's own submit is a second entry carrying B's: it was never part of A's
    database.saveConfiguration(b);
    assertThat(submitted).hasSize(2);
    assertThat(submittedBuckets(1)).isEqualTo(7);
  }

  @Test
  void aRefusedSubmitOfTheSecondStatementLeavesTheFirstOnesValueTheOnlyOneShipped() throws IOException {
    final LocalDatabase.SettingChange a = set(5);
    final LocalDatabase.SettingChange b = set(7);

    database.saveConfiguration(a);
    // B's submit is refused (the capability interlock, a quorum failure, a broker refusal): it never reaches the broker, and
    // the undo of its change leaves what A's entry applies
    local.undoSettingChange(b);

    assertThat(submitted).as("nothing of B's reached the log").hasSize(1);
    assertThat(submittedBuckets(0)).isEqualTo(5);
    assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
        .as("this node holds what the cluster holds once B is undone before A's entry is applied here: A's value, which B replaced")
        .isEqualTo(5);
  }

  @Test
  void aRefusedSubmitUndoneAfterTheFirstEntryWasAppliedHereLeavesThatEntrysValue() throws IOException {
    final LocalDatabase.SettingChange a = set(5);
    final LocalDatabase.SettingChange b = set(7);

    database.saveConfiguration(a);
    // The apply of A's entry on this node: what every node, the originator included, does with a committed entry
    local.applyConfigurationChanges(submitted.getFirst());
    assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(5);

    local.undoSettingChange(b);

    assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
        .as("another writer changed the setting since B stored it, so its value stands").isEqualTo(5);
    assertThat(submitted).hasSize(1);
  }

  @Test
  void theKeyedSaveWithNoChangeStillReadsTheValueMemoryHolds() throws IOException {
    local.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 9);

    database.saveConfiguration(KEY);

    assertThat(submittedBuckets(0)).isEqualTo(9);
  }
}
