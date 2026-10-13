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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.TransactionOutcomeUnknownException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9904: an {@code ALTER DATABASE} whose replicated entry was dispatched and then timed out has an unknown
 * outcome, so the setting stays in force on the node that ran it. If the entry committed, every node ends on it. If it
 * never did, the originator used to keep the value in memory only - a read different from the rest of the cluster - and
 * ship it with the next unrelated change. It now finds the entry's fate and, when the entry did not commit, puts the
 * settings back to what its {@code configuration.json} holds.
 * <p>
 * Both outcomes of the same exception are driven through the real Raft path: {@code TEST_FORCE_DISPATCHED_TIMEOUT}
 * abandons the wait of an entry that is committed regardless; {@code TEST_SWALLOW_DISPATCHED_ENTRY} withdraws the entry
 * before it is dispatched, so it is never in the log.
 */
class Issue9904UnknownConfigurationOutcomeIT extends BaseRaftHATest {

  private static final String SETTING_A = "arcadedb.typeDefaultBuckets";
  private static final String SETTING_B = "arcadedb.dateImplementation";

  /** An entry of this change: the key travels in the entry's UTF-8 document. */
  private static Predicate<byte[]> carries(final String key) {
    final String fileKey = key.substring(GlobalConfiguration.PREFIX.length());
    return entry -> new String(entry, StandardCharsets.UTF_8).contains(fileKey);
  }

  @AfterEach
  void clearHooks() {
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = null;
    RaftGroupCommitter.TEST_SWALLOW_DISPATCHED_ENTRY = null;
  }

  @Test
  void anEntryThatCommitsLaterLeavesEveryNodeOnTheNewValue() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = carries(SETTING_A);

    assertThatThrownBy(() -> leader.command("sql", "ALTER DATABASE `" + SETTING_A + "` 5"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);

    await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++) {
        final DatabaseInternal database = (DatabaseInternal) getServerDatabase(i, getDatabaseName());
        assertThat(buckets(database)).as("server %d in memory", i).isEqualTo(5);
        assertThat(readConfigurationFile(database).getInt("typeDefaultBuckets", -1)).as("server %d file", i).isEqualTo(5);
      }
      assertThat(unresolved(leaderIndex)).as("the originator knows the fate").isZero();
    });
    assertThat(buckets((DatabaseInternal) leader)).as("committed: the originator keeps it").isEqualTo(5);
  }

  @Test
  void anEntryThatNeverCommitsGoesBackOnTheOriginator() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final int defaultValue = buckets((DatabaseInternal) leader);
    RaftGroupCommitter.TEST_SWALLOW_DISPATCHED_ENTRY = carries(SETTING_A);

    assertThatThrownBy(() -> leader.command("sql", "ALTER DATABASE `" + SETTING_A + "` 5"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);

    await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
      assertThat(unresolved(leaderIndex)).as("the originator knows the fate").isZero();
      for (int i = 0; i < getServerCount(); i++) {
        final DatabaseInternal database = (DatabaseInternal) getServerDatabase(i, getDatabaseName());
        assertThat(buckets(database)).as("server %d in memory", i).isEqualTo(defaultValue);
        assertThat(readConfigurationFile(database).has("typeDefaultBuckets")).as("server %d file", i).isFalse();
      }
    });
    assertThat(((LocalDatabase) ((DatabaseInternal) leader).getEmbedded()).pendingConfigurationChanges())
        .as("nothing left to ride the next change").isNull();
  }

  @Test
  void aLaterUnrelatedChangeDoesNotShipASettingThatNeverCommitted() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final int defaultValue = buckets((DatabaseInternal) leader);

    // The first change never reaches the log, and neither does anything that would settle it: the originator stays
    // undecided, which is when the next change used to carry the first one along
    final Predicate<byte[]> settingA = carries(SETTING_A);
    final Predicate<byte[]> barrier = entry -> new String(entry, StandardCharsets.UTF_8).contains(UnresolvedConfigurationChanges.FENCE_DOCUMENT);
    RaftGroupCommitter.TEST_SWALLOW_DISPATCHED_ENTRY = entry -> settingA.test(entry) || barrier.test(entry);
    assertThatThrownBy(() -> leader.command("sql", "ALTER DATABASE `" + SETTING_A + "` 5"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);
    assertThat(unresolved(leaderIndex)).isEqualTo(1);

    leader.command("sql", "ALTER DATABASE `" + SETTING_B + "` 'java.util.Date'");

    await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++) {
        final DatabaseInternal database = (DatabaseInternal) getServerDatabase(i, getDatabaseName());
        assertThat(database.getConfiguration().getValueAsString(GlobalConfiguration.DATE_IMPLEMENTATION))
            .as("server %d got the change that was made", i).isEqualTo("java.util.Date");
        assertThat(buckets(database)).as("server %d must not get the setting that never committed", i)
            .isEqualTo(i == leaderIndex ? 5 : defaultValue);
        assertThat(readConfigurationFile(database).has("typeDefaultBuckets")).as("server %d file", i).isFalse();
      }
    });

    // Once the cluster can settle it, the originator converges
    RaftGroupCommitter.TEST_SWALLOW_DISPATCHED_ENTRY = null;
    await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
      assertThat(unresolved(leaderIndex)).isZero();
      assertThat(buckets((DatabaseInternal) leader)).isEqualTo(defaultValue);
    });
  }

  @Test
  void theSameChangeSentAgainWhileItsOutcomeIsUnknownIsNotReportedDone() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final int defaultValue = buckets((DatabaseInternal) leader);

    final Predicate<byte[]> settingA = carries(SETTING_A);
    final Predicate<byte[]> barrier = entry -> new String(entry, StandardCharsets.UTF_8).contains(UnresolvedConfigurationChanges.FENCE_DOCUMENT);
    RaftGroupCommitter.TEST_SWALLOW_DISPATCHED_ENTRY = entry -> settingA.test(entry) || barrier.test(entry);
    assertThatThrownBy(() -> leader.command("sql", "ALTER DATABASE `" + SETTING_A + "` 5"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);

    // The retry the error invites: nothing is submitted, and it must not read as a success that is then undone
    assertThatThrownBy(() -> leader.command("sql", "ALTER DATABASE `" + SETTING_A + "` 5"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);

    RaftGroupCommitter.TEST_SWALLOW_DISPATCHED_ENTRY = null;
    await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
      assertThat(unresolved(leaderIndex)).isZero();
      assertThat(buckets((DatabaseInternal) leader)).isEqualTo(defaultValue);
    });

    // Resolved: the same change is now an ordinary one and travels
    leader.command("sql", "ALTER DATABASE `" + SETTING_A + "` 5");
    await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++)
        assertThat(buckets((DatabaseInternal) getServerDatabase(i, getDatabaseName()))).as("server %d", i).isEqualTo(5);
    });
  }

  private int unresolved(final int serverIndex) {
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());
    return ((RaftReplicatedDatabase) database.getWrappedDatabaseInstance()).unresolvedConfigurationChangeCount();
  }

  private static int buckets(final DatabaseInternal database) {
    return database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
  }

  private static JSONObject readConfigurationFile(final DatabaseInternal database) throws IOException {
    final File file = new File(database.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
    return new JSONObject(Files.readString(file.toPath(), StandardCharsets.UTF_8)).getJSONObject("configuration");
  }
}
