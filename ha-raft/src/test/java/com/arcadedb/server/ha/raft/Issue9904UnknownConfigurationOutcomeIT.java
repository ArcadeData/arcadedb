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
import com.arcadedb.server.ServerControlPlane;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9904: the {@code DATABASE_CONFIGURATION_ENTRY} that {@code ALTER DATABASE} and "set database setting" submit
 * can be dispatched to Raft and then given up on ({@link ReplicationDispatchedTimeoutException}). The entry still
 * commits, and every node applies it - the one that submitted it included. The caller used to put the previous value
 * back on any failure: when this node's own apply had already run, that left it alone on the old value in memory while
 * its disk and every other node held the new one.
 * <p>
 * {@link RaftGroupCommitter#TEST_FORCE_DISPATCHED_TIMEOUT} gives up on the entry after dispatching it for real. To
 * reproduce the losing order deterministically the hook first waits for the submitting node to apply the entry, i.e.
 * for its {@code configuration.json} to hold the new value.
 */
class Issue9904UnknownConfigurationOutcomeIT extends BaseRaftHATest {

  /** Hang detector for the submitting node's own apply, not a latency bound. */
  private static final long APPLY_WAIT_MS = 30_000L;

  @AfterEach
  void clearHook() {
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = null;
  }

  @Test
  void alterDatabaseAppliedBeforeTheWaitGaveUpStaysInForceOnTheLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final AtomicBoolean fired = giveUpOnTheNextConfigurationEntryOnceApplied(leaderIndex, "typeDefaultBuckets", 13);

    assertThatThrownBy(() -> leader.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
        .isInstanceOf(ReplicationDispatchedTimeoutException.class)
        .isInstanceOf(TransactionOutcomeUnknownException.class);
    assertThat(fired.get()).as("the dispatched-timeout fault fired").isTrue();

    assertEveryNodeHolds(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, "typeDefaultBuckets", 13);
  }

  @Test
  void setDatabaseSettingAppliedBeforeTheWaitGaveUpStaysInForceOnTheFollower() throws Exception {
    final int followerIndex = (findLeaderIndex() + 1) % getServerCount();
    final AtomicBoolean fired = giveUpOnTheNextConfigurationEntryOnceApplied(followerIndex, "txRetries", 17);

    assertThatThrownBy(() -> new ServerControlPlane(getServer(followerIndex)).setDatabaseSetting(getDatabaseName(),
        GlobalConfiguration.TX_RETRIES.getKey(), "17"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);
    assertThat(fired.get()).as("the dispatched-timeout fault fired").isTrue();

    assertEveryNodeHolds(GlobalConfiguration.TX_RETRIES, "txRetries", 17);
  }

  /**
   * The other order: the wait gives up at once, before the entry is applied anywhere. The node that ran the change
   * goes back to the previous value, and the apply of the entry, which still commits, installs the new one.
   */
  @Test
  void alterDatabaseCommittedAfterTheWaitGaveUpIsInstalledOnEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final AtomicBoolean fired = new AtomicBoolean();
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = entry -> isConfigurationEntry(entry) && fired.compareAndSet(false, true);

    assertThatThrownBy(() -> leader.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 21"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);
    assertThat(fired.get()).isTrue();

    assertEveryNodeHolds(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, "typeDefaultBuckets", 21);
  }

  // -----------------------------------------------------------------------------------------------------------

  /**
   * Arms the fault for the next configuration entry of this database: once the node at {@code submitterIndex} has
   * applied it (its file holds {@code fileKey} = {@code expected}), the wait gives up with the indeterminate outcome.
   */
  private AtomicBoolean giveUpOnTheNextConfigurationEntryOnceApplied(final int submitterIndex, final String fileKey,
      final int expected) {
    final DatabaseInternal submitter = (DatabaseInternal) getServerDatabase(submitterIndex, getDatabaseName());
    final AtomicBoolean fired = new AtomicBoolean();
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = entry -> {
      if (!isConfigurationEntry(entry) || !fired.compareAndSet(false, true))
        return false;
      final long deadline = System.currentTimeMillis() + APPLY_WAIT_MS;
      try {
        while (readConfigurationFile(submitter).getInt(fileKey, -1) != expected) {
          if (System.currentTimeMillis() > deadline)
            throw new AssertionError("server " + submitterIndex + " never applied its own configuration entry");
          Thread.sleep(50);
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      } catch (final IOException e) {
        throw new AssertionError(e);
      }
      return true;
    };
    return fired;
  }

  private boolean isConfigurationEntry(final byte[] entry) {
    try {
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(ByteString.copyFrom(entry));
      return decoded.type() == RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY && getDatabaseName().equals(decoded.databaseName());
    } catch (final Exception e) {
      return false;
    }
  }

  private void assertEveryNodeHolds(final GlobalConfiguration setting, final String fileKey, final int expected)
      throws Exception {
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(i, getDatabaseName());
      waitForAppliedValue(i, database, setting, expected);
      assertThat(database.getConfiguration().getValueAsInteger(setting)).as("server %d in memory", i).isEqualTo(expected);
      assertThat(readConfigurationFile(database).getInt(fileKey)).as("server %d configuration.json", i).isEqualTo(expected);
    }
  }

  /** The entry may commit after the command returned, so each node is given time to apply it. A hang detector. */
  private static void waitForAppliedValue(final int index, final DatabaseInternal database,
      final GlobalConfiguration setting, final int expected) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + APPLY_WAIT_MS;
    while (database.getConfiguration().getValueAsInteger(setting) != expected && System.currentTimeMillis() < deadline)
      Thread.sleep(100);
  }

  private static JSONObject readConfigurationFile(final DatabaseInternal database) throws IOException {
    final File file = new File(database.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
    if (!file.exists())
      return new JSONObject();
    final String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
    return content.isBlank() ? new JSONObject() :
        new JSONObject(content).getJSONObject("configuration", new JSONObject());
  }
}
