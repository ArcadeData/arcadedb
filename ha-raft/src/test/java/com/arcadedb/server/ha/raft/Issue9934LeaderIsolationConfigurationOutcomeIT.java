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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.TransactionOutcomeUnknownException;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerControlPlane;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9934: the configuration change of unknown outcome (#9904) driven through a REAL leader isolation rather than
 * a hook that gives up on an entry the cluster then commits normally.
 * <p>
 * The leader is cut off from both followers ({@link RaftNetworkPartition}) and runs {@code ALTER DATABASE} or
 * "set database setting" ({@link ServerControlPlane#setDatabaseSetting}). Its Raft log takes the
 * {@code DATABASE_CONFIGURATION_ENTRY}, which can never reach a quorum; the wait gives up with
 * {@link TransactionOutcomeUnknownException}, and the node settles the setting on its {@code configuration.json},
 * i.e. on the previous value. The followers elect a leader of their own, and when the partition heals the old
 * leader's uncommitted entry is truncated by the new leader's log. Then two outcomes:
 * <ul>
 *   <li><b>never committed</b>: the entry is gone everywhere, and every node, the one that ran the command included,
 *       holds the previous value; a later change made on that node does not carry it along;</li>
 *   <li><b>committed late</b>: the old leader's Raft client retried the request against the other side of the cut,
 *       and that request, delayed in flight, commits there AFTER the caller settled on the previous value. Its apply
 *       installs the new value on every node, the old leader included once it has caught up.</li>
 * </ul>
 * Which one happens is the Raft client's retry, not the state machine's: ArcadeDB's wait gives up, Ratis's client does
 * not, so an isolated leader's change would routinely commit late on the majority side. The partition parks that retry
 * (late commit, released once the caller settled) or refuses it (never committed), so each order runs every time.
 */
class Issue9934LeaderIsolationConfigurationOutcomeIT extends BaseRaftHATest {

  /** Short enough that the wait gives up quickly on an entry that cannot reach a quorum. */
  private static final long QUORUM_TIMEOUT_MS   = 3_000L;
  /** Hang detector for every convergence below, not a latency bound. */
  private static final long CONVERGENCE_WAIT_MS = 60_000L;

  private RaftNetworkPartition partition;

  @Override
  protected int getServerCount() {
    // A majority on the far side of the cut: two followers that elect a leader without the isolated one
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, QUORUM_TIMEOUT_MS);
    // Faster failover: the isolated leader steps down, and the followers elect, within a few seconds
    config.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN, 1_500);
    config.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX, 3_000);
  }

  @AfterEach
  void healPartition() {
    if (partition != null)
      partition.close();
    // Also when the test failed before a partition object existed: both injections are JVM-wide
    RaftNetworkPartition.resetAll();
  }

  @Test
  void alterDatabaseOfAnIsolatedLeaderThatNeverCommitsLeavesThePreviousValueEverywhere() throws Exception {
    final SettingChange alterDatabase = (server, value) -> getServerDatabase(server, getDatabaseName())
        .command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` " + value);
    assertChangeThatNeverCommitsLeavesThePreviousValueEverywhere(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, alterDatabase,
        GlobalConfiguration.TX_RETRIES);
  }

  @Test
  void setDatabaseSettingOfAnIsolatedLeaderThatNeverCommitsLeavesThePreviousValueEverywhere() throws Exception {
    final SettingChange setDatabaseSetting = (server, value) -> new ServerControlPlane(getServer(server))
        .setDatabaseSetting(getDatabaseName(), GlobalConfiguration.TX_RETRIES.getKey(), String.valueOf(value));
    assertChangeThatNeverCommitsLeavesThePreviousValueEverywhere(GlobalConfiguration.TX_RETRIES, setDatabaseSetting,
        GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
  }

  @Test
  void changeOfAnIsolatedLeaderCommittedLateOnTheMajorityIsInstalledOnEveryNode() throws Exception {
    final int oldLeader = findLeaderIndex();
    assertThat(oldLeader).isGreaterThanOrEqualTo(0);
    final Database database = getServerDatabase(oldLeader, getDatabaseName());
    final int previous = configuredValue(oldLeader, GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    // Any other valid value: the setting accepts any positive integer
    final int changed = previous + 13;
    final long baseline = lastRaftLogIndex(raftOf(oldLeader));

    partition = RaftNetworkPartition.isolateHoldingClientRequests(peerIdForIndex(oldLeader), this::isConfigurationEntry);

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` " + changed))
        .isInstanceOf(TransactionOutcomeUnknownException.class);
    assertThat(configuredValue(oldLeader, GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).as("settled value on the old leader")
        .isEqualTo(previous);
    final LogEntryProto appended = configurationEntryInLogAfter(raftOf(oldLeader), baseline);
    assertThat(appended).as("the old leader appended the configuration entry to its own log").isNotNull();

    final int newLeader = awaitLeaderOtherThan(oldLeader);
    awaitTrue(() -> partition.heldClientRequests() > 0, "the old leader's Raft client retried its request on the new leader");

    // The request delayed in flight arrives now, after the caller settled on the previous value: it commits on the
    // majority side while the old leader is still cut off and still on the previous value
    partition.releaseClientRequests();
    for (int i = 0; i < getServerCount(); i++)
      if (i != oldLeader)
        awaitConfiguredValue(i, GlobalConfiguration.TYPE_DEFAULT_BUCKETS, changed);
    assertThat(configurationEntryInLogAfter(raftOf(newLeader), baseline)).as("the late request committed on the new leader")
        .isNotNull();
    assertThat(configuredValue(oldLeader, GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
        .as("the old leader, still cut off, keeps the previous value").isEqualTo(previous);

    partition.healServerTraffic();

    // The old leader's own copy of the entry is replaced by the new leader's log, whose copy committed: its apply
    // installs the new value over the one the old leader settled on
    awaitTrue(() -> isTruncated(raftOf(oldLeader), appended), "the old leader's uncommitted entry is truncated");
    assertEveryNodeHolds(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, changed);
  }

  // -----------------------------------------------------------------------------------------------------------

  /** Runs a configuration change on one server: {@code ALTER DATABASE} or "set database setting". */
  @FunctionalInterface
  private interface SettingChange {
    void apply(int serverIndex, int value) throws Exception;
  }

  /**
   * The never-committed outcome, for one of the two commands that change a database setting: the isolated leader
   * settles on the previous value, its entry is truncated, no node ever installs the new value, and a later change of
   * {@code followUp} made on the same node does not carry the abandoned value along.
   */
  private void assertChangeThatNeverCommitsLeavesThePreviousValueEverywhere(final GlobalConfiguration setting,
      final SettingChange change, final GlobalConfiguration followUp)
      throws Exception {
    final int oldLeader = findLeaderIndex();
    assertThat(oldLeader).isGreaterThanOrEqualTo(0);
    final int previous = configuredValue(oldLeader, setting);
    // Any other valid value: both settings accept any positive integer
    final int changed = previous + 11;
    final int followUpValue = configuredValue(oldLeader, followUp) + 7;
    final long baseline = lastRaftLogIndex(raftOf(oldLeader));

    partition = RaftNetworkPartition.isolateDroppingClientRequests(peerIdForIndex(oldLeader), this::isConfigurationEntry);

    assertThatThrownBy(() -> change.apply(oldLeader, changed)).isInstanceOf(TransactionOutcomeUnknownException.class);

    // The node settled on its configuration.json, which no apply wrote: the previous value
    assertThat(configuredValue(oldLeader, setting)).as("settled value on the old leader").isEqualTo(previous);
    final LogEntryProto appended = configurationEntryInLogAfter(raftOf(oldLeader), baseline);
    assertThat(appended).as("the old leader appended the configuration entry to its own log").isNotNull();

    final int newLeader = awaitLeaderOtherThan(oldLeader);
    // Ratis's client keeps retrying the request ArcadeDB gave up on, against the far side of the cut: the partition is
    // what refuses it, so prove it was refused rather than that it never came
    awaitTrue(() -> partition.droppedClientRequests() > 0, "the old leader's Raft client retried its request on the new leader");

    partition.healServerTraffic();

    awaitTrue(() -> isTruncated(raftOf(oldLeader), appended), "the old leader's uncommitted entry is truncated");
    assertThat(configurationEntryInLogAfter(raftOf(newLeader), baseline)).as("no configuration entry ever committed")
        .isNull();
    assertEveryNodeHolds(setting, previous);

    // Nothing was left pending on the node that ran the change: its next change does not carry the abandoned value
    // along. The old leader is a follower now, and the change goes through its fresh Raft client
    partition.close();
    partition = null;
    getServerDatabase(oldLeader, getDatabaseName()).command("sql",
        "ALTER DATABASE `" + followUp.getKey() + "` " + followUpValue);
    assertEveryNodeHolds(followUp, followUpValue);
    assertEveryNodeHolds(setting, previous);
  }

  private RaftHAServer raftOf(final int serverIndex) {
    return getRaftPlugin(serverIndex).getRaftHAServer();
  }

  private int awaitLeaderOtherThan(final int oldLeader) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + CONVERGENCE_WAIT_MS;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++)
        if (i != oldLeader && getRaftPlugin(i).isLeader())
          return i;
      Thread.sleep(100);
    }
    throw new AssertionError("the followers never elected a leader of their own while server " + oldLeader + " was cut off");
  }

  private static void awaitTrue(final BooleanSupplier condition, final String what) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + CONVERGENCE_WAIT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline)
        throw new AssertionError("timed out waiting until " + what);
      Thread.sleep(100);
    }
  }

  private boolean isConfigurationEntry(final ByteString data) {
    try {
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(data);
      return decoded.type() == RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY && getDatabaseName().equals(decoded.databaseName());
    } catch (final Exception e) {
      // Not an entry this test intercepts; logged so a codec change that made every entry unreadable is visible
      LogManager.instance().log(this, Level.FINE, "TEST: cannot decode a Raft log entry: %s", e.getMessage());
      return false;
    }
  }

  /** The first configuration entry of this database in {@code server}'s log after {@code afterIndex}, committed or not. */
  private LogEntryProto configurationEntryInLogAfter(final RaftHAServer server, final long afterIndex) throws Exception {
    final RaftLog log = server.getRaftDivision().getRaftLog();
    final long last = log.getLastEntryTermIndex().getIndex();
    for (long i = afterIndex + 1; i <= last; i++) {
      final LogEntryProto entry = log.get(i);
      if (entry != null && entry.hasStateMachineLogEntry() && isConfigurationEntry(entry.getStateMachineLogEntry().getLogData()))
        return entry;
    }
    return null;
  }

  /** Whether the entry {@code appended} at its index and term is no longer what {@code server}'s log holds there. */
  private static boolean isTruncated(final RaftHAServer server, final LogEntryProto appended) {
    try {
      final LogEntryProto current = server.getRaftDivision().getRaftLog().get(appended.getIndex());
      return current != null && current.getTerm() != appended.getTerm();
    } catch (final Exception e) {
      LogManager.instance().log(Issue9934LeaderIsolationConfigurationOutcomeIT.class, Level.FINE,
          "TEST: cannot read Raft log entry %d: %s", appended.getIndex(), e.getMessage());
      return false;
    }
  }

  private int configuredValue(final int serverIndex, final GlobalConfiguration setting) {
    return ((DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName())).getConfiguration()
        .getValueAsInteger(setting);
  }

  private void awaitConfiguredValue(final int serverIndex, final GlobalConfiguration setting, final int expected)
      throws InterruptedException {
    awaitTrue(() -> configuredValue(serverIndex, setting) == expected,
        "server " + serverIndex + " holds " + setting.getKey() + "=" + expected);
  }

  private void assertEveryNodeHolds(final GlobalConfiguration setting, final int expected) throws Exception {
    // configuration.json names a setting without the "arcadedb." prefix
    final String fileKey = setting.getKey().substring(GlobalConfiguration.PREFIX.length());
    for (int i = 0; i < getServerCount(); i++) {
      awaitConfiguredValue(i, setting, expected);
      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(i, getDatabaseName());
      assertThat(database.getConfiguration().getValueAsInteger(setting)).as("server %d in memory", i).isEqualTo(expected);
      final JSONObject saved = readConfigurationFile(database);
      // A setting never changed on this database is not in its file: it holds the default there, and the in-memory
      // check above is the whole assertion for it
      if (saved.has(fileKey) || expected != setting.getValueAsInteger())
        assertThat(saved.getInt(fileKey, -1)).as("server %d configuration.json", i).isEqualTo(expected);
    }
  }

  private static JSONObject readConfigurationFile(final DatabaseInternal database) throws IOException {
    final File file = new File(database.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
    if (!file.exists())
      return new JSONObject();
    final String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
    return content.isBlank() ? new JSONObject() : new JSONObject(content).getJSONObject("configuration", new JSONObject());
  }
}
