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

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9762: under Raft HA, {@code ALTER TYPE ... NAME} renamed the type and its bucket and index files on the leader
 * only. The rename ended in a bare schema save, so no {@code SCHEMA_ENTRY} was proposed, and a schema entry could not
 * have carried it anyway: it lists files to create and to remove, not renames. A follower kept answering to the old name,
 * and the next entry shipping the whole schema named bucket files it did not have.
 * <p>
 * Every node is checked three ways after the rename: its in-memory schema, its own {@code schema.json}, and the files
 * on its disk - the new names present, the old ones gone.
 */
class Issue9762TypeRenameReplicationIT extends BaseRaftHATest {

  private static final int RECORDS = 50;

  @Test
  void aDocumentTypeRenameReachesEveryNodeWithItsBucketAndIndexFiles() throws Exception {
    final Database leader = leader();
    leader.command("sql", "CREATE DOCUMENT TYPE R BUCKETS 2");
    leader.command("sql", "CREATE PROPERTY R.id INTEGER");
    leader.command("sql", "CREATE INDEX ON R (id) UNIQUE");
    leader.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        leader.newDocument("R").set("id", i).save();
    });
    assertClusterConsistency();

    leader.command("sql", "ALTER TYPE R NAME R2");

    assertRenamedOnEveryNode("R", "R2", RECORDS);

    // The renamed files keep their ids, so the pages of a later transaction land in them on every node
    leader().transaction(() -> leader().newDocument("R2").set("id", RECORDS).save());
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType("R2", false)).as("server %d", i).isEqualTo(RECORDS + 1);
  }

  @Test
  void aVertexTypeRenameMovesItsEdgeBucketsOnEveryNode() throws Exception {
    final Database leader = leader();
    // Not V1/V2/E1/E2: the fixture creates those
    leader.command("sql", "CREATE VERTEX TYPE Vr");
    leader.command("sql", "CREATE EDGE TYPE Er");
    leader.transaction(() -> {
      MutableVertex previous = null;
      for (int i = 0; i < RECORDS; i++) {
        final MutableVertex v = leader.newVertex("Vr").set("id", i).save();
        if (previous != null)
          previous.newEdge("Er", v).save();
        previous = v;
      }
    });
    assertClusterConsistency();

    leader.command("sql", "ALTER TYPE Vr NAME Vr2");

    assertRenamedOnEveryNode("Vr", "Vr2", RECORDS);
    for (int i = 0; i < getServerCount(); i++)
      try (final ResultSet rs = getServerDatabase(i, getDatabaseName()).query("sql",
          "SELECT count(*) AS c FROM (SELECT expand(out('Er')) FROM Vr2)")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).as("server %d", i).isEqualTo(RECORDS - 1);
      }
  }

  /**
   * The user's path: the statement sent over HTTP to a FOLLOWER, which forwards it to the leader.
   */
  @Test
  void aRenameSentOverHttpToAFollowerReachesEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final Database leader = leader();
    leader.command("sql", "CREATE DOCUMENT TYPE H");
    leader.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        leader.newDocument("H").set("id", i).save();
    });
    assertClusterConsistency();

    assertThat(executeCommand(followerIndex, "sql", "ALTER TYPE H NAME H2")).isNotNull();

    assertRenamedOnEveryNode("H", "H2", RECORDS);
  }

  /**
   * A follower survives a restart after the rename: it reopens with the renamed files and a schema naming them, rather
   * than dropping the type's buckets because the schema and the disk disagree.
   */
  @Test
  void aFollowerReopensTheRenamedTypeAfterARestart() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final Database leader = leader();
    leader.command("sql", "CREATE DOCUMENT TYPE S");
    leader.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        leader.newDocument("S").set("id", i).save();
    });
    assertClusterConsistency();

    leader.command("sql", "ALTER TYPE S NAME S2");
    assertRenamedOnEveryNode("S", "S2", RECORDS);

    restartServer(followerIndex);

    assertRenamedOnEveryNode("S", "S2", RECORDS);
  }

  /**
   * The Java API on a FOLLOWER's database has no exchange to forward through: the rename is refused there before a single
   * file moves, rather than renaming the files on that node alone.
   */
  @Test
  void aRenameOnAFollowerIsRefusedAndNothingMoves() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leader().command("sql", "CREATE DOCUMENT TYPE F");
    assertClusterConsistency();

    final Database follower = getServerDatabase(followerIndex, getDatabaseName());
    final Throwable refusal = catchThrowable(() -> follower.getSchema().getType("F").rename("F2"));
    assertThat(causeChainOf(refusal)).as("refused on the follower: %s", refusal)
        .anyMatch(ServerIsNotTheLeaderException.class::isInstance);

    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServerDatabase(i, getDatabaseName());
      assertThat(database.getSchema().existsType("F")).as("server %d", i).isTrue();
      assertThat(database.getSchema().existsType("F2")).as("server %d", i).isFalse();
      assertThat(filesNamedAfter(database, "F2")).as("server %d", i).isEmpty();
    }
  }

  /**
   * A peer that cannot decode the rename section must not be sent one, and there is no other form of the rename to send
   * it: the rename is refused before a single file moves, and goes through unchanged once the peer advertises it.
   */
  @Test
  void aRenameIsRefusedWhileAPeerCannotDecodeItAndNothingMoves() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final Database leader = leader();
    leader.command("sql", "CREATE DOCUMENT TYPE C");
    leader.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        leader.newDocument("C").set("id", i).save();
    });
    assertClusterConsistency();

    final RaftHAServer follower = getRaftPlugin(followerIndex).getRaftHAServer();
    final Set<String> withoutRenames = new HashSet<>(PeerCapabilities.LOCAL);
    withoutRenames.remove(PeerCapabilities.SCHEMA_FILE_RENAMES);
    follower.setAdvertisedCapabilities(withoutRenames);
    try {
      awaitLeaderSees(leaderIndex, followerIndex, false);

      final Throwable refusal = catchThrowable(() -> leader.command("sql", "ALTER TYPE C NAME C2"));
      assertThat(causeChainOf(refusal)).as("the rename is refused naming the capability: %s", refusal)
          .anyMatch(ClusterCapabilityNotReadyException.class::isInstance);

      // Nothing moved, on any node
      for (int i = 0; i < getServerCount(); i++) {
        final Database database = getServerDatabase(i, getDatabaseName());
        assertThat(database.getSchema().existsType("C")).as("server %d", i).isTrue();
        assertThat(database.getSchema().existsType("C2")).as("server %d", i).isFalse();
        assertThat(filesNamedAfter(database, "C2")).as("server %d", i).isEmpty();
        assertThat(filesNamedAfter(database, "C")).as("server %d", i).isNotEmpty();
      }
    } finally {
      follower.setAdvertisedCapabilities(PeerCapabilities.LOCAL);
    }

    awaitLeaderSees(leaderIndex, followerIndex, true);
    leader.command("sql", "ALTER TYPE C NAME C2");
    assertRenamedOnEveryNode("C", "C2", RECORDS);
  }

  private void assertRenamedOnEveryNode(final String oldName, final String newName, final long records)
      throws IOException {
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServerDatabase(i, getDatabaseName());
      final Schema schema = database.getSchema();
      assertThat(schema.existsType(newName)).as("server %d knows type %s", i, newName).isTrue();
      assertThat(schema.existsType(oldName)).as("server %d still knows type %s", i, oldName).isFalse();
      assertThat(database.countType(newName, false)).as("server %d records", i).isEqualTo(records);
      for (final var bucket : schema.getType(newName).getBuckets(false))
        assertThat(bucket.getName()).as("server %d bucket", i).startsWith(newName + "_");

      final JSONObject file = new JSONObject(
          Files.readString(new File(database.getDatabasePath(), "schema.json").toPath(), StandardCharsets.UTF_8));
      assertThat(file.getJSONObject("types").has(newName)).as("server %d schema.json", i).isTrue();
      assertThat(file.getJSONObject("types").has(oldName)).as("server %d schema.json", i).isFalse();

      assertThat(filesNamedAfter(database, newName)).as("server %d files of %s", i, newName).isNotEmpty();
      assertThat(filesNamedAfter(database, oldName)).as("server %d files of %s", i, oldName).isEmpty();

      if (!schema.getType(newName).getAllIndexes(false).isEmpty())
        try (final ResultSet rs = database.query("sql", "SELECT FROM " + newName + " WHERE id = 7")) {
          assertThat(rs.hasNext()).as("server %d index lookup", i).isTrue();
        }
    }
  }

  /** The component files on disk whose name is the type's name followed by its separator. */
  private static List<String> filesNamedAfter(final Database database, final String typeName) {
    final List<String> names = new ArrayList<>();
    final File[] files = new File(database.getDatabasePath()).listFiles();
    if (files != null)
      for (final File f : files)
        if (f.getName().startsWith(typeName + "_"))
          names.add(f.getName());
    return names;
  }

  private void awaitLeaderSees(final int leaderIndex, final int followerIndex, final boolean capable)
      throws InterruptedException {
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final String followerPeerId = peerIdForIndex(followerIndex);
    final long deadline = System.currentTimeMillis() + 60_000L;
    while (System.currentTimeMillis() < deadline) {
      final PeerCapabilityRegistry.Advertisement advertisement =
          leaderRaft.getPeerCapabilityRegistry().freshAdvertisementOf(followerPeerId);
      if (advertisement != null && advertisement.capabilities().contains(PeerCapabilities.SCHEMA_FILE_RENAMES) == capable)
        return;
      Thread.sleep(200);
    }
    throw new AssertionError("the leader never observed peer " + followerPeerId + " advertising "
        + (capable ? "" : "no ") + PeerCapabilities.SCHEMA_FILE_RENAMES);
  }

  private static List<Throwable> causeChainOf(final Throwable t) {
    final List<Throwable> chain = new ArrayList<>();
    for (Throwable c = t; c != null && !chain.contains(c); c = c.getCause())
      chain.add(c);
    return chain;
  }

  private Database leader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    return getServerDatabase(leaderIndex, getDatabaseName());
  }
}
