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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11257: on a replicated database the postings of an {@code LSM_SPARSE_VECTOR} index that were not sealed into a segment
 * lived in the leader's memtable only. A follower answered a sparse search without them until the leader flushed, and a leader
 * that went away took them along. The leader now logs them with the commit, as a standalone database does (#11239), and every
 * node feeds the log pages it applies into its own memtable.
 * <p>
 * No test here flushes before reading a follower: that is the point. Each document holds dimension 0 with a weight of its own,
 * so a query on dimension 0 ranks the documents by that weight and the answer is known in advance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11257SparsePostingsOnFollowersIT extends BaseRaftHATest {
  private static final String TYPE_NAME  = "Issue11257Doc";
  private static final String IDX_NAME   = "Issue11257Doc[tokens,weights]";
  private static final int    DIMENSIONS = 50;
  private static final int    TOP_K      = 1_000;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 30_000L);
  }

  @Override
  protected void populateDatabase() {
    // The schema and the writes are the test's own
  }

  /** Inserts, updates and deletes reach every follower's answer without a flush, and a flush in between changes nothing. */
  @Test
  void unsealedPostingsAnswerOnEveryFollower() {
    final int leader = requireLeaderIndex();
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    createIndex(leaderDb);

    final List<RID> rids = new ArrayList<>();
    for (int batch = 0; batch < 4; batch++)
      rids.addAll(insert(leaderDb, batch * 25, 25));
    assertEveryServerAnswers(leader, 100);

    // An update rewrites a document's postings (tombstones for the old dimensions, the new weight for dimension 0), a delete
    // tombstones them all
    leaderDb.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final MutableDocument doc = rids.get(i).asDocument().modify();
        doc.set("tokens", new int[] { 0, 49 });
        doc.set("weights", new float[] { 1_000f + i, 1f });
        doc.save();
      }
      for (int i = 90; i < 100; i++)
        rids.get(i).asDocument().delete();
    });
    assertEveryServerAnswers(leader, 90);

    // The leader seals what it holds and resets the log that held it; the followers drop what the log held once the segment
    // carrying it has arrived
    flushOn(leaderDb);
    assertEveryServerAnswers(leader, 90);

    insert(leaderDb, 100, 20);
    assertEveryServerAnswers(leader, 110);
    flushOn(leaderDb);
    insert(leaderDb, 120, 5);
    assertEveryServerAnswers(leader, 115);
  }

  /** A follower that restarts finds the postings again in the logs it applied. */
  @Test
  void restartedFollowerReplaysItsLogs() {
    final int leader = requireLeaderIndex();
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    createIndex(leaderDb);
    insert(leaderDb, 0, 40);
    assertEveryServerAnswers(leader, 40);

    restartServer(firstFollowerOf(leader));
    final int leaderAfter = requireLeaderIndex();
    insert(getServerDatabase(leaderAfter, getDatabaseName()), 40, 10);
    assertEveryServerAnswers(leaderAfter, 50);
  }

  /** The next leader already holds the postings the previous one had not sealed, and goes on logging and sealing them. */
  @Test
  void nextLeaderHoldsWhatThePreviousOneDidNotSeal() {
    final int leader = requireLeaderIndex();
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    createIndex(leaderDb);
    insert(leaderDb, 0, 30);
    assertEveryServerAnswers(leader, 30);

    assertThat(getRaftPlugin(leader).getRaftHAServer().transferLeadership(15_000)).as("leadership moved").isTrue();
    final int next = requireLeaderIndex();
    assertThat(next).isNotEqualTo(leader);
    assertEveryServerAnswers(next, 30);

    final Database nextDb = getServerDatabase(next, getDatabaseName());
    insert(nextDb, 30, 10);
    assertEveryServerAnswers(next, 40);
    flushOn(nextDb);
    insert(nextDb, 40, 10);
    assertEveryServerAnswers(next, 50);
  }

  private static void createIndex(final Database leaderDb) {
    leaderDb.transaction(() -> {
      final DocumentType type = leaderDb.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      leaderDb.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" }).withSparseVectorType()
          .withDimensions(DIMENSIONS).create();
    });
  }

  /** Document {@code n} holds dimension 0 weighted {@code n + 1} and two more dimensions. */
  private static List<RID> insert(final Database db, final int first, final int count) {
    final List<RID> rids = new ArrayList<>(count);
    db.transaction(() -> {
      for (int n = first; n < first + count; n++) {
        final MutableDocument doc = db.newDocument(TYPE_NAME);
        doc.set("tokens", new int[] { 0, 1 + n % 24, 25 + n % 24 });
        doc.set("weights", new float[] { n + 1f, 0.5f, 0.25f });
        doc.save();
        rids.add(doc.getIdentity());
      }
    });
    return rids;
  }

  private static void flushOn(final Database leaderDb) {
    for (final IndexInternal sub : ((TypeIndex) leaderDb.getSchema().getIndexByName(IDX_NAME)).getIndexesOnBuckets())
      sub.flush();
  }

  private void assertEveryServerAnswers(final int leader, final int expectedDocuments) {
    for (int i = 0; i < getServerCount(); i++)
      assertThat(awaitReplicationIsCompleted(i)).as("server %d applied what the leader applied", i).isTrue();

    final List<String> expected = answerOf(leader);
    assertThat(expected).as("the leader answers every live document").hasSize(expectedDocuments);
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        assertThat(answerOf(i)).as("server %d answers like the leader without a flush", i).isEqualTo(expected);
  }

  /** The ranked answer of a query on dimension 0, as "rid=score" rows. */
  private List<String> answerOf(final int serverIndex) {
    return withResyncRetry(serverIndex, db -> {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = db.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))", IDX_NAME,
          new int[] { 0 }, new float[] { 1f }, TOP_K)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows.add(row.getProperty("@rid") + "=" + row.getProperty("score"));
        }
      }
      return rows;
    });
  }
}
