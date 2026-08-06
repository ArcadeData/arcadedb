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

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.ReadConsistency;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteGraphBatch;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying that the {@link RemoteDatabase} client's
 * {@link ReadConsistency#READ_YOUR_WRITES} consistency mode works end-to-end with
 * commit-index header tracking.
 *
 * <p>A client writes via the leader, the server responds with an
 * {@code X-ArcadeDB-Commit-Index} header that the client captures. A second client
 * pointing at a follower sends {@code X-ArcadeDB-Commit-Index} in the read request
 * so the follower waits until it has applied at least that index before responding.
 * The test verifies the inserted record is visible.
 */
class RaftRemoteReadYourWritesIT extends BaseRaftHATest {

  private static final String TYPE_NAME = "Ryw";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void remoteClientSeesWriteOnFollowerWithReadYourWrites() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final int leaderPort = 2480 + leaderIndex;
    final int followerPort = 2480 + followerIndex;
    final String dbName = getDatabaseName();
    final String password = BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;

    try (final RemoteDatabase writer = new RemoteDatabase("127.0.0.1", leaderPort, dbName, "root", password)) {
      writer.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);

      writer.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
      writer.command("sql", "INSERT INTO " + TYPE_NAME + " SET id = 'v1'");

      final long writerIndex = writer.getLastCommitIndex();
      assertThat(writerIndex).as("Leader must have returned a commit index after the write").isGreaterThanOrEqualTo(0);

      try (final RemoteDatabase reader = new RemoteDatabase("127.0.0.1", followerPort, dbName, "root", password)) {
        reader.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);

        // Seed the reader with the writer's commit index via reflection, since
        // updateLastCommitIndex is package-private in com.arcadedb.remote.
        final var updateMethod = RemoteDatabase.class.getDeclaredMethod("updateLastCommitIndex", long.class);
        updateMethod.setAccessible(true);
        updateMethod.invoke(reader, writerIndex);

        assertThat(reader.getLastCommitIndex())
            .as("Reader's lastCommitIndex must be seeded with the writer's index")
            .isEqualTo(writerIndex);

        final ResultSet rs = reader.query("sql", "SELECT FROM " + TYPE_NAME);
        final long count = rs.stream().count();
        assertThat(count)
            .as("Follower must see the record written by the leader when using READ_YOUR_WRITES")
            .isEqualTo(1);
      }
    }
  }

  @Test
  void readerCommitIndexAdvancesMonotonicallyAcrossWrites() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final int leaderPort = 2480 + leaderIndex;
    final int followerPort = 2480 + followerIndex;
    final String dbName = getDatabaseName();
    final String password = BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;

    try (final RemoteDatabase writer = new RemoteDatabase("127.0.0.1", leaderPort, dbName, "root", password)) {
      writer.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);

      writer.command("sql", "CREATE DOCUMENT TYPE RywMonotonic");

      long previousIndex = writer.getLastCommitIndex();
      assertThat(previousIndex).as("First DDL must produce a commit index").isGreaterThanOrEqualTo(0);

      for (int i = 0; i < 3; i++) {
        writer.command("sql", "INSERT INTO RywMonotonic SET seq = " + i);
        final long currentIndex = writer.getLastCommitIndex();
        assertThat(currentIndex)
            .as("lastCommitIndex must be monotonically non-decreasing after insert %d", i)
            .isGreaterThanOrEqualTo(previousIndex);
        previousIndex = currentIndex;
      }

      final long finalWriterIndex = writer.getLastCommitIndex();

      try (final RemoteDatabase reader = new RemoteDatabase("127.0.0.1", followerPort, dbName, "root", password)) {
        reader.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);

        final var updateMethod = RemoteDatabase.class.getDeclaredMethod("updateLastCommitIndex", long.class);
        updateMethod.setAccessible(true);
        updateMethod.invoke(reader, finalWriterIndex);

        final ResultSet rs = reader.query("sql", "SELECT FROM RywMonotonic");
        final long count = rs.stream().count();
        assertThat(count)
            .as("Follower must see all 3 records after seeding the commit index")
            .isEqualTo(3);

        // Reader's commit index must also advance after the follower responds
        assertThat(reader.getLastCommitIndex())
            .as("Reader's lastCommitIndex must advance after querying the follower")
            .isGreaterThanOrEqualTo(finalWriterIndex);
      }
    }
  }

  /**
   * Regression test for issue #5862: {@code RemoteGraphBatch} imports go through
   * {@code RemoteDatabase.sendBatch()} / the server's {@code PostBatchHandler}, a write path that bypasses
   * {@code DatabaseAbstractHandler} entirely (GraphBatch commits internally, one Raft entry per
   * {@code commitEvery} chunk). Before the fix neither side of that path touched the
   * {@code X-ArcadeDB-Commit-Index} bookmark, so a client importing through {@code RemoteGraphBatch} had no
   * bookmark to carry into a follower read - unlike the {@code writer.command(...)} path exercised by
   * {@link #remoteClientSeesWriteOnFollowerWithReadYourWrites()} above.
   * <p>
   * {@code writer} is a connection dedicated to the batch call and nothing else: its {@code lastCommitIndex}
   * starts at the documented sentinel {@code -1} and the ONLY request it ever issues is the GraphBatch
   * import, so an unchanged {@code -1} afterward can only mean {@code sendBatch()} failed to capture the
   * response header. A shared connection that had already run a prior command (whose own response also
   * carries the header) would leave that question ambiguous - a stale-but-non-negative index looks
   * identical to a freshly captured one.
   */
  @Test
  void remoteClientSeesGraphBatchWriteOnFollowerWithReadYourWrites() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final int leaderPort = 2480 + leaderIndex;
    final int followerPort = 2480 + followerIndex;
    final String dbName = getDatabaseName();
    final String password = BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
    final String vertexType = "RywBatchVertex";

    // Dedicated connection, used only to create the vertex type: kept separate from "writer" below so the
    // regression assertion is not muddied by an earlier response's bookmark.
    try (final RemoteDatabase setup = new RemoteDatabase("127.0.0.1", leaderPort, dbName, "root", password)) {
      setup.command("sql", "CREATE VERTEX TYPE " + vertexType);
    }

    try (final RemoteDatabase writer = new RemoteDatabase("127.0.0.1", leaderPort, dbName, "root", password)) {
      writer.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);
      assertThat(writer.getLastCommitIndex()).as("Fresh connection must start with no bookmark captured").isEqualTo(-1L);

      try (final RemoteGraphBatch batch = writer.batch().build()) {
        batch.createVertex(vertexType, "id", "v1");
        batch.createVertex(vertexType, "id", "v2");
      }

      final long writerIndex = writer.getLastCommitIndex();
      assertThat(writerIndex)
          .as("sendBatch() must capture the X-ArcadeDB-Commit-Index header from the PostBatchHandler response")
          .isGreaterThanOrEqualTo(0);

      try (final RemoteDatabase reader = new RemoteDatabase("127.0.0.1", followerPort, dbName, "root", password)) {
        reader.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);

        // Seed the reader with the writer's commit index via reflection, since
        // updateLastCommitIndex is package-private in com.arcadedb.remote.
        final var updateMethod = RemoteDatabase.class.getDeclaredMethod("updateLastCommitIndex", long.class);
        updateMethod.setAccessible(true);
        updateMethod.invoke(reader, writerIndex);

        final ResultSet rs = reader.query("sql", "SELECT FROM " + vertexType);
        final long count = rs.stream().count();
        assertThat(count)
            .as("Follower must see the vertices batch-imported on the leader when using READ_YOUR_WRITES")
            .isEqualTo(2);
      }
    }
  }

  /**
   * Regression test for issue #5862's forwarding half: a batch request that lands on a follower is
   * relayed to the leader by {@code PostBatchHandler.forwardBatchToLeader}, and {@code ExecutionResponse}
   * carries only status + body - so the leader's {@code X-ArcadeDB-Commit-Index} header has to be copied
   * onto the follower's own response explicitly, or a client that happened to connect to a follower would
   * never see the bookmark despite the leader having just emitted it.
   */
  @Test
  void remoteClientOnFollowerCapturesCommitIndexForwardedFromLeaderOnGraphBatch() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final int leaderPort = 2480 + leaderIndex;
    final int followerPort = 2480 + followerIndex;
    final String dbName = getDatabaseName();
    final String password = BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
    final String vertexType = "RywBatchForwardedVertex";

    try (final RemoteDatabase setup = new RemoteDatabase("127.0.0.1", leaderPort, dbName, "root", password)) {
      setup.command("sql", "CREATE VERTEX TYPE " + vertexType);
    }

    // Connected directly to the FOLLOWER: PostBatchHandler on this node sees ha.isLeader() == false and
    // forwards the request to the leader instead of running GraphBatch locally.
    try (final RemoteDatabase writer = new RemoteDatabase("127.0.0.1", followerPort, dbName, "root", password)) {
      writer.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);
      assertThat(writer.getLastCommitIndex()).as("Fresh connection must start with no bookmark captured").isEqualTo(-1L);

      try (final RemoteGraphBatch batch = writer.batch().build()) {
        batch.createVertex(vertexType, "id", "v1");
      }

      assertThat(writer.getLastCommitIndex())
          .as("The follower must relay the leader's X-ArcadeDB-Commit-Index header back to the client, "
              + "not just the status code and body of the forwarded response")
          .isGreaterThanOrEqualTo(0);
    }
  }
}
