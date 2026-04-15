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
}
