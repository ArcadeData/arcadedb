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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * Issue #10067: the type a follower has the leader create for a client transaction EXTENDS a type that has an index (the composite
 * type of two labels, one of them {@code V1}, whose {@code id} is indexed). Creating it takes more than one schema entry (the type,
 * then the index the super type's index grows for its buckets), so a transaction that already wrote is NOT vouched for and is told to
 * retry - failing closed, never open - and the retry commits. Either way every node ends up finding both records through the index of
 * the super type: no record is left out of an index.
 * <p>
 * Kept apart from {@link Issue10067ImplicitTypeInClientTransactionOnAFollowerIT} because it takes more than one schema entry to
 * create the type. The database comparison that ends every test of the fixture covers the composite type too (issue #10093), and the
 * check that matters for this issue is made through the indexes themselves, on every node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10067IndexedSuperTypeOnAFollowerIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(180)
  void aNewTypeExtendingAnIndexedTypeNeverLeavesARecordOutOfTheIndex() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    boolean committed = false;
    for (int attempt = 1; attempt <= 2 && !committed; attempt++) {
      followerDb.begin();
      try {
        // What the Bolt and Postgres executors mark on the transaction a client opens with BEGIN (issue #10040)
        ((DatabaseInternal) followerDb).getTransaction().setClientManaged(true);
        followerDb.command("opencypher", "CREATE (n:V1 {id: 1003000})").close();
        final Throwable told = catchThrowable(
            () -> followerDb.command("opencypher", "CREATE (m:V1:Issue10067Poly {id: 1003001})").close());
        if (told == null) {
          followerDb.commit();
          committed = true;
        } else
          assertThat(CauseChain.find(told, NeedRetryException.class)).as("a refusal here must be the retryable one, got %s", told)
              .isNotNull();
      } finally {
        if (followerDb.isTransactionActive())
          followerDb.rollback();
      }
    }
    assertThat(committed).as("the retry of a transaction told to retry commits").isTrue();

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
        for (final long id : new long[] { 1003000L, 1003001L })
          try (final ResultSet rs = getServerDatabase(server, getDatabaseName()).query("sql", "SELECT FROM V1 WHERE id = ?", id)) {
            assertThat(rs.stream().count()).as("server %d finds V1 id %d through the index of V1", server, id).isEqualTo(1L);
          }
      });
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType("Issue10067Poly~V1")).as("server %d", i).isTrue();
    }
  }
}
