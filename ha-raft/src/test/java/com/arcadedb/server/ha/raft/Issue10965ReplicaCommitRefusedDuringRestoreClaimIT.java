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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10965: while an {@code overwrite} restore holds the target on the leader, a commit made on a replica is refused instead
 * of acknowledged (and then destroyed by the swap), and is accepted again once the claim is released.
 */
class Issue10965ReplicaCommitRefusedDuringRestoreClaimIT extends BaseRaftHATest {

  private static final String TYPE_NAME = "Issue10965";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void releaseClaim() {
    for (int i = 0; i < getServerCount(); i++)
      if (getServer(i) != null && getServer(i).isStarted())
        getServer(i).releaseDatabaseNameReservedForRestore(getDatabaseName());
  }

  @Test
  void aReplicaCommitIsRefusedWhileTheLeaderHoldsTheRestoreClaim() throws Exception {
    final int leaderIndex = requireLeaderIndex();
    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().createDocumentType(TYPE_NAME));
    waitForAllServers();

    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());

    getServer(leaderIndex).reserveDatabaseNameForRestore(getDatabaseName());
    getServer(leaderIndex).awaitCommitsInFlightOn(getDatabaseName());

    assertThatThrownBy(() -> followerDb.transaction(() -> followerDb.newDocument(TYPE_NAME).set("id", 1).save(), false, 1))
        .isInstanceOf(NeedRetryException.class);

    getServer(leaderIndex).releaseDatabaseNameReservedForRestore(getDatabaseName());

    followerDb.transaction(() -> followerDb.newDocument(TYPE_NAME).set("id", 2).save());
    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(countOf(getServerDatabase(i, getDatabaseName()))).as("server " + i + " holds only the commit made after the claim")
          .isEqualTo(1L);
  }

  private static long countOf(final Database db) {
    try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS cnt FROM " + TYPE_NAME)) {
      return rs.next().<Number>getProperty("cnt").longValue();
    }
  }
}
