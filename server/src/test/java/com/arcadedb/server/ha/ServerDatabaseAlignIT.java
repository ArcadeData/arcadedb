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
package com.arcadedb.server.ha;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ServerDatabaseAlignIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  void alignNotNecessary() throws Exception {
    ArcadeDBServer leader = getLeader();
    final Database database = leader.getDatabase(getDatabaseName());

    database.transaction(() -> {
      final Record edge = database.iterateType(EDGE2_TYPE_NAME, true).next();

      database.deleteRecord(edge);
    });

    final Result result;
    try (ResultSet resultset = leader.getDatabase(getDatabaseName())
        .command("sql", "align database")) {

      assertThat(resultset.hasNext()).isTrue();
      result = resultset.next();
      assertThat(result.hasProperty(leader.getServerName())).isFalse();

//      assertThat(result.hasProperty("ArcadeDB_0")).isFalse();
//      assertThat(result.hasProperty("ArcadeDB_1")).isTrue();
//      assertThat(result.<List<int[]>>getProperty("ArcadeDB_1")).hasSize(0);
//      assertThat(result.hasProperty("ArcadeDB_2")).isTrue();
//      assertThat(result.<List<int[]>>getProperty("ArcadeDB_2")).hasSize(0);
    }

  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  void  alignNecessary() throws Exception {
    ArcadeDBServer leader = getLeader();
    final DatabaseInternal database = leader.getDatabase(getDatabaseName()).getEmbedded().getEmbedded();

    // EXPLICIT TX ON THE UNDERLYING DATABASE IS THE ONLY WAY TO BYPASS REPLICATED DATABASE
    database.begin();
    final Record edge = database.iterateType(EDGE1_TYPE_NAME, true).next();
    edge.delete();
    database.commit();

    assertThatThrownBy(() -> checkDatabasesAreIdentical())
        .isInstanceOf(DatabaseComparator.DatabaseAreNotIdentical.class);

    final Result result;
    try (ResultSet resultset = leader.getDatabase(getDatabaseName()).command("sql", "align database")) {
      assertThat(resultset.hasNext()).isTrue();
      result = resultset.next();
      assertThat(result.hasProperty(leader.getServerName())).isFalse();
    }
  }

  private ArcadeDBServer getLeader() {
    for (int i = 0; i < getServerCount(); ++i) {
      ArcadeDBServer server = getServer(i);
      if (server.getHA().isLeader())
        return server;
    }
    return null;
  }
}
