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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12324: {@code CALL ... IN TRANSACTIONS} with a writing body failed with a ConcurrentModificationException when an outer
 * row produced before a batch committed held a record that batch wrote: the next batch's transaction updated the version from
 * before the commit. The outer rows of a later batch must see what the earlier batches committed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCallInTransactionsStaleRowIssue12324Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:N {id: 1})-[:R {id: 1}]->(b:N {id: 2})-[:R {id: 2}]->(c:N {id: 3})-[:R {id: 3}]->(d:N {id: 4})"));
  }

  @Test
  void pathRelationshipsWrittenAcrossBatches() {
    database.command("opencypher",
        "MATCH p0 = ()-[*1..2]-() CALL (p0) { UNWIND relationships(p0) AS rel SET rel.k3 = 'K' } IN TRANSACTIONS OF 3 ROWS");
    assertThat(values("MATCH ()-[r:R]->() RETURN r.k3 AS v")).containsOnly("K").hasSize(3);
  }

  @Test
  void pathRelationshipsWrittenAcrossBatchesAfterWith() {
    database.command("opencypher",
        "MATCH p0 = ()-[*1..2]-() WITH p0 CALL (p0) { UNWIND relationships(p0) AS rel SET rel.k3 = 'K' } IN TRANSACTIONS OF 3 ROWS");
    assertThat(values("MATCH ()-[r:R]->() RETURN r.k3 AS v")).containsOnly("K").hasSize(3);
  }

  @Test
  void aCounterIncrementedFromEveryBatchLosesNoUpdate() {
    // every node is in 4 rows, spread over batches of 3: each batch reads the count the batches before committed
    database.command("opencypher",
        "MATCH (a:N), (b:N) WITH a ORDER BY b.id, a.id CALL (a) { SET a.c = coalesce(a.c, 0) + 1 } IN TRANSACTIONS OF 3 ROWS");
    assertThat(values("MATCH (a:N) RETURN a.c AS v")).containsOnly(4L).hasSize(4);
  }

  @Test
  void aRecordInsideAListIsReadAgain() {
    database.command("opencypher",
        "MATCH (a:N), (b:N) WITH [a] AS l ORDER BY b.id CALL (l) { UNWIND l AS x SET x.c = coalesce(x.c, 0) + 1 } IN TRANSACTIONS OF 2 ROWS");
    assertThat(values("MATCH (a:N) RETURN a.c AS v")).containsOnly(4L).hasSize(4);
  }

  @Test
  void theRowsReturnedAfterABatchSeeItsWrites() {
    final List<Object> after = values(
        "MATCH (a:N), (b:N) WITH a ORDER BY b.id, a.id CALL (a) { SET a.c = coalesce(a.c, 0) + 1 } IN TRANSACTIONS OF 3 ROWS "
            + "RETURN a.c AS v");
    assertThat(after).hasSize(16);
    // the last row of each node is read after its fourth increment committed
    assertThat(after.subList(12, 16)).containsOnly(4L);
  }

  private List<Object> values(final String cypher) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = database.command("opencypher", cypher)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final Object v = row.getProperty("v");
        values.add(v instanceof Number n && !(v instanceof Double) ? (Object) n.longValue() : v);
      }
    }
    return values;
  }
}
