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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8633: a LET between two BEGIN/COMMIT blocks of a SQL script made the first block's CREATE EDGE run twice.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8633SqlScriptLetBetweenTransactionsTest extends TestHelper {
  @Test
  void letBetweenTwoTransactionsDoesNotRepeatTheFirstCreateEdgeOnUnidirectionalType() {
    assertEachBlockRunsOnce(false);
  }

  @Test
  void letBetweenTwoTransactionsDoesNotRepeatTheFirstCreateEdgeOnBidirectionalType() {
    assertEachBlockRunsOnce(true);
  }

  @Test
  void everyBlockOfAScriptRunsExactlyOnce() {
    database.getSchema().createDocumentType("Log");
    database.command("sqlscript", """
        BEGIN;
        INSERT INTO Log SET n = 1;
        COMMIT;
        BEGIN;
        INSERT INTO Log SET n = 2;
        COMMIT;
        LET x = SELECT count(*) AS c FROM Log;
        BEGIN;
        INSERT INTO Log SET n = 3;
        COMMIT;
        """).close();
    assertThat(database.countType("Log", false)).isEqualTo(3);
  }

  @Test
  void aRetryBlockAndAPlainBlockEachRunOnceInBothOrders() {
    database.getSchema().createDocumentType("Log");
    database.command("sqlscript", """
        BEGIN;
        INSERT INTO Log SET n = 1;
        COMMIT RETRY 3;
        BEGIN;
        INSERT INTO Log SET n = 2;
        COMMIT;
        BEGIN;
        INSERT INTO Log SET n = 3;
        COMMIT RETRY 3;
        """).close();
    assertThat(database.countType("Log", false)).isEqualTo(3);
  }

  private void assertEachBlockRunsOnce(final boolean bidirectional) {
    database.getSchema().createVertexType("Question");
    database.getSchema().createVertexType("Tag");
    database.getSchema().buildEdgeType().withName("TAGGED_WITH").withBidirectional(bidirectional).create();
    database.transaction(() -> {
      database.newVertex("Tag").set("name", "s").save();
      database.newVertex("Question").set("qid", 1).save();
      database.newVertex("Question").set("qid", 2).save();
    });

    final ResultSet rs = database.command("sqlscript", """
        BEGIN;
        CREATE EDGE TAGGED_WITH FROM (SELECT FROM Question WHERE qid = 1) TO (SELECT FROM Tag WHERE name = 's');
        COMMIT;
        LET a = SELECT out('TAGGED_WITH').size() AS n FROM Question WHERE qid = 1;
        BEGIN;
        CREATE EDGE TAGGED_WITH FROM (SELECT FROM Question WHERE qid = 2) TO (SELECT FROM Tag WHERE name = 's');
        COMMIT;
        LET c = SELECT @out.qid AS n FROM TAGGED_WITH;
        RETURN $c;
        """);
    final List<Integer> qids = new ArrayList<>();
    while (rs.hasNext())
      qids.add(rs.next().<Integer>getProperty("n"));
    rs.close();

    assertThat(qids).containsExactlyInAnyOrder(1, 2);
    assertThat(database.countType("TAGGED_WITH", false)).isEqualTo(2);
  }
}
