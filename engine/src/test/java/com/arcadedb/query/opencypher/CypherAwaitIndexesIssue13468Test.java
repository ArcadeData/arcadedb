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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #13468: {@code CALL db.awaitIndexes()}, which a Neo4j client calls after {@code CREATE INDEX}, was
 * an unknown procedure.
 */
class CypherAwaitIndexesIssue13468Test extends TestHelper {
  @Test
  void awaitIndexesReturnsNoRowsAndTheIndexAnswersTheNextQuery() {
    database.transaction(() -> database.command("opencypher", "UNWIND range(1, 50) AS i CREATE (:Idx13468 {p: i})").close());
    database.command("opencypher", "CREATE INDEX IF NOT EXISTS FOR (n:Idx13468) ON (n.p)").close();

    try (final ResultSet rs = database.command("opencypher", "CALL db.awaitIndexes()")) {
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.command("opencypher", "CALL db.awaitIndexes(300)")) {
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Idx13468 {p: 7}) RETURN count(n) AS c")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
    }
  }
}
