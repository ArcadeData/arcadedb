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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #9292: a {@code WHERE} pattern predicate accepted a node variable reused as a
 * relationship variable (and the reverse), while the neighbouring spelling {@code ()-[p]-()} was rejected.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherWherePatternPredicateVariableTypeIssue9292Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:N)-[:R]->(:N)");
  }

  @Test
  void nodeVariableReusedAsRelationshipInPredicateIsRejected() {
    assertRejected("MATCH (p)-[]-() WHERE (p)-[p]-() RETURN TRUE AS accepted");
  }

  @Test
  void nodeVariableReusedAsRelationshipInPredicateNotFirstIsRejected() {
    assertRejected("MATCH (p)-[]-() WHERE ()-[p]-() RETURN TRUE AS accepted");
  }

  @Test
  void relationshipVariableReusedAsNodeInPredicateIsRejected() {
    assertRejected("MATCH ()-[r]-() WHERE (r)-[]-() RETURN TRUE AS accepted");
  }

  @Test
  void relationshipVariableReusedAsEndNodeInPredicateIsRejected() {
    assertRejected("MATCH ()-[r]-() WHERE ()-[]-(r) RETURN TRUE AS accepted");
  }

  @Test
  void validPredicateStillWorks() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (p)-[r]-() WHERE (p)-[r]-() RETURN TRUE AS accepted")) {
      int count = 0;
      while (rs.hasNext()) {
        assertThat((Boolean) rs.next().getProperty("accepted")).isTrue();
        count++;
      }
      assertThat(count).isEqualTo(2);
    }
  }

  @Test
  void patternComprehensionReusingNodeAsRelationshipIsRejected() {
    assertRejected("MATCH (p) RETURN [(p)-[p]-() | 1] AS x");
  }

  @Test
  void optionalMatchWherePredicateIsRejected() {
    assertRejected("MATCH (p) OPTIONAL MATCH (p)-[]-(q) WHERE (p)-[p]-() RETURN q");
  }

  @Test
  void withWhereAfterRenameIsRejected() {
    assertRejected("MATCH (p)-[]-() WITH p AS q WHERE (q)-[q]-() RETURN q");
  }

  @Test
  void validComprehensionAndRenameStillWork() {
    assertThat(count("MATCH (p)-[r]-() RETURN [(p)-[r]-() | 1] AS x")).isEqualTo(2);
    assertThat(count("MATCH (p)-[]-() WITH p AS q WHERE (q)-[]-() RETURN q")).isEqualTo(2);
    assertThat(count("MATCH (a) WHERE (a)-[]-() WITH a AS x MATCH ()-[a]-() RETURN a")).isEqualTo(4);
  }

  @Test
  void declaringPatternsAreStillSkipped() {
    assertThat(count("MATCH (a:N) OPTIONAL MATCH (a)-[r:R]->(b) RETURN r")).isEqualTo(2);
    assertThat(count("MATCH (a)-[r:R]->() WHERE (a)-[r]->() RETURN r")).isEqualTo(1);
  }

  @Test
  void variableLengthRelationshipVariableReusedAsNodeIsRejected() {
    assertRejected("MATCH ()-[r*1..2]-() WHERE (r)-[]-() RETURN r");
  }

  private int count(final String query) {
    int count = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }

  private void assertRejected(final String query) {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          rs.next();
      }
    }).isInstanceOf(RuntimeException.class).hasMessageContaining("VariableTypeConflict");
  }
}
