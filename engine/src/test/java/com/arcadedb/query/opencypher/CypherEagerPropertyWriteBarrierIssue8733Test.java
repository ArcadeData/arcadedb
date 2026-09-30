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
 * Issue #8733: a MATCH that reads a property an earlier clause writes needs an eager barrier in front of it, and one
 * that reads nothing the writes touched must keep streaming. Checked on the plan, because the row-level symptom depends on
 * how many rows the consumer above pulls at a time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherEagerPropertyWriteBarrierIssue8733Test extends TestHelper {
  private static final String EAGER = "EAGER";

  @Test
  void matchReadingAKeyWrittenByAnAbsorbedMergeSetIsEager() {
    assertThat(explain("MATCH (a:A) MERGE (n:X {id: a.id}) SET n.k = true WITH n MATCH (m {k: true}) RETURN count(*) AS c")).contains(EAGER);
  }

  @Test
  void matchReadingAKeyWrittenByMergeOnMatchIsEager() {
    assertThat(explain("MATCH (a:A) MERGE (n:X {id: a.id}) ON MATCH SET n.k = true WITH n MATCH (m {k: true}) RETURN count(*) AS c")).contains(EAGER);
  }

  @Test
  void matchReadingAKeyWrittenBySetIsEager() {
    assertThat(explain("MATCH (a:A) SET a.k = 1 WITH a MATCH (m {k: 1}) RETURN count(*) AS c")).contains(EAGER);
  }

  @Test
  void matchReadingAKeyWrittenByRemoveIsEager() {
    assertThat(explain("MATCH (a:A) REMOVE a.k WITH a MATCH (m) WHERE m.k IS NULL RETURN count(*) AS c")).contains(EAGER);
  }

  @Test
  void mapWriteIsEagerForAnyKeyRead() {
    assertThat(explain("MATCH (a:A) SET a += {z: 1} WITH a MATCH (m {k: 1}) RETURN count(*) AS c")).contains(EAGER);
    assertThat(explain("MATCH (a:A) SET a = {z: 1} WITH a MATCH (m {k: 1}) RETURN count(*) AS c")).contains(EAGER);
  }

  @Test
  void matchReadingAnUnrelatedKeyStaysStreaming() {
    assertThat(explain("MATCH (a:A) SET a.k = 1 WITH a MATCH (m {other: 1}) RETURN count(*) AS c")).doesNotContain(EAGER);
  }

  @Test
  void labelWriteWithNoReadInFlightNeedsNoBarrier() {
    assertThat(explain("UNWIND [1,2] AS i CREATE (n:A) SET n:B RETURN count(*) AS c")).doesNotContain(EAGER);
  }

  @Test
  void labelWriteAfterAMatchIsEager() {
    assertThat(explain("MATCH (a:A) SET a:B RETURN count(*) AS c")).contains(EAGER);
    assertThat(explain("MATCH (a:A) REMOVE a:B RETURN count(*) AS c")).contains(EAGER);
  }

  private String explain(final String query) {
    try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query)) {
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
