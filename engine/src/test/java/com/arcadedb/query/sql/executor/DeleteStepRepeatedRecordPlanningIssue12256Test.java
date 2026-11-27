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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12256: a {@link DeleteStep} deleting from a source that can name one record twice remembers what it deleted, for the whole
 * statement. That is one entry per deleted record, which a scan of a type or of a bucket - the commonest DELETE, and the one run
 * under {@code BATCH n} to bound memory - has no reason to pay: it cannot name a record twice. The planner tells the step which it is
 * dealing with.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class DeleteStepRepeatedRecordPlanningIssue12256Test extends TestHelper {

  private RID createVertex() {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableVertex vertex = database.newVertex("V").set("k", 1).save();
      rid[0] = vertex.getIdentity();
    });
    return rid[0];
  }

  private boolean dropsRepeatedRecords(final String sql, final Object... args) {
    final boolean[] answer = new boolean[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", sql, args)) {
        final DeleteStep step = rs.getExecutionPlan().orElseThrow().getSteps().stream().filter(DeleteStep.class::isInstance)
            .map(DeleteStep.class::cast).findFirst().orElseThrow();
        answer[0] = step.dropsRepeatedRecords();
      }
    });
    return answer[0];
  }

  @Test
  void scanOfATypeKeepsNothing() {
    database.getSchema().createVertexType("V");
    database.getSchema().createDocumentType("D");
    assertThat(dropsRepeatedRecords("DELETE VERTEX FROM V")).isFalse();
    assertThat(dropsRepeatedRecords("DELETE VERTEX FROM V WHERE k = 1")).isFalse();
    assertThat(dropsRepeatedRecords("DELETE VERTEX FROM V WHERE k IN [1, 2] LIMIT 10 BATCH 5")).isFalse();
    assertThat(dropsRepeatedRecords("DELETE FROM D")).isFalse();
  }

  @Test
  void scanOfABucketKeepsNothing() {
    database.getSchema().createVertexType("V");
    final String bucket = database.getSchema().getType("V").getBuckets(false).getFirst().getName();
    assertThat(dropsRepeatedRecords("DELETE VERTEX FROM bucket:" + bucket)).isFalse();
    assertThat(dropsRepeatedRecords("DELETE VERTEX FROM bucket:" + bucket + " WHERE k = 1")).isFalse();
  }

  @Test
  void subqueryKeepsWhatItDeleted() {
    database.getSchema().createVertexType("V");
    assertThat(dropsRepeatedRecords("DELETE VERTEX FROM (SELECT FROM V)")).isTrue();
    assertThat(dropsRepeatedRecords("DELETE VERTEX FROM (SELECT expand(out()) FROM V WHERE k = 0) BATCH 10")).isTrue();
  }

  @Test
  void listOfRidsKeepsWhatItDeleted() {
    database.getSchema().createVertexType("V");
    final RID first = createVertex();
    assertThat(dropsRepeatedRecords("DELETE VERTEX FROM [" + first + ", " + first + "]")).isTrue();
    final RID second = createVertex();
    assertThat(dropsRepeatedRecords("DELETE VERTEX FROM " + second)).isTrue();
  }

  @Test
  void parametersKeepWhatTheyDeleted() {
    database.getSchema().createVertexType("V");
    final RID first = createVertex();
    assertThat(dropsRepeatedRecords("DELETE VERTEX FROM [?, ?]", first, first)).isTrue();
    final RID second = createVertex();
    assertThat(dropsRepeatedRecords("DELETE VERTEX FROM ?", second)).isTrue();
  }
}
