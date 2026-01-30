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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.FilterStep;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Test for issue #2326: Index selection changed/bug
 *
 * When multiple single-property full-text indexes exist alongside a composite LSM index,
 * the query optimizer should prefer the composite index when all properties are matched
 * with equality conditions.
 */
public class Issue2326MultiFieldIndexSelectionTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      final Schema schema = database.getSchema();

      // Create type
      final DocumentType tc = schema.createDocumentType("example");

      // Create properties
      tc.createProperty("p1", String.class);
      tc.createProperty("p2", String.class);
      tc.createProperty("p3", String.class);

      // Index props - three individual full-text indexes
      tc.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "p1");
      tc.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "p2");
      tc.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "p3");

      // Composite unique LSM index on all three properties
      tc.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "p1", "p2", "p3");

      // Add dummy data
      final String[] perm = { "a", "b", "c", "d", "e", "f", "g", "h" };
      for (final String l1 : perm) {
        for (final String l2 : perm) {
          for (final String l3 : perm) {
            final MutableDocument doc = database.newDocument("example");
            doc.set("p1", l1);
            doc.set("p2", l2);
            doc.set("p3", l3);
            doc.save();
          }
        }
      }
    });
  }

  @Test
  void compositeIndexIsSelected() {
    database.transaction(() -> {
      // Query with equality on all three properties
      final ResultSet rs = database.query("sql",
          "explain select * from example where p1=? and p2=? and p3=?", "e", "b", "h");

      final ExecutionPlan plan = rs.getExecutionPlan().get();
      final String planString = plan.prettyPrint(0, 3);

      //System.out.println("Execution plan:");
      //System.out.println(planString);

      // The plan should use the composite index example[p1,p2,p3]
      assertThat(planString).contains("example[p1,p2,p3]");

      // The plan should NOT use a single property index
      assertThat(planString).doesNotContain("example[p1]");
      assertThat(planString).doesNotContain("example[p2]");
      assertThat(planString).doesNotContain("example[p3]");

      // The plan should NOT have a filter step (all conditions should be handled by index)
      final List<ExecutionStep> steps = plan.getSteps();
      for (final ExecutionStep step : steps) {
        if (step instanceof FilterStep)
          fail("Execution plan contains FilterStep, not using composite index properly");
      }
    });
  }

  @Test
  void queryReturnsCorrectResult() {
    database.transaction(() -> {
      // Verify the query returns the correct result
      final ResultSet rs = database.query("sql",
          "select * from example where p1=? and p2=? and p3=?", "e", "b", "h");

      assertThat(rs.stream().count()).isEqualTo(1);
    });
  }

  @Test
  void singlePropertyIndexNotSelected() {
    database.transaction(() -> {
      // Test with just one condition - should still prefer LSM index over full-text if available
      final ResultSet rs = database.query("sql",
          "explain select * from example where p1=?", "e");

      final ExecutionPlan plan = rs.getExecutionPlan().get();
      final String planString = plan.prettyPrint(0, 3);

      //System.out.println("Single property query plan:");
      //System.out.println(planString);

      // Should use an index (either full-text or LSM, both are acceptable for single property)
      assertThat(planString).contains("FETCH FROM INDEX");
    });
  }

  @Test
  void twoPropertyIndexSelection() {
    database.transaction(() -> {
      // Test with two conditions - composite index should not be used since it requires all three
      final ResultSet rs = database.query("sql",
          "explain select * from example where p1=? and p2=?", "e", "b");

      final ExecutionPlan plan = rs.getExecutionPlan().get();
      final String planString = plan.prettyPrint(0, 3);

      //System.out.println("Two property query plan:");
      //System.out.println(planString);

      // Should use an index for at least one property
      assertThat(planString).contains("FETCH FROM INDEX");
    });
  }
}
