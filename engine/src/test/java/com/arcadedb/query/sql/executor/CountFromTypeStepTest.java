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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class CountFromTypeStepTest {

  private static final String ALIAS = "size";

  @Test
  void shouldCountRecordsOfClass() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String className = TestHelper.createRandomType(db).getName();
      for (int i = 0; i < 20; i++) {
        final MutableDocument document = db.newDocument(className);
        document.save();
      }

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      final CountFromTypeStep step = new CountFromTypeStep(className, ALIAS, context);

      final ResultSet result = step.syncPull(context, 20);
      assertThat((long) result.next().getProperty(ALIAS)).isEqualTo(20);
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void countStarWithLimitShouldUseOptimizedPlan() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String className = TestHelper.createRandomType(db).getName();
      for (int i = 0; i < 100; i++)
        db.newDocument(className).save();

      // This simulates what Studio does: SELECT count(*) FROM Type limit 2000
      final ResultSet rs = db.query("sql", "SELECT count(*) FROM " + className + " limit 2000");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((long) row.getProperty("count(*)")).isEqualTo(100);

      // Verify the execution plan uses CountFromTypeStep (CALCULATE USERTYPE SIZE), not FETCH FROM TYPE
      final String plan = rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
      assertThat(plan).contains("CALCULATE USERTYPE SIZE");
      assertThat(plan).doesNotContain("FETCH FROM TYPE");
    });
  }
}
