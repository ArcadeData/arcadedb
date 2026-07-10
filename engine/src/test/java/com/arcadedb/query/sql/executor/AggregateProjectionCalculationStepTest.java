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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.SelectStatement;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AggregateProjectionCalculationStepTest {

  /**
   * The ResultSet returned by syncPull must contain at most nRecords items per
   * pull (issue #4539): hasNext()/next() compared localNext with &lt;= / &gt;,
   * returning nRecords + 1 items per batch.
   */
  @Test
  void shouldRespectBatchSizeInSyncPull() throws Exception {
    TestHelper.executeInNewDatabase(database -> {
      final int groups = 10;
      final int batchSize = 3;

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final SelectStatement select = (SelectStatement) new SQLAntlrParser(null).parse(
          "SELECT name, count(*) as cnt FROM X GROUP BY name");

      final AggregateProjectionCalculationStep step = new AggregateProjectionCalculationStep(
          select.getProjection(), select.getGroupBy(), -1, context, -1);

      final AbstractExecutionStep previous = new AbstractExecutionStep(context) {
        boolean done = false;

        @Override
        public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
          final InternalResultSet result = new InternalResultSet();
          if (!done) {
            for (int i = 0; i < groups; i++) {
              final ResultInternal item = new ResultInternal(database);
              item.setProperty("name", "group-" + i);
              result.add(item);
            }
            done = true;
          }
          return result;
        }
      };
      step.setPrevious(previous);

      int total = 0;
      while (true) {
        final ResultSet batch = step.syncPull(context, batchSize);
        int batchCount = 0;
        while (batch.hasNext()) {
          assertThat(batch.next().<String>getProperty("name")).startsWith("group-");
          batchCount++;
        }
        if (batchCount == 0)
          break;
        assertThat(batchCount).isLessThanOrEqualTo(batchSize);
        total += batchCount;
      }

      // every group is still returned across pulls, just not more than
      // batchSize at a time
      assertThat(total).isEqualTo(groups);
    });
  }
}
