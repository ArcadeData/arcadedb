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

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StreamingAggregationStepTest {

  private StreamingAggregationStep buildStep(final BasicCommandContext context) throws Exception {
    final SelectStatement select = (SelectStatement) new SQLAntlrParser(null).parse(
        "SELECT name, count(*) as cnt FROM X GROUP BY name");
    return new StreamingAggregationStep(select.getProjection(), select.getGroupBy(), context);
  }

  /**
   * Issue #4589: hasNext() must probe upstream. On an empty previous step,
   * hasNext() previously returned true and next() then threw NoSuchElementException.
   */
  @Test
  void emptyInputReturnsEmptyResultSet() throws Exception {
    TestHelper.executeInNewDatabase(database -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final StreamingAggregationStep step = buildStep(context);
      step.setPrevious(new AbstractExecutionStep(context) {
        @Override
        public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
          return new InternalResultSet(); // empty upstream
        }
      });

      final ResultSet rs = step.syncPull(context, 100);

      // hasNext() must not lie: with no upstream rows there is nothing to return
      assertThat(rs.hasNext()).isFalse();

      int count = 0;
      while (rs.hasNext())
        count += rs.next() != null ? 1 : 0;
      assertThat(count).isEqualTo(0);
    });
  }

  /**
   * Sanity check that aggregation over sorted input still works after the fix:
   * groups are emitted as the GROUP BY key changes.
   */
  @Test
  void aggregatesSortedGroups() throws Exception {
    TestHelper.executeInNewDatabase(database -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final StreamingAggregationStep step = buildStep(context);
      step.setPrevious(new AbstractExecutionStep(context) {
        boolean done = false;

        @Override
        public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
          final InternalResultSet result = new InternalResultSet();
          if (!done) {
            // sorted input: a,a,b,c,c,c
            addRow(result, "a");
            addRow(result, "a");
            addRow(result, "b");
            addRow(result, "c");
            addRow(result, "c");
            addRow(result, "c");
            done = true;
          }
          return result;
        }

        private void addRow(final InternalResultSet rs, final String name) {
          final ResultInternal item = new ResultInternal(database);
          item.setProperty("name", name);
          rs.add(item);
        }
      });

      int rows = 0;
      long total = 0;
      while (true) {
        final ResultSet batch = step.syncPull(context, 100);
        if (!batch.hasNext())
          break;
        while (batch.hasNext()) {
          final ResultInternal r = (ResultInternal) batch.next();
          // aggregate final values are exposed as temporary properties by this step
          total += ((Number) r.getTemporaryProperty("cnt")).longValue();
          rows++;
        }
      }

      // 3 groups (a,b,c) totalling 6 rows
      assertThat(rows).isEqualTo(3);
      assertThat(total).isEqualTo(6);
    });
  }
}
