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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.SelectStatement;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4590: {@code AggregateProjectionCalculationStep} used to destructively
 * call {@code setElement(null)} on every input row to release the backing {@link com.arcadedb.database.Document}
 * for GC. That assumption (the upstream step owns the row exclusively) is false when the same
 * {@code Result} instance is referenced elsewhere - e.g. a materialized LET variable, a shared
 * {@code ResultSet} replayed via {@code reset()}, or a parallel sub-plan. The side effect made
 * later reads of {@code getElement()} silently return null.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AggregateProjectionElementRetentionTest {

  @Test
  void shouldNotClearElementOfSharedInputRows() throws Exception {
    TestHelper.executeInNewDatabase(database -> {
      database.getSchema().createDocumentType("Sale");

      final List<ResultInternal> sharedRows = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 5; i++) {
          final MutableDocument doc = database.newDocument("Sale").set("region", "North").set("amount", i * 10);
          doc.save();
          // The same Result instance is handed both to the aggregation step (below) and kept here,
          // simulating a row shared with another consumer of the pipeline.
          sharedRows.add(new ResultInternal(doc));
        }
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final SelectStatement select = (SelectStatement) new SQLAntlrParser(null).parse(
          "SELECT region, sum(amount) as total FROM Sale GROUP BY region");

      final AggregateProjectionCalculationStep step = new AggregateProjectionCalculationStep(
          select.getProjection(), select.getGroupBy(), -1, context, -1);

      final AbstractExecutionStep previous = new AbstractExecutionStep(context) {
        boolean done = false;

        @Override
        public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
          final InternalResultSet result = new InternalResultSet();
          if (!done) {
            for (final ResultInternal row : sharedRows)
              result.add(row);
            done = true;
          }
          return result;
        }
      };
      step.setPrevious(previous);

      // Drain the aggregation. The bare step exposes the aggregate value as a temporary property.
      long total = 0;
      int groups = 0;
      while (true) {
        final ResultSet batch = step.syncPull(context, 100);
        boolean any = false;
        while (batch.hasNext()) {
          final ResultInternal r = (ResultInternal) batch.next();
          assertThat(r.<Object>getProperty("region")).isEqualTo("North");
          total += ((Number) r.getTemporaryProperty("total")).longValue();
          groups++;
          any = true;
        }
        if (!any)
          break;
      }

      assertThat(groups).isEqualTo(1);
      assertThat(total).isEqualTo(0 + 10 + 20 + 30 + 40);

      // The shared rows must still expose their backing element after the aggregation ran.
      for (final ResultInternal row : sharedRows) {
        assertThat(row.isElement()).as("input row element must survive aggregation").isTrue();
        assertThat(row.getElement()).isPresent();
        assertThat(row.<Object>getProperty("region")).isEqualTo("North");
        assertThat(row.<Number>getProperty("amount")).isNotNull();
      }
    });
  }
}
