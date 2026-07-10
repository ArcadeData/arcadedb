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
 * Regression test for issue #4593: when the UNWIND target is null or a scalar (non-collection)
 * value there is nothing to flatten, so {@code UnwindStep} used to forward the <i>original</i>
 * upstream {@code Result} instance unchanged. Aliasing the input record is a mutation hazard:
 * a later UNWIND field or a downstream projection step (e.g. {@code setElement(null)}) would
 * corrupt the row still referenced upstream. The fix forwards a defensive copy in every branch
 * while preserving the unwound property value (a scalar behaves like a single-element collection).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class UnwindStepAliasingTest {

  @Test
  void shouldNotAliasInputRowWhenUnwindingScalar() throws Exception {
    TestHelper.executeInNewDatabase(database -> {
      database.getSchema().createDocumentType("Doc");

      final List<ResultInternal> sharedRows = new ArrayList<>();
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc").set("id", 1).set("single", "value");
        doc.save();
        // The same Result instance is handed to the UNWIND step (below) and kept here, simulating a
        // row shared with another consumer of the pipeline.
        sharedRows.add(new ResultInternal(doc));
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final SelectStatement select = (SelectStatement) new SQLAntlrParser(null).parse(
          "SELECT FROM Doc UNWIND single");

      final UnwindStep step = new UnwindStep(select.getUnwind(), context);

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

      final List<Result> output = new ArrayList<>();
      final ResultSet rs = step.syncPull(context, 100);
      while (rs.hasNext())
        output.add(rs.next());

      // One row is produced and the scalar value is preserved (not flattened to null).
      assertThat(output).hasSize(1);
      final ResultInternal unwound = (ResultInternal) output.getFirst();
      assertThat(unwound.<String>getProperty("single")).isEqualTo("value");

      // The forwarded row must be a defensive copy, not the upstream instance: mutating it must not
      // bleed back into the row still referenced upstream.
      assertThat(unwound).isNotSameAs(sharedRows.getFirst());
      unwound.setProperty("single", "MUTATED");
      assertThat(sharedRows.getFirst().<String>getProperty("single")).isEqualTo("value");
    });
  }

  @Test
  void shouldNotAliasInputRowWhenUnwindingNull() throws Exception {
    TestHelper.executeInNewDatabase(database -> {
      database.getSchema().createDocumentType("Doc");

      final List<ResultInternal> sharedRows = new ArrayList<>();
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc").set("id", 1); // no 'tags' field -> null
        doc.save();
        sharedRows.add(new ResultInternal(doc));
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final SelectStatement select = (SelectStatement) new SQLAntlrParser(null).parse(
          "SELECT FROM Doc UNWIND tags");

      final UnwindStep step = new UnwindStep(select.getUnwind(), context);

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

      final List<Result> output = new ArrayList<>();
      final ResultSet rs = step.syncPull(context, 100);
      while (rs.hasNext())
        output.add(rs.next());

      assertThat(output).hasSize(1);
      final ResultInternal unwound = (ResultInternal) output.getFirst();

      // Mutating the forwarded copy must not corrupt the shared upstream row.
      assertThat(unwound).isNotSameAs(sharedRows.getFirst());
      unwound.setProperty("id", 999);
      assertThat(sharedRows.getFirst().<Integer>getProperty("id")).isEqualTo(1);
    });
  }

  @Test
  void shouldPreserveScalarValueEndToEnd() throws Exception {
    TestHelper.executeInNewDatabase(database -> {
      database.getSchema().createDocumentType("Doc");

      database.transaction(() -> database.newDocument("Doc").set("id", 1).set("single", "value").save());

      database.transaction(() -> {
        final ResultSet result = database.query("sql", "SELECT id, single FROM Doc UNWIND single");

        final List<Result> rows = new ArrayList<>();
        while (result.hasNext())
          rows.add(result.next());

        // UNWIND of a scalar yields a single row with the value preserved (single-element semantics).
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().<Integer>getProperty("id")).isEqualTo(1);
        assertThat(rows.getFirst().<String>getProperty("single")).isEqualTo("value");
      });
    });
  }
}
