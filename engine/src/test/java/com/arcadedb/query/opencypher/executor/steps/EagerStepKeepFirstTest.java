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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The barrier planted before a LIMIT drains every upstream row (so the writes behind it run) but keeps only the first rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EagerStepKeepFirstTest {

  private static int run(final long keepFirst, final int rows, final AtomicInteger pulled) {
    final CommandContext context = new BasicCommandContext();
    final EagerStep step = new EagerStep(context, keepFirst);
    step.setPrevious(new AbstractExecutionStep(context) {
      @Override
      public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
        final List<ResultInternal> list = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
          pulled.incrementAndGet();
          list.add(new ResultInternal().setProperty("i", i));
        }
        return new IteratorResultSet(list.iterator());
      }
    });
    int kept = 0;
    final ResultSet rs = step.syncPull(context, 100);
    while (rs.hasNext()) {
      final Result row = rs.next();
      assertThat((Integer) row.getProperty("i")).isEqualTo(kept);
      kept++;
    }
    return kept;
  }

  @Test
  void keepsOnlyTheFirstRowsButDrainsAll() {
    final AtomicInteger pulled = new AtomicInteger();
    assertThat(run(3, 1_000, pulled)).isEqualTo(3);
    assertThat(pulled.get()).isEqualTo(1_000);
  }

  @Test
  void keepFirstZeroDrainsAndKeepsNothing() {
    final AtomicInteger pulled = new AtomicInteger();
    assertThat(run(0, 50, pulled)).isZero();
    assertThat(pulled.get()).isEqualTo(50);
  }

  @Test
  void negativeKeepsEverything() {
    assertThat(run(-1, 40, new AtomicInteger())).isEqualTo(40);
  }
}
