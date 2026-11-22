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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The cap {@link ProfiledLimitResultSet} puts on the rows of a top-level openCypher statement of a user under a {@code resultSetLimit}
 * (issue #12888), and the per-thread counter that tells the engine a statement it runs now is a part of a capped one. The counter must
 * be back to zero whatever way the statement or the reading of a row ended, or every later statement of that pooled thread would
 * run uncapped.
 */
class ProfiledLimitResultSetTest {

  private static InternalResultSet rows(final int count) {
    final InternalResultSet resultSet = new InternalResultSet();
    for (int i = 0; i < count; i++) {
      final ResultInternal row = new ResultInternal();
      row.setProperty("k", i);
      resultSet.add(row);
    }
    return resultSet;
  }

  private static int drain(final ResultSet resultSet) {
    int count = 0;
    while (resultSet.hasNext()) {
      resultSet.next();
      ++count;
    }
    return count;
  }

  @Test
  void capsTheRowsAndStopsAnsweringPastThem() {
    final ResultSet capped = ProfiledLimitResultSet.run(3, () -> rows(10));

    assertThat(drain(capped)).isEqualTo(3);
    assertThat(capped.hasNext()).isFalse();
    assertThatThrownBy(capped::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void aStatementThatAnswersLessThanTheCapIsNotPadded() {
    assertThat(drain(ProfiledLimitResultSet.run(5, () -> rows(2)))).isEqualTo(2);
    assertThat(drain(ProfiledLimitResultSet.run(5, () -> rows(0)))).isZero();
  }

  /** A result set read again from its start gets its whole allowance again, as the statement's LIMIT would give. */
  @Test
  void resetStartsTheCountAgain() {
    final ResultSet capped = ProfiledLimitResultSet.run(3, () -> rows(10));
    assertThat(drain(capped)).isEqualTo(3);

    capped.reset();

    assertThat(drain(capped)).as("rows after reset()").isEqualTo(3);
    capped.reset();
    capped.next();
    capped.reset();
    assertThat(drain(capped)).as("a partial read is forgotten as well").isEqualTo(3);
  }

  /** The rows are read after run() returned: the statement is a capped one only while it executes or one of its rows is pulled. */
  @Test
  void theThreadIsRunningACappedStatementOnlyWhileItExecutesOrAPullIsInFlight() {
    final boolean[] duringExecution = new boolean[1];
    final boolean[] duringPull = new boolean[1];
    final InternalResultSet delegate = new InternalResultSet() {
      @Override
      public boolean hasNext() {
        duringPull[0] = ProfiledLimitResultSet.isRunning();
        return super.hasNext();
      }
    };
    delegate.add(new ResultInternal());

    assertThat(ProfiledLimitResultSet.isRunning()).isFalse();
    final ResultSet capped = ProfiledLimitResultSet.run(3, () -> {
      duringExecution[0] = ProfiledLimitResultSet.isRunning();
      return delegate;
    });
    assertThat(duringExecution[0]).as("while the statement executes").isTrue();
    assertThat(ProfiledLimitResultSet.isRunning()).as("after execute() returned, the rows not yet read").isFalse();

    assertThat(drain(capped)).isEqualTo(1);
    assertThat(duringPull[0]).as("while a row is pulled").isTrue();
    assertThat(ProfiledLimitResultSet.isRunning()).as("once the rows are read").isFalse();
  }

  /** A capped statement run by a pull of another one holds the counter above one, and does not clear it for the outer one. */
  @Test
  void aNestedCappedStatementDoesNotClearTheCounterForTheOuterOne() {
    final boolean[] afterInner = new boolean[1];
    ProfiledLimitResultSet.run(3, () -> {
      ProfiledLimitResultSet.run(3, () -> rows(1));
      afterInner[0] = ProfiledLimitResultSet.isRunning();
      return rows(1);
    });

    assertThat(afterInner[0]).as("the outer statement is still executing").isTrue();
    assertThat(ProfiledLimitResultSet.isRunning()).isFalse();
  }

  @Test
  void theCounterIsBackToZeroWhenTheExecutionFails() {
    assertThatThrownBy(() -> ProfiledLimitResultSet.run(3, () -> {
      throw new IllegalStateException("execution failed");
    })).isInstanceOf(IllegalStateException.class);

    assertThat(ProfiledLimitResultSet.isRunning()).isFalse();
    // and the next statement of the thread is capped again
    assertThat(drain(ProfiledLimitResultSet.run(2, () -> rows(10)))).isEqualTo(2);
  }

  @Test
  void theCounterIsBackToZeroWhenReadingARowFails() {
    final ResultSet capped = ProfiledLimitResultSet.run(3, () -> new InternalResultSet() {
      @Override
      public boolean hasNext() {
        throw new IllegalStateException("hasNext failed");
      }

      @Override
      public Result next() {
        throw new IllegalStateException("next failed");
      }
    });

    assertThatThrownBy(capped::hasNext).hasMessage("hasNext failed");
    assertThat(ProfiledLimitResultSet.isRunning()).as("after a failed hasNext()").isFalse();
    assertThatThrownBy(capped::next).hasMessage("next failed");
    assertThat(ProfiledLimitResultSet.isRunning()).as("after a failed next()").isFalse();
  }
}
