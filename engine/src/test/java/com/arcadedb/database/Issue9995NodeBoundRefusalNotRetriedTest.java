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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9995: the local retry loops ({@code Database.transaction}, {@code COMMIT RETRY}) retried every
 * {@link NeedRetryException}, including a refusal this node repeats until something outside it changes - a follower refusing
 * leader-only work and naming the leader - so the caller learnt where to go only after every attempt and every retry delay.
 * Such a refusal answers {@link NeedRetryException#isRetryableOnThisNode()} with {@code false} and is rethrown at once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9995NodeBoundRefusalNotRetriedTest extends TestHelper {
  private static final String FUNCTION = "issue9995_refuse";

  /** A refusal that holds on this node, as a follower's refusal of leader-only work naming the leader does. */
  private static class RefusalHoldingHere extends NeedRetryException {
    RefusalHoldingHere() {
      super("leader-only work refused on this node");
    }

    @Override
    public boolean isRetryableOnThisNode() {
      return false;
    }
  }

  @Test
  void transactionRethrowsANodeBoundRefusalWithoutRetrying() {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger errorCallbacks = new AtomicInteger();
    final RefusalHoldingHere refusal = new RefusalHoldingHere();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      database.newDocument("Doc9995").set("a", 1).save();
      throw refusal;
    }, false, 5, null, e -> errorCallbacks.incrementAndGet())).isSameAs(refusal);

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(errorCallbacks.get()).isEqualTo(1);
    assertThat(database.isTransactionActive()).as("the attempt is rolled back").isFalse();
    assertThat(database.countType("Doc9995", false)).isZero();
  }

  @Test
  void transactionStillRetriesAnOrdinaryRetryableFailure() {
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      throw new NeedRetryException("conflict");
    }, false, 3)).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).isEqualTo(3);
  }

  @Test
  void commitRetryRethrowsANodeBoundRefusalWithoutRetrying() {
    final AtomicInteger calls = new AtomicInteger();
    final AtomicReference<NeedRetryException> toThrow = new AtomicReference<>();
    final SQLQueryEngine sql = (SQLQueryEngine) database.getQueryEngine("sql");
    sql.getFunctionFactory().register(new SQLFunctionAbstract(FUNCTION) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        calls.incrementAndGet();
        throw toThrow.get();
      }

      @Override
      public String getSyntax() {
        return FUNCTION + "()";
      }
    });
    try {
      // THE CONTROL FIRST: AN ORDINARY CONFLICT RAISED BY THE FUNCTION REACHES THE RETRY STEP AND IS RETRIED, SO THE ASSERTION
      // BELOW CANNOT PASS BECAUSE THE EXCEPTION NEVER GOT THERE
      toThrow.set(new NeedRetryException("conflict"));
      assertThatThrownBy(() -> database.command("sqlscript", "BEGIN; SELECT " + FUNCTION + "(); COMMIT RETRY 4;"))
          .isInstanceOf(NeedRetryException.class);
      assertThat(calls.get()).as("an ordinary conflict uses every attempt").isEqualTo(4);

      calls.set(0);
      toThrow.set(new RefusalHoldingHere());
      assertThatThrownBy(() -> database.command("sqlscript", "BEGIN; SELECT " + FUNCTION + "(); COMMIT RETRY 4;"))
          .isInstanceOf(RefusalHoldingHere.class);
      assertThat(calls.get()).as("a refusal that holds on this node is not retried").isEqualTo(1);
    } finally {
      sql.getFunctionFactory().unregister(FUNCTION);
      if (database.isTransactionActive())
        database.rollback();
    }
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc9995");
  }
}
