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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10135: a {@code transaction()} call that JOINED the caller's transaction owns nothing, so a failure of its
 * block must leave the caller's transaction exactly as it found it - for EVERY exit of the loop, not only for the
 * retryable one. Before the fix the general {@code catch (Throwable)} arm rolled the caller's transaction back on any
 * non-retryable exception, so the caller's earlier work was discarded and its next write failed with
 * "Transaction not begun".
 */
class Issue10135JoinedTransactionFailureKeepsCallerTxTest extends TestHelper {
  private static final String TYPE = "Issue10135Doc";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE).createProperty("k", String.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "k");
    });
  }

  static Stream<Arguments> failures() {
    return Stream.of(//
        Arguments.of("retryable", true, (Supplier<Throwable>) () -> new ConcurrentModificationException("simulated conflict")),
        Arguments.of("duplicated key", true, (Supplier<Throwable>) () -> new DuplicatedKeyException("idx", "k", null)),
        Arguments.of("plain RuntimeException", false, (Supplier<Throwable>) () -> new IllegalStateException("boom")),
        Arguments.of("validation", false, (Supplier<Throwable>) () -> new ValidationException("invalid")),
        Arguments.of("Error", false, (Supplier<Throwable>) () -> new AssertionError("fatal")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("failures")
  void aJoinedBlockThatThrowsLeavesTheCallersTransactionIntact(final String label, final boolean retryable,
      final Supplier<Throwable> failure) {
    database.begin();
    try {
      database.newDocument(TYPE).set("k", "outer").save();
      final int nestedBefore = database.getNestedTransactions();

      final Throwable expected = failure.get();
      final AtomicReference<Throwable> reported = new AtomicReference<>();

      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument(TYPE).set("k", "inner").save();
        throwUnchecked(expected);
      }, true, 3, null, reported::set)).isSameAs(expected);

      assertThat(database.isTransactionActive()).as("the caller's transaction must survive a %s", label).isTrue();
      assertThat(database.getNestedTransactions()).isEqualTo(nestedBefore);
      if (retryable)
        assertThat(reported.get()).as("the joined retryable arm hands the failure to the owner without the callback")
            .isNull();
      else
        assertThat(reported.get()).as("the error callback must still be told on the non-retryable arm")
            .isSameAs(expected);

      // THE CALLER CAN KEEP WRITING AND COMMIT ITS OWN WORK
      database.newDocument(TYPE).set("k", "outer2").save();
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(keys()).contains("outer", "outer2");
  }

  @Test
  void theDefaultOverloadJoinsAndLeavesTheCallersTransactionIntact() {
    // THE EXACT REPRO FROM THE ISSUE: db.transaction(block) NESTED IN db.transaction(block)
    database.transaction(() -> {
      database.newDocument(TYPE).set("k", "outer").save();
      try {
        database.transaction(() -> {
          throw new IllegalStateException("boom");
        });
      } catch (final IllegalStateException ignore) {
        // EXPECTED
      }
      assertThat(database.isTransactionActive()).isTrue();
      database.newDocument(TYPE).set("k", "outer2").save();
    });

    assertThat(keys()).containsExactlyInAnyOrder("outer", "outer2");
  }

  @Test
  void aSubTransactionTheBlockOpenedItselfIsRolledBackButTheCallersIsNot() {
    database.begin();
    try {
      database.newDocument(TYPE).set("k", "outer").save();
      final int nestedBefore = database.getNestedTransactions();

      assertThatThrownBy(() -> database.transaction(() -> {
        database.begin(); // A LEVEL THE BLOCK OWNS AND LEAVES BEHIND
        database.newDocument(TYPE).set("k", "leaked").save();
        throw new IllegalStateException("boom");
      }, true)).isInstanceOf(IllegalStateException.class);

      assertThat(database.isTransactionActive()).isTrue();
      assertThat(database.getNestedTransactions()).as("the level the block opened must be rolled back")
          .isEqualTo(nestedBefore);

      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(keys()).containsExactly("outer");
  }

  @Test
  void aSubTransactionCreatedWithoutJoiningIsRolledBackAndTheCallersSurvives() {
    database.begin();
    try {
      database.newDocument(TYPE).set("k", "outer").save();
      final int nestedBefore = database.getNestedTransactions();

      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument(TYPE).set("k", "inner").save();
        throw new IllegalStateException("boom");
      }, false)).isInstanceOf(IllegalStateException.class);

      assertThat(database.isTransactionActive()).isTrue();
      assertThat(database.getNestedTransactions()).isEqualTo(nestedBefore);

      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(keys()).containsExactly("outer");
  }

  @Test
  void anOwnedTransactionIsFullyRolledBackEvenIfTheBlockLeftASubTransactionOpen() {
    assertThat(database.isTransactionActive()).isFalse();

    assertThatThrownBy(() -> database.transaction(() -> {
      database.newDocument(TYPE).set("k", "owned").save();
      database.begin();
      database.newDocument(TYPE).set("k", "leaked").save();
      throw new IllegalStateException("boom");
    })).isInstanceOf(IllegalStateException.class);

    assertThat(database.isTransactionActive()).as("the transaction this call opened must not outlive it").isFalse();
    assertThat(keys()).isEmpty();
  }

  @Test
  void aRetriedAttemptStartsFromTheCallersDepthEvenIfTheFailedOneLeftASubTransactionOpen() {
    final int[] attempts = { 0 };

    database.transaction(() -> {
      database.newDocument(TYPE).set("k", "owned" + attempts[0]).save();
      if (attempts[0]++ == 0) {
        database.begin();
        throw new ConcurrentModificationException("simulated conflict");
      }
      assertThat(database.getNestedTransactions()).as("the retry must not run inside the leaked level").isEqualTo(1);
    }, false, 3);

    assertThat(attempts[0]).isEqualTo(2);
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(keys()).containsExactly("owned1");
  }

  private List<String> keys() {
    return database.query("sql", "select k from " + TYPE).stream().map(r -> r.<String>getProperty("k")).toList();
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void throwUnchecked(final Throwable t) throws T {
    throw (T) t;
  }
}
