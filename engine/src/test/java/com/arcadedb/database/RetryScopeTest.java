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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The state of a retried {@code transaction()} call survives the rollback between its attempts and ends with the call
 * (issue #9322).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RetryScopeTest extends TestHelper {

  private RetryScope scope() {
    return DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).getRetryScope();
  }

  @Test
  void slotsSurviveTheAttemptsAndAreRealignedOnEachRetry() {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger created = new AtomicInteger();
    final List<Integer> seen = new ArrayList<>();

    database.transaction(() -> {
      final RetryScope scope = scope();
      final int first = scope.nextSlot("k", created::incrementAndGet);
      final int second = scope.nextSlot("k", created::incrementAndGet);
      seen.add(first);
      seen.add(second);
      if (attempts.incrementAndGet() < 3)
        throw new ConcurrentModificationException("retry");
    }, false, 3);

    assertThat(attempts.get()).isEqualTo(3);
    assertThat(created.get()).as("each position is created once, by the first attempt").isEqualTo(2);
    assertThat(seen).containsExactly(1, 2, 1, 2, 1, 2);
  }

  @Test
  void nestedJoinedCallsShareTheOwnersScopeAndTheScopeEndsWithTheOwner() {
    database.transaction(() -> {
      final RetryScope outer = scope();
      database.transaction(() -> assertThat(scope()).isSameAs(outer));
      assertThat(scope()).as("an inner call does not end the owner's scope").isSameAs(outer);
    });
    assertThat(scope()).as("no transaction call is running").isNull();

    final AtomicInteger created = new AtomicInteger();
    database.transaction(() -> scope().nextSlot("k", created::incrementAndGet));
    database.transaction(() -> scope().nextSlot("k", created::incrementAndGet));
    assertThat(created.get()).as("a later call starts with a fresh scope").isEqualTo(2);
  }

  @Test
  void theScopeEndsWhenTheOwnerGivesUp() {
    try {
      database.transaction(() -> {
        scope().nextSlot("k", Object::new);
        throw new ConcurrentModificationException("always");
      }, false, 2);
    } catch (final ConcurrentModificationException expected) {
      // GIVES UP AFTER THE LAST ATTEMPT
    }
    assertThat(scope()).isNull();
  }

  @Test
  void aSlotIsNotInheritedByADifferentKey() {
    final AtomicInteger attempts = new AtomicInteger();
    final List<Integer> seen = new ArrayList<>();
    final AtomicInteger created = new AtomicInteger();

    database.transaction(() -> {
      // The second attempt reaches position 0 with another statement: it must not receive the first one's slot
      final String key = attempts.incrementAndGet() == 1 ? "INCR a" : "GETDEL a";
      seen.add(scope().nextSlot(key, created::incrementAndGet));
      if (attempts.get() < 2)
        throw new ConcurrentModificationException("retry");
    }, false, 2);

    assertThat(seen).containsExactly(1, 2);
  }

  @Test
  void anInnerRetryDoesNotRealignTheOwnersPositionAndTheOwnerKeepsItsSlots() {
    final AtomicInteger innerAttempts = new AtomicInteger();
    final AtomicInteger created = new AtomicInteger();
    final List<Integer> seen = new ArrayList<>();

    database.transaction(() -> {
      seen.add(scope().nextSlot("outer", created::incrementAndGet));
      // A nested, non-joining call that retries on its own
      database.transaction(() -> {
        seen.add(scope().nextSlot("inner" + innerAttempts.get(), created::incrementAndGet));
        if (innerAttempts.incrementAndGet() < 2)
          throw new ConcurrentModificationException("inner retry");
      }, false, 2);
    }, false, 1);

    assertThat(seen.getFirst()).isEqualTo(1);
    assertThat(seen).as("the outer slot is untouched and the inner retry only ever gets fresh slots").hasSize(3);
    assertThat(seen.get(1)).isNotEqualTo(seen.get(0));
    assertThat(seen.get(2)).isNotEqualTo(seen.get(1));
  }
}
