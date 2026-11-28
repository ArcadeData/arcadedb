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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The error callback of {@code Database.transaction(block, join, attempts, ok, error)} is told about each failed attempt once,
 * and about the failure the call ends with exactly once: the exhausted-retries tail used to call it a second time with the
 * exception of the last attempt, which the attempt itself had just reported.
 */
class TransactionErrorCallbackTest extends TestHelper {

  @Test
  void aSingleFailingAttemptReportsItsFailureOnce() {
    final List<Throwable> reported = new ArrayList<>();
    final ConcurrentModificationException failure = new ConcurrentModificationException("conflict");

    assertThatThrownBy(() -> database.transaction(() -> {
      throw failure;
    }, false, 1, null, reported::add)).isSameAs(failure);

    assertThat(reported).containsExactly(failure);
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void exhaustedRetriesReportEachAttemptOnceAndTheLastFailureIsNotRepeated() {
    final List<Throwable> reported = new ArrayList<>();
    final List<Throwable> thrown = new ArrayList<>();
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      final ConcurrentModificationException failure = new ConcurrentModificationException("conflict " + attempts.incrementAndGet());
      thrown.add(failure);
      throw failure;
    }, false, 3, null, reported::add)).isSameAs(thrown.get(2));

    assertThat(attempts.get()).isEqualTo(3);
    assertThat(reported).as("one report per failed attempt, in order").containsExactlyElementsOf(thrown);
  }

  @Test
  void aDuplicatedKeyThatIsNotRetriedFurtherReportsItsFailuresOnceEach() {
    final List<Throwable> reported = new ArrayList<>();
    final List<Throwable> thrown = new ArrayList<>();

    assertThatThrownBy(() -> database.transaction(() -> {
      final DuplicatedKeyException failure = new DuplicatedKeyException("idx", "k", null);
      thrown.add(failure);
      throw failure;
    }, false, 5, null, reported::add)).isInstanceOf(DuplicatedKeyException.class);

    assertThat(thrown).as("a duplicated key gets one retry, whatever the attempts").hasSize(2);
    assertThat(reported).containsExactlyElementsOf(thrown);
  }

  @Test
  void aTransactionThatSucceedsOnTheLastAttemptCallsOkOnceAndNothingAfterTheFailures() {
    final List<Throwable> reported = new ArrayList<>();
    final AtomicInteger ok = new AtomicInteger();
    final AtomicInteger attempts = new AtomicInteger();

    final boolean created = database.transaction(() -> {
      if (attempts.incrementAndGet() < 3)
        throw new ConcurrentModificationException("conflict");
    }, false, 3, ok::incrementAndGet, reported::add);

    assertThat(created).isTrue();
    assertThat(ok.get()).isEqualTo(1);
    assertThat(reported).hasSize(2);
  }
}
