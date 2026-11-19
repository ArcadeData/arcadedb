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
package com.arcadedb.index.vector;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The ledger the vector indexes of a database charge their buffered payloads to (issue #12379): the budget is handed
 * out once whatever the number of accounts drawing on it, and an account gives back exactly what it charged.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorDeltaPayloadLedgerTest {
  private static final long PAYLOAD = 128L;

  @Test
  void theBudgetIsHandedOutOnceAcrossAccounts() {
    final VectorDeltaPayloadLedger ledger = new VectorDeltaPayloadLedger();
    final VectorDeltaPayloadLedger.Account first = new VectorDeltaPayloadLedger.Account();
    final VectorDeltaPayloadLedger.Account second = new VectorDeltaPayloadLedger.Account();
    final long budget = 3 * PAYLOAD;

    assertThat(first.tryCharge(ledger, PAYLOAD, budget)).isTrue();
    assertThat(second.tryCharge(ledger, PAYLOAD, budget)).isTrue();
    assertThat(first.tryCharge(ledger, PAYLOAD, budget)).isTrue();
    assertThat(second.tryCharge(ledger, PAYLOAD, budget))
        .as("the room the first account took is not there for the second").isFalse();
    assertThat(first.tryCharge(ledger, PAYLOAD, budget)).isFalse();

    assertThat(ledger.bytes()).isEqualTo(budget);
    assertThat(first.charged()).isEqualTo(2 * PAYLOAD);
    assertThat(second.charged()).isEqualTo(PAYLOAD);
  }

  @Test
  void aPayloadThatDoesNotFitWholeIsRefused() {
    final VectorDeltaPayloadLedger ledger = new VectorDeltaPayloadLedger();
    final VectorDeltaPayloadLedger.Account account = new VectorDeltaPayloadLedger.Account();

    assertThat(account.tryCharge(ledger, PAYLOAD, PAYLOAD - 1)).isFalse();
    assertThat(account.tryCharge(ledger, PAYLOAD, 0L)).isFalse();
    assertThat(ledger.bytes()).isZero();
  }

  @Test
  void anExplicitSizeChargesTheLedgerWithoutBeingRefusedByIt() {
    final VectorDeltaPayloadLedger ledger = new VectorDeltaPayloadLedger();
    final VectorDeltaPayloadLedger.Account account = new VectorDeltaPayloadLedger.Account();
    account.set(ledger, Long.MAX_VALUE / 2);

    assertThat(account.tryCharge(ledger, PAYLOAD, VectorDeltaPayloadLedger.NO_SHARED_BUDGET))
        .as("no arithmetic on the unbounded budget may wrap into a refusal").isTrue();
    assertThat(ledger.bytes()).isEqualTo(Long.MAX_VALUE / 2 + PAYLOAD);
  }

  @Test
  void settingWhatAnAccountHoldsMovesTheLedgerByTheDifference() {
    final VectorDeltaPayloadLedger ledger = new VectorDeltaPayloadLedger();
    final VectorDeltaPayloadLedger.Account first = new VectorDeltaPayloadLedger.Account();
    final VectorDeltaPayloadLedger.Account second = new VectorDeltaPayloadLedger.Account();

    first.set(ledger, 10 * PAYLOAD);
    second.set(ledger, 4 * PAYLOAD);
    assertThat(ledger.bytes()).isEqualTo(14 * PAYLOAD);

    first.set(ledger, 3 * PAYLOAD);
    assertThat(ledger.bytes()).as("the entries that left free their room").isEqualTo(7 * PAYLOAD);

    first.set(ledger, -PAYLOAD);
    assertThat(first.charged()).as("an account never holds less than nothing").isZero();
    assertThat(ledger.bytes()).isEqualTo(4 * PAYLOAD);
  }

  @Test
  void aClosedAccountGivesBackWhatItChargedOnceAndChargesNothingMore() {
    final VectorDeltaPayloadLedger ledger = new VectorDeltaPayloadLedger();
    final VectorDeltaPayloadLedger.Account retired = new VectorDeltaPayloadLedger.Account();
    final VectorDeltaPayloadLedger.Account live = new VectorDeltaPayloadLedger.Account();
    retired.set(ledger, 5 * PAYLOAD);
    live.set(ledger, 2 * PAYLOAD);

    retired.close();
    assertThat(ledger.bytes()).as("only the live account still counts").isEqualTo(2 * PAYLOAD);

    retired.close();
    assertThat(ledger.bytes()).as("a second close gives nothing back twice").isEqualTo(2 * PAYLOAD);

    // The counters of a retired index keep moving while the last queries drain it: none of it reaches the ledger
    retired.set(ledger, 9 * PAYLOAD);
    assertThat(retired.tryCharge(ledger, PAYLOAD, VectorDeltaPayloadLedger.NO_SHARED_BUDGET)).isFalse();
    retired.set(ledger, 0L);
    assertThat(retired.charged()).isZero();
    assertThat(ledger.bytes()).isEqualTo(2 * PAYLOAD);
  }

  @Test
  void anAccountThatNeverChargedClosesWithoutALedger() {
    final VectorDeltaPayloadLedger.Account account = new VectorDeltaPayloadLedger.Account();
    account.close();
    assertThat(account.charged()).isZero();
  }

  /**
   * The property the per-index sizing lacked: accounts filling at the same moment cannot each take the room that is
   * left. Every thread admits as fast as it can against one budget; what they hold together is the budget, exactly.
   */
  @Test
  void accountsFillingAtTheSameMomentNeverExceedTheBudgetTogether() throws Exception {
    final int threads = 8;
    final int attemptsPerThread = 20_000;
    final long budget = 10_000 * PAYLOAD;

    final VectorDeltaPayloadLedger ledger = new VectorDeltaPayloadLedger();
    final VectorDeltaPayloadLedger.Account[] accounts = new VectorDeltaPayloadLedger.Account[threads];
    final AtomicLong admitted = new AtomicLong();
    final CountDownLatch start = new CountDownLatch(1);
    final Thread[] workers = new Thread[threads];
    for (int t = 0; t < threads; t++) {
      final VectorDeltaPayloadLedger.Account account = accounts[t] = new VectorDeltaPayloadLedger.Account();
      workers[t] = new Thread(() -> {
        try {
          start.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        for (int i = 0; i < attemptsPerThread; i++)
          if (account.tryCharge(ledger, PAYLOAD, budget))
            admitted.incrementAndGet();
      });
      workers[t].start();
    }
    start.countDown();
    for (final Thread worker : workers)
      worker.join();

    assertThat(admitted.get() * PAYLOAD)
        .as("%d threads asked for %d payloads each: the budget is filled, and not by one byte more", threads, attemptsPerThread)
        .isEqualTo(budget);
    assertThat(ledger.bytes()).isEqualTo(budget);

    long charged = 0;
    for (final VectorDeltaPayloadLedger.Account account : accounts)
      charged += account.charged();
    assertThat(charged).as("and the accounts remember exactly what the ledger holds").isEqualTo(budget);

    for (final VectorDeltaPayloadLedger.Account account : accounts)
      account.close();
    assertThat(ledger.bytes()).isZero();
  }
}
