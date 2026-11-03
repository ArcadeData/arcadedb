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

import java.time.Duration;

import static org.awaitility.Awaitility.await;

/**
 * For the tests that read the {@link QueryHeapBudget} all the queries of the JVM share. A test class that ran before in the same
 * JVM may have left queries it never closed, some on purpose to test the {@code Cleaner}: their reservations stay in the budget
 * until the garbage collector finds them unreachable, and then go back to it asynchronously, at any moment of a later test.
 * <p>
 * A test is exposed to them both ways: a leftover still held when the test shrinks the budget answers a refusal the test does
 * not expect, or fails a query the test expects to fit; a leftover given back while the test measures the counter moves it by
 * more than the test's own query does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class QueryHeapBudgetTestSupport {
  private QueryHeapBudgetTestSupport() {
  }

  /**
   * Waits until no query holds anything of the budget, collecting what the abandoned ones held: from then on the counter moves
   * only with what the calling test runs. A hang detector, not a latency bound.
   * <p>
   * Not a narrowing of the race but its end: only a reservation still held can be given back later, and the wait returns when
   * none is, so nothing an earlier test left can move the counter afterwards. What it cannot cover is a query of another thread
   * started meanwhile, which no test of a fork does.
   */
  public static void awaitNoReservationLeft() {
    await().alias("the queries other tests abandoned give their heap budget back").atMost(Duration.ofSeconds(30)).until(() -> {
      System.gc();
      return QueryHeapBudget.getReservedBytes() == 0;
    });
  }
}
