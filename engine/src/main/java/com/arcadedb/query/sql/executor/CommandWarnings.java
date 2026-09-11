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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The "an expensive plan ran again" counter behind {@link GlobalConfiguration#COMMAND_WARNINGS_EVERY}: a query the
 * planner cannot make cheap is worth telling an operator about, and worth telling them about once every N times
 * rather than on every execution of a query in a loop.
 * <p>
 * One implementation rather than a copy per warning site. The copies had the interval arithmetic wrong in the same
 * way - {@code counter % every == 1} silences the warning completely at {@code every = 1}, which is the setting's
 * most verbose value and is documented as "every occurrence" (found while fixing issue #7477). The count is per key and per JVM,
 * like the setting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CommandWarnings {
  private static final ConcurrentHashMap<String, AtomicInteger> OCCURRENCES = new ConcurrentHashMap<>();

  /**
   * Entries above which the whole table is dropped and counting restarts.
   * <p>
   * The key includes the database name, so a server that creates and drops databases - a multi-tenant one, a test
   * suite - would otherwise accumulate a counter per (database, situation) pair forever, for databases that no
   * longer exist. Throttle counts are best-effort by definition: losing them costs one extra warning per surviving
   * key, which is the cheapest possible way to be wrong here. The bound is high enough that a fixed set of
   * databases never reaches it.
   */
  private static final int MAX_KEYS = 10_000;

  private CommandWarnings() {
  }

  /**
   * Whether command warnings are switched on at all, for a caller that has to compute something solely to decide
   * what to put in one - it should not pay for that when nothing will read it.
   */
  public static boolean isEnabled() {
    return GlobalConfiguration.COMMAND_WARNINGS_EVERY.getValueAsInteger() > 0;
  }

  /**
   * Records one occurrence of the situation {@code key} names <b>in {@code database}</b> and answers how many times
   * it has now happened when this one is due to be reported, or 0 when it is not - so the caller both decides and
   * has the number to print:
   * <pre>
   * final int occurrences = CommandWarnings.occurrencesWhenDue(database, "Person.scan");
   * if (occurrences &gt; 0)
   *   LogManager.instance().log(this, Level.WARNING, "... %d times ...", occurrences);
   * </pre>
   * The database is part of the key rather than left to the caller to remember: two tenants on one server can both
   * have a {@code Person} type, and sharing a counter between them would throttle one database's warning on the
   * other's traffic while each message names its own database.
   * <p>
   * A configured interval of 0 disables the warning and answers 0 without counting.
   */
  public static int occurrencesWhenDue(final DatabaseInternal database, final String key) {
    final int every = GlobalConfiguration.COMMAND_WARNINGS_EVERY.getValueAsInteger();
    if (every <= 0)
      return 0;

    // Checked before the insert rather than after, so the table cannot be observed above the bound.
    if (OCCURRENCES.size() >= MAX_KEYS)
      OCCURRENCES.clear();

    final int occurrences =
        OCCURRENCES.computeIfAbsent(database.getName() + "/" + key, k -> new AtomicInteger()).incrementAndGet();

    // Zero-based, so the FIRST occurrence is always reported and every Nth after it: at every = 1 that is all of
    // them, which is what "every occurrence" has to mean.
    return (occurrences - 1) % every == 0 ? occurrences : 0;
  }

  /** How many keys the table holds. For the test that pins the bound. */
  static int keyCountForTests() {
    return OCCURRENCES.size();
  }

  /** Forgets every count. For tests, which must not inherit a counter from whatever ran before them. */
  public static void resetForTests() {
    OCCURRENCES.clear();
  }
}
