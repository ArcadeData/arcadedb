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
   * Records one occurrence of the situation {@code key} names and answers how many times it has now happened when
   * this one is due to be reported, or 0 when it is not - so the caller both decides and has the number to print:
   * <pre>
   * final int occurrences = CommandWarnings.occurrencesWhenDue("Person.scan");
   * if (occurrences &gt; 0)
   *   LogManager.instance().log(this, Level.WARNING, "... %d times ...", occurrences);
   * </pre>
   * A configured interval of 0 disables the warning and answers 0 without counting.
   */
  public static int occurrencesWhenDue(final String key) {
    final int every = GlobalConfiguration.COMMAND_WARNINGS_EVERY.getValueAsInteger();
    if (every <= 0)
      return 0;

    final int occurrences = OCCURRENCES.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();

    // Zero-based, so the FIRST occurrence is always reported and every Nth after it: at every = 1 that is all of
    // them, which is what "every occurrence" has to mean.
    return (occurrences - 1) % every == 0 ? occurrences : 0;
  }

  /** Forgets every count. For tests, which must not inherit a counter from whatever ran before them. */
  public static void resetForTests() {
    OCCURRENCES.clear();
  }
}
