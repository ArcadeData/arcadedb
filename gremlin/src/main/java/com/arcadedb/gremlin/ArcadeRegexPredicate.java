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
package com.arcadedb.gremlin;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.utility.TimeBoundRegex;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Text;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * The predicate behind {@code TextP.regex()} and {@code TextP.notRegex()} once {@link ArcadeRegexStrategy} has replaced TinkerPop's:
 * the same answer, but the search runs through {@link TimeBoundRegex} within the {@link Budget} of its statement.
 * <p>
 * It extends the TinkerPop predicate, so everything that recognises that one (the name written to explain output, the equality, the
 * {@code ClassCastException} a value that is not a string raises, which {@link GremlinTraversalErrors} reads as an error of the
 * traversal) keeps working.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public final class ArcadeRegexPredicate extends Text.RegexPredicate {
  private final Pattern pattern;
  private final Budget  budget;

  ArcadeRegexPredicate(final String regex, final boolean negate, final Budget budget) {
    super(regex, negate);
    this.pattern = Pattern.compile(regex);
    this.budget = budget;
  }

  @Override
  public boolean test(final String value, final String ignored) {
    // A NULL VALUE IS LEFT TO TINKERPOP'S PREDICATE, WHICH RAISES THE NullPointerException A TRAVERSAL OF NULLS IS ANSWERED WITH
    // (GremlinTraversalErrors READS THE ORIGIN OF THE ERROR FROM ITS STACK)
    if (value == null)
      return super.test(null, ignored);

    // THE PATTERN IS THE ONE THE PREDICATE WAS BUILT WITH: THE SECOND ARGUMENT IS THE VALUE OF THE P, WHICH TINKERPOP'S OWN IGNORES TOO
    return budget.find(pattern, value) != isNegate();
  }

  @Override
  public PBiPredicate<String, String> negate() {
    return new ArcadeRegexPredicate(getPattern(), !isNegate(), budget);
  }

  /**
   * The time a statement may spend in regular expressions, shared by every search of the statement: {@code arcadedb.command.regexTimeout}
   * counts the time inside the expressions only, not the reading of the records around them, so a long scan with an ordinary
   * expression is not cut short, while one that runs past the limit is stopped. The clock is read every few hundred steps of a search,
   * so a very short value never trips it. Not thread-safe: a traversal is consumed by one thread at a time.
   */
  static final class Budget implements Serializable {
    // 0 = NO BOUND
    private final long    timeoutNanos;
    private       long    remainingNanos;
    private       boolean exhausted;

    static Budget ofMillis(final long timeoutMillis) {
      long nanos = 0;
      if (timeoutMillis > 0)
        try {
          nanos = Math.multiplyExact(timeoutMillis, 1_000_000L);
        } catch (final ArithmeticException e) {
          // AN OVERSIZED TIMEOUT SATURATES (IT NEVER EXPIRES) INSTEAD OF WRAPPING AROUND INTO A DEADLINE IN THE PAST
          nanos = Long.MAX_VALUE;
        }
      // A BUDGET OF CENTURIES IS NO BUDGET: IT SATURATES, SO THAT start + remaining CAN NEVER WRAP AROUND
      if (nanos > (Long.MAX_VALUE >> 2))
        nanos = Long.MAX_VALUE;
      return new Budget(nanos);
    }

    private Budget(final long timeoutNanos) {
      this.timeoutNanos = timeoutNanos;
      this.remainingNanos = timeoutNanos;
    }

    boolean find(final Pattern pattern, final String input) {
      if (timeoutNanos <= 0 || timeoutNanos == Long.MAX_VALUE)
        return pattern.matcher(input).find();

      if (exhausted)
        throw new TimeoutException(
            "Regular expression time of the traversal exhausted (arcadedb.command.regexTimeout): the limit is shared by every search of the statement");

      final long start = System.nanoTime();
      try {
        return TimeBoundRegex.findUntil(pattern, input, start + remainingNanos);
      } finally {
        remainingNanos -= System.nanoTime() - start;
        exhausted = remainingNanos <= 0;
      }
    }
  }
}
