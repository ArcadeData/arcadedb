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
package com.arcadedb.function;

import com.arcadedb.exception.CommandSemanticException;

/**
 * The one wording for "wrong number of arguments", shared by every path that can detect it: the functions' own
 * runtime guard ({@link Function#checkArity}), the procedures' ({@code Procedure.checkArity}) and the Cypher
 * parser's declaration gate ({@code FunctionValidator}). A client is told the same thing whichever caught the
 * mistake - the point of issue #5484, extended to the runtime side in #5602 and to procedures in #5627.
 * <p>
 * It lives beside {@link Function} rather than in the Cypher helper it started in, because nothing about counting
 * arguments is language-specific and {@link Function} is the query-language-neutral abstraction: having the base
 * interface reach into a {@code cypher} package for its own error message was an inversion.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class FunctionArity {

  private FunctionArity() {
    // utility class
  }

  /**
   * Resolves the two spellings of "no limit" to one number.
   * <p>
   * The Cypher parser's registry writes an unbounded maximum as {@code -1}, while {@link Function#getMaxArgs} defaults
   * to {@link Integer#MAX_VALUE}. Every caller that compares against a maximum has to go through here: a raw
   * {@code count > maxArgs} against a {@code -1} rejects <em>every</em> call, since any count exceeds {@code -1}, while
   * {@link #describe} would still be phrasing it as "at least N". Accepting both spellings in one place is what keeps
   * the check and the message from disagreeing.
   */
  public static int effectiveMax(final int maxArgs) {
    return maxArgs == -1 ? Integer.MAX_VALUE : maxArgs;
  }

  /**
   * Phrases an accepted argument count, e.g. {@code "1 argument"}, {@code "2-3 arguments"} or
   * {@code "at least 1 argument"}.
   *
   * @param minArgs fewest arguments accepted
   * @param maxArgs most arguments accepted; {@code -1} and {@link Integer#MAX_VALUE} both mean "no limit"
   */
  public static String describe(final int minArgs, final int maxArgs) {
    final int max = effectiveMax(maxArgs);
    if (minArgs == max)
      return minArgs + " argument" + (minArgs == 1 ? "" : "s");
    if (max == Integer.MAX_VALUE)
      return "at least " + minArgs + " argument" + (minArgs == 1 ? "" : "s");
    return minArgs + "-" + max + " arguments";
  }

  /**
   * The function-flavoured shorthand for {@link #message(String, String, String, int)}.
   *
   * @param functionName function name without parentheses, e.g. {@code "abs"}
   * @param expectedArgs the accepted count, phrased for the message, e.g. {@code "1 argument"} - usually from
   *                     {@link #describe}, but spelled out by the few functions whose accepted counts are not a
   *                     contiguous range
   * @param actualArgs   how many arguments the call actually carried
   */
  public static String message(final String functionName, final String expectedArgs, final int actualArgs) {
    return message("Function", functionName, expectedArgs, actualArgs);
  }

  /**
   * @param kind         what the name denotes, capitalised because it opens the sentence: {@code "Function"} or
   *                     {@code "Procedure"}. Procedures are a separate abstraction - their own interface, their own
   *                     registry, their own {@code CALL} handling - so telling a caller that {@code algo.dijkstra}
   *                     is a function would be wrong. Everything after the noun is shared, which is what makes the
   *                     same mistake read the same way whichever kind the name resolved to (#5627).
   * @param callableName the name without parentheses, e.g. {@code "abs"} or {@code "algo.dijkstra"}
   * @param expectedArgs the accepted count, phrased for the message, e.g. {@code "1 argument"}
   * @param actualArgs   how many arguments the call actually carried
   */
  public static String message(final String kind, final String callableName, final String expectedArgs,
      final int actualArgs) {
    return kind + " '" + callableName + "' expects " + expectedArgs + " but got " + actualArgs;
  }

  /**
   * Rejects a call whose argument count falls outside {@code minArgs}..{@code maxArgs}.
   * <p>
   * The body behind both runtime guards - {@link Function#checkArity} and {@code Procedure.checkArity}. They were
   * byte-identical but for the noun, which is the shape the two <em>messages</em> had before #5627 and is how they
   * came to disagree; keeping the check itself in one place is what stops that recurring.
   *
   * @param kind         see {@link #message(String, String, String, int)}
   * @param callableName the name without parentheses, used in the message
   * @param minArgs      fewest arguments accepted
   * @param maxArgs      most arguments accepted; {@code -1} and {@link Integer#MAX_VALUE} both mean "no limit"
   * @param args         the arguments the call carried, {@code null} counting as none - a couple of executors used
   *                     to defend against a null array by hand, and folding that in here keeps the count check in
   *                     one place. Note that this only rejects the null array for a callable that requires at least
   *                     one argument: one declaring {@code minArgs == 0} is handed it unchanged and must still
   *                     tolerate it.
   */
  public static void check(final String kind, final String callableName, final int minArgs, final int maxArgs,
      final Object[] args) {
    final int actualArgs = args == null ? 0 : args.length;
    // effectiveMax(), not maxArgs, so an implementation that spells "unbounded" the registry's way (-1) is not read
    // as "at most -1 arguments" - which would reject every call while the message still said "at least N".
    if (actualArgs < minArgs || actualArgs > effectiveMax(maxArgs))
      throw mismatch(kind, callableName, describe(minArgs, maxArgs), actualArgs);
  }

  /**
   * The exception form of {@link #message(String, String, int)}: a wrong argument count is the caller's mistake, so
   * it is a client error (HTTP 400) rather than an internal failure.
   */
  public static CommandSemanticException mismatch(final String functionName, final String expectedArgs,
      final int actualArgs) {
    return new CommandSemanticException(message(functionName, expectedArgs, actualArgs));
  }

  /**
   * The exception form of {@link #message(String, String, String, int)}.
   */
  public static CommandSemanticException mismatch(final String kind, final String callableName,
      final String expectedArgs, final int actualArgs) {
    return new CommandSemanticException(message(kind, callableName, expectedArgs, actualArgs));
  }
}
