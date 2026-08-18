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
package com.arcadedb.query.opencypher;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

/**
 * Where the LOAD CSV row context - the file {@code file()} answers and the line {@code linenumber()} answers - is
 * written, carried and read.
 * <p>
 * Both are functions of the <b>row</b>, not of the caller: two rows of the same query have two different line
 * numbers, and a query reading several files has a different file per row. So the values travel on the row that
 * {@code LOAD CSV} emitted them with, and the {@link CommandContext} variables the two functions read are set from
 * that row immediately before the function runs.
 * <p>
 * Written here rather than at each consumer because the propagation existed on exactly one of the two expression
 * evaluation paths: a projection went through {@code ExpressionEvaluator}, which lifted the row values into the
 * context, and a {@code WHERE} predicate went through the AST node, which did not. {@code file()} was therefore
 * {@code null} inside a predicate - {@code WHERE file() IS NOT NULL} silently dropped every row of the file - while
 * the same call in the {@code RETURN} of the same query answered correctly, and two predicate forms disagreed with
 * each other (issue #6402). One writer, every consumer served: the same arrangement issue #6354 arrived at for
 * arithmetic.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LoadCSVRowContext {
  /**
   * Row property, and context variable, carrying the URL of the file the row was read from.
   * <p>
   * Named with the {@link InternalVariables#PREFIX} marker because it is execution state and not a variable the
   * query wrote: without it {@code LOAD CSV FROM ... AS row RETURN *} answered with two columns nobody asked for,
   * which is exactly the leak issue #5444 closed for the executor's other generated bindings.
   */
  public static final String FILE = InternalVariables.PREFIX + "loadCSV_file";

  /** Row property, and context variable, carrying the 1-based line the row was read from. */
  public static final String LINE_NUMBER = InternalVariables.PREFIX + "loadCSV_linenumber";

  private LoadCSVRowContext() {
    // utility class
  }

  /**
   * Stamps a freshly emitted LOAD CSV row with the file and line it came from.
   * <p>
   * {@code lineNumber} is {@code int}, matching {@code LoadCSVStep}'s own counter, so {@code linenumber()} keeps
   * answering an {@code Integer} rather than silently widening to a {@code Long} that a caller comparing against
   * a literal would no longer equal.
   */
  public static void stamp(final ResultInternal row, final String file, final int lineNumber) {
    row.setProperty(FILE, file);
    row.setProperty(LINE_NUMBER, lineNumber);
  }

  /**
   * Carries the row context of an input row onto a row projected from it, so that a clause after a {@code WITH}
   * still knows which line it is looking at.
   * <p>
   * A projection keeps only what it names, and {@code WITH row} names one variable, so without this the context
   * ends at the first {@code WITH} - and, because the functions read a context variable that no later row resets,
   * {@code WITH row RETURN linenumber()} answered every row with the line number of whichever row happened to be
   * evaluated last. Neo4j documents both functions as usable anywhere in the query following {@code LOAD CSV}.
   * <p>
   * Deliberately unconditional: this carries the context forward whether or not the {@code WITH} that produced
   * {@code target} actually projects {@code row} or anything derived from it - {@code WITH 1 AS x} still keeps
   * {@code file()}/{@code linenumber()} answering for the rest of the query. That reading follows from "usable
   * anywhere in the query following LOAD CSV": the two functions are a property of the query's position relative
   * to the LOAD CSV clause, the same way {@code count(*)} needs no variable in scope, not a property of whether
   * the row variable itself survived the projection. Pinned by
   * {@code CypherLoadCSVRowContextIssue6402Test.theRowContextSurvivesAProjectionThatDropsRowEntirely}
   * (issue #6402 code review).
   */
  public static void carryOver(final Result source, final ResultInternal target) {
    if (source == null || target == null || !source.hasProperty(FILE))
      return;
    if (!target.hasProperty(FILE)) {
      target.setProperty(FILE, source.getProperty(FILE));
      target.setProperty(LINE_NUMBER, source.getProperty(LINE_NUMBER));
    }
  }

  /**
   * Publishes the row's context on the command context, so {@code file()} and {@code linenumber()} - which, like
   * every {@code StatelessFunction}, are handed arguments and a context but not the row - read the values of the
   * row being evaluated. Called from the single place a Cypher function is invoked with a row in hand.
   * <p>
   * A row carrying no LOAD CSV context leaves the variables alone rather than clearing them: the row a nested
   * evaluation is handed is not always the driving row, and clearing on every function call would blank the values
   * an enclosing LOAD CSV row legitimately set.
   */
  public static void bind(final Result row, final CommandContext context) {
    if (row == null || context == null || !row.hasProperty(FILE))
      return;
    context.setVariable(FILE, row.getProperty(FILE));
    context.setVariable(LINE_NUMBER, row.getProperty(LINE_NUMBER));
  }
}
