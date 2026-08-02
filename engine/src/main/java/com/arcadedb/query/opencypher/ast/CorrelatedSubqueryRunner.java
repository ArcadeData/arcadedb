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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.opencypher.executor.CypherExecutionPlan;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.List;
import java.util.Map;

/**
 * Runs the body of one of the three subquery expressions - {@code EXISTS { }}, {@code COUNT { }} and
 * {@code COLLECT { }} - against the outer row, from the body's AST.
 * <p>
 * The outer row is handed to the body as a <b>seed row</b>, the same mechanism a {@code CALL { }} clause uses
 * ({@link CypherExecutionPlan#executeWithSeedRow}): the row enters the step chain as its first input, so every outer
 * variable the body mentions is already bound when the body's first clause runs. Nothing is rewritten, and nothing is
 * re-parsed.
 * <p>
 * That replaces {@link CorrelatedSubqueryRewriter}, which correlated the body by <b>editing its text</b> - splicing an
 * extra {@code MATCH (v)} into it, merging an {@code id(v) = $param} condition into whatever {@code WHERE} it could
 * find, prepending a {@code WITH $param AS v} - and then handing the result to {@code database.query("opencypher",
 * ...)} as a standalone statement. Deciding where a clause begins, where a {@code WHERE} body ends, and whether a
 * keyword is a clause or an identifier, all by scanning text with a keyword table and a bracket-depth counter, is what
 * produced issues #4995/#5165 (an injected {@code AND} binding tighter than an inner {@code OR}), #5464 (an inline
 * pattern predicate mistaken for the clause-level {@code WHERE}), #5461 (a clause keyword missing from the table) and
 * #5541 (a {@code SET} inside user data read as a clause). None of those decisions exists here.
 * <p>
 * The rewriter stays for the one case this cannot serve: an expression whose body has no AST, either because the
 * statement builder declined it (the best-effort build of issue #5626) or because the expression was synthesized at
 * runtime from a pattern rather than parsed. {@link #canRun} is what tells the two apart.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CorrelatedSubqueryRunner {

  private CorrelatedSubqueryRunner() {
    // utility class
  }

  /**
   * Tells whether {@code body} can be run from its AST.
   * <p>
   * A null body is one the parse could not build; an empty clause list is one
   * {@link CypherExecutionPlan#executeWithSeedRow} would answer by echoing the seed row - a single row, which for
   * {@code EXISTS} is the difference between true and false. Both fall back to the text path rather than being
   * answered wrongly.
   */
  public static boolean canRun(final CypherStatement body) {
    if (body == null)
      return false;

    if (body instanceof UnionStatement union) {
      final List<CypherStatement> branches = union.getQueries();
      if (branches == null || branches.isEmpty())
        return false;
      for (int i = 0; i < branches.size(); i++)
        if (!canRun(branches.get(i)))
          return false;
      return true;
    }

    final List<ClauseEntry> clauses = body.getClausesInOrder();
    return clauses != null && !clauses.isEmpty();
  }

  /**
   * Executes {@code body} once, seeded with {@code outerRow}.
   * <p>
   * The caller closes the returned result set. Any failure propagates: a subquery that cannot run is not a subquery
   * that does not match, and the three callers no longer confuse the two.
   *
   * @param body     the body as parsed, which {@link #canRun} has accepted
   * @param outerRow the outer row, or null when the expression is evaluated standalone
   * @param context  the outer command context
   */
  public static ResultSet run(final CypherStatement body, final Result outerRow, final CommandContext context) {
    return planFor(body, context).executeWithSeedRow(seedRow(outerRow), context);
  }

  /**
   * How many rows {@code body} produces against {@code outerRow}, which is all {@code COUNT { }} wants.
   * <p>
   * Asking for the number rather than for the rows is what lets a body whose row count is its match count be
   * answered by a count push-down instead of a materialized scan (issue #5715). A body that is not of that shape
   * produces its rows and they are counted, exactly as {@link #run} would.
   */
  public static long countRows(final CypherStatement body, final Result outerRow, final CommandContext context) {
    return planFor(body, context).countRows(seedRow(outerRow), context);
  }

  private static CypherExecutionPlan planFor(final CypherStatement body, final CommandContext context) {
    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final Map<String, Object> parameters = context.getInputParameters();

    // No physical plan: the optimizer is keyed on a query string this body never had, and the seed row is exactly the
    // input the physical operators do not model. The step chain is built per row, as it is for a CALL { } body.
    return new CypherExecutionPlan(database, body, parameters != null ? parameters : Map.of(),
        database.getConfiguration(), null, OpenCypherQueryEngine.getExpressionEvaluator());
  }

  /**
   * The outer row as the body sees it: every variable the query bound, minus the engine's own bookkeeping.
   * <p>
   * Internal variables are held on the row under space-prefixed names, which no Cypher identifier can be, so a body
   * cannot reference one - but the step chain reads them, and handing a body the ones belonging to the enclosing
   * query makes it plan against bindings that are not its own. The text rewriter skipped them for the same reason.
   */
  private static Result seedRow(final Result outerRow) {
    if (outerRow == null)
      return new ResultInternal();

    // The seed step copies the row again on its way into the chain, so a row with nothing to filter is handed over
    // as it is rather than copied twice.
    boolean hasInternalNames = false;
    for (final String name : outerRow.getPropertyNames())
      if (name.startsWith(" ")) {
        hasInternalNames = true;
        break;
      }
    if (!hasInternalNames)
      return outerRow;

    final ResultInternal seed = new ResultInternal();
    for (final String name : outerRow.getPropertyNames())
      if (!name.startsWith(" "))
        seed.setProperty(name, outerRow.getProperty(name));
    return seed;
  }
}
