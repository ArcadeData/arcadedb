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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.IdentityHashMap;
import java.util.Locale;

/**
 * The subquery bodies one execution of a read-only statement has planned, so that an {@code EXISTS { }},
 * {@code COUNT { }}, {@code COLLECT { }} or {@code CALL { }} body is planned once for the statement rather than once per
 * outer row (issue #12136). The plan keeps the step chain it built for the last outer row and, when every step of it can
 * be pulled again, runs it again for the next row with the new row as its seed.
 * <p>
 * Only a statement that cannot write gets one: a write made for one outer row has to be seen by the body run for the next
 * one, and a chain that is run again keeps what its steps remember of the rows they read (a scan snapshot, a memo). A
 * nested plan shares the object of the statement it is part of, and gets none when that statement can write.
 * <p>
 * Confined to the thread that runs the statement: a parallel worker evaluating a filter asks {@link #of} and gets null,
 * so it plans its own body exactly as before.
 */
public final class SubqueryPlans {
  /** Space-prefixed like every internal name, so no Cypher or SQL variable can read or shadow it. */
  private static final String CONTEXT_KEY = " cypherSubqueryPlans";

  private final Thread                                        owner = Thread.currentThread();
  private       IdentityHashMap<CypherStatement, CypherExecutionPlan> plans;
  private       long                                          chainsBuilt;
  private       long                                          chainsReused;

  private SubqueryPlans() {
  }

  /**
   * The plans of the statement {@code context} belongs to, or null when its subquery bodies are planned per row: the
   * statement can write, or the caller is not the thread the statement runs on.
   */
  public static SubqueryPlans of(final CommandContext context) {
    if (context == null)
      return null;
    return context.getVariable(CONTEXT_KEY) instanceof SubqueryPlans subqueryPlans
        && subqueryPlans.owner == Thread.currentThread() ? subqueryPlans : null;
  }

  /**
   * Gives {@code context}, the context a plan for {@code statement} runs on, the plans its subquery bodies share: those of
   * the enclosing statement when there is one, new ones for a top-level statement, none when either can write.
   * <p>
   * An enclosing context that carries the read-only flag of a Cypher plan but no plans belongs to a statement that can
   * write, and passes that on to everything nested in it.
   */
  static void install(final BasicCommandContext context, final CommandContext outerContext, final CypherStatement statement) {
    if (!statement.isReadOnly())
      return;

    if (outerContext == null) {
      context.setVariable(CONTEXT_KEY, new SubqueryPlans());
      return;
    }

    final Object outer = outerContext.getVariable(CONTEXT_KEY);
    if (outer instanceof SubqueryPlans)
      context.setVariable(CONTEXT_KEY, outer);
    else if (outerContext.getVariable(MatchNodeStep.READ_ONLY_STATEMENT_KEY) == null)
      context.setVariable(CONTEXT_KEY, new SubqueryPlans());
  }

  /** The plan of {@code body} built earlier in this statement execution, or null. */
  public CypherExecutionPlan get(final CypherStatement body) {
    return plans != null ? plans.get(body) : null;
  }

  public void put(final CypherStatement body, final CypherExecutionPlan plan) {
    if (plans == null)
      plans = new IdentityHashMap<>();
    plans.put(body, plan);
  }

  void chainBuilt() {
    ++chainsBuilt;
  }

  void chainReused() {
    ++chainsReused;
  }

  /**
   * The line a profile of the statement {@code context} ran for reports, or null when no subquery body ran. Read whatever
   * the thread: the profile is asked for once the rows are read, by whoever read them.
   */
  static String describe(final CommandContext context) {
    if (!(context.getVariable(CONTEXT_KEY) instanceof SubqueryPlans subqueryPlans)
        || subqueryPlans.chainsBuilt == 0 && subqueryPlans.chainsReused == 0)
      return null;
    return String.format(Locale.ROOT, "Subquery step chains: %d built, %d reused\n", subqueryPlans.chainsBuilt,
        subqueryPlans.chainsReused);
  }
}
