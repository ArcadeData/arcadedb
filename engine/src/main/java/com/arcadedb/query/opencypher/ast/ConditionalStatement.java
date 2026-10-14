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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Cypher 25 conditional query (issue #9940):
 * <pre>
 * WHEN $mode = 'all' THEN MATCH (n) RETURN n.name AS name
 * WHEN $mode = 'none' THEN RETURN null AS name
 * ELSE MATCH (n:Person) RETURN n.name AS name
 * </pre>
 * The first branch whose condition is true runs, the {@code ELSE} branch when none is, and nothing at all when none is and
 * there is no {@code ELSE}. The conditions read parameters only - a conditional query is a whole statement, so there is no
 * row for them to read - and the branch is chosen when the statement is executed, by {@code OpenCypherQueryEngine}, which
 * then plans and runs that branch alone. A branch that is not taken is never planned, so its writes never happen.
 * <p>
 * The statement-level answers ({@link #isReadOnly()}, {@link #hasCreate()}, ...) cover every branch, because they are asked
 * before the branch is known: a statement any of whose branches writes is a write statement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ConditionalStatement implements CypherStatement {
  private final List<Expression>      conditions;
  private final List<CypherStatement> branches;
  private final CypherStatement       elseBranch;
  private final List<CypherStatement> allBranches;

  /**
   * @param conditions one condition per {@code WHEN} branch, in written order
   * @param branches   the query each {@code WHEN} branch runs, parallel to {@code conditions}
   * @param elseBranch the {@code ELSE} query, or null when there is none
   */
  public ConditionalStatement(final List<Expression> conditions, final List<CypherStatement> branches,
      final CypherStatement elseBranch) {
    // OpenCypherQueryEngine runs the chosen branch by recursing into execute(): a branch that is itself conditional would
    // be a nested conditional query, which the parser refuses, so it must never get here
    for (final CypherStatement branch : branches)
      requireNotConditional(branch);
    requireNotConditional(elseBranch);
    if (conditions.isEmpty() || conditions.size() != branches.size())
      throw new IllegalArgumentException("A conditional query needs one query per WHEN condition");
    this.conditions = List.copyOf(conditions);
    this.branches = List.copyOf(branches);
    this.elseBranch = elseBranch;
    final List<CypherStatement> all = new ArrayList<>(branches.size() + 1);
    all.addAll(branches);
    if (elseBranch != null)
      all.add(elseBranch);
    this.allBranches = Collections.unmodifiableList(all);
  }

  public List<Expression> getConditions() {
    return conditions;
  }

  public List<CypherStatement> getBranches() {
    return branches;
  }

  /**
   * @return the {@code ELSE} query, or null when there is none
   */
  public CypherStatement getElseBranch() {
    return elseBranch;
  }

  /**
   * @return every query this statement can run, the {@code ELSE} one last
   */
  public List<CypherStatement> getAllBranches() {
    return allBranches;
  }

  @Override
  public boolean isReadOnly() {
    for (final CypherStatement branch : getAllBranches())
      if (!branch.isReadOnly())
        return false;
    return true;
  }

  /**
   * Every branch returns the same columns (the parser's validation enforces it), so the first branch's RETURN names the
   * statement's columns.
   */
  @Override
  public ReturnClause getReturnClause() {
    return branches.getFirst().getReturnClause();
  }

  @Override
  public boolean hasCreate() {
    for (final CypherStatement branch : getAllBranches())
      if (branch.hasCreate())
        return true;
    return false;
  }

  @Override
  public boolean hasMerge() {
    for (final CypherStatement branch : getAllBranches())
      if (branch.hasMerge())
        return true;
    return false;
  }

  @Override
  public boolean hasDelete() {
    for (final CypherStatement branch : getAllBranches())
      if (branch.hasDelete())
        return true;
    return false;
  }

  @Override
  public boolean hasSet() {
    for (final CypherStatement branch : getAllBranches())
      if (branch.hasSet())
        return true;
    return false;
  }

  @Override
  public boolean hasRemove() {
    for (final CypherStatement branch : getAllBranches())
      if (branch.hasRemove())
        return true;
    return false;
  }

  private static void requireNotConditional(final CypherStatement branch) {
    if (branch instanceof ConditionalStatement)
      throw new IllegalArgumentException("A branch of a conditional query cannot be a conditional query");
  }
}
