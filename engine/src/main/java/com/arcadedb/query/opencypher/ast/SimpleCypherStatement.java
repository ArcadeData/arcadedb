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
import java.util.List;

/**
 * Simple implementation of CypherStatement for Phase 1.
 * This is a basic implementation to get the module compiling.
 * Will be replaced with full ANTLR-based implementation in later phases.
 */
public class SimpleCypherStatement implements CypherStatement {
  private final String             originalQuery;
  private final List<MatchClause>  matchClauses;
  private final WhereClause        whereClause;
  private final ReturnClause       returnClause;
  private final OrderByClause      orderByClause;
  private final Expression          skip;
  private final Expression          limit;
  private final CreateClause       createClause;
  private final SetClause          setClause;
  private final DeleteClause       deleteClause;
  private final MergeClause        mergeClause;
  private final List<UnwindClause> unwindClauses;
  private final List<WithClause>   withClauses;
  private final List<CallClause>   callClauses;
  private final List<RemoveClause> removeClauses;
  private final List<ClauseEntry>  clausesInOrder;
  private final boolean            hasCreate;
  private final boolean            hasMerge;
  private final boolean            hasDelete;
  private final boolean            hasRemove;
  private final boolean            readOnly;
  private final boolean            hasVariableLengthPath;
  private final boolean            hasUnwindBeforeMatch;
  private final boolean            hasWithBeforeMatch;
  private final boolean            hasSubquery;

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final boolean hasCreate, final boolean hasMerge,
                               final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, null, null, null, null, null, null, null, null, null
        , null,
        hasCreate, hasMerge, hasDelete);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final OrderByClause orderByClause,
                               final Expression skip, final Expression limit, final boolean hasCreate,
                               final boolean hasMerge, final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, orderByClause, skip, limit, null, null, null, null,
        null, null, null,
        hasCreate, hasMerge, hasDelete);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final OrderByClause orderByClause,
                               final Expression skip, final Expression limit, final CreateClause createClause,
                               final SetClause setClause,
                               final DeleteClause deleteClause, final MergeClause mergeClause,
                               final List<UnwindClause> unwindClauses,
                               final List<WithClause> withClauses,
                               final boolean hasCreate, final boolean hasMerge, final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, orderByClause, skip, limit, createClause, setClause,
        deleteClause, mergeClause, unwindClauses, withClauses, null, hasCreate, hasMerge, hasDelete);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final OrderByClause orderByClause,
                               final Expression skip, final Expression limit, final CreateClause createClause,
                               final SetClause setClause,
                               final DeleteClause deleteClause, final MergeClause mergeClause,
                               final List<UnwindClause> unwindClauses,
                               final List<WithClause> withClauses, final List<ClauseEntry> clausesInOrder,
                               final boolean hasCreate, final boolean hasMerge, final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, orderByClause, skip, limit, createClause, setClause,
        deleteClause, mergeClause, unwindClauses, withClauses, null, clausesInOrder, hasCreate, hasMerge, hasDelete);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final OrderByClause orderByClause,
                               final Expression skip, final Expression limit, final CreateClause createClause,
                               final SetClause setClause,
                               final DeleteClause deleteClause, final MergeClause mergeClause,
                               final List<UnwindClause> unwindClauses,
                               final List<WithClause> withClauses, final List<CallClause> callClauses,
                               final List<ClauseEntry> clausesInOrder,
                               final boolean hasCreate, final boolean hasMerge, final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, orderByClause, skip, limit, createClause, setClause,
        deleteClause, mergeClause, unwindClauses, withClauses, callClauses, null, clausesInOrder,
        hasCreate, hasMerge, hasDelete, false);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final OrderByClause orderByClause,
                               final Expression skip, final Expression limit, final CreateClause createClause,
                               final SetClause setClause,
                               final DeleteClause deleteClause, final MergeClause mergeClause,
                               final List<UnwindClause> unwindClauses,
                               final List<WithClause> withClauses, final List<CallClause> callClauses,
                               final List<RemoveClause> removeClauses,
                               final List<ClauseEntry> clausesInOrder,
                               final boolean hasCreate, final boolean hasMerge, final boolean hasDelete,
                               final boolean hasRemove) {
    this.originalQuery = originalQuery;
    this.matchClauses = matchClauses != null ? matchClauses : new ArrayList<>();
    this.whereClause = whereClause;
    this.returnClause = returnClause;
    this.orderByClause = orderByClause;
    this.skip = skip;
    this.limit = limit;
    this.createClause = createClause;
    this.setClause = setClause;
    this.deleteClause = deleteClause;
    this.mergeClause = mergeClause;
    this.unwindClauses = unwindClauses != null ? unwindClauses : new ArrayList<>();
    this.withClauses = withClauses != null ? withClauses : new ArrayList<>();
    this.callClauses = callClauses != null ? callClauses : new ArrayList<>();
    this.removeClauses = removeClauses != null ? removeClauses : new ArrayList<>();
    this.clausesInOrder = clausesInOrder != null ? clausesInOrder : new ArrayList<>();
    this.hasCreate = hasCreate;
    this.hasMerge = hasMerge;
    this.hasDelete = hasDelete;
    this.hasRemove = hasRemove;
    final boolean hasForeach = this.clausesInOrder.stream().anyMatch(c -> c.getType() == ClauseEntry.ClauseType.FOREACH);
    this.readOnly = !hasCreate && !hasMerge && !hasDelete && !hasRemove && !hasForeach && (setClause == null || setClause.isEmpty());

    // Pre-compute flags used by CypherExecutionPlan.execute() to avoid repeated clause scanning
    this.hasVariableLengthPath = computeHasVariableLengthPath();
    this.hasUnwindBeforeMatch = computeHasClauseBeforeMatch(ClauseEntry.ClauseType.UNWIND);
    this.hasWithBeforeMatch = computeHasClauseBeforeMatch(ClauseEntry.ClauseType.WITH);
    this.hasSubquery = computeHasSubquery();
  }

  private boolean computeHasVariableLengthPath() {
    if (matchClauses == null)
      return false;
    for (final MatchClause matchClause : matchClauses)
      for (final PathPattern path : matchClause.getPathPatterns())
        for (int i = 0; i < path.getRelationshipCount(); i++)
          if (path.getRelationship(i).isVariableLength())
            return true;
    return false;
  }

  private boolean computeHasClauseBeforeMatch(final ClauseEntry.ClauseType clauseType) {
    if (clausesInOrder == null || clausesInOrder.isEmpty()) {
      if (clauseType == ClauseEntry.ClauseType.UNWIND)
        return !unwindClauses.isEmpty() && !matchClauses.isEmpty();
      if (clauseType == ClauseEntry.ClauseType.WITH)
        return !withClauses.isEmpty() && !matchClauses.isEmpty();
      return false;
    }
    int firstClauseOrder = Integer.MAX_VALUE;
    int firstMatchOrder = Integer.MAX_VALUE;
    for (final ClauseEntry entry : clausesInOrder) {
      if (entry.getType() == clauseType)
        firstClauseOrder = Math.min(firstClauseOrder, entry.getOrder());
      else if (entry.getType() == ClauseEntry.ClauseType.MATCH)
        firstMatchOrder = Math.min(firstMatchOrder, entry.getOrder());
    }
    return firstClauseOrder < firstMatchOrder;
  }

  private boolean computeHasSubquery() {
    if (clausesInOrder == null || clausesInOrder.isEmpty())
      return false;
    for (final ClauseEntry entry : clausesInOrder)
      if (entry.getType() == ClauseEntry.ClauseType.SUBQUERY)
        return true;
    return false;
  }

  @Override
  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public List<MatchClause> getMatchClauses() {
    return matchClauses;
  }

  @Override
  public WhereClause getWhereClause() {
    return whereClause;
  }

  @Override
  public ReturnClause getReturnClause() {
    return returnClause;
  }

  @Override
  public boolean hasCreate() {
    return hasCreate;
  }

  @Override
  public boolean hasMerge() {
    return hasMerge;
  }

  @Override
  public boolean hasDelete() {
    return hasDelete;
  }

  @Override
  public OrderByClause getOrderByClause() {
    return orderByClause;
  }

  @Override
  public Expression getSkip() {
    return skip;
  }

  @Override
  public Expression getLimit() {
    return limit;
  }

  @Override
  public CreateClause getCreateClause() {
    return createClause;
  }

  @Override
  public SetClause getSetClause() {
    return setClause;
  }

  @Override
  public DeleteClause getDeleteClause() {
    return deleteClause;
  }

  @Override
  public MergeClause getMergeClause() {
    return mergeClause;
  }

  @Override
  public List<UnwindClause> getUnwindClauses() {
    return unwindClauses;
  }

  @Override
  public List<WithClause> getWithClauses() {
    return withClauses;
  }

  @Override
  public List<ClauseEntry> getClausesInOrder() {
    return clausesInOrder;
  }

  @Override
  public List<CallClause> getCallClauses() {
    return callClauses;
  }

  @Override
  public List<RemoveClause> getRemoveClauses() {
    return removeClauses;
  }

  public boolean hasRemove() {
    return hasRemove;
  }

  @Override
  public boolean hasVariableLengthPath() {
    return hasVariableLengthPath;
  }

  @Override
  public boolean hasUnwindBeforeMatch() {
    return hasUnwindBeforeMatch;
  }

  @Override
  public boolean hasWithBeforeMatch() {
    return hasWithBeforeMatch;
  }

  @Override
  public boolean hasSubquery() {
    return hasSubquery;
  }

  public String getOriginalQuery() {
    return originalQuery;
  }
}
