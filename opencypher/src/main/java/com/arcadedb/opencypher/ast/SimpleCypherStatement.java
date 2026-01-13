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
package com.arcadedb.opencypher.ast;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple implementation of CypherStatement for Phase 1.
 * This is a basic implementation to get the module compiling.
 * Will be replaced with full ANTLR-based implementation in later phases.
 */
public class SimpleCypherStatement implements CypherStatement {
  private final String originalQuery;
  private final List<MatchClause> matchClauses;
  private final WhereClause whereClause;
  private final ReturnClause returnClause;
  private final OrderByClause orderByClause;
  private final Integer skip;
  private final Integer limit;
  private final CreateClause createClause;
  private final SetClause setClause;
  private final DeleteClause deleteClause;
  private final MergeClause mergeClause;
  private final List<UnwindClause> unwindClauses;
  private final boolean hasCreate;
  private final boolean hasMerge;
  private final boolean hasDelete;

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
      final WhereClause whereClause, final ReturnClause returnClause, final boolean hasCreate, final boolean hasMerge,
      final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, null, null, null, null, null, null, null, null, hasCreate, hasMerge,
        hasDelete);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
      final WhereClause whereClause, final ReturnClause returnClause, final OrderByClause orderByClause,
      final Integer skip, final Integer limit, final boolean hasCreate, final boolean hasMerge, final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, orderByClause, skip, limit, null, null, null, null, null,
        hasCreate, hasMerge, hasDelete);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
      final WhereClause whereClause, final ReturnClause returnClause, final OrderByClause orderByClause,
      final Integer skip, final Integer limit, final CreateClause createClause, final SetClause setClause,
      final DeleteClause deleteClause, final MergeClause mergeClause, final List<UnwindClause> unwindClauses,
      final boolean hasCreate, final boolean hasMerge, final boolean hasDelete) {
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
    this.hasCreate = hasCreate;
    this.hasMerge = hasMerge;
    this.hasDelete = hasDelete;
  }

  @Override
  public boolean isReadOnly() {
    return !hasCreate && !hasMerge && !hasDelete;
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
  public Integer getSkip() {
    return skip;
  }

  @Override
  public Integer getLimit() {
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

  public String getOriginalQuery() {
    return originalQuery;
  }
}
