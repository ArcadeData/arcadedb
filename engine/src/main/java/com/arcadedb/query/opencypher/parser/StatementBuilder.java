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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.query.opencypher.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for accumulating Cypher statement components during parsing.
 * Replaces the numerous local variables in visitSingleQuery with a clean builder pattern.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StatementBuilder {
  private final List<MatchClause> matchClauses = new ArrayList<>();
  private final List<UnwindClause> unwindClauses = new ArrayList<>();
  private final List<WithClause> withClauses = new ArrayList<>();
  private final List<CallClause> callClauses = new ArrayList<>();
  private final List<RemoveClause> removeClauses = new ArrayList<>();
  private final List<ClauseEntry> clausesInOrder = new ArrayList<>();

  private CreateClause createClause;
  private SetClause setClause;
  private DeleteClause deleteClause;
  private MergeClause mergeClause;
  private WhereClause whereClause;
  private ReturnClause returnClause;
  private OrderByClause orderByClause;
  private Expression skip;
  private Expression limit;

  private int clauseOrder = 0;

  void addMatch(final MatchClause match) {
    matchClauses.add(match);
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.MATCH, match, clauseOrder++));
  }

  void setCreate(final CreateClause create) {
    this.createClause = create;
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.CREATE, create, clauseOrder++));
  }

  void setSet(final SetClause set) {
    this.setClause = set;
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.SET, set, clauseOrder++));
  }

  void setDelete(final DeleteClause delete) {
    this.deleteClause = delete;
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.DELETE, delete, clauseOrder++));
  }

  void setMerge(final MergeClause merge) {
    this.mergeClause = merge;
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.MERGE, merge, clauseOrder++));
  }

  void addUnwind(final UnwindClause unwind) {
    unwindClauses.add(unwind);
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.UNWIND, unwind, clauseOrder++));
  }

  void addWith(final WithClause with) {
    withClauses.add(with);
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.WITH, with, clauseOrder++));
  }

  void addCall(final CallClause call) {
    callClauses.add(call);
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.CALL, call, clauseOrder++));
  }

  void addRemove(final RemoveClause remove) {
    removeClauses.add(remove);
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.REMOVE, remove, clauseOrder++));
  }

  void addForeach(final ForeachClause foreach) {
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.FOREACH, foreach, clauseOrder++));
  }

  void addSubquery(final SubqueryClause subquery) {
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.SUBQUERY, subquery, clauseOrder++));
  }

  void addLoadCSV(final LoadCSVClause loadCSV) {
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.LOAD_CSV, loadCSV, clauseOrder++));
  }

  void setReturn(final ReturnClause returnClause) {
    this.returnClause = returnClause;
    clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.RETURN, returnClause, clauseOrder++));
  }

  void setOrderBy(final OrderByClause orderBy) {
    this.orderByClause = orderBy;
  }

  void setSkip(final Expression skip) {
    this.skip = skip;
  }

  void setLimit(final Expression limit) {
    this.limit = limit;
  }

  void setWhere(final WhereClause where) {
    this.whereClause = where;
  }

  SimpleCypherStatement build() {
    final boolean hasCreate = createClause != null;
    final boolean hasMerge = mergeClause != null;
    final boolean hasDelete = deleteClause != null;
    final boolean hasRemove = !removeClauses.isEmpty();

    return new SimpleCypherStatement(
        "", // Original query string (set later)
        matchClauses,
        whereClause,
        returnClause,
        orderByClause,
        skip,
        limit,
        createClause,
        setClause,
        deleteClause,
        mergeClause,
        unwindClauses,
        withClauses,
        callClauses,
        removeClauses,
        clausesInOrder,
        hasCreate,
        hasMerge,
        hasDelete,
        hasRemove
    );
  }
}
