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

import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Dispatches clause contexts to their appropriate handlers.
 * Uses a strategy pattern with a dispatch table to eliminate cascading if/else statements.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ClauseDispatcher {

  private final Map<Function<Cypher25Parser.ClauseContext, ?>, ClauseHandler> handlers = new HashMap<>();

  ClauseDispatcher() {
    // Register all clause handlers
    register(Cypher25Parser.ClauseContext::matchClause, this::handleMatch);
    register(Cypher25Parser.ClauseContext::createClause, this::handleCreate);
    register(Cypher25Parser.ClauseContext::insertClause, this::handleInsert);
    register(Cypher25Parser.ClauseContext::setClause, this::handleSet);
    register(Cypher25Parser.ClauseContext::deleteClause, this::handleDelete);
    register(Cypher25Parser.ClauseContext::mergeClause, this::handleMerge);
    register(Cypher25Parser.ClauseContext::unwindClause, this::handleUnwind);
    register(Cypher25Parser.ClauseContext::forUnwindClause, this::handleForUnwind);
    register(Cypher25Parser.ClauseContext::withClause, this::handleWith);
    register(Cypher25Parser.ClauseContext::returnClause, this::handleReturn);
    register(Cypher25Parser.ClauseContext::orderBySkipLimitClause, this::handleOrderBySkipLimit);
    register(Cypher25Parser.ClauseContext::callClause, this::handleCall);
    register(Cypher25Parser.ClauseContext::removeClause, this::handleRemove);
    register(Cypher25Parser.ClauseContext::foreachClause, this::handleForeach);
    register(Cypher25Parser.ClauseContext::subqueryClause, this::handleSubquery);
    register(Cypher25Parser.ClauseContext::loadCSVClause, this::handleLoadCSV);
    register(Cypher25Parser.ClauseContext::finishClause, this::handleFinish);
  }

  /**
   * Dispatch a clause context to its appropriate handler.
   *
   * @param ctx the clause context
   * @param builder the statement builder
   * @param astBuilder the AST builder
   */
  void dispatch(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder, final CypherASTBuilder astBuilder) {
    for (final Map.Entry<Function<Cypher25Parser.ClauseContext, ?>, ClauseHandler> entry : handlers.entrySet()) {
      final Object clauseCtx = entry.getKey().apply(ctx);
      if (clauseCtx != null) {
        entry.getValue().handle(ctx, builder, astBuilder);
        return;
      }
    }
    // No handler found - this should not happen with valid grammar
  }

  private <T> void register(final Function<Cypher25Parser.ClauseContext, T> accessor, final ClauseHandler handler) {
    handlers.put(accessor, handler);
  }

  // ============================================================================
  // Clause Handlers
  // ============================================================================

  private void handleMatch(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                           final CypherASTBuilder astBuilder) {
    builder.addMatch(astBuilder.visitMatchClause(ctx.matchClause()));
  }

  private void handleCreate(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                            final CypherASTBuilder astBuilder) {
    builder.setCreate(astBuilder.visitCreateClause(ctx.createClause()));
  }

  private void handleInsert(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                            final CypherASTBuilder astBuilder) {
    // GQL INSERT is synonymous with CREATE (issue #3365 section 1.1).
    // Routed through visitInsertClause which produces a CreateClause from the stricter insertPattern grammar.
    builder.setCreate(astBuilder.visitInsertClause(ctx.insertClause()));
  }

  private void handleSet(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                         final CypherASTBuilder astBuilder) {
    builder.setSet(astBuilder.visitSetClause(ctx.setClause()));
  }

  private void handleDelete(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                            final CypherASTBuilder astBuilder) {
    builder.setDelete(astBuilder.visitDeleteClause(ctx.deleteClause()));
  }

  private void handleMerge(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                           final CypherASTBuilder astBuilder) {
    builder.setMerge(astBuilder.visitMergeClause(ctx.mergeClause()));
  }

  private void handleUnwind(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                            final CypherASTBuilder astBuilder) {
    builder.addUnwind(astBuilder.visitUnwindClause(ctx.unwindClause()));
  }

  private void handleForUnwind(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                               final CypherASTBuilder astBuilder) {
    // GQL FOR ... IN ... is synonymous with UNWIND (issue #3365 section 1.2).
    builder.addUnwind(astBuilder.visitForUnwindClause(ctx.forUnwindClause()));
  }

  private void handleWith(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                          final CypherASTBuilder astBuilder) {
    builder.addWith(astBuilder.visitWithClause(ctx.withClause()));
  }

  private void handleReturn(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                            final CypherASTBuilder astBuilder) {
    final Cypher25Parser.ReturnBodyContext body = ctx.returnClause().returnBody();
    builder.setReturn(astBuilder.visitReturnClause(ctx.returnClause()));

    // Extract ORDER BY, SKIP, LIMIT from returnBody
    if (body.orderBy() != null)
      builder.setOrderBy(astBuilder.visitOrderBy(body.orderBy()));

    if (body.skip() != null)
      builder.setSkip((Expression) astBuilder.visitSkip(body.skip()));

    if (body.limit() != null)
      builder.setLimit((Expression) astBuilder.visitLimit(body.limit()));
  }

  private void handleOrderBySkipLimit(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                                      final CypherASTBuilder astBuilder) {
    final Cypher25Parser.OrderBySkipLimitClauseContext orderBySkipLimit = ctx.orderBySkipLimitClause();

    // Cypher 25 allows a standalone ORDER BY / SKIP / LIMIT as a clause on its own.
    // It applies to the rows produced by the preceding clauses and forwards them to the
    // following clauses, which is semantically equivalent to an implicit `WITH *`.
    // Representing it as a WithClause guarantees the sort/skip/limit is applied at the
    // correct position in clausesInOrder instead of being deferred to the end of the
    // statement (issue #3950).
    final OrderByClause orderBy = orderBySkipLimit.orderBy() != null
        ? astBuilder.visitOrderBy(orderBySkipLimit.orderBy())
        : null;
    final Expression skipExpr = orderBySkipLimit.skip() != null
        ? (Expression) astBuilder.visitSkip(orderBySkipLimit.skip())
        : null;
    final Expression limitExpr = orderBySkipLimit.limit() != null
        ? (Expression) astBuilder.visitLimit(orderBySkipLimit.limit())
        : null;

    final List<ReturnClause.ReturnItem> starItems = new ArrayList<>();
    starItems.add(new ReturnClause.ReturnItem(new VariableExpression("*"), "*"));
    final WithClause implicitWith = new WithClause(starItems, false, null, orderBy, skipExpr, limitExpr);
    builder.addWith(implicitWith);
  }

  private void handleCall(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                          final CypherASTBuilder astBuilder) {
    builder.addCall(astBuilder.visitCallClause(ctx.callClause()));
  }

  private void handleRemove(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                            final CypherASTBuilder astBuilder) {
    builder.addRemove(astBuilder.visitRemoveClause(ctx.removeClause()));
  }

  private void handleForeach(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                             final CypherASTBuilder astBuilder) {
    builder.addForeach(astBuilder.visitForeachClause(ctx.foreachClause()));
  }

  private void handleSubquery(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                              final CypherASTBuilder astBuilder) {
    builder.addSubquery(astBuilder.visitSubqueryClause(ctx.subqueryClause()));
  }

  private void handleLoadCSV(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                              final CypherASTBuilder astBuilder) {
    builder.addLoadCSV(astBuilder.visitLoadCSVClause(ctx.loadCSVClause()));
  }

  private void handleFinish(final Cypher25Parser.ClauseContext ctx, final StatementBuilder builder,
                            final CypherASTBuilder astBuilder) {
    // FINISH is a marker; AST visitor returns FinishClause.INSTANCE for symmetry with other
    // clauses. The builder records its presence and rejects co-occurrence with RETURN.
    astBuilder.visitFinishClause(ctx.finishClause());
    builder.addFinish();
  }
}
