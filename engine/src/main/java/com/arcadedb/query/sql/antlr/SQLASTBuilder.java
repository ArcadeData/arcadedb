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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.database.Identifiable;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.query.sql.grammar.SQLParserBaseVisitor;
import com.arcadedb.query.sql.parser.*;
import com.arcadedb.utility.CollectionUtils;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ANTLR4 visitor that builds ArcadeDB's internal AST from the SQL parse tree.
 * Transforms ANTLR's parse tree into Statement objects that are compatible
 * with the existing execution engine.
 * <p>
 * This visitor produces identical AST structures to the JavaCC parser,
 * ensuring 100% backward compatibility with existing execution planners
 * and executor steps.
 */
public class SQLASTBuilder extends SQLParserBaseVisitor<Object> {

  /**
   * Namespaces that are recognized as function call prefixes in dotted SQL syntax (e.g. {@code geo.point(x, y)}).
   * This rewriting is performed in {@link #visitIdentifierChain} for the ANTLR parser only.
   * The JavaCC parser does not support unquoted dotted function names; use backtick-quoted names
   * (e.g. {@code `geo.point`(x, y)}) for compatibility with both parsers.
   * Known function namespace prefixes. When the parser sees {@code namespace.method(args)} and the namespace
   * is in this set, the AST builder produces a {@link FunctionCall} node with the qualified name
   * (e.g., "ts.first") instead of an identifier chain with a method modifier.
   * Identifier prefixes that are treated as function namespaces rather than field names.
   * When an identifierChain matches "namespace.method(args)", it is rewritten as a
   * namespace-qualified function call (e.g. "geo.point(x,y)" → FunctionCall("geo.point")).
   */
  private static final Set<String> FUNCTION_NAMESPACES = Set.of("ts","geo");


  private int positionalParamCounter = 0;

  // ENTRY POINTS

  /**
   * Main entry point for parsing a single SQL statement.
   */
  @Override
  public Statement visitParse(final SQLParser.ParseContext ctx) {
    return (Statement) visit(ctx.statement());
  }

  /**
   * Entry point for parsing multiple SQL statements (script).
   */
  @Override
  public List<Statement> visitParseScript(final SQLParser.ParseScriptContext ctx) {
    final List<Statement> statements = new ArrayList<>();
    for (final SQLParser.ScriptStatementContext stmtCtx : ctx.scriptStatement()) {
      statements.add((Statement) visit(stmtCtx));
    }
    return statements;
  }

  /**
   * Entry point for parsing an expression.
   */
  @Override
  public Expression visitParseExpression(final SQLParser.ParseExpressionContext ctx) {
    return (Expression) visit(ctx.expression());
  }

  /**
   * Entry point for parsing a WHERE condition.
   */
  @Override
  public WhereClause visitParseCondition(final SQLParser.ParseConditionContext ctx) {
    return (WhereClause) visit(ctx.whereClause());
  }

  // QUERY STATEMENTS

  /**
   * SELECT statement rule visitor (for selectStatement grammar rule).
   * This is called when visiting a selectStatement rule directly (e.g., from INSERT...SELECT).
   */
  @Override
  public SelectStatement visitSelectStatement(final SQLParser.SelectStatementContext ctx) {
    final SelectStatement stmt = new SelectStatement(-1);

    // Projection
    if (ctx.projection() != null) {
      stmt.projection = (Projection) visit(ctx.projection());
    }

    // FROM clause
    if (ctx.fromClause() != null) {
      stmt.target = (FromClause) visit(ctx.fromClause());
    }

    // LET clause
    if (ctx.letClause() != null) {
      stmt.letClause = (LetClause) visit(ctx.letClause());
    }

    // WHERE clause
    if (ctx.whereClause() != null) {
      stmt.whereClause = (WhereClause) visit(ctx.whereClause());
    }

    // GROUP BY clause
    if (ctx.groupBy() != null) {
      stmt.groupBy = (GroupBy) visit(ctx.groupBy());
    }

    // ORDER BY clause
    if (ctx.orderBy() != null) {
      stmt.orderBy = (OrderBy) visit(ctx.orderBy());
    }

    // UNWIND clause
    if (ctx.unwind() != null) {
      stmt.unwind = (Unwind) visit(ctx.unwind());
    }

    // SKIP and LIMIT clauses
    if (ctx.skip() != null) {
      stmt.skip = (Skip) visit(ctx.skip());
    }
    if (ctx.limit() != null) {
      stmt.limit = (Limit) visit(ctx.limit());
    }

    // TIMEOUT clause
    if (ctx.timeout() != null) {
      stmt.timeout = (Timeout) visit(ctx.timeout());
    }

    return stmt;
  }

  /**
   * SELECT statement visitor (for selectStmt labeled alternative).
   * Maps to SelectStatement AST class.
   */
  @Override
  public SelectStatement visitSelectStmt(final SQLParser.SelectStmtContext ctx) {
    final SelectStatement stmt = new SelectStatement(-1);

    // Get the selectStatement context from the labeled alternative
    final SQLParser.SelectStatementContext selectCtx = ctx.selectStatement();

    // Projection
    if (selectCtx.projection() != null) {
      stmt.projection = (Projection) visit(selectCtx.projection());
    }

    // FROM clause
    if (selectCtx.fromClause() != null) {
      stmt.target = (FromClause) visit(selectCtx.fromClause());
    }

    // LET clause
    if (selectCtx.letClause() != null) {
      stmt.letClause = (LetClause) visit(selectCtx.letClause());
    }

    // WHERE clause
    if (selectCtx.whereClause() != null) {
      stmt.whereClause = (WhereClause) visit(selectCtx.whereClause());
    }

    // GROUP BY clause
    if (selectCtx.groupBy() != null) {
      stmt.groupBy = (GroupBy) visit(selectCtx.groupBy());
    }

    // ORDER BY clause
    if (selectCtx.orderBy() != null) {
      stmt.orderBy = (OrderBy) visit(selectCtx.orderBy());
    }

    // UNWIND clause
    if (selectCtx.unwind() != null) {
      stmt.unwind = (Unwind) visit(selectCtx.unwind());
    }

    // SKIP clause
    if (selectCtx.skip() != null) {
      stmt.skip = (Skip) visit(selectCtx.skip());
    }

    // LIMIT clause
    if (selectCtx.limit() != null) {
      stmt.limit = (Limit) visit(selectCtx.limit());
    }

    // TIMEOUT clause
    if (selectCtx.timeout() != null) {
      stmt.timeout = (Timeout) visit(selectCtx.timeout());
    }

    return stmt;
  }

  /**
   * MATCH statement rule visitor (for matchStatement grammar rule).
   * This is called when visiting a matchStatement rule directly (e.g., from subqueries).
   */
  @Override
  public MatchStatement visitMatchStatement(final SQLParser.MatchStatementContext ctx) {
    final MatchStatement stmt = new MatchStatement(-1);

    // Parse match expressions (both positive and negative patterns)
    final List<MatchExpression> matchExpressions = new ArrayList<>();
    final List<MatchExpression> notMatchExpressions = new ArrayList<>();

    if (CollectionUtils.isNotEmpty(ctx.matchExpression())) {
      for (int i = 0; i < ctx.matchExpression().size(); i++) {
        final SQLParser.MatchExpressionContext exprCtx = ctx.matchExpression(i);
        final MatchExpression matchExpr = (MatchExpression) visit(exprCtx);

        // Check if this is a NOT expression (only possible for non-first expressions)
        if (i > 0 && ctx.NOT(i - 1) != null) {
          notMatchExpressions.add(matchExpr);
        } else {
          matchExpressions.add(matchExpr);
        }
      }
    }

    stmt.setMatchExpressions(matchExpressions);
    stmt.setNotMatchExpressions(notMatchExpressions);

    // Parse RETURN clause
    stmt.setReturnDistinct(ctx.DISTINCT() != null);

    // Parse return items (expressions with optional aliases and nested projections)
    final List<Expression> returnItems = new ArrayList<>();
    final List<Identifier> returnAliases = new ArrayList<>();
    final List<NestedProjection> returnNestedProjections = new ArrayList<>();

    if (CollectionUtils.isNotEmpty(ctx.matchReturnItem())) {
      for (final SQLParser.MatchReturnItemContext itemCtx : ctx.matchReturnItem()) {
        // Extract expression
        final Expression expr = (Expression) visit(itemCtx.expression());
        returnItems.add(expr);

        // Extract optional alias
        if (itemCtx.identifier() != null) {
          returnAliases.add((Identifier) visit(itemCtx.identifier()));
        } else {
          returnAliases.add(null);
        }

        // Extract optional nested projection
        if (itemCtx.nestedProjection() != null) {
          returnNestedProjections.add((NestedProjection) visit(itemCtx.nestedProjection()));
        } else {
          returnNestedProjections.add(null);
        }
      }
    }

    stmt.setReturnItems(returnItems);
    stmt.setReturnAliases(returnAliases);
    stmt.setReturnNestedProjections(returnNestedProjections);

    // Parse GROUP BY clause
    if (ctx.groupBy() != null) {
      stmt.setGroupBy((GroupBy) visit(ctx.groupBy()));
    }

    // Parse ORDER BY clause
    if (ctx.orderBy() != null) {
      stmt.setOrderBy((OrderBy) visit(ctx.orderBy()));
    }

    // Parse UNWIND clause
    if (ctx.unwind() != null) {
      stmt.setUnwind((Unwind) visit(ctx.unwind()));
    }

    // Parse SKIP clause
    if (ctx.skip() != null) {
      stmt.setSkip((Skip) visit(ctx.skip()));
    }

    // Parse LIMIT clause
    if (ctx.limit() != null) {
      stmt.setLimit((Limit) visit(ctx.limit()));
    }

    return stmt;
  }

  /**
   * MATCH statement visitor (for matchStmt labeled alternative).
   * Maps to MatchStatement AST class.
   */
  @Override
  public MatchStatement visitMatchStmt(final SQLParser.MatchStmtContext ctx) {
    final MatchStatement stmt = new MatchStatement(-1);

    // Get the matchStatement context from the labeled alternative
    final SQLParser.MatchStatementContext matchCtx = ctx.matchStatement();

    // Parse match expressions (both positive and negative patterns)
    final List<MatchExpression> matchExpressions = new ArrayList<>();
    final List<MatchExpression> notMatchExpressions = new ArrayList<>();

    if (CollectionUtils.isNotEmpty(matchCtx.matchExpression())) {
      for (int i = 0; i < matchCtx.matchExpression().size(); i++) {
        final SQLParser.MatchExpressionContext exprCtx = matchCtx.matchExpression(i);
        final MatchExpression matchExpr = (MatchExpression) visit(exprCtx);

        // Check if this is a NOT expression (only possible for non-first expressions)
        if (i > 0 && matchCtx.NOT(i - 1) != null) {
          notMatchExpressions.add(matchExpr);
        } else {
          matchExpressions.add(matchExpr);
        }
      }
    }

    stmt.setMatchExpressions(matchExpressions);
    stmt.setNotMatchExpressions(notMatchExpressions);

    // Parse RETURN clause
    stmt.setReturnDistinct(matchCtx.DISTINCT() != null);

    // Parse return items (expressions with optional aliases and nested projections)
    final List<Expression> returnItems = new ArrayList<>();
    final List<Identifier> returnAliases = new ArrayList<>();
    final List<NestedProjection> returnNestedProjections = new ArrayList<>();

    if (CollectionUtils.isNotEmpty(matchCtx.matchReturnItem())) {
      for (final SQLParser.MatchReturnItemContext itemCtx : matchCtx.matchReturnItem()) {
        // Extract expression
        final Expression expr = (Expression) visit(itemCtx.expression());
        returnItems.add(expr);

        // Extract optional alias
        if (itemCtx.identifier() != null) {
          returnAliases.add((Identifier) visit(itemCtx.identifier()));
        } else {
          returnAliases.add(null);
        }

        // Extract optional nested projection
        if (itemCtx.nestedProjection() != null) {
          returnNestedProjections.add((NestedProjection) visit(itemCtx.nestedProjection()));
        } else {
          returnNestedProjections.add(null);
        }
      }
    }

    stmt.setReturnItems(returnItems);
    stmt.setReturnAliases(returnAliases);
    stmt.setReturnNestedProjections(returnNestedProjections);

    // GROUP BY clause
    if (matchCtx.groupBy() != null) {
      stmt.setGroupBy((GroupBy) visit(matchCtx.groupBy()));
    }

    // ORDER BY clause
    if (matchCtx.orderBy() != null) {
      stmt.setOrderBy((OrderBy) visit(matchCtx.orderBy()));
    }

    // UNWIND clause
    if (matchCtx.unwind() != null) {
      stmt.setUnwind((Unwind) visit(matchCtx.unwind()));
    }

    // SKIP clause
    if (matchCtx.skip() != null) {
      stmt.setSkip((Skip) visit(matchCtx.skip()));
    }

    // LIMIT clause
    if (matchCtx.limit() != null) {
      stmt.limit = (Limit) visit(matchCtx.limit());
    }

    return stmt;
  }

  /**
   * Match expression visitor.
   * Handles pattern expressions like: {type:Person, as: p}.out('Friend').in('Knows'){as: f}
   * Also handles arrow syntax: {type:Person}-Friend->{as: f}<-Knows-{as: k}
   */
  @Override
  public MatchExpression visitMatchExpression(final SQLParser.MatchExpressionContext ctx) {
    final MatchExpression matchExpr = new MatchExpression(-1);
    final List<MatchPathItem> items = new ArrayList<>();

    // Parse match path items
    if (CollectionUtils.isNotEmpty(ctx.matchPathItem())) {
      // First item becomes the origin
      if (ctx.matchPathItem().size() > 0) {
        final SQLParser.MatchPathItemContext firstItemCtx = ctx.matchPathItem(0);

        // Set origin from the first filter (if present)
        if (firstItemCtx.matchFilter() != null) {
          matchExpr.setOrigin((MatchFilter) visit(firstItemCtx.matchFilter()));
        }

        // Process methods in the first item
        if (CollectionUtils.isNotEmpty(firstItemCtx.matchMethod())) {
          for (final SQLParser.MatchMethodContext methodCtx : firstItemCtx.matchMethod()) {
            final MatchPathItem pathItem = (MatchPathItem) visit(methodCtx);
            if (pathItem != null) {
              // If this is a MultiMatchPathItem from chained method calls (not nested syntax), flatten it
              if (pathItem instanceof final MultiMatchPathItem multiItem && !((MultiMatchPathItem) pathItem).isNestedPath()) {
                // Add each sub-item as a separate path item
                if (multiItem.getItems() != null) {
                  for (final MatchPathItem subItem : multiItem.getItems()) {
                    // For MatchPathItemFirst, convert to regular MatchPathItem
                    if (subItem instanceof final MatchPathItemFirst firstSubItem) {
                      final MatchPathItem regularItem = new MatchPathItem(-1);
                      // Convert function to method
                      if (firstSubItem.getFunction() != null) {
                        final MethodCall method = new MethodCall(-1);
                        method.methodName = firstSubItem.getFunction().name;
                        method.params = new ArrayList<>(firstSubItem.getFunction().params);
                        regularItem.setMethod(method);
                      }
                      regularItem.setFilter(firstSubItem.getFilter());
                      items.add(regularItem);
                    } else {
                      items.add(subItem);
                    }
                  }
                  // The filter on the MultiMatchPathItem should go on the LAST item
                  if (multiItem.getFilter() != null && !items.isEmpty()) {
                    items.get(items.size() - 1).setFilter(multiItem.getFilter());
                  }
                }
              } else {
                items.add(pathItem);
              }
            }
          }
        }
      }

      // Process remaining path items
      for (int i = 1; i < ctx.matchPathItem().size(); i++) {
        final SQLParser.MatchPathItemContext itemCtx = ctx.matchPathItem(i);

        // Each path item can have a filter and methods
        if (itemCtx.matchFilter() != null) {
          final MatchPathItem pathItem = new MatchPathItem(-1);
          pathItem.setFilter((MatchFilter) visit(itemCtx.matchFilter()));
          items.add(pathItem);
        }

        // Process methods
        if (CollectionUtils.isNotEmpty(itemCtx.matchMethod())) {
          for (final SQLParser.MatchMethodContext methodCtx : itemCtx.matchMethod()) {
            final MatchPathItem pathItem = (MatchPathItem) visit(methodCtx);
            if (pathItem != null) {
              // If this is a MultiMatchPathItem from chained method calls (not nested syntax), flatten it
              if (pathItem instanceof final MultiMatchPathItem multiItem && !((MultiMatchPathItem) pathItem).isNestedPath()) {
                // Add each sub-item as a separate path item
                if (multiItem.getItems() != null) {
                  for (final MatchPathItem subItem : multiItem.getItems()) {
                    // For MatchPathItemFirst, convert to regular MatchPathItem
                    if (subItem instanceof final MatchPathItemFirst firstSubItem) {
                      final MatchPathItem regularItem = new MatchPathItem(-1);
                      // Convert function to method
                      if (firstSubItem.getFunction() != null) {
                        final MethodCall method = new MethodCall(-1);
                        method.methodName = firstSubItem.getFunction().name;
                        method.params = new ArrayList<>(firstSubItem.getFunction().params);
                        regularItem.setMethod(method);
                      }
                      regularItem.setFilter(firstSubItem.getFilter());
                      items.add(regularItem);
                    } else {
                      items.add(subItem);
                    }
                  }
                  // The filter on the MultiMatchPathItem should go on the LAST item
                  if (multiItem.getFilter() != null && !items.isEmpty()) {
                    items.get(items.size() - 1).setFilter(multiItem.getFilter());
                  }
                }
              } else {
                items.add(pathItem);
              }
            }
          }
        }
      }
    }

    matchExpr.setItems(items);
    return matchExpr;
  }

  /**
   * Match method visitor.
   * Handles: .out('Friend') or -Friend-> or <-Friend- or -Friend-
   */
  @Override
  public MatchPathItem visitMatchMethod(final SQLParser.MatchMethodContext ctx) {
    // Check if this is DOT method syntax: .out('Friend'){as:x} or .(chain){as:x}
    if (ctx.DOT() != null) {
      // Check for .(method().method(){...}) syntax - nested match path
      if (ctx.nestedMatchPath() != null) {
        // This is .(methodChain) - a nested sequence of match methods
        // Create a MultiMatchPathItem to hold the nested method chain
        final MultiMatchPathItem multiPathItem = new MultiMatchPathItem(-1);
        multiPathItem.setNestedPath(true); // Mark as nested path - should NOT be flattened
        final SQLParser.NestedMatchPathContext nestedCtx = ctx.nestedMatchPath();

        // nestedMatchPath : nestedMatchMethod+
        // Process all nestedMatchMethod elements in the nested path
        final List<SQLParser.NestedMatchMethodContext> pathItems = nestedCtx.nestedMatchMethod();

        for (int i = 0; i < pathItems.size(); i++) {
          final MatchPathItem pathItem = (MatchPathItem) visit(pathItems.get(i));
          if (pathItem != null) {
            if (i == 0) {
              // First item should be MatchPathItemFirst
              final MatchPathItemFirst firstItem = new MatchPathItemFirst(-1);
              // Copy method and filter from the pathItem
              firstItem.setFilter(pathItem.getFilter());
              if (pathItem.getMethod() != null) {
                // Convert MethodCall to FunctionCall for first item
                final FunctionCall funcCall = new FunctionCall(-1);
                funcCall.name = pathItem.getMethod().methodName;
                if (pathItem.getMethod().params != null) {
                  funcCall.params = new ArrayList<>(pathItem.getMethod().params);
                }
                firstItem.setFunction(funcCall);
              }
              multiPathItem.getItems().add(firstItem);
            } else {
              // Subsequent items are regular MatchPathItem
              multiPathItem.getItems().add(pathItem);
            }
          }
        }

        // Add properties if present at the end (matchProperties for the whole nested path): {as:x, where:...}
        if (ctx.matchProperties() != null) {
          multiPathItem.setFilter((MatchFilter) visit(ctx.matchProperties()));
        }
        return multiPathItem;
      }

      // Standard .methodCall() syntax
      final Object methodObj = visit(ctx.matchMethodCall());

      // Check if the matchMethodCall is a functionCall with chained methodCalls
      // Grammar: functionCall : identifier LPAREN ... RPAREN methodCall* ...
      // If we have .out('Friend').out('Friend'), the second .out() is a methodCall chain
      if (ctx.matchMethodCall().functionCall() != null) {
        final SQLParser.FunctionCallContext funcCtx = ctx.matchMethodCall().functionCall();
        if (CollectionUtils.isNotEmpty(funcCtx.methodCall())) {
          // We have chained method calls - create MultiMatchPathItem
          final MultiMatchPathItem multiPathItem = new MultiMatchPathItem(-1);

          // First item from the function call
          final MatchPathItemFirst firstItem = new MatchPathItemFirst(-1);
          if (methodObj instanceof FunctionCall) {
            firstItem.setFunction((FunctionCall) methodObj);
          }
          multiPathItem.getItems().add(firstItem);

          // Additional items from methodCall chains
          for (final SQLParser.MethodCallContext methodCallCtx : funcCtx.methodCall()) {
            final MatchPathItem item = new MatchPathItem(-1);

            // Extract method name and params from methodCall
            // methodCall: DOT identifier LPAREN (expression (COMMA expression)*)? RPAREN
            final Identifier methodName = (Identifier) visit(methodCallCtx.identifier());
            final MethodCall method = new MethodCall(-1);
            method.methodName = methodName;

            if (CollectionUtils.isNotEmpty(methodCallCtx.expression())) {
              for (final SQLParser.ExpressionContext exprCtx : methodCallCtx.expression()) {
                method.params.add((Expression) visit(exprCtx));
              }
            }

            item.setMethod(method);
            multiPathItem.getItems().add(item);
          }

          // Add properties if present at the end
          if (ctx.matchProperties() != null) {
            multiPathItem.setFilter((MatchFilter) visit(ctx.matchProperties()));
          }
          return multiPathItem;
        }
      }

      // Check if this is a field access (.identifier) or a method call (.method())
      if (methodObj instanceof Identifier) {
        // This is .identifier (field access) - create FieldMatchPathItem
        final FieldMatchPathItem fieldPathItem = new FieldMatchPathItem(-1);
        fieldPathItem.field = (Identifier) methodObj;

        // Add properties if present: {as:x, where:...}
        if (ctx.matchProperties() != null) {
          fieldPathItem.setFilter((MatchFilter) visit(ctx.matchProperties()));
        }
        return fieldPathItem;
      }

      // This is a method call (.method() or .function(...))
      final MatchPathItem pathItem = new MatchPathItem(-1);
      final MethodCall method;

      if (methodObj instanceof MethodCall) {
        method = (MethodCall) methodObj;
      } else if (methodObj instanceof final FunctionCall funcCall) {
        // Convert FunctionCall to MethodCall (they have the same structure)
        method = new MethodCall(-1);
        method.methodName = funcCall.name;
        method.params.addAll(funcCall.params);
      } else {
        throw new IllegalStateException("Unexpected method call type: " +
            (methodObj != null ? methodObj.getClass().getName() : "null"));
      }

      pathItem.setMethod(method);

      // Add properties if present: {as:x, where:...}
      if (ctx.matchProperties() != null) {
        pathItem.setFilter((MatchFilter) visit(ctx.matchProperties()));
      }
      return pathItem;
    }

    // Handle anonymous arrow syntax: --> <-- --
    if (ctx.DECR() != null || (ctx.ARROW_LEFT() != null && ctx.identifier() == null)) {
      final MatchPathItem pathItem = new MatchPathItem(-1);
      String methodName;
      if (ctx.GT() != null || (ctx.MINUS() != null && ctx.ARROW_LEFT() == null)) {
        // DECR GT = --> = outgoing
        methodName = "out";
      } else if (ctx.ARROW_LEFT() != null) {
        // ARROW_LEFT MINUS = <-- = incoming
        methodName = "in";
      } else {
        // DECR = -- = bidirectional
        methodName = "both";
      }

      final MethodCall method = new MethodCall(-1);
      method.methodName = new Identifier(methodName);
      pathItem.setMethod(method);

      // Add properties if present: {as:x, where:...}
      if (ctx.matchProperties() != null) {
        pathItem.setFilter((MatchFilter) visit(ctx.matchProperties()));
      }

      return pathItem;
    }

    // Arrow syntax with identifier: -EdgeType-> <-EdgeType- -EdgeType-
    // Determine direction based on arrow combination
    final MatchPathItem pathItem = new MatchPathItem(-1);
    final boolean leftIsArrow = ctx.ARROW_LEFT() != null;
    final boolean rightIsArrow = ctx.ARROW_RIGHT() != null;

    String methodName;
    if (leftIsArrow && !rightIsArrow) {
      // <-EdgeType- = incoming
      methodName = "in";
    } else if (!leftIsArrow && rightIsArrow) {
      // -EdgeType-> = outgoing
      methodName = "out";
    } else {
      // -EdgeType- = bidirectional
      methodName = "both";
    }

    // Create a MethodCall with the edge type as parameter
    final MethodCall method = new MethodCall(-1);
    method.methodName = new Identifier(methodName);

    // Edge type becomes the parameter (as a string literal Expression)
    final Identifier edgeLabel = (Identifier) visit(ctx.identifier());
    final BaseExpression baseExpr = new BaseExpression(-1);
    baseExpr.string = "'" + edgeLabel.getStringValue() + "'";

    final Expression edgeTypeParam = new Expression(-1);
    edgeTypeParam.mathExpression = baseExpr;
    method.params.add(edgeTypeParam);

    pathItem.setMethod(method);

    // Add properties if present: {as:x, where:...}
    if (ctx.matchProperties() != null) {
      pathItem.setFilter((MatchFilter) visit(ctx.matchProperties()));
    }

    return pathItem;
  }

  /**
   * Nested match method visitor.
   * Handles methods within .(chain) syntax including arrow notation.
   * Grammar alternatives:
   * - identifier LPAREN ... RPAREN matchProperties? (function call without chains)
   * - DOT identifier LPAREN ... RPAREN matchProperties? (explicit dot prefix)
   * - identifier matchProperties? (simple method name)
   * - (MINUS | ARROW_LEFT) identifier (MINUS | ARROW_RIGHT) matchProperties?
   * - DECR GT matchProperties?
   * - ARROW_LEFT MINUS matchProperties?
   * - DECR matchProperties?
   */
  @Override
  public MatchPathItem visitNestedMatchMethod(final SQLParser.NestedMatchMethodContext ctx) {
    final MatchPathItem pathItem = new MatchPathItem(-1);

    // Check for function call syntax: identifier(...) or .identifier(...)
    if (ctx.LPAREN() != null) {
      final Identifier methodName = (Identifier) visit(ctx.identifier());
      final MethodCall method = new MethodCall(-1);
      method.methodName = methodName;

      // Add parameters if present
      if (CollectionUtils.isNotEmpty(ctx.expression())) {
        for (final SQLParser.ExpressionContext exprCtx : ctx.expression()) {
          method.params.add((Expression) visit(exprCtx));
        }
      }

      pathItem.setMethod(method);

      // Add matchProperties if present
      if (ctx.matchProperties() != null) {
        pathItem.setFilter((MatchFilter) visit(ctx.matchProperties()));
      }

      return pathItem;
    }

    // Check for arrow syntax alternatives
    if (ctx.MINUS() != null || ctx.ARROW_LEFT() != null || ctx.DECR() != null) {
      // Determine method name based on arrow direction
      String methodName;
      if (ctx.ARROW_LEFT() != null && ctx.MINUS() != null && ctx.identifier() == null) {
        // <-- : anonymous incoming edge
        methodName = "in";
      } else if (ctx.DECR() != null && ctx.GT() != null) {
        // --> : anonymous outgoing edge
        methodName = "out";
      } else if (ctx.DECR() != null && ctx.GT() == null) {
        // -- : anonymous bidirectional edge
        methodName = "both";
      } else if (ctx.ARROW_LEFT() != null && ctx.identifier() != null) {
        // <-EdgeType- : incoming edge with type
        methodName = "in";
      } else {
        // -EdgeType-> or -EdgeType- : outgoing or bidirectional edge with type
        if (ctx.ARROW_RIGHT() != null) {
          methodName = "out";
        } else {
          methodName = "both";
        }
      }

      // Create a MethodCall
      final MethodCall method = new MethodCall(-1);
      method.methodName = new Identifier(methodName);

      // Edge type becomes the parameter if identifier is present
      if (ctx.identifier() != null) {
        final Identifier edgeLabel = (Identifier) visit(ctx.identifier());
        final BaseExpression baseExpr = new BaseExpression(-1);
        baseExpr.string = "'" + edgeLabel.getStringValue() + "'";

        final Expression edgeTypeParam = new Expression(-1);
        edgeTypeParam.mathExpression = baseExpr;
        method.params.add(edgeTypeParam);
      }

      pathItem.setMethod(method);

      // Add properties if present
      if (ctx.matchProperties() != null) {
        pathItem.setFilter((MatchFilter) visit(ctx.matchProperties()));
      }
    } else if (ctx.identifier() != null) {
      // Simple identifier without parentheses: methodName{...}
      final Identifier methodName = (Identifier) visit(ctx.identifier());
      final MethodCall method = new MethodCall(-1);
      method.methodName = methodName;
      pathItem.setMethod(method);

      // Add matchProperties if present
      if (ctx.matchProperties() != null) {
        pathItem.setFilter((MatchFilter) visit(ctx.matchProperties()));
      }
    }

    return pathItem;
  }

  /**
   * Match method call visitor.
   * Handles: functionCall | identifier (method name part of .out('Friend'))
   */
  @Override
  public Object visitMatchMethodCall(final SQLParser.MatchMethodCallContext ctx) {
    if (ctx.functionCall() != null) {
      return visit(ctx.functionCall());
    }
    if (ctx.identifier() != null) {
      return visit(ctx.identifier());
    }
    return null;
  }

  /**
   * Match properties visitor.
   * Handles: {type: Person, as: p, where: (name='John')}
   */
  @Override
  public MatchFilter visitMatchProperties(final SQLParser.MatchPropertiesContext ctx) {
    final MatchFilter filter = new MatchFilter(-1);

    // Handle filter items in braces: {type: Person, as: p, where: (name='John')}
    if (CollectionUtils.isNotEmpty(ctx.matchFilterItem())) {
      for (final SQLParser.MatchFilterItemContext itemCtx : ctx.matchFilterItem()) {
        final MatchFilterItem item = (MatchFilterItem) visit(itemCtx);
        filter.items.add(item);
      }
    }

    return filter;
  }

  /**
   * Match filter visitor.
   * Handles: identifier | functionCall | {type: Person, as: p}
   */
  @Override
  public MatchFilter visitMatchFilter(final SQLParser.MatchFilterContext ctx) {
    final MatchFilter filter = new MatchFilter(-1);

    // Handle function call (e.g., out(), in(), both())
    if (ctx.functionCall() != null) {
      // For function calls in MATCH, we don't store them in filter items
      // They will be handled by execution planner
      // Just return an empty filter for now
      return filter;
    }

    // Handle identifier (e.g., edge type name)
    if (ctx.identifier() != null) {
      // For simple identifiers, create a filter item
      // This represents an edge type or pattern name
      return filter;
    }

    // Handle properties: {type: Person, as: p, where: (name='John')}
    if (ctx.matchProperties() != null) {
      return (MatchFilter) visit(ctx.matchProperties());
    }

    return filter;
  }

  /**
   * Match filter item key visitor.
   * Handles property names in match filter items (including keywords).
   */
  @Override
  public Identifier visitMatchFilterItemKey(final SQLParser.MatchFilterItemKeyContext ctx) {
    if (ctx.identifier() != null) {
      return (Identifier) visit(ctx.identifier());
    }

    // Handle keyword tokens as identifiers
    final String keywordText;
    if (ctx.TYPE() != null) {
      keywordText = "type";
    } else if (ctx.TYPES() != null) {
      keywordText = "types";
    } else if (ctx.BUCKET() != null) {
      keywordText = "bucket";
    } else if (ctx.AS() != null) {
      keywordText = "as";
    } else if (ctx.WHERE() != null) {
      keywordText = "where";
    } else if (ctx.WHILE() != null) {
      keywordText = "while";
    } else if (ctx.MAXDEPTH() != null) {
      keywordText = "maxdepth";
    } else if (ctx.OPTIONAL() != null) {
      keywordText = "optional";
    } else if (ctx.RID() != null) {
      keywordText = "rid";
    } else if (ctx.PATH_ALIAS() != null) {
      keywordText = "pathAlias";
    } else if (ctx.DEPTH_ALIAS() != null) {
      keywordText = "depthAlias";
    } else {
      throw new IllegalStateException("Unexpected matchFilterItemKey token");
    }

    return new Identifier(keywordText);
  }

  /**
   * Match filter item visitor.
   * Handles individual filter properties: identifier COLON expression
   * Examples: type: Person, as: p, where: (name='John'), maxdepth: 3
   */
  @Override
  public MatchFilterItem visitMatchFilterItem(final SQLParser.MatchFilterItemContext ctx) {
    final MatchFilterItem item = new MatchFilterItem(-1);

    // Handle special case for BUCKET_IDENTIFIER (bucket:name) and BUCKET_NUMBER_IDENTIFIER (bucket:123)
    if (ctx.BUCKET_IDENTIFIER() != null) {
      // Extract the bucket name from the token (after the colon)
      final String tokenText = ctx.BUCKET_IDENTIFIER().getText();
      final String bucketName = tokenText.substring(7); // Remove "bucket:" prefix
      item.bucketName = new Identifier(bucketName);

      // Expression is optional for these special tokens
      if (ctx.expression() != null) {
        // This shouldn't normally happen for bucket identifier, but handle it
      }
      return item;
    }

    if (ctx.BUCKET_NUMBER_IDENTIFIER() != null) {
      // Extract the bucket number from the token (after the colon)
      final String tokenText = ctx.BUCKET_NUMBER_IDENTIFIER().getText();
      final String bucketNumStr = tokenText.substring(7); // Remove "bucket:" prefix
      item.bucketId = new PInteger(Integer.parseInt(bucketNumStr));
      return item;
    }

    // Get the key (could be identifier or keyword token)
    final Identifier key;
    final Object keyObj = visit(ctx.matchFilterItemKey());
    if (keyObj instanceof Identifier) {
      key = (Identifier) keyObj;
    } else {
      // Shouldn't happen with current grammar, but handle gracefully
      throw new IllegalStateException("Unexpected matchFilterItemKey type: " +
          (keyObj != null ? keyObj.getClass().getName() : "null"));
    }

    final Object valueObj = visit(ctx.expression());

    final String keyName = key.getStringValue().toLowerCase();

    // Map the key to the appropriate field
    switch (keyName) {
      case "type":
        item.typeName = (Expression) valueObj;
        break;
      case "types":
        item.typeNames = (Expression) valueObj;
        break;
      case "bucket":
        // Could be bucket name (string) or bucket id (integer)
        if (valueObj instanceof PInteger) {
          item.bucketId = (PInteger) valueObj;
        } else if (valueObj instanceof final BaseExpression baseExpr) {
          if (baseExpr.number instanceof PInteger) {
            item.bucketId = (PInteger) baseExpr.number;
          } else {
            item.bucketName = new Identifier(valueObj.toString());
          }
        } else {
          item.bucketName = new Identifier(valueObj.toString());
        }
        break;
      case "rid":
        if (valueObj instanceof Rid) {
          item.rid = (Rid) valueObj;
        }
        // RID might be embedded in a complex expression, for now just skip if not direct Rid
        break;
      case "as":
        // Extract identifier name from the expression
        if (valueObj instanceof BaseIdentifier) {
          // Create Identifier from the string representation
          final StringBuilder sb = new StringBuilder();
          ((BaseIdentifier) valueObj).toString(Collections.emptyMap(), sb);
          item.alias = new Identifier(sb.toString());
        } else if (valueObj instanceof final BaseExpression baseExpr) {
          if (baseExpr.identifier != null) {
            final StringBuilder sb = new StringBuilder();
            baseExpr.identifier.toString(Collections.emptyMap(), sb);
            item.alias = new Identifier(sb.toString());
          } else {
            item.alias = new Identifier(valueObj.toString().replace("'", ""));
          }
        } else {
          // If value is a string literal, extract the alias name
          item.alias = new Identifier(valueObj.toString().replace("'", ""));
        }
        break;
      case "where":
        if (valueObj instanceof WhereClause) {
          item.filter = (WhereClause) valueObj;
        } else if (valueObj instanceof BooleanExpression) {
          final WhereClause whereClause = new WhereClause(-1);
          whereClause.baseExpression = (BooleanExpression) valueObj;
          item.filter = whereClause;
        } else if (valueObj instanceof final Expression expr) {
          // Expression containing the WHERE condition: where:(name = 'n1')
          // Check if it's a BaseExpression containing a boolean value like (true) or (false)
          if (expr.mathExpression instanceof final BaseExpression baseExpr) {
            if (baseExpr.expression != null && baseExpr.expression.booleanValue != null) {
              // Handle boolean literals wrapped in BaseExpression: where: (true)
              final WhereClause whereClause = new WhereClause(-1);
              final BooleanExpression boolExpr = createBooleanLiteral(baseExpr.expression.booleanValue);
              whereClause.baseExpression = boolExpr;
              item.filter = whereClause;
            }
          }
          // For parenthesized conditions like (name = 'n1'), mathExpression is a ParenthesisExpression
          else if (expr.mathExpression instanceof final ParenthesisExpression parenExpr) {
            // Extract the inner expression's whereCondition from the parentheses
            if (parenExpr.expression != null && parenExpr.expression.whereCondition != null) {
              item.filter = parenExpr.expression.whereCondition;
            } else if (parenExpr.expression != null && parenExpr.expression.booleanValue != null) {
              // Handle boolean literals like (true) or (false)
              final WhereClause whereClause = new WhereClause(-1);
              final BooleanExpression boolExpr = createBooleanLiteral(parenExpr.expression.booleanValue);
              whereClause.baseExpression = boolExpr;
              item.filter = whereClause;
            }
          } else if (expr.whereCondition != null) {
            // Direct WHERE clause
            item.filter = expr.whereCondition;
          } else if (expr.booleanValue != null) {
            // Direct boolean literal
            final WhereClause whereClause = new WhereClause(-1);
            final BooleanExpression boolExpr = createBooleanLiteral(expr.booleanValue);
            whereClause.baseExpression = boolExpr;
            item.filter = whereClause;
          }
        }
        break;
      case "while":
        if (valueObj instanceof WhereClause) {
          item.whileCondition = (WhereClause) valueObj;
        } else if (valueObj instanceof BooleanExpression) {
          final WhereClause whereClause = new WhereClause(-1);
          whereClause.baseExpression = (BooleanExpression) valueObj;
          item.whileCondition = whereClause;
        } else if (valueObj instanceof final Expression expr) {
          // Expression containing the WHILE condition
          // Check if it's a BaseExpression containing a boolean value like (true) or (false)
          if (expr.mathExpression instanceof final BaseExpression baseExpr) {
            if (baseExpr.expression != null && baseExpr.expression.booleanValue != null) {
              // Handle boolean literals wrapped in BaseExpression: while: (true)
              final WhereClause whereClause = new WhereClause(-1);
              final BooleanExpression boolExpr = createBooleanLiteral(baseExpr.expression.booleanValue);
              whereClause.baseExpression = boolExpr;
              item.whileCondition = whereClause;
            }
          }
          // For parenthesized conditions, mathExpression is a ParenthesisExpression
          else if (expr.mathExpression instanceof final ParenthesisExpression parenExpr) {
            // Extract the inner expression's whereCondition from the parentheses
            if (parenExpr.expression != null && parenExpr.expression.whereCondition != null) {
              item.whileCondition = parenExpr.expression.whereCondition;
            } else if (parenExpr.expression != null && parenExpr.expression.booleanValue != null) {
              // Handle boolean literals like (true) or (false)
              final WhereClause whereClause = new WhereClause(-1);
              final BooleanExpression boolExpr = createBooleanLiteral(parenExpr.expression.booleanValue);
              whereClause.baseExpression = boolExpr;
              item.whileCondition = whereClause;
            }
          } else if (expr.whereCondition != null) {
            // Direct WHERE clause (used for WHILE)
            item.whileCondition = expr.whereCondition;
          } else if (expr.booleanValue != null) {
            // Direct boolean literal
            final WhereClause whereClause = new WhereClause(-1);
            final BooleanExpression boolExpr = createBooleanLiteral(expr.booleanValue);
            whereClause.baseExpression = boolExpr;
            item.whileCondition = whereClause;
          }
        }
        break;
      case "maxdepth":
        if (valueObj instanceof PInteger) {
          item.maxDepth = (PInteger) valueObj;
        } else if (valueObj instanceof final BaseExpression baseExpr) {
          if (baseExpr.number instanceof PInteger) {
            item.maxDepth = (PInteger) baseExpr.number;
          }
        } else if (valueObj instanceof final Expression expr) {
          if (expr.mathExpression instanceof final BaseExpression baseExpr) {
            if (baseExpr.number instanceof PInteger) {
              item.maxDepth = (PInteger) baseExpr.number;
            }
          }
        }
        break;
      case "depth":
        // depth can be an ArrayRangeSelector like depth: [0..3]
        if (valueObj instanceof ArrayRangeSelector) {
          item.depth = (ArrayRangeSelector) valueObj;
        } else if (valueObj instanceof BaseExpression) {
          // Try to extract range from expression
          // For now, just skip if it's not already an ArrayRangeSelector
        }
        break;
      case "optional":
        item.optional = true;
        break;
      case "depthalias":
        // Extract identifier name from the expression
        if (valueObj instanceof BaseIdentifier) {
          final StringBuilder sb = new StringBuilder();
          ((BaseIdentifier) valueObj).toString(Collections.emptyMap(), sb);
          item.depthAlias = new Identifier(sb.toString());
        } else if (valueObj instanceof final BaseExpression baseExpr) {
          if (baseExpr.identifier != null) {
            final StringBuilder sb = new StringBuilder();
            baseExpr.identifier.toString(Collections.emptyMap(), sb);
            item.depthAlias = new Identifier(sb.toString());
          }
        } else if (valueObj instanceof final Expression expr) {
          if (expr.mathExpression instanceof final BaseExpression baseExpr) {
            if (baseExpr.identifier != null) {
              final StringBuilder sb = new StringBuilder();
              baseExpr.identifier.toString(Collections.emptyMap(), sb);
              item.depthAlias = new Identifier(sb.toString());
            }
          }
        }
        break;
      case "pathalias":
        // Extract identifier name from the expression
        if (valueObj instanceof BaseIdentifier) {
          final StringBuilder sb = new StringBuilder();
          ((BaseIdentifier) valueObj).toString(Collections.emptyMap(), sb);
          item.pathAlias = new Identifier(sb.toString());
        } else if (valueObj instanceof final BaseExpression baseExpr) {
          if (baseExpr.identifier != null) {
            final StringBuilder sb = new StringBuilder();
            baseExpr.identifier.toString(Collections.emptyMap(), sb);
            item.pathAlias = new Identifier(sb.toString());
          }
        } else if (valueObj instanceof final Expression expr) {
          if (expr.mathExpression instanceof final BaseExpression baseExpr) {
            if (baseExpr.identifier != null) {
              final StringBuilder sb = new StringBuilder();
              baseExpr.identifier.toString(Collections.emptyMap(), sb);
              item.pathAlias = new Identifier(sb.toString());
            }
          }
        }
        break;
      default:
        // Unknown key - ignore or log warning
        break;
    }

    return item;
  }

  /**
   * TRAVERSE statement visitor.
   * Maps to TraverseStatement AST class.
   */
  @Override
  public TraverseStatement visitTraverseStmt(final SQLParser.TraverseStmtContext ctx) {
    final TraverseStatement stmt = new TraverseStatement(-1);

    // Get the traverseStatement context from the labeled alternative
    final SQLParser.TraverseStatementContext traverseCtx = ctx.traverseStatement();

    // Parse projection items (optional)
    final List<TraverseProjectionItem> projections = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(traverseCtx.traverseProjectionItem())) {
      for (final SQLParser.TraverseProjectionItemContext itemCtx : traverseCtx.traverseProjectionItem()) {
        final TraverseProjectionItem item = (TraverseProjectionItem) visit(itemCtx);
        projections.add(item);
      }
    }
    stmt.setProjections(projections);

    // FROM clause (required)
    if (traverseCtx.fromClause() != null) {
      stmt.setTarget((FromClause) visit(traverseCtx.fromClause()));
    }

    // MAXDEPTH clause (optional)
    if (traverseCtx.pInteger() != null) {
      stmt.setMaxDepth((PInteger) visit(traverseCtx.pInteger()));
    }

    // WHILE clause (optional)
    if (traverseCtx.whereClause() != null) {
      stmt.setWhileClause((WhereClause) visit(traverseCtx.whereClause()));
    }

    // LIMIT clause (optional)
    if (traverseCtx.limit() != null) {
      stmt.limit = (Limit) visit(traverseCtx.limit());
    }

    // STRATEGY clause (optional)
    if (traverseCtx.DEPTH_FIRST() != null) {
      stmt.setStrategy(TraverseStatement.Strategy.DEPTH_FIRST);
    } else if (traverseCtx.BREADTH_FIRST() != null) {
      stmt.setStrategy(TraverseStatement.Strategy.BREADTH_FIRST);
    }

    return stmt;
  }

  /**
   * Traverse projection item visitor.
   * Handles individual projection items in TRAVERSE statement.
   */
  @Override
  public TraverseProjectionItem visitTraverseProjectionItem(final SQLParser.TraverseProjectionItemContext ctx) {
    final TraverseProjectionItem item = new TraverseProjectionItem(-1);

    // Expression (required) - convert to BaseIdentifier
    if (ctx.expression() != null) {
      final Object exprObj = visit(ctx.expression());
      // TraverseProjectionItem expects a BaseIdentifier
      if (exprObj instanceof BaseIdentifier) {
        item.base = (BaseIdentifier) exprObj;
      } else if (exprObj instanceof Expression expr) {
        // Expression.mathExpression contains the actual expression data
        if (expr.mathExpression instanceof BaseExpression baseExpr) {
          if (baseExpr.identifier != null) {
            item.base = baseExpr.identifier;
            // Also carry over any modifier from the BaseExpression
            item.modifier = baseExpr.modifier;
          }
        }
      } else if (exprObj instanceof BaseExpression baseExpr) {
        // Direct BaseExpression (shouldn't happen, but handle it)
        if (baseExpr.identifier != null) {
          item.base = baseExpr.identifier;
          item.modifier = baseExpr.modifier;
        }
      }
      // If we still don't have a base, create a simple one
      if (item.base == null) {
        item.base = new BaseIdentifier(-1);
      }
    }

    // Optional alias is handled by the modifier system in TraverseProjectionItem
    // For now, we'll let the execution planner handle alias mapping

    return item;
  }

  public Projection visitProjection(final SQLParser.ProjectionContext ctx) {
    final Projection projection = new Projection(-1);

    // DISTINCT flag
    final boolean distinct = ctx.DISTINCT() != null;

    // Projection items
    final List<ProjectionItem> items = new ArrayList<>();

    // Handle * wildcard
    if (ctx.STAR() != null) {
      // Add wildcard projection item
      final ProjectionItem item = new ProjectionItem(-1);
      item.setAll(true);
      items.add(item);
    }

    // Handle projection item list
    if (CollectionUtils.isNotEmpty(ctx.projectionItem())) {
      for (final SQLParser.ProjectionItemContext itemCtx : ctx.projectionItem()) {
        items.add((ProjectionItem) visit(itemCtx));
      }
    }

    // Set items and distinct flag directly
    projection.items = items;
    projection.distinct = distinct;

    return projection;
  }

  /**
   * Projection item visitor.
   * Handles individual projection items with optional aliases.
   */
  @Override
  public ProjectionItem visitProjectionItem(final SQLParser.ProjectionItemContext ctx) {
    final ProjectionItem item = new ProjectionItem(-1);

    // BANG (exclude flag) - e.g., !surname
    if (ctx.BANG() != null)
      item.exclude = true;

    // Expression
    item.expression = (Expression) visit(ctx.expression());

    // Nested projection (e.g., {field1, field2})
    if (ctx.nestedProjection() != null) {
      item.nestedProjection = (NestedProjection) visit(ctx.nestedProjection());
    } else {
      // The nested projection might have been parsed as a modifier on the expression
      // (e.g., elem1:{*} where :{*} becomes a modifier). Extract it and remove from
      // the modifier chain so it doesn't appear in the expression's toString().
      item.nestedProjection = extractNestedProjectionFromExpression(item.expression);
    }
    // Note: For array concat expressions (||), nested projections are handled directly
    // on the ArrayConcatExpressionElement objects in visitArrayConcat, not here.

    // Alias (AS identifier)
    if (ctx.identifier() != null)
      item.alias = (Identifier) visit(ctx.identifier());

    return item;
  }

  /**
   * Extracts nested projection from expression's modifier chain if present.
   * Nested projections can be parsed as modifiers when they appear after function calls,
   * e.g., list({'x':1}):{x} where :{x} becomes a modifier instead of a separate nested projection.
   * Also handles binary operations like list({'x':1}):{x} || [] where the nested projection
   * is on the left operand.
   */
  private NestedProjection extractNestedProjectionFromExpression(final Expression expression) {
    if (expression == null) {
      return null;
    }

    // Try to extract from mathExpression if it's a BaseExpression
    if (expression.mathExpression instanceof BaseExpression baseExpr) {
      final NestedProjection result = extractNestedProjectionFromBaseExpression(baseExpr);
      if (result != null) {
        return result;
      }
    }

    // Try to extract from arrayConcatExpression (|| operator)
    if (expression.arrayConcatExpression != null) {
      final ArrayConcatExpression arrayConcat = expression.arrayConcatExpression;
      final List<ArrayConcatExpressionElement> children = arrayConcat.getChildExpressions();
      if (children != null && !children.isEmpty()) {
        // Check the first child expression
        final ArrayConcatExpressionElement firstElement = children.get(0);
        // Recursively extract from the first element
        final NestedProjection result = extractNestedProjectionFromExpression(firstElement);
        return result;
      }
    }

    return null;
  }

  /**
   * Extracts nested projection from an Expression's modifier chain.
   * Overloaded version that handles Expression wrappers.
   */
  private NestedProjection extractNestedProjectionFromBaseExpression(final Expression expression) {
    if (expression == null || expression.mathExpression == null) {
      return null;
    }

    if (expression.mathExpression instanceof BaseExpression baseExpr) {
      return extractNestedProjectionFromBaseExpression(baseExpr);
    }

    return null;
  }

  /**
   * Extracts nested projection from a BaseExpression's modifier chain.
   */
  private NestedProjection extractNestedProjectionFromBaseExpression(final BaseExpression baseExpr) {
    if (baseExpr == null || baseExpr.modifier == null) {
      return null;
    }

    // Find the last modifier with nestedProjection
    Modifier prev = null;
    Modifier current = baseExpr.modifier;
    Modifier lastNested = null;
    Modifier prevNested = null;

    while (current != null) {
      if (current.nestedProjection != null) {
        lastNested = current;
        prevNested = prev;
      }
      prev = current;
      current = current.next;
    }

    // If we found a nested projection modifier, extract it
    if (lastNested != null) {
      // Remove it from the chain
      if (prevNested != null) {
        prevNested.next = lastNested.next;
      } else {
        // It's the first modifier
        baseExpr.modifier = lastNested.next;
      }

      return lastNested.nestedProjection;
    }

    return null;
  }

  /**
   * Nested projection visitor (e.g., :{field1, field2}).
   */
  @Override
  public NestedProjection visitNestedProjection(final SQLParser.NestedProjectionContext ctx) {
    final NestedProjection nestedProjection = new NestedProjection(-1);

    @SuppressWarnings("unchecked") final List<NestedProjectionItem> includeItems =
        nestedProjection.includeItems;
    @SuppressWarnings("unchecked") final List<NestedProjectionItem> excludeItems =
        nestedProjection.excludeItems;

    for (final SQLParser.NestedProjectionItemContext itemCtx : ctx.nestedProjectionItem()) {
      final NestedProjectionItem item = (NestedProjectionItem) visit(itemCtx);
      final boolean isExclude = item.exclude;
      final boolean isStar = item.star;

      if (isStar)
        nestedProjection.starItem = item;
      else if (isExclude)
        excludeItems.add(item);
      else
        includeItems.add(item);
    }

    return nestedProjection;
  }

  /**
   * Nested projection item visitor.
   */
  @Override
  public NestedProjectionItem visitNestedProjectionItem(final SQLParser.NestedProjectionItemContext ctx) {
    final NestedProjectionItem item = new NestedProjectionItem(-1);

    // STAR
    if (ctx.STAR() != null && ctx.expression() == null)
      item.star = true;

    // BANG (exclude)
    if (ctx.BANG() != null)
      item.exclude = true;

    // Expression
    if (ctx.expression() != null)
      item.expression = (Expression) visit(ctx.expression());

    // Right wildcard (expression followed by *)
    if (ctx.expression() != null && ctx.STAR() != null)
      item.rightWildcard = true;

    // Nested expansion
    if (ctx.nestedProjection() != null)
      item.expansion = (NestedProjection) visit(ctx.nestedProjection());

    // Alias
    if (ctx.identifier() != null)
      item.alias = (Identifier) visit(ctx.identifier());

    return item;
  }

  /**
   * FROM clause visitor.
   */
  @Override
  public FromClause visitFromClause(final SQLParser.FromClauseContext ctx) {
    final FromClause fromClause = new FromClause(-1);

    // FROM item - get the first one (JavaCC uses single item)
    if (ctx.fromItem() != null) {
      fromClause.item = (FromItem) visit(ctx.fromItem());
    }

    return fromClause;
  }

  /**
   * FROM identifier visitor (e.g., FROM User or FROM User AS u).
   */
  @Override
  public FromItem visitFromIdentifier(final SQLParser.FromIdentifierContext ctx) {
    final FromItem fromItem = new FromItem(-1);

    // First identifier is the main FROM target
    if (CollectionUtils.isNotEmpty(ctx.identifier())) {
      fromItem.identifier = (Identifier) visit(ctx.identifier(0));
    }

    // Handle modifiers if present - chain them using modifier.next
    if (CollectionUtils.isNotEmpty(ctx.modifier())) {
      Modifier firstModifier = null;
      Modifier currentModifier = null;

      for (final SQLParser.ModifierContext modCtx : ctx.modifier()) {
        final Modifier modifier = (Modifier) visit(modCtx);

        if (firstModifier == null) {
          firstModifier = modifier;
          currentModifier = modifier;
        } else {
          currentModifier.next = modifier;
          currentModifier = modifier;
        }
      }

      fromItem.modifier = firstModifier;
    }

    // Handle alias (second identifier after AS)
    if (ctx.identifier().size() > 1) {
      fromItem.alias = (Identifier) visit(ctx.identifier(1));
    }

    return fromItem;
  }

  /**
   * FROM RIDs visitor (e.g., FROM #12:1, #12:2).
   */
  @Override
  public FromItem visitFromRids(final SQLParser.FromRidsContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    fromItem.rids = new ArrayList<>();

    for (final SQLParser.RidContext ridCtx : ctx.rid()) {
      fromItem.rids.add((Rid) visit(ridCtx));
    }

    return fromItem;
  }

  /**
   * FROM RID array visitor (e.g., FROM [#12:1, #12:2]).
   */
  @Override
  public FromItem visitFromRidArray(final SQLParser.FromRidArrayContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    fromItem.rids = new ArrayList<>();

    for (final SQLParser.RidContext ridCtx : ctx.rid()) {
      fromItem.rids.add((Rid) visit(ridCtx));
    }

    return fromItem;
  }

  /**
   * FROM empty array visitor (e.g., FROM [] or TO []).
   * Returns a FromItem with an empty rids list, which produces zero results.
   */
  @Override
  public FromItem visitFromEmptyArray(final SQLParser.FromEmptyArrayContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    fromItem.rids = new ArrayList<>();
    return fromItem;
  }

  /**
   * FROM parameter array visitor (e.g., FROM [:param1, :param2]).
   */
  @Override
  public FromItem visitFromParamArray(final SQLParser.FromParamArrayContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    fromItem.inputParams = new ArrayList<>();

    for (final SQLParser.InputParameterContext paramCtx : ctx.inputParameter()) {
      fromItem.inputParams.add((InputParameter) visit(paramCtx));
    }

    return fromItem;
  }

  /**
   * FROM single parameter visitor (e.g., FROM :rid or FROM ?).
   */
  @Override
  public FromItem visitFromParam(final SQLParser.FromParamContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    // Use inputParam (singular) for single parameter - this is what TraverseExecutionPlanner expects
    fromItem.inputParam = (InputParameter) visit(ctx.inputParameter());
    return fromItem;
  }

  /**
   * FROM bucket visitor (e.g., FROM bucket:users or bucket:123).
   * Grammar: BUCKET_IDENTIFIER | BUCKET_NUMBER_IDENTIFIER
   */
  @Override
  public FromItem visitFromBucket(final SQLParser.FromBucketContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    final Bucket bucket = new Bucket(-1);

    if (ctx.BUCKET_IDENTIFIER() != null) {
      // BUCKET:name format
      final String text = ctx.BUCKET_IDENTIFIER().getText();
      bucket.bucketName = text.substring("bucket:".length());
    } else if (ctx.BUCKET_NUMBER_IDENTIFIER() != null) {
      // BUCKET:123 format
      final String text = ctx.BUCKET_NUMBER_IDENTIFIER().getText();
      final String bucketNumberStr = text.substring("bucket:".length());
      bucket.bucketNumber = Integer.parseInt(bucketNumberStr);
    }

    fromItem.bucket = bucket;
    return fromItem;
  }

  /**
   * FROM bucket parameter visitor (e.g., FROM bucket::paramName, FROM bucket:?).
   * Grammar: BUCKET_NAMED_PARAM | BUCKET_POSITIONAL_PARAM
   */
  @Override
  public FromItem visitFromBucketParameter(final SQLParser.FromBucketParameterContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    final Bucket bucket = new Bucket(-1);

    // Extract the parameter from the token
    if (ctx.BUCKET_NAMED_PARAM() != null) {
      // bucket::paramName - extract the parameter name after "bucket::"
      final String text = ctx.BUCKET_NAMED_PARAM().getText();
      final String paramName = text.substring("bucket::".length());
      final NamedParameter param = new NamedParameter(-1);
      param.paramName = paramName;
      param.paramNumber = positionalParamCounter++;
      bucket.inputParam = param;
    } else if (ctx.BUCKET_POSITIONAL_PARAM() != null) {
      // bucket:? - create a positional parameter
      final PositionalParameter param = new PositionalParameter(-1);
      param.paramNumber = positionalParamCounter++;
      bucket.inputParam = param;
    }

    fromItem.bucket = bucket;
    return fromItem;
  }

  /**
   * FROM bucket list visitor (e.g., FROM bucket:[users, admins]).
   */
  @Override
  public FromItem visitFromBucketList(final SQLParser.FromBucketListContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    fromItem.bucketList = (BucketList) visit(ctx.bucketList());
    return fromItem;
  }

  /**
   * FROM index visitor (e.g., FROM index:User.name).
   */
  @Override
  public FromItem visitFromIndex(final SQLParser.FromIndexContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    fromItem.index = (IndexIdentifier) visit(ctx.indexIdentifier());
    return fromItem;
  }

  /**
   * FROM schema visitor (e.g., FROM schema:User).
   */
  @Override
  public FromItem visitFromSchema(final SQLParser.FromSchemaContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    fromItem.schema = (SchemaIdentifier) visit(ctx.schemaIdentifier());
    return fromItem;
  }

  /**
   * FROM subquery visitor (e.g., FROM (SELECT * FROM User)).
   */
  @Override
  public FromItem visitFromSubquery(final SQLParser.FromSubqueryContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    fromItem.statement = (Statement) visit(ctx.statement());

    // Handle modifiers if present - chain them using modifier.next
    if (CollectionUtils.isNotEmpty(ctx.modifier())) {
      Modifier firstModifier = null;
      Modifier currentModifier = null;

      for (final SQLParser.ModifierContext modCtx : ctx.modifier()) {
        final Modifier modifier = (Modifier) visit(modCtx);

        if (firstModifier == null) {
          firstModifier = modifier;
          currentModifier = modifier;
        } else {
          currentModifier.next = modifier;
          currentModifier = modifier;
        }
      }

      fromItem.modifier = firstModifier;
    }

    // Handle alias if present
    if (ctx.identifier() != null) {
      fromItem.alias = (Identifier) visit(ctx.identifier());
    }

    return fromItem;
  }

  /**
   * WHERE clause visitor.
   * Maps to WhereClause which contains a BooleanExpression.
   */
  @Override
  public WhereClause visitWhereClause(final SQLParser.WhereClauseContext ctx) {
    final WhereClause whereClause = new WhereClause(-1);

    // The orBlock is the root of the boolean expression tree
    whereClause.baseExpression = (BooleanExpression) visit(ctx.orBlock());

    return whereClause;
  }

  /**
   * OR block visitor (logical OR of AND blocks).
   */
  @Override
  public BooleanExpression visitOrBlock(final SQLParser.OrBlockContext ctx) {
    // orBlock: andBlock (OR andBlock)*
    final List<SQLParser.AndBlockContext> andBlocks = ctx.andBlock();

    if (andBlocks.size() == 1) {
      // Single AND block, no OR needed
      return (BooleanExpression) visit(andBlocks.get(0));
    }

    // Multiple AND blocks connected with OR
    final OrBlock orBlock = new OrBlock(-1);
    final List<BooleanExpression> subBlocks = new ArrayList<>();
    for (final SQLParser.AndBlockContext andCtx : andBlocks) {
      subBlocks.add((BooleanExpression) visit(andCtx));
    }
    orBlock.subBlocks = subBlocks;

    return orBlock;
  }

  /**
   * AND block visitor (logical AND of NOT blocks).
   */
  @Override
  public BooleanExpression visitAndBlock(final SQLParser.AndBlockContext ctx) {
    // andBlock: notBlock (AND notBlock)*
    final List<SQLParser.NotBlockContext> notBlocks = ctx.notBlock();

    if (notBlocks.size() == 1) {
      // Single NOT block, no AND needed
      return (BooleanExpression) visit(notBlocks.get(0));
    }

    // Multiple NOT blocks connected with AND
    final AndBlock andBlock = new AndBlock(-1);
    for (final SQLParser.NotBlockContext notCtx : notBlocks) {
      andBlock.subBlocks.add((BooleanExpression) visit(notCtx));
    }

    return andBlock;
  }

  /**
   * NOT block visitor (optional NOT prefix).
   */
  @Override
  public BooleanExpression visitNotBlock(final SQLParser.NotBlockContext ctx) {
    final BooleanExpression condition = (BooleanExpression) visit(ctx.conditionBlock());

    if (ctx.NOT() != null) {
      // Apply NOT operator
      final NotBlock notBlock = new NotBlock(-1);
      notBlock.sub = condition;
      notBlock.negate = true;
      return notBlock;
    }

    return condition;
  }

  /**
   * Comparison condition (e.g., a = b, x < y).
   */
  @Override
  public BinaryCondition visitComparisonCondition(final SQLParser.ComparisonConditionContext ctx) {
    final BinaryCondition condition = new BinaryCondition(-1);

    // Left expression
    condition.left = (Expression) visit(ctx.expression(0));

    // Operator
    condition.operator = mapComparisonOperator(ctx.comparisonOperator());

    // Right expression
    condition.right = (Expression) visit(ctx.expression(1));

    return condition;
  }

  /**
   * IS NULL / IS NOT NULL condition.
   * Grammar: expression IS NOT? NULL
   */
  @Override
  public BooleanExpression visitIsNullCondition(final SQLParser.IsNullConditionContext ctx) {
    final Expression expr = (Expression) visit(ctx.expression());

    try {
      if (ctx.NOT() != null) {
        // IS NOT NULL
        final IsNotNullCondition condition = new IsNotNullCondition(-1);
        condition.expression = expr;
        return condition;
      } else {
        // IS NULL
        final IsNullCondition condition = new IsNullCondition(-1);
        condition.expression = expr;
        return condition;
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build IS NULL condition: " + e.getMessage(), e);
    }
  }

  /**
   * IN condition (e.g., x IN (1, 2, 3)).
   */
  @Override
  public BooleanExpression visitInCondition(final SQLParser.InConditionContext ctx) {
    final InCondition condition = new InCondition(-1);

    // Check if left side is a parenthesized statement (subquery): (SELECT ...) IN tags
    final SQLParser.ExpressionContext leftExprCtx = ctx.expression(0);
    if (leftExprCtx instanceof SQLParser.MathExprContext) {
      final SQLParser.MathExpressionContext leftMathCtx = ((SQLParser.MathExprContext) leftExprCtx).mathExpression();
      if (leftMathCtx instanceof SQLParser.BaseContext) {
        final SQLParser.BaseExpressionContext leftBaseCtx = ((SQLParser.BaseContext) leftMathCtx).baseExpression();
        if (leftBaseCtx instanceof final SQLParser.ParenthesizedStmtContext leftParenCtx) {
          if (leftParenCtx.statement() != null) {
            // (SELECT ...) IN tags - create expression wrapper for subquery
            condition.left = createStatementExpression((SelectStatement) visit(leftParenCtx.statement()));
            condition.not = ctx.NOT() != null;

            // Process right side normally
            if (ctx.LPAREN() != null) {
              final List<Expression> expressions = new ArrayList<>();
              for (int i = 1; i < ctx.expression().size(); i++) {
                expressions.add((Expression) visit(ctx.expression(i)));
              }
              condition.right = expressions;
            } else {
              final Expression rightExpr = (Expression) visit(ctx.expression(1));
              if (rightExpr.mathExpression instanceof final BaseExpression baseExpr) {
                if (baseExpr.inputParam != null) {
                  condition.rightParam = baseExpr.inputParam;
                  return condition;
                }
              }
              if (rightExpr.mathExpression != null) {
                condition.rightMathExpression = rightExpr.mathExpression;
              } else {
                condition.right = rightExpr;
              }
            }

            return condition;
          }
        }
      }
    }

    // Normal case: left side is a regular expression
    condition.left = (Expression) visit(leftExprCtx);
    condition.not = ctx.NOT() != null;

    // Right side can be:
    // 1. IN (SELECT ...) - subquery
    // 2. IN (expr1, expr2, ...) - parenthesized list
    // 3. IN [expr1, expr2, ...] - array literal
    // 4. IN (?) or IN (:param) - single input parameter in parentheses
    // 5. IN ? or IN :param - input parameter (without parentheses)

    if (ctx.LPAREN() != null) {
      // Form: IN (expr1, expr2, ...) - explicit parentheses at IN level
      // Check if it's a single parameter: IN (?)
      if (ctx.expression().size() == 2) {
        final Expression rightExpr = (Expression) visit(ctx.expression(1));

        // Check if this is an input parameter
        if (rightExpr.mathExpression instanceof final BaseExpression baseExpr) {
          if (baseExpr.inputParam != null) {
            // IN (?), IN (:name), or IN ($1)
            condition.rightParam = baseExpr.inputParam;
            return condition;
          }
        }
      }

      // Multiple expressions or non-parameter: IN (expr1, expr2, ...)
      final List<Expression> expressions = new ArrayList<>();
      for (int i = 1; i < ctx.expression().size(); i++) {
        expressions.add((Expression) visit(ctx.expression(i)));
      }
      condition.right = expressions;
    } else {
      // Form: IN expression (could be array literal, input parameter, subquery, etc.)
      final SQLParser.ExpressionContext exprCtx = ctx.expression(1);

      // Check if the expression is a parenthesized statement (subquery): IN (SELECT ...)
      if (exprCtx instanceof SQLParser.MathExprContext) {
        final SQLParser.MathExpressionContext mathCtx = ((SQLParser.MathExprContext) exprCtx).mathExpression();
        if (mathCtx instanceof SQLParser.BaseContext) {
          final SQLParser.BaseExpressionContext baseCtx = ((SQLParser.BaseContext) mathCtx).baseExpression();
          if (baseCtx instanceof final SQLParser.ParenthesizedStmtContext parenCtx) {
            if (parenCtx.statement() != null) {
              // IN (SELECT ...) - extract the subquery
              condition.rightStatement = (SelectStatement) visit(parenCtx.statement());
              return condition;
            }
          }
        }
      }

      // Not a subquery - process normally
      final Expression rightExpr = (Expression) visit(exprCtx);

      // Check if it's an input parameter
      if (rightExpr.mathExpression instanceof final BaseExpression baseExpr) {
        if (baseExpr.inputParam != null) {
          condition.rightParam = baseExpr.inputParam;
          return condition;
        }
      }

      // Check if it's a math expression (array literal would be ArrayLiteralExpression)
      if (rightExpr.mathExpression != null) {
        condition.rightMathExpression = rightExpr.mathExpression;
      } else {
        condition.right = rightExpr;
      }
    }

    return condition;
  }

  /**
   * TRUE condition literal - WHERE TRUE.
   */
  @Override
  public BooleanExpression visitTrueCondition(final SQLParser.TrueConditionContext ctx) {
    return BooleanExpression.TRUE;
  }

  /**
   * FALSE condition literal - WHERE FALSE.
   */
  @Override
  public BooleanExpression visitFalseCondition(final SQLParser.FalseConditionContext ctx) {
    return BooleanExpression.FALSE;
  }

  /**
   * NULL condition literal - WHERE NULL.
   * In SQL three-valued logic, NULL represents an unknown value.
   */
  @Override
  public BooleanExpression visitNullCondition(final SQLParser.NullConditionContext ctx) {
    // NULL in boolean context evaluates to NULL (unknown) - proper SQL three-valued logic
    return BooleanExpression.NULL;
  }

  /**
   * Parenthesized condition - critical for nested WHERE clauses.
   */
  @Override
  public BooleanExpression visitParenthesizedCondition(final SQLParser.ParenthesizedConditionContext ctx) {
    // Simply delegate to the inner whereClause
    final WhereClause inner = visitWhereClause(ctx.whereClause());
    return inner.baseExpression;
  }

  /**
   * BETWEEN condition (e.g., x BETWEEN 1 AND 10).
   * Grammar: expression NOT? BETWEEN expression AND expression
   */
  @Override
  public BooleanExpression visitBetweenCondition(final SQLParser.BetweenConditionContext ctx) {
    final Expression first = (Expression) visit(ctx.expression(0));
    final Expression second = (Expression) visit(ctx.expression(1));
    final Expression third = (Expression) visit(ctx.expression(2));

    try {
      final BetweenCondition condition = new BetweenCondition(-1);
      condition.first = first;

      condition.second = second;

      condition.third = third;

      // Handle NOT BETWEEN
      if (ctx.NOT() != null) {
        final NotBlock notBlock = new NotBlock(-1);
        notBlock.sub = condition;
        notBlock.negate = true;
        return notBlock;
      }

      return condition;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build BETWEEN condition: " + e.getMessage(), e);
    }
  }

  /**
   * CONTAINS condition - checks if collection contains a value or satisfies a condition.
   * Grammar: expression CONTAINS (LPAREN whereClause RPAREN | expression)
   */
  @Override
  public BooleanExpression visitContainsCondition(final SQLParser.ContainsConditionContext ctx) {
    final Expression left = (Expression) visit(ctx.expression(0));

    try {
      final ContainsCondition condition = new ContainsCondition(-1);
      condition.left = left;

      if (ctx.whereClause() != null) {
        // Form: expression CONTAINS (whereClause)
        final WhereClause whereClause = (WhereClause) visit(ctx.whereClause());
        condition.condition = whereClause.baseExpression;
      } else {
        // Form: expression CONTAINS expression
        // Check if the expression is a parenthesized statement (subquery): CONTAINS (SELECT ...)
        final SQLParser.ExpressionContext exprCtx = ctx.expression(1);
        if (exprCtx instanceof SQLParser.MathExprContext) {
          final SQLParser.MathExpressionContext mathCtx = ((SQLParser.MathExprContext) exprCtx).mathExpression();
          if (mathCtx instanceof SQLParser.BaseContext) {
            final SQLParser.BaseExpressionContext baseCtx = ((SQLParser.BaseContext) mathCtx).baseExpression();
            if (baseCtx instanceof final SQLParser.ParenthesizedStmtContext parenCtx) {
              if (parenCtx.statement() != null) {
                // CONTAINS (SELECT ...) - need to handle as subquery
                // ContainsCondition doesn't have a rightStatement field, so wrap in expression
                final Expression right = createStatementExpression((SelectStatement) visit(parenCtx.statement()));
                condition.right = right;
                return condition;
              }
            }
          }
        }

        // Not a subquery - process normally
        final Expression right = (Expression) visit(exprCtx);

        // Check if the expression is a parenthesized where clause (e.g., (@this ILIKE "C"))
        // In this case, use the where condition as the CONTAINS condition
        if (right.whereCondition != null) {
          condition.condition = right.whereCondition.baseExpression;
        } else {
          condition.right = right;
        }
      }

      return condition;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build CONTAINS condition: " + e.getMessage(), e);
    }
  }

  /**
   * CONTAINSALL condition - checks if collection contains all specified values or satisfies condition.
   * Grammar: expression CONTAINSALL (LPAREN whereClause RPAREN | expression)
   */
  @Override
  public BooleanExpression visitContainsAllCondition(final SQLParser.ContainsAllConditionContext ctx) {
    final Expression left = (Expression) visit(ctx.expression(0));

    try {
      final ContainsAllCondition condition = new ContainsAllCondition(-1);
      condition.left = left;

      if (ctx.whereClause() != null) {
        // Form: expression CONTAINSALL (whereClause)
        final WhereClause whereClause = (WhereClause) visit(ctx.whereClause());
        condition.rightBlock = (OrBlock) whereClause.baseExpression;
      } else {
        // Form: expression CONTAINSALL expression
        final Expression right = (Expression) visit(ctx.expression(1));
        condition.right = right;
      }

      return condition;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build CONTAINSALL condition: " + e.getMessage(), e);
    }
  }

  /**
   * CONTAINSANY condition - checks if collection contains any of the specified values or satisfies condition.
   * Grammar: expression CONTAINSANY (LPAREN whereClause RPAREN | expression)
   */
  @Override
  public BooleanExpression visitContainsAnyCondition(final SQLParser.ContainsAnyConditionContext ctx) {
    final Expression left = (Expression) visit(ctx.expression(0));

    try {
      final ContainsAnyCondition condition = new ContainsAnyCondition(-1);
      condition.left = left;

      if (ctx.whereClause() != null) {
        // Form: expression CONTAINSANY (whereClause)
        final WhereClause whereClause = (WhereClause) visit(ctx.whereClause());
        condition.rightBlock = (OrBlock) whereClause.baseExpression;
      } else {
        // Form: expression CONTAINSANY expression
        final Expression right = (Expression) visit(ctx.expression(1));
        condition.right = right;
      }

      return condition;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build CONTAINSANY condition: " + e.getMessage(), e);
    }
  }

  /**
   * CONTAINSKEY condition - for map key checking.
   */
  @Override
  public BooleanExpression visitContainsKeyCondition(final SQLParser.ContainsKeyConditionContext ctx) {
    // Use CONTAINS operator for key checking
    final BinaryCondition condition = new BinaryCondition(-1);
    condition.left = (Expression) visit(ctx.expression(0));
    condition.right = (Expression) visit(ctx.expression(1));
    condition.operator = new ContainsKeyOperator(-1);
    return condition;
  }

  /**
   * CONTAINSVALUE condition - for map value checking.
   */
  @Override
  public BooleanExpression visitContainsValueCondition(final SQLParser.ContainsValueConditionContext ctx) {
    final Expression left = (Expression) visit(ctx.expression(0));
    final Expression right = (Expression) visit(ctx.expression(1));

    try {
      final ContainsValueCondition condition = new ContainsValueCondition(-1);
      condition.left = left;

      condition.expression = right;

      return condition;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build CONTAINSVALUE condition: " + e.getMessage(), e);
    }
  }

  /**
   * CONTAINSTEXT condition - for full-text search.
   */
  @Override
  public BooleanExpression visitContainsTextCondition(final SQLParser.ContainsTextConditionContext ctx) {
    final Expression left = (Expression) visit(ctx.expression(0));
    final Expression right = (Expression) visit(ctx.expression(1));

    try {
      final ContainsTextCondition condition = new ContainsTextCondition(-1);
      condition.left = left;

      condition.right = right;

      return condition;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build CONTAINSTEXT condition: " + e.getMessage(), e);
    }
  }

  /**
   * LIKE condition (e.g., name LIKE 'John%').
   * Grammar: expression NOT? LIKE expression
   * Supports PostgreSQL-style semantic negation: name NOT LIKE 'pattern'
   */
  @Override
  public BooleanExpression visitLikeCondition(final SQLParser.LikeConditionContext ctx) {
    final BinaryCondition condition = new BinaryCondition(-1);
    condition.left = (Expression) visit(ctx.expression(0));
    condition.right = (Expression) visit(ctx.expression(1));
    condition.operator = new LikeOperator(-1);

    // Handle NOT LIKE
    if (ctx.NOT() != null) {
      final NotBlock notBlock = new NotBlock(-1);
      notBlock.sub = condition;
      notBlock.negate = true;
      return notBlock;
    }

    return condition;
  }

  /**
   * ILIKE condition (case-insensitive LIKE).
   * Grammar: expression NOT? ILIKE expression
   * Supports PostgreSQL-style semantic negation: name NOT ILIKE 'pattern'
   */
  @Override
  public BooleanExpression visitIlikeCondition(final SQLParser.IlikeConditionContext ctx) {
    final BinaryCondition condition = new BinaryCondition(-1);
    condition.left = (Expression) visit(ctx.expression(0));
    condition.right = (Expression) visit(ctx.expression(1));
    condition.operator = new ILikeOperator(-1);

    // Handle NOT ILIKE
    if (ctx.NOT() != null) {
      final NotBlock notBlock = new NotBlock(-1);
      notBlock.sub = condition;
      notBlock.negate = true;
      return notBlock;
    }

    return condition;
  }

  /**
   * MATCHES condition (regex matching).
   * Grammar: expression MATCHES expression
   */
  @Override
  public BooleanExpression visitMatchesCondition(final SQLParser.MatchesConditionContext ctx) {
    final Expression leftExpr = (Expression) visit(ctx.expression(0));
    final Expression rightExpr = (Expression) visit(ctx.expression(1));

    try {
      final MatchesCondition condition = new MatchesCondition(-1);
      condition.expression = leftExpr;

      // Set rightExpression (the regex pattern)
      condition.rightExpression = rightExpr;

      return condition;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build MATCHES condition: " + e.getMessage(), e);
    }
  }

  /**
   * INSTANCEOF condition.
   * Grammar: expression INSTANCEOF (identifier | STRING_LITERAL)
   */
  @Override
  public BooleanExpression visitInstanceofCondition(final SQLParser.InstanceofConditionContext ctx) {
    final Expression left = (Expression) visit(ctx.expression());
    final InstanceofCondition condition = new InstanceofCondition(-1);

    try {
      condition.left = left;

      if (ctx.identifier() != null) {
        // Right side is an identifier
        final Identifier right = (Identifier) visit(ctx.identifier());
        condition.right = right;
      } else if (ctx.STRING_LITERAL() != null) {
        // Right side is a string literal
        final String rightString = ctx.STRING_LITERAL().getText();
        condition.rightString = rightString;
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build INSTANCEOF condition: " + e.getMessage(), e);
    }

    return condition;
  }

  /**
   * IS DEFINED / IS NOT DEFINED condition.
   * Grammar: expression IS NOT? DEFINED
   */
  @Override
  public BooleanExpression visitIsDefinedCondition(final SQLParser.IsDefinedConditionContext ctx) {
    final Expression expr = (Expression) visit(ctx.expression());

    try {
      if (ctx.NOT() != null) {
        // IS NOT DEFINED
        final IsNotDefinedCondition condition = new IsNotDefinedCondition(-1);
        condition.expression = expr;
        return condition;
      } else {
        // IS DEFINED
        final IsDefinedCondition condition = new IsDefinedCondition(-1);
        condition.expression = expr;
        return condition;
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build IS DEFINED condition: " + e.getMessage(), e);
    }
  }

  // EXPRESSION VISITORS

  /**
   * Expression visitor - delegates to labeled alternatives.
   * Each labeled alternative (arrayConcat, mathExpr, etc.) has its own visitor method.
   */
  public Expression visitExpression(final SQLParser.ExpressionContext ctx) {
    // This is just a dispatcher - actual work is done in labeled alternative visitors
    return (Expression) visitChildren(ctx);
  }

  /**
   * Array concatenation expression (||).
   * Grammar: expression SC_OR expression
   * Flattens chained concatenations (e.g. A || B || C) into a single ArrayConcatExpression with N children.
   */
  @Override
  public Expression visitArrayConcat(final SQLParser.ArrayConcatContext ctx) {
    final ArrayConcatExpression concatExpr = new ArrayConcatExpression(-1);

    // Visit left side
    final Expression leftExpr = (Expression) visit(ctx.expression(0));
    // If the left side is itself an array concat, flatten its children
    if (leftExpr.arrayConcatExpression != null)
      concatExpr.getChildExpressions().addAll(leftExpr.arrayConcatExpression.getChildExpressions());
    else {
      final ArrayConcatExpressionElement leftElement = new ArrayConcatExpressionElement(-1);
      copyExpressionFields(leftExpr, leftElement);
      leftElement.nestedProjection = extractNestedProjectionFromBaseExpression(leftExpr);
      concatExpr.getChildExpressions().add(leftElement);
    }

    // Visit right side
    final Expression rightExpr = (Expression) visit(ctx.expression(1));
    final ArrayConcatExpressionElement rightElement = new ArrayConcatExpressionElement(-1);
    copyExpressionFields(rightExpr, rightElement);
    rightElement.nestedProjection = extractNestedProjectionFromBaseExpression(rightExpr);
    concatExpr.getChildExpressions().add(rightElement);

    // Wrap in Expression
    final Expression result = new Expression(-1);
    result.arrayConcatExpression = concatExpr;
    return result;
  }

  /**
   * Helper method to copy fields from Expression to ArrayConcatExpressionElement.
   */
  private void copyExpressionFields(final Expression from, final ArrayConcatExpressionElement to) {
    to.mathExpression = from.mathExpression;
    to.isNull = from.isNull;
    to.booleanValue = from.booleanValue;
    to.rid = from.rid;
    to.whereCondition = from.whereCondition;
    to.json = from.json;
  }

  /**
   * Math expression alternative.
   */
  @Override
  public Expression visitMathExpr(final SQLParser.MathExprContext ctx) {
    final Expression expr = new Expression(-1);
    expr.mathExpression = (MathExpression) visit(ctx.mathExpression());
    return expr;
  }

  /**
   * NULL literal alternative.
   */
  @Override
  public Expression visitNullLiteral(final SQLParser.NullLiteralContext ctx) {
    final Expression expr = new Expression(-1);
    expr.isNull = true;
    return expr;
  }

  /**
   * TRUE literal alternative.
   */
  @Override
  public Expression visitTrueLiteral(final SQLParser.TrueLiteralContext ctx) {
    final Expression expr = new Expression(-1);
    expr.booleanValue = Boolean.TRUE;
    return expr;
  }

  /**
   * FALSE literal alternative.
   */
  @Override
  public Expression visitFalseLiteral(final SQLParser.FalseLiteralContext ctx) {
    final Expression expr = new Expression(-1);
    expr.booleanValue = Boolean.FALSE;
    return expr;
  }

  /**
   * RID literal alternative.
   */
  @Override
  public Expression visitRidLiteral(final SQLParser.RidLiteralContext ctx) {
    final Expression expr = new Expression(-1);
    expr.rid = (Rid) visit(ctx.rid());
    return expr;
  }

  /**
   * Parenthesized WHERE clause alternative.
   * Handles conditions in parentheses like (1 > 0).
   */
  @Override
  public Expression visitParenthesizedWhereExpr(final SQLParser.ParenthesizedWhereExprContext ctx) {
    final Expression expr = new Expression(-1);
    expr.whereCondition = (WhereClause) visit(ctx.whereClause());
    return expr;
  }

  /**
   * JSON literal alternative.
   */
  @Override
  public Expression visitJsonLiteral(final SQLParser.JsonLiteralContext ctx) {
    final Expression expr = new Expression(-1);
    expr.json = (Json) visit(ctx.json());
    return expr;
  }

  /**
   * JSON visitor - handles mapLiteral.
   * Grammar: json : mapLiteral
   * NOTE: This is called from expression alternative, not from visitJsonLiteral.
   * It needs to return Json (not Expression) for use in INSERT CONTENT clauses.
   */
  @Override
  public Json visitJson(final SQLParser.JsonContext ctx) {
    return (Json) visit(ctx.mapLiteral());
  }

  /**
   * JSON array visitor - handles [json, json, ...].
   * Grammar: jsonArray : LBRACKET (json (COMMA json)*)? RBRACKET
   * Used in INSERT/CREATE VERTEX CONTENT clauses.
   */
  @Override
  public JsonArray visitJsonArray(final SQLParser.JsonArrayContext ctx) {
    final JsonArray jsonArray = new JsonArray(-1);

    if (ctx.json() != null) {
      for (final SQLParser.JsonContext jsonCtx : ctx.json()) {
        jsonArray.items.add((Json) visit(jsonCtx));
      }
    }

    return jsonArray;
  }

  /**
   * Map literal as baseExpression alternative - handles {} in expressions.
   * Grammar: baseExpression : mapLiteral # mapLit
   */
  @Override
  public BaseExpression visitMapLit(final SQLParser.MapLitContext ctx) {
    final Json json = (Json) visit(ctx.mapLiteral());

    // Wrap in Expression
    final Expression expression = new Expression(-1);
    expression.json = json;

    // Wrap in BaseExpression
    final BaseExpression baseExpr = new BaseExpression(-1);
    try {
      baseExpr.expression = expression;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to wrap map literal in BaseExpression: " + e.getMessage(), e);
    }

    return baseExpr;
  }

  /**
   * Map literal visitor - handles {key: value, ...}.
   * Grammar: mapLiteral : LBRACE (mapEntry (COMMA mapEntry)*)? RBRACE
   */
  @Override
  public Json visitMapLiteral(final SQLParser.MapLiteralContext ctx) {
    try {
      final Json json = new Json(-1);

      if (ctx.mapEntry() != null) {
        for (final SQLParser.MapEntryContext entryCtx : ctx.mapEntry()) {
          final JsonItem item = (JsonItem) visit(entryCtx);
          json.items.add(item);
        }
      }

      return json;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build map literal: " + e.getMessage(), e);
    }
  }

  /**
   * Map entry visitor - handles key: value.
   * Grammar: mapEntry : (identifier | STRING_LITERAL) COLON expression
   */
  @Override
  public JsonItem visitMapEntry(final SQLParser.MapEntryContext ctx) {
    try {
      final JsonItem item = new JsonItem();

      // Left side (key) can be identifier or string literal
      if (ctx.identifier() != null) {
        item.leftIdentifier = (Identifier) visit(ctx.identifier());
      } else if (ctx.STRING_LITERAL() != null) {
        // Remove quotes from string literal
        String str = ctx.STRING_LITERAL().getText();
        if (str.length() >= 2 && str.startsWith("\"") && str.endsWith("\"")) {
          str = str.substring(1, str.length() - 1);
        } else if (str.length() >= 2 && str.startsWith("'") && str.endsWith("'")) {
          str = str.substring(1, str.length() - 1);
        }
        // Unescape string
        str = BaseExpression.decode(str);

        item.leftString = str;
      }

      // Right side (value) is an expression
      item.right = (Expression) visit(ctx.expression());

      return item;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build map entry: " + e.getMessage(), e);
    }
  }

  /**
   * Math expression visitor.
   * Handles arithmetic operations with proper precedence.
   * Delegates to labeled alternative visitors.
   */
  public MathExpression visitMathExpression(final SQLParser.MathExpressionContext ctx) {
    // Delegates to labeled alternative visitors (unary, multiplicative, additive, etc.)
    return (MathExpression) visitChildren(ctx);
  }

  /**
   * Base alternative - when mathExpression is just a baseExpression.
   */
  @Override
  public MathExpression visitBase(final SQLParser.BaseContext ctx) {
    return (MathExpression) visit(ctx.baseExpression());
  }

  /**
   * Unary +/- operator.
   */
  @Override
  public MathExpression visitUnary(final SQLParser.UnaryContext ctx) {
    final MathExpression innerExpr = (MathExpression) visit(ctx.mathExpression());

    if (ctx.PLUS() != null) {
      // Unary plus: just return the expression as-is
      return innerExpr;
    } else if (ctx.MINUS() != null) {
      // Unary minus: create 0 - expression
      // Create a MathExpression with zero and the MINUS operator
      final MathExpression result = new MathExpression(-1);

      // Create zero expression (BaseExpression with PInteger(0))
      final MathExpression zeroExpr = new MathExpression(-1);
      final BaseExpression baseZero = new BaseExpression(-1);
      final PInteger zero = new PInteger(-1);
      zero.setValue(0);
      baseZero.number = zero;
      zeroExpr.childExpressions.add(baseZero);

      // Add children and operator
      result.childExpressions.add(zeroExpr);
      result.childExpressions.add(innerExpr);
      result.operators.add(MathExpression.Operator.MINUS);

      return result;
    }

    throw new CommandSQLParsingException("Unknown unary operator");
  }

  /**
   * Multiplicative operations (*, /, %).
   */
  @Override
  public MathExpression visitMultiplicative(final SQLParser.MultiplicativeContext ctx) {
    final MathExpression left = (MathExpression) visit(ctx.mathExpression(0));
    final MathExpression right = (MathExpression) visit(ctx.mathExpression(1));

    final MathExpression result = new MathExpression(-1);
    result.childExpressions.add(left);
    result.childExpressions.add(right);

    if (ctx.STAR() != null) {
      result.operators.add(MathExpression.Operator.STAR);
    } else if (ctx.SLASH() != null) {
      result.operators.add(MathExpression.Operator.SLASH);
    } else if (ctx.REM() != null) {
      result.operators.add(MathExpression.Operator.REM);
    }

    return result;
  }

  /**
   * Additive operations (+, -).
   */
  @Override
  public MathExpression visitAdditive(final SQLParser.AdditiveContext ctx) {
    final MathExpression left = (MathExpression) visit(ctx.mathExpression(0));
    final MathExpression right = (MathExpression) visit(ctx.mathExpression(1));

    final MathExpression result = new MathExpression(-1);
    result.childExpressions.add(left);
    result.childExpressions.add(right);

    if (ctx.PLUS() != null) {
      result.operators.add(MathExpression.Operator.PLUS);
    } else if (ctx.MINUS() != null) {
      result.operators.add(MathExpression.Operator.MINUS);
    }

    return result;
  }

  /**
   * Shift operations (<<, >>, >>>).
   */
  @Override
  public MathExpression visitShift(final SQLParser.ShiftContext ctx) {
    final MathExpression left = (MathExpression) visit(ctx.mathExpression(0));
    final MathExpression right = (MathExpression) visit(ctx.mathExpression(1));

    final MathExpression result = new MathExpression(-1);
    result.childExpressions.add(left);
    result.childExpressions.add(right);

    if (ctx.LSHIFT() != null) {
      result.operators.add(MathExpression.Operator.LSHIFT);
    } else if (ctx.RSHIFT() != null) {
      result.operators.add(MathExpression.Operator.RSHIFT);
    } else if (ctx.RUNSIGNEDSHIFT() != null) {
      result.operators.add(MathExpression.Operator.RUNSIGNEDSHIFT);
    }

    return result;
  }

  /**
   * Bitwise AND operation.
   */
  @Override
  public MathExpression visitBitwiseAnd(final SQLParser.BitwiseAndContext ctx) {
    final MathExpression left = (MathExpression) visit(ctx.mathExpression(0));
    final MathExpression right = (MathExpression) visit(ctx.mathExpression(1));

    final MathExpression result = new MathExpression(-1);
    result.childExpressions.add(left);
    result.childExpressions.add(right);
    result.operators.add(MathExpression.Operator.BIT_AND);

    return result;
  }

  /**
   * Bitwise XOR operation.
   */
  @Override
  public MathExpression visitBitwiseXor(final SQLParser.BitwiseXorContext ctx) {
    final MathExpression left = (MathExpression) visit(ctx.mathExpression(0));
    final MathExpression right = (MathExpression) visit(ctx.mathExpression(1));

    final MathExpression result = new MathExpression(-1);
    result.childExpressions.add(left);
    result.childExpressions.add(right);
    result.operators.add(MathExpression.Operator.XOR);

    return result;
  }

  /**
   * Bitwise OR operation.
   */
  @Override
  public MathExpression visitBitwiseOr(final SQLParser.BitwiseOrContext ctx) {
    final MathExpression left = (MathExpression) visit(ctx.mathExpression(0));
    final MathExpression right = (MathExpression) visit(ctx.mathExpression(1));

    final MathExpression result = new MathExpression(-1);
    result.childExpressions.add(left);
    result.childExpressions.add(right);
    result.operators.add(MathExpression.Operator.BIT_OR);

    return result;
  }

  /**
   * Base expression visitor.
   * Handles numbers, identifiers, strings, function calls, etc.
   * This delegates to labeled alternative visitors.
   */
  public BaseExpression visitBaseExpression(final SQLParser.BaseExpressionContext ctx) {
    // This should never be called directly since we have labeled alternatives
    // Let ANTLR dispatch to the correct alternative visitor
    return (BaseExpression) visitChildren(ctx);
  }

  /**
   * Integer literal visitor.
   */
  @Override
  public BaseExpression visitIntegerLiteral(final SQLParser.IntegerLiteralContext ctx) {
    final BaseExpression baseExpr = new BaseExpression(-1);

    final PInteger number = new PInteger(-1);
    final String text = ctx.INTEGER_LITERAL().getText();
    try {
      if (text.endsWith("L") || text.endsWith("l")) {
        number.setValue(Long.parseLong(text.substring(0, text.length() - 1)));
      } else {
        try {
          number.setValue(Integer.parseInt(text));
        } catch (final NumberFormatException e) {
          // If it's too large for int, try long
          number.setValue(Long.parseLong(text));
        }
      }
    } catch (final NumberFormatException e) {
      throw new CommandSQLParsingException("Invalid integer: " + text);
    }

    baseExpr.number = number;
    return baseExpr;
  }

  /**
   * Floating point literal visitor.
   */
  @Override
  public BaseExpression visitFloatLiteral(final SQLParser.FloatLiteralContext ctx) {
    final BaseExpression baseExpr = new BaseExpression(-1);

    final PNumber number = new PNumber(-1);
    final String text = ctx.FLOATING_POINT_LITERAL().getText();
    try {
      if (text.endsWith("F") || text.endsWith("f")) {
        number.value = Float.parseFloat(text.substring(0, text.length() - 1));
      } else if (text.endsWith("D") || text.endsWith("d")) {
        number.value = Double.parseDouble(text.substring(0, text.length() - 1));
      } else {
        // Default to Float for compatibility with JavaCC parser
        number.value = Float.parseFloat(text);
      }
    } catch (final NumberFormatException e) {
      throw new CommandSQLParsingException("Invalid floating point: " + text);
    }

    baseExpr.number = number;
    return baseExpr;
  }

  /**
   * String literal visitor.
   */
  @Override
  public BaseExpression visitStringLiteral(final SQLParser.StringLiteralContext ctx) {
    final BaseExpression baseExpr = new BaseExpression(-1);

    final String text = ctx.STRING_LITERAL().getText();
    // Store the string literal as-is (including quotes)
    // BaseExpression.execute() will handle unquoting and escape sequence decoding
    baseExpr.string = text;

    // Process modifiers (method calls like .prefix(), .toUpperCase(), etc.)
    if (CollectionUtils.isNotEmpty(ctx.modifier())) {
      Modifier firstModifier = null;
      Modifier currentModifier = null;

      for (final SQLParser.ModifierContext modCtx : ctx.modifier()) {
        final Modifier modifier = (Modifier) visit(modCtx);
        if (firstModifier == null) {
          firstModifier = modifier;
          currentModifier = modifier;
        } else {
          currentModifier.next = modifier;
          currentModifier = modifier;
        }
      }

      baseExpr.modifier = firstModifier;
    }

    return baseExpr;
  }

  /**
   * NULL literal visitor.
   */
  @Override
  public BaseExpression visitNullBaseExpr(final SQLParser.NullBaseExprContext ctx) {
    final BaseExpression baseExpr = new BaseExpression(-1);
    baseExpr.isNull = true;
    return baseExpr;
  }

  /**
   * @this literal visitor.
   */
  @Override
  public BaseExpression visitThisLiteral(final SQLParser.ThisLiteralContext ctx) {
    final BaseExpression baseExpr = new BaseExpression(-1);
    final RecordAttribute attr = new RecordAttribute(-1);
    attr.setName("@this");
    final BaseIdentifier baseId = new BaseIdentifier(attr);
    baseExpr.identifier = baseId;
    return baseExpr;
  }

  /**
   * Input parameter visitor.
   */
  @Override
  public BaseExpression visitInputParam(final SQLParser.InputParamContext ctx) {
    final BaseExpression baseExpr = new BaseExpression(-1);
    baseExpr.inputParam = (InputParameter) visit(ctx.inputParameter());
    return baseExpr;
  }

  /**
   * Function call visitor (e.g., EXPAND(arr), SUM(price)).
   */
  @Override
  public BaseExpression visitFunctionCallExpr(final SQLParser.FunctionCallExprContext ctx) {
    final BaseExpression baseExpr = new BaseExpression(-1);

    try {
      // Create the AST structure: BaseExpression -> BaseIdentifier -> LevelZeroIdentifier -> FunctionCall
      final FunctionCall funcCall = (FunctionCall) visit(ctx.functionCall());

      final LevelZeroIdentifier levelZero = new LevelZeroIdentifier(-1);
      levelZero.functionCall = funcCall;

      final BaseIdentifier baseId = new BaseIdentifier(-1);
      baseId.levelZero = levelZero;

      baseExpr.identifier = baseId;

      // Handle nested projections, method calls, array selectors, and modifiers on function call
      // Grammar: functionCall: identifier LPAREN ... RPAREN nestedProjection* methodCall* arraySelector* modifier*
      final SQLParser.FunctionCallContext funcCtx = ctx.functionCall();

      Modifier firstModifier = null;
      Modifier currentModifier = null;

      // Process nested projections (:{x}, :{x,y}, etc.) - these come FIRST
      if (funcCtx.nestedProjection() != null) {
        for (final SQLParser.NestedProjectionContext projCtx : funcCtx.nestedProjection()) {
          final NestedProjection nestedProj = (NestedProjection) visit(projCtx);
          // Convert to a Modifier for the AST structure
          final Modifier modifier = new Modifier(-1);
          modifier.nestedProjection = nestedProj;
          if (firstModifier == null) {
            firstModifier = modifier;
            currentModifier = modifier;
          } else {
            currentModifier.next = modifier;
            currentModifier = modifier;
          }
        }
      }

      // Process method calls (.out('Follows'), etc.)
      if (funcCtx.methodCall() != null) {
        for (final SQLParser.MethodCallContext methodCtx : funcCtx.methodCall()) {
          final Modifier modifier = createModifierForMethodCall(methodCtx);
          if (firstModifier == null) {
            firstModifier = modifier;
            currentModifier = modifier;
          } else {
            currentModifier.next = modifier;
            currentModifier = modifier;
          }
        }
      }

      // Process array selectors
      if (funcCtx.arraySelector() != null) {
        for (final SQLParser.ArraySelectorContext selectorCtx : funcCtx.arraySelector()) {
          final Modifier modifier = createModifierForArraySelector(selectorCtx);
          if (firstModifier == null) {
            firstModifier = modifier;
            currentModifier = modifier;
          } else {
            currentModifier.next = modifier;
            currentModifier = modifier;
          }
        }
      }

      // Process modifiers
      if (funcCtx.modifier() != null) {
        for (final SQLParser.ModifierContext modCtx : funcCtx.modifier()) {
          final Modifier modifier = (Modifier) visit(modCtx);
          if (firstModifier == null) {
            firstModifier = modifier;
            currentModifier = modifier;
          } else {
            currentModifier.next = modifier;
            currentModifier = modifier;
          }
        }
      }

      // Set the first modifier on the base expression
      if (firstModifier != null) {
        baseExpr.modifier = firstModifier;
      }

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build function call expression: " + e.getMessage(), e);
    }

    return baseExpr;
  }

  /**
   * Array literal visitor - handles [expr1, expr2, ...] syntax with optional modifiers.
   */
  @Override
  public BaseExpression visitArrayLit(final SQLParser.ArrayLitContext ctx) {
    final ArrayLiteralExpression arrayLiteral = new ArrayLiteralExpression(-1);

    // Visit each expression in the array literal
    if (ctx.arrayLiteral().expression() != null) {
      for (final SQLParser.ExpressionContext exprCtx : ctx.arrayLiteral().expression()) {
        final Object visited = visit(exprCtx);

        // Handle case where visit returns Json instead of Expression (for JSON literals)
        final Expression expr;
        if (visited instanceof Json json) {
          // Wrap Json in Expression
          expr = new Expression(-1);
          expr.json = json;
        } else {
          expr = (Expression) visited;
        }

        arrayLiteral.addItem(expr);
      }
    }

    // Wrap in BaseExpression with mathExpression set to our array literal
    final Expression expression = new Expression(-1);
    expression.mathExpression = arrayLiteral;

    final BaseExpression baseExpr = new BaseExpression(-1);
    baseExpr.expression = expression;

    // Process modifiers (method calls like .keys(), .values(), etc.)
    if (ctx.modifier() != null && !ctx.modifier().isEmpty()) {
      Modifier firstModifier = null;
      Modifier currentModifier = null;
      for (final SQLParser.ModifierContext modCtx : ctx.modifier()) {
        final Modifier modifier = (Modifier) visit(modCtx);
        if (firstModifier == null) {
          firstModifier = modifier;
          currentModifier = modifier;
        } else {
          currentModifier.next = modifier;
          currentModifier = modifier;
        }
      }
      baseExpr.modifier = firstModifier;
    }

    return baseExpr;
  }

  /**
   * Function call visitor - parses function name and parameters.
   * Grammar: identifier LPAREN (STAR | expression (COMMA expression)*)? RPAREN
   */
  @Override
  public FunctionCall visitFunctionCall(final SQLParser.FunctionCallContext ctx) {
    final FunctionCall funcCall = new FunctionCall(-1);

    try {
      // Function name
      final Identifier funcName = (Identifier) visit(ctx.identifier());
      funcCall.name = funcName;

      // Parameters (using reflection for protected field)
      if (ctx.STAR() != null) {
        // Handle COUNT(*), SUM(*), etc. - create a parameter representing *
        final List<Expression> params = new ArrayList<>();
        final Expression starExpr = new Expression(-1);
        final BaseExpression baseExpr = new BaseExpression(-1);

        // Create a special identifier for * using SuffixIdentifier with star flag
        final BaseIdentifier starId = new BaseIdentifier(-1);
        final SuffixIdentifier suffix = new SuffixIdentifier(-1);
        suffix.star = true;
        starId.suffix = suffix;

        baseExpr.identifier = starId;
        starExpr.mathExpression = baseExpr;
        params.add(starExpr);
        funcCall.params = params;
      } else if (CollectionUtils.isNotEmpty(ctx.expression())) {
        // Regular parameters
        final List<Expression> params = new ArrayList<>();
        for (final SQLParser.ExpressionContext exprCtx : ctx.expression()) {
          params.add((Expression) visit(exprCtx));
        }
        funcCall.params = params;
      }

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build function call: " + e.getMessage(), e);
    }

    return funcCall;
  }

  /**
   * Identifier chain visitor (foo.bar.baz with modifiers).
   */
  @Override
  public BaseExpression visitIdentifierChain(final SQLParser.IdentifierChainContext ctx) {
    final BaseExpression baseExpr = new BaseExpression(-1);

    // Build identifier chain
    if (CollectionUtils.isNotEmpty(ctx.identifier())) {
      final SQLParser.IdentifierContext firstIdCtx = ctx.identifier(0);

      // Detect namespace-qualified function calls: geo.methodName(args)
      // Pattern: exactly one base identifier that is a known namespace, no additional DOT-identifiers,
      // and exactly one methodCall → rewrite as FunctionCall("namespace.methodName", args).
      if (ctx.identifier().size() == 1 && CollectionUtils.isNotEmpty(ctx.methodCall()) && ctx.methodCall().size() == 1
          && firstIdCtx.RID_ATTR() == null && firstIdCtx.TYPE_ATTR() == null
          && firstIdCtx.IN_ATTR() == null && firstIdCtx.OUT_ATTR() == null && firstIdCtx.THIS() == null) {
        final String baseIdText = firstIdCtx.getText();
        if (FUNCTION_NAMESPACES.contains(baseIdText.toLowerCase())) {
          return buildNamespaceQualifiedFunctionCall(baseIdText, ctx.methodCall(0), ctx);
        }
      }

      // Check if the first identifier is a record attribute (@rid, @type, @in, @out, @this)
      if (firstIdCtx.RID_ATTR() != null || firstIdCtx.TYPE_ATTR() != null ||
          firstIdCtx.IN_ATTR() != null || firstIdCtx.OUT_ATTR() != null ||
          firstIdCtx.THIS() != null) {
        // Handle record attributes specially
        final RecordAttribute attr = new RecordAttribute(-1);
        attr.setName(firstIdCtx.getText());
        final BaseIdentifier baseId = new BaseIdentifier(attr);
        baseExpr.identifier = baseId;
      } else {
        // Regular identifier
        final Identifier firstId = (Identifier) visit(firstIdCtx);
        // Use BaseIdentifier constructor that automatically creates SuffixIdentifier
        final BaseIdentifier baseId = new BaseIdentifier(firstId);
        baseExpr.identifier = baseId;
      }

      // Check for namespaced function call pattern: namespace.method(args)
      // e.g., ts.first(value, ts) → builds FunctionCall with name "ts.first"
      if (ctx.identifier().size() == 1
          && ctx.methodCall() != null && ctx.methodCall().size() == 1
          && (ctx.arraySelector() == null || ctx.arraySelector().isEmpty())
          && (ctx.modifier() == null || ctx.modifier().isEmpty())) {
        final String baseIdName = ctx.identifier(0).getText();

        if (FUNCTION_NAMESPACES.contains(baseIdName)) {
          final SQLParser.MethodCallContext methodCtx = ctx.methodCall(0);
          final String qualifiedName = baseIdName + "." + methodCtx.identifier().getText();

          final FunctionCall funcCall = new FunctionCall(-1);
          funcCall.name = new Identifier(qualifiedName);
          funcCall.params = new ArrayList<>();
          if (methodCtx.expression() != null)
            for (final SQLParser.ExpressionContext exprCtx : methodCtx.expression())
              funcCall.params.add((Expression) visit(exprCtx));

          final LevelZeroIdentifier levelZero = new LevelZeroIdentifier(-1);
          levelZero.functionCall = funcCall;

          final BaseIdentifier baseId2 = new BaseIdentifier(-1);
          baseId2.levelZero = levelZero;

          baseExpr.identifier = baseId2;
          return baseExpr;
        }
      }

      // Build modifier chain from additional identifiers, methodCalls, arraySelectors and modifiers
      Modifier firstModifier = null;
      Modifier currentModifier = null;

      try {

        // Process additional identifiers (DOT identifier)* - e.g., "custom.label"
        if (ctx.identifier().size() > 1) {
          for (int i = 1; i < ctx.identifier().size(); i++) {
            final Modifier modifier = new Modifier(-1);
            final SQLParser.IdentifierContext idCtx = ctx.identifier(i);
            final SuffixIdentifier suffix;

            // Check if this identifier is a record attribute (@rid, @type, @in, @out, @this)
            if (idCtx.RID_ATTR() != null || idCtx.TYPE_ATTR() != null ||
                idCtx.IN_ATTR() != null || idCtx.OUT_ATTR() != null ||
                idCtx.THIS() != null) {
              final RecordAttribute attr = new RecordAttribute(-1);
              attr.setName(idCtx.getText());
              suffix = new SuffixIdentifier(attr);
            } else {
              final Identifier id = (Identifier) visit(idCtx);
              suffix = new SuffixIdentifier(id);
            }

            modifier.suffix = suffix;

            if (firstModifier == null) {
              firstModifier = modifier;
              currentModifier = modifier;
            } else {
              currentModifier.next = modifier;
              currentModifier = modifier;
            }
          }
        }

        // Process method calls (.substring(0,1), etc.)
        if (ctx.methodCall() != null) {
          for (final SQLParser.MethodCallContext methodCtx : ctx.methodCall()) {
            final Modifier modifier = createModifierForMethodCall(methodCtx);
            if (firstModifier == null) {
              firstModifier = modifier;
              currentModifier = modifier;
            } else {
              currentModifier.next = modifier;
              currentModifier = modifier;
            }
          }
        }

        // Process array selectors
        if (ctx.arraySelector() != null) {
          for (final SQLParser.ArraySelectorContext selectorCtx : ctx.arraySelector()) {
            final Modifier modifier = createModifierForArraySelector(selectorCtx);
            if (firstModifier == null) {
              firstModifier = modifier;
              currentModifier = modifier;
            } else {
              currentModifier.next = modifier;
              currentModifier = modifier;
            }
          }
        }

        // Process modifiers (DOT identifier or more array selectors)
        if (ctx.modifier() != null) {
          for (final SQLParser.ModifierContext modCtx : ctx.modifier()) {
            final Modifier modifier = (Modifier) visit(modCtx);
            if (firstModifier == null) {
              firstModifier = modifier;
              currentModifier = modifier;
            } else {
              currentModifier.next = modifier;
              currentModifier = modifier;
            }
          }
        }
      } catch (final Exception e) {
        throw new CommandSQLParsingException("Failed to build modifier chain: " + e.getMessage(), e);
      }

      // Set the first modifier on the base expression
      if (firstModifier != null) {
        baseExpr.modifier = firstModifier;
      }
    }

    return baseExpr;
  }

  /**
   * Builds a namespace-qualified FunctionCall BaseExpression from an identifierChain that looks like
   * "namespace.functionName(args)" — e.g. "geo.point(x, y)" or "geo.within(geom, geo.point(x, y))".
   * <p>
   * The identifierChain grammar rule captures "namespace" as the base identifier and ".functionName(args)"
   * as a methodCall. This helper recombines them into a proper FunctionCall AST node so that the
   * execution engine resolves "geo.point" as a registered SQL function rather than a field access.
   * <p>
   * Any arraySelectors or modifiers that follow the method call on the identifierChain are preserved
   * and chained onto the returned BaseExpression.
   */
  private BaseExpression buildNamespaceQualifiedFunctionCall(final String namespace,
      final SQLParser.MethodCallContext methodCtx, final SQLParser.IdentifierChainContext chainCtx) {
    final BaseExpression baseExpr = new BaseExpression(-1);

    try {
      // Build the combined function name: "geo.point", "geo.within", etc.
      final Identifier methodName = (Identifier) visit(methodCtx.identifier());
      final FunctionCall funcCall = new FunctionCall(-1);
      funcCall.name = new Identifier(namespace + "." + methodName.getStringValue());

      // Collect arguments from the method call
      if (CollectionUtils.isNotEmpty(methodCtx.expression())) {
        final List<Expression> params = new ArrayList<>();
        for (final SQLParser.ExpressionContext exprCtx : methodCtx.expression()) {
          params.add((Expression) visit(exprCtx));
        }
        funcCall.params = params;
      }

      // Wrap the FunctionCall in the standard BaseExpression structure
      final LevelZeroIdentifier levelZero = new LevelZeroIdentifier(-1);
      levelZero.functionCall = funcCall;
      final BaseIdentifier baseId = new BaseIdentifier(-1);
      baseId.levelZero = levelZero;
      baseExpr.identifier = baseId;

      // Preserve any arraySelectors or modifiers that follow the method call
      Modifier firstModifier = null;
      Modifier currentModifier = null;

      if (CollectionUtils.isNotEmpty(chainCtx.arraySelector())) {
        for (final SQLParser.ArraySelectorContext selectorCtx : chainCtx.arraySelector()) {
          final Modifier modifier = createModifierForArraySelector(selectorCtx);
          if (firstModifier == null) {
            firstModifier = modifier;
            currentModifier = modifier;
          } else {
            currentModifier.next = modifier;
            currentModifier = modifier;
          }
        }
      }

      if (CollectionUtils.isNotEmpty(chainCtx.modifier())) {
        for (final SQLParser.ModifierContext modCtx : chainCtx.modifier()) {
          final Modifier modifier = (Modifier) visit(modCtx);
          if (firstModifier == null) {
            firstModifier = modifier;
            currentModifier = modifier;
          } else {
            currentModifier.next = modifier;
            currentModifier = modifier;
          }
        }
      }

      if (firstModifier != null)
        baseExpr.modifier = firstModifier;

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build namespace-qualified function call: " + e.getMessage(), e);
    }

    return baseExpr;
  }

  /**
   * Create a Modifier for an array selector context.
   * <p>
   * Note: visitArraySingleSelector can return ArraySelector OR ArrayRangeSelector
   * (when the selector contains INTEGER_RANGE or ELLIPSIS_INTEGER_RANGE).
   */
  private Modifier createModifierForArraySelector(final SQLParser.ArraySelectorContext selectorCtx) {
    // Visit the selector to get the appropriate type
    final Object selector = visit(selectorCtx);

    // If visit() already returned a Modifier (e.g., from arrayFilterSelector), use it directly
    if (selector instanceof Modifier) {
      return (Modifier) selector;
    }

    // Otherwise, create a Modifier and wrap the selector
    final Modifier modifier = new Modifier(-1);

    try {
      // Set squareBrackets flag
      modifier.squareBrackets = true;

      if (selector instanceof ArrayRangeSelector) {
        // Range selector [0..3] or [0...3] or INTEGER_RANGE from arraySingleSelector
        modifier.arrayRange = (ArrayRangeSelector) selector;
      } else if (selector instanceof ArraySingleValuesSelector) {
        // Multi-value selector [0, 1, 3] from arrayMultiSelector
        modifier.arraySingleValues = (ArraySingleValuesSelector) selector;
      } else if (selector instanceof ArraySelector) {
        // Single selector - wrap in ArraySingleValuesSelector
        final ArraySingleValuesSelector singleValues = new ArraySingleValuesSelector(-1);
        singleValues.items.add((ArraySelector) selector);

        modifier.arraySingleValues = singleValues;
      } else if (selector instanceof OrBlock) {
        // Condition selector [whereClause]
        modifier.condition = (OrBlock) selector;
      }
      // arrayFilterSelector returns Modifier directly, handled above

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to create modifier for array selector: " + e.getMessage(), e);
    }

    return modifier;
  }

  /**
   * Create a Modifier for a method call context.
   * Grammar: methodCall: DOT identifier LPAREN (expression (COMMA expression)*)? RPAREN
   * <p>
   * Example: type.substring(0,1) creates a Modifier with a MethodCall
   */
  private Modifier createModifierForMethodCall(final SQLParser.MethodCallContext methodCtx) {
    final Modifier modifier = new Modifier(-1);

    try {
      // Create MethodCall object
      final MethodCall methodCall = new MethodCall(-1);

      // Set method name
      methodCall.methodName = (Identifier) visit(methodCtx.identifier());

      // Set parameters
      if (methodCtx.expression() != null) {
        for (final SQLParser.ExpressionContext exprCtx : methodCtx.expression()) {
          methodCall.params.add((Expression) visit(exprCtx));
        }
      }

      // Set methodCall field in Modifier
      modifier.methodCall = methodCall;

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to create modifier for method call: " + e.getMessage(), e);
    }

    return modifier;
  }

  /**
   * Visit modifier (DOT identifier with optional parentheses or array selector).
   * Grammar: modifier: DOT identifier (LPAREN (expression (COMMA expression)*)? RPAREN)? | arraySelector
   * <p>
   * Handles both property access (.identifier) and method calls (.identifier(args))
   */
  @Override
  public Modifier visitModifier(final SQLParser.ModifierContext ctx) {
    final Modifier modifier = new Modifier(-1);

    try {
      if (ctx.identifier() != null) {
        // Check if this is a method call (has parentheses) or property access (no parentheses)
        if (ctx.LPAREN() != null) {
          // Method call: .identifier(args)
          final MethodCall methodCall = new MethodCall(-1);
          methodCall.methodName = (Identifier) visit(ctx.identifier());

          // Add parameters if present
          if (ctx.expression() != null) {
            for (final SQLParser.ExpressionContext exprCtx : ctx.expression()) {
              methodCall.params.add((Expression) visit(exprCtx));
            }
          }

          modifier.methodCall = methodCall;
        } else {
          // Property access: .identifier
          final Identifier id = (Identifier) visit(ctx.identifier());
          final SuffixIdentifier suffix = new SuffixIdentifier(id);
          modifier.suffix = suffix;
        }
      } else if (ctx.arraySelector() != null) {
        // Array selector modifier
        return createModifierForArraySelector(ctx.arraySelector());
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build modifier: " + e.getMessage(), e);
    }

    return modifier;
  }

  /**
   * Parenthesized statement visitor.
   * Handles (statement) - subqueries and nested statements.
   * Grammar: LPAREN statement RPAREN modifier*
   */
  @Override
  public BaseExpression visitParenthesizedStmt(final SQLParser.ParenthesizedStmtContext ctx) {
    final Statement stmt = (Statement) visit(ctx.statement());
    final BaseExpression result;
    if (stmt instanceof SelectStatement) {
      // Return a SubqueryExpression that wraps the SELECT statement
      result = new SubqueryExpression((SelectStatement) stmt);
    } else {
      // For other statements (INSERT, UPDATE, DELETE, etc.), wrap them in a StatementExpression
      // This allows statements like: INSERT INTO foo SET x = (INSERT INTO bar SET y = 1)
      result = new StatementExpression(stmt);
    }

    // Process modifiers if present (e.g., (SELECT ...).name or (SELECT ...)[0])
    if (CollectionUtils.isNotEmpty(ctx.modifier())) {
      Modifier firstModifier = null;
      Modifier currentModifier = null;

      for (final SQLParser.ModifierContext modCtx : ctx.modifier()) {
        final Modifier modifier = (Modifier) visit(modCtx);

        if (firstModifier == null) {
          firstModifier = modifier;
          currentModifier = modifier;
        } else {
          // Find the end of the current modifier chain
          while (currentModifier.next != null)
            currentModifier = currentModifier.next;
          currentModifier.next = modifier;
          currentModifier = modifier;
        }
      }
      result.setModifier(firstModifier);
    }

    return result;
  }

  /**
   * Parenthesized expression visitor.
   * Handles (expression) - pure expressions in parentheses.
   * Grammar: LPAREN expression RPAREN modifier*
   */
  @Override
  public BaseExpression visitParenthesizedExpr(final SQLParser.ParenthesizedExprContext ctx) {
    // Regular parenthesized expression
    final BaseExpression baseExpr = new BaseExpression(-1);
    baseExpr.expression = (Expression) visit(ctx.expression());

    // Process modifiers if present
    if (CollectionUtils.isNotEmpty(ctx.modifier())) {
      Modifier firstModifier = null;
      Modifier currentModifier = null;

      for (final SQLParser.ModifierContext modCtx : ctx.modifier()) {
        final Modifier modifier = (Modifier) visit(modCtx);

        if (firstModifier == null) {
          firstModifier = modifier;
          currentModifier = modifier;
        } else {
          // Find the end of the current modifier chain
          while (currentModifier.next != null)
            currentModifier = currentModifier.next;
          currentModifier.next = modifier;
          currentModifier = modifier;
        }
      }
      baseExpr.setModifier(firstModifier);
    }

    return baseExpr;
  }

  /**
   * Simple CASE expression visitor.
   * Grammar: caseExpression : CASE caseAlternative+ (ELSE expression)? END
   * Grammar: caseAlternative : WHEN whereClause THEN expression
   */
  public BaseExpression visitCaseExpr(final SQLParser.CaseExprContext ctx) {
    final SQLParser.CaseExpressionContext caseCtx = ctx.caseExpression();

    // Build list of alternatives
    final List<CaseAlternative> alternatives = new ArrayList<>();
    for (final SQLParser.CaseAlternativeContext altCtx : caseCtx.caseAlternative()) {
      // WHEN clause is a whereClause (boolean condition), THEN clause is an expression
      final WhereClause whereClause = (WhereClause) visit(altCtx.whereClause());
      final Expression thenExpression = (Expression) visit(altCtx.expression());

      alternatives.add(new CaseAlternative(whereClause, thenExpression));
    }

    // Handle ELSE clause (may be null)
    Expression elseExpression = null;
    if (caseCtx.ELSE() != null && caseCtx.expression() != null)
      elseExpression = (Expression) visit(caseCtx.expression());

    // Create CaseExpression (simple form - no case expression)
    final CaseExpression caseExpression = new CaseExpression(alternatives, elseExpression);

    // Wrap in Expression, then in BaseExpression
    final Expression wrapperExpression = new Expression(-1);
    wrapperExpression.mathExpression = caseExpression;

    final BaseExpression result = new BaseExpression(-1);
    result.expression = wrapperExpression;

    // Process modifiers if any
    if (ctx.modifier() != null && !ctx.modifier().isEmpty()) {
      Modifier firstModifier = null;
      Modifier currentModifier = null;
      for (final SQLParser.ModifierContext modCtx : ctx.modifier()) {
        final Modifier modifier = (Modifier) visit(modCtx);
        if (firstModifier == null) {
          firstModifier = modifier;
          currentModifier = modifier;
        } else {
          currentModifier.next = modifier;
          currentModifier = modifier;
        }
      }
      result.modifier = firstModifier;
    }

    return result;
  }

  /**
   * Extended CASE expression visitor.
   * Grammar: extendedCaseExpression : CASE expression extendedCaseAlternative+ (ELSE expression)? END
   * Grammar: extendedCaseAlternative : WHEN expression THEN expression
   */
  public BaseExpression visitExtendedCaseExpr(final SQLParser.ExtendedCaseExprContext ctx) {
    final SQLParser.ExtendedCaseExpressionContext caseCtx = ctx.extendedCaseExpression();

    // Get the case expression (the value being tested) - first expression in the list
    final List<SQLParser.ExpressionContext> exprList = caseCtx.expression();
    final Expression testExpression = (Expression) visit(exprList.get(0));

    // Build list of alternatives
    final List<CaseAlternative> alternatives = new ArrayList<>();
    for (final SQLParser.ExtendedCaseAlternativeContext altCtx : caseCtx.extendedCaseAlternative()) {
      // Each alternative has 2 expressions: WHEN expression THEN expression
      final List<SQLParser.ExpressionContext> exprs = altCtx.expression();
      final Expression whenExpression = (Expression) visit(exprs.get(0));
      final Expression thenExpression = (Expression) visit(exprs.get(1));
      alternatives.add(new CaseAlternative(whenExpression, thenExpression));
    }

    // Handle ELSE clause (may be null)
    // ELSE expression is the last expression in the list (if ELSE exists)
    Expression elseExpression = null;
    if (caseCtx.ELSE() != null && exprList.size() > 1)
      elseExpression = (Expression) visit(exprList.get(exprList.size() - 1));

    // Create CaseExpression (extended form - with case expression)
    final CaseExpression caseExpression = new CaseExpression(testExpression, alternatives, elseExpression);

    // Wrap in Expression, then in BaseExpression
    final Expression wrapperExpression = new Expression(-1);
    wrapperExpression.mathExpression = caseExpression;

    final BaseExpression result = new BaseExpression(-1);
    result.expression = wrapperExpression;

    // Process modifiers if any
    if (ctx.modifier() != null && !ctx.modifier().isEmpty()) {
      Modifier firstModifier = null;
      Modifier currentModifier = null;
      for (final SQLParser.ModifierContext modCtx : ctx.modifier()) {
        final Modifier modifier = (Modifier) visit(modCtx);
        if (firstModifier == null) {
          firstModifier = modifier;
          currentModifier = modifier;
        } else {
          currentModifier.next = modifier;
          currentModifier = modifier;
        }
      }
      result.modifier = firstModifier;
    }

    return result;
  }

  // HELPER METHODS

  /**
   * Create an Expression that wraps a SelectStatement for execution.
   * When the expression is evaluated, it executes the subquery and returns the result.
   * For (SELECT ...) IN collection, we need to extract values from Results and check if ANY match.
   */
  private Expression createStatementExpression(final SelectStatement statement) {
    final Expression expr = new Expression(-1);
    expr.mathExpression = new SubqueryExpression(statement);
    return expr;
  }

  /**
   * Convert a SQL Expression to an OpenCypher Expression for use in CASE statements.
   * Creates a wrapper that delegates to the SQL Expression's execute method.
   */
  private com.arcadedb.query.opencypher.ast.Expression convertToOpenCypherExpression(final Expression sqlExpression) {
    return new com.arcadedb.query.opencypher.ast.Expression() {
      @Override
      public Object evaluate(final Result result, final CommandContext context) {
        return sqlExpression.execute(result, context);
      }

      @Override
      public boolean isAggregation() {
        // Check if expression is aggregate - pass null context as it's typically not needed for this check
        return sqlExpression.isAggregate(null);
      }

      @Override
      public String getText() {
        return sqlExpression.toString();
      }
    };
  }

  /**
   * Convert a SQL WhereClause to an OpenCypher Expression that evaluates to boolean.
   * Creates a wrapper that delegates to the WhereClause's evaluateExpression method.
   */
  private com.arcadedb.query.opencypher.ast.Expression convertWhereClauseToExpression(final WhereClause whereClause) {
    return new com.arcadedb.query.opencypher.ast.Expression() {
      @Override
      public Object evaluate(final Result result, final CommandContext context) {
        return whereClause.evaluateExpression(result, context);
      }

      @Override
      public boolean isAggregation() {
        // WHERE clauses in CASE typically don't contain aggregate functions
        // If they did, it would be handled by the underlying OrBlock
        return false;
      }

      @Override
      public String getText() {
        return whereClause.toString();
      }
    };
  }

  /**
   * Map ANTLR comparison operator to ArcadeDB BinaryCompareOperator.
   */
  private BinaryCompareOperator mapComparisonOperator(final SQLParser.ComparisonOperatorContext ctx) {
    if (ctx.EQ() != null || ctx.EQEQ() != null) {
      return new EqualsCompareOperator(-1);
    } else if (ctx.NE() != null) {
      return new NeOperator(-1);
    } else if (ctx.NEQ() != null) {
      return new NeqOperator(-1);
    } else if (ctx.LT() != null) {
      return new LtOperator(-1);
    } else if (ctx.GT() != null) {
      return new GtOperator(-1);
    } else if (ctx.LE() != null) {
      return new LeOperator(-1);
    } else if (ctx.GE() != null) {
      return new GeOperator(-1);
    } else if (ctx.NSEQ() != null) {
      return new NullSafeEqualsCompareOperator(-1);
    }

    throw new CommandSQLParsingException("Unknown comparison operator: " + ctx.getText());
  }

  /**
   * Visit identifier and create Identifier AST object.
   */
  @Override
  public Identifier visitIdentifier(final SQLParser.IdentifierContext ctx) {
    if (ctx.IDENTIFIER() != null) {
      return new Identifier(ctx.IDENTIFIER().getText());
    } else if (ctx.QUOTED_IDENTIFIER() != null) {
      final String quoted = ctx.QUOTED_IDENTIFIER().getText();
      // Remove backticks
      final String unquoted = quoted.substring(1, quoted.length() - 1);
      final Identifier id = new Identifier(unquoted);
      id.setQuotedStringValue(quoted);
      return id;
    } else {
      // Handle keywords used as identifiers (NAME, VALUE, TYPE, etc.)
      // Note: Record attributes (@rid, @type, @in, @out) are handled in visitIdentifierChain
      return new Identifier(ctx.getText());
    }
  }

  /**
   * Visit rid (record ID).
   * Supports negative cluster IDs: #-1:-1
   */
  @Override
  public Rid visitRid(final SQLParser.RidContext ctx) {
    final Rid rid = new Rid(-1);

    // Check if it's an expression-based RID: {rid: expr} or {"@rid": expr}
    if (ctx.expression() != null) {
      rid.expression = (Expression) visit(ctx.expression());
      rid.legacy = false;
    } else {
      // Legacy format: #bucket:position or bucket:position (supports negative numbers)
      final List<SQLParser.IntegerContext> integers = ctx.integer();
      if (integers.size() == 2) {
        rid.bucket = (PInteger) visit(integers.get(0));
        rid.position = (PInteger) visit(integers.get(1));
        rid.legacy = true;
      }
    }

    return rid;
  }

  /**
   * Visit input parameter (?, :name, $1).
   */
  @Override
  public InputParameter visitInputParameter(final SQLParser.InputParameterContext ctx) {
    if (ctx.HOOK() != null) {
      // Positional parameter: ?
      // Increment counter to assign sequential parameter numbers
      final PositionalParameter param = new PositionalParameter(-1);
      param.paramNumber = positionalParamCounter;
      positionalParamCounter++;
      return param;
    } else if (ctx.identifier() != null) {
      // Named parameter: :name
      final Identifier id = (Identifier) visit(ctx.identifier());
      final NamedParameter param = new NamedParameter(-1);
      param.paramName = id.getValue();
      param.paramNumber = positionalParamCounter;
      positionalParamCounter++;
      return param;
    } else if (ctx.INTEGER_LITERAL() != null) {
      // Positional parameter: $1, $2, etc.
      final int paramNum = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
      final PositionalParameter param = new PositionalParameter(-1);
      param.paramNumber = paramNum;
      return param;
    } else if (ctx.COLON() != null && ctx.INTEGER_LITERAL() != null) {
      // Named parameter: :1, :2, etc. (numeric named params)
      final int paramNum = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
      final NamedParameter param = new NamedParameter(-1);
      param.paramName = String.valueOf(paramNum);
      param.paramNumber = paramNum;
      return param;
    }

    throw new CommandSQLParsingException("Unknown input parameter format");
  }

  /**
   * Visit bucket list (bucket:[name1, name2, ...]).
   */
  @Override
  public BucketList visitBucketList(final SQLParser.BucketListContext ctx) {
    final BucketList bucketList = new BucketList(-1);

    for (final SQLParser.IdentifierContext idCtx : ctx.identifier()) {
      final Identifier id = (Identifier) visit(idCtx);
      bucketList.buckets.add(id);
    }

    return bucketList;
  }

  /**
   * Visit index identifier (index:name or special index types).
   */
  @Override
  public IndexIdentifier visitIndexIdentifier(final SQLParser.IndexIdentifierContext ctx) {
    final IndexIdentifier indexId = new IndexIdentifier(-1);

    if (ctx.identifier() != null) {
      // index:name format
      indexId.indexName = (Identifier) visit(ctx.identifier());
      indexId.type = IndexIdentifier.Type.INDEX;
    } else if (ctx.INDEXVALUES_IDENTIFIER() != null) {
      indexId.indexNameString = ctx.INDEXVALUES_IDENTIFIER().getText();
      indexId.type = IndexIdentifier.Type.VALUES;
    } else if (ctx.INDEXVALUESASC_IDENTIFIER() != null) {
      indexId.indexNameString = ctx.INDEXVALUESASC_IDENTIFIER().getText();
      indexId.type = IndexIdentifier.Type.VALUESASC;
    } else if (ctx.INDEXVALUESDESC_IDENTIFIER() != null) {
      indexId.indexNameString = ctx.INDEXVALUESDESC_IDENTIFIER().getText();
      indexId.type = IndexIdentifier.Type.VALUESDESC;
    }

    return indexId;
  }

  /**
   * Visit schema identifier (schema:name).
   */
  @Override
  public SchemaIdentifier visitSchemaIdentifier(final SQLParser.SchemaIdentifierContext ctx) {
    final SchemaIdentifier schemaId = new SchemaIdentifier(-1);

    if (ctx.SCHEMA_IDENTIFIER() != null) {
      final String text = ctx.SCHEMA_IDENTIFIER().getText();
      schemaId.name = text.substring("schema:".length());
    }

    return schemaId;
  }

  /**
   * Visit pInteger (positive integer).
   */
  @Override
  public PInteger visitPInteger(final SQLParser.PIntegerContext ctx) {
    final PInteger pInt = new PInteger(-1);
    final String text = ctx.INTEGER_LITERAL().getText();

    try {
      pInt.setValue(Integer.parseInt(text));
    } catch (final NumberFormatException e) {
      // If it's too large for int, try long
      pInt.setValue(Long.parseLong(text));
    }

    return pInt;
  }

  /**
   * Visit integer (positive or negative integer for RIDs).
   */
  @Override
  public PInteger visitInteger(final SQLParser.IntegerContext ctx) {
    final PInteger pInt = new PInteger(-1);
    final String text = ctx.getText();  // Includes optional MINUS sign

    try {
      pInt.setValue(Integer.parseInt(text));
    } catch (final NumberFormatException e) {
      // If it's too large for int, try long
      pInt.setValue(Long.parseLong(text));
    }

    return pInt;
  }

  /**
   * Visit LIMIT clause.
   */
  @Override
  public Limit visitLimit(final SQLParser.LimitContext ctx) {
    final Limit limit = new Limit(-1);

    // The limit expression can be a number or an input parameter
    final Expression expr = (Expression) visit(ctx.expression());

    // Extract the number or parameter from the expression
    if (expr.mathExpression instanceof BaseExpression baseExpr) {
      if (baseExpr.number instanceof PInteger) {
        limit.num = (PInteger) baseExpr.number;
      } else if (baseExpr.inputParam != null) {
        limit.inputParam = baseExpr.inputParam;
      } else {
        // For other base expressions (like identifiers from LET variables), store the expression
        limit.expression = expr;
      }
    } else {
      // Handle other expressions like -1 (negative numbers)
      // Try to evaluate the expression as a constant
      try {
        final Object value = expr.execute((Result) null, null);
        if (value instanceof Number number) {
          limit.setValue(number.intValue());
        } else {
          // Can't evaluate as constant, store expression for runtime evaluation
          limit.expression = expr;
        }
      } catch (final Exception ignored) {
        // Expression needs runtime context, store expression for runtime evaluation
        limit.expression = expr;
      }
    }

    return limit;
  }

  /**
   * Visit SKIP clause.
   */
  @Override
  public Skip visitSkip(final SQLParser.SkipContext ctx) {
    final Skip skip = new Skip(-1);

    // The skip expression can be a number or an input parameter
    final Expression expr = (Expression) visit(ctx.expression());

    // Extract the number or parameter from the expression
    try {
      if (expr.mathExpression instanceof BaseExpression baseExpr) {
        if (baseExpr.number instanceof PInteger) {
          skip.num = (PInteger) baseExpr.number;
        } else if (baseExpr.inputParam != null) {
          skip.inputParam = baseExpr.inputParam;
        } else {
          // For other base expressions (like identifiers from LET variables), store the expression
          skip.expression = expr;
        }
      } else {
        // Handle other expressions like negative numbers
        try {
          final Object value = expr.execute((Result) null, null);
          if (value instanceof Number number) {
            skip.num = new PInteger(-1).setValue(number.intValue());
          } else {
            // Can't evaluate as constant, store expression for runtime evaluation
            skip.expression = expr;
          }
        } catch (final Exception ignored) {
          // Expression needs runtime context, store expression for runtime evaluation
          skip.expression = expr;
        }
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build SKIP clause: " + e.getMessage(), e);
    }

    return skip;
  }

  /**
   * Visit TIMEOUT clause.
   */
  @Override
  public Timeout visitTimeout(final SQLParser.TimeoutContext ctx) {
    final Timeout timeout = new Timeout(-1);

    // The timeout expression can be a number or an input parameter
    final Expression expr = (Expression) visit(ctx.expression());

    // Extract the number from the expression
    try {
      if (expr.mathExpression instanceof final BaseExpression baseExpr) {

        if (baseExpr.number instanceof PInteger) {
          timeout.setValue(baseExpr.number.getValue().longValue());
        } else if (baseExpr.number instanceof PNumber) {
          timeout.setValue(baseExpr.number.getValue());
        }
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build TIMEOUT clause: " + e.getMessage(), e);
    }

    return timeout;
  }

  /**
   * Visit GROUP BY clause.
   */
  @Override
  public GroupBy visitGroupBy(final SQLParser.GroupByContext ctx) {
    final GroupBy groupBy = new GroupBy(-1);

    // GROUP BY has a list of expressions
    try {
      for (final SQLParser.ExpressionContext exprCtx : ctx.expression()) {
        final Expression expr = (Expression) visit(exprCtx);
        groupBy.items.add(expr);
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build GROUP BY clause: " + e.getMessage(), e);
    }

    return groupBy;
  }

  /**
   * Visit ORDER BY clause.
   */
  @Override
  public OrderBy visitOrderBy(final SQLParser.OrderByContext ctx) {
    final OrderBy orderBy = new OrderBy(-1);

    // ORDER BY has a list of orderByItem
    final List<OrderByItem> items = new ArrayList<>();
    for (final SQLParser.OrderByItemContext itemCtx : ctx.orderByItem()) {
      final OrderByItem item = (OrderByItem) visit(itemCtx);
      items.add(item);
    }
    orderBy.setItems(items);

    return orderBy;
  }

  /**
   * Visit ORDER BY item.
   */
  @Override
  public OrderByItem visitOrderByItem(final SQLParser.OrderByItemContext ctx) {
    final OrderByItem item = new OrderByItem();

    // Get the expression and convert it to the appropriate field
    final Expression expr = (Expression) visit(ctx.expression());

    try {
      // ORDER BY item can be an identifier or expression
      // Try to extract simple identifier case
      if (expr.mathExpression instanceof final BaseExpression baseExpr) {

        // Check if there's a modifier chain (like .num2 in a.num2)
        if (baseExpr.modifier != null) {
          // Complex expression with modifiers - set base alias and modifier chain
          // Extract base identifier
          if (baseExpr.identifier != null && baseExpr.identifier.suffix != null &&
              baseExpr.identifier.suffix.identifier != null) {
            item.setAlias(baseExpr.identifier.suffix.identifier.getValue());
            // Set the modifier chain
            item.modifier = baseExpr.modifier;
          } else {
            // Fallback: use string representation
            final StringBuilder sb = new StringBuilder();
            baseExpr.toString(Collections.emptyMap(), sb);
            item.setAlias(sb.toString());
          }
        } else {
          // No modifiers - try simple identifier extraction
          final SuffixIdentifier suffix = baseExpr.identifier.suffix;

          if (suffix != null) {
            final Identifier id = suffix.identifier;

            if (id != null) {
              // Simple identifier case
              final String identifierValue = id.getValue();

              // For system attributes like @rid, @type, etc., use recordAttr instead of alias
              if (identifierValue != null && identifierValue.startsWith("@")) {
                item.recordAttr = identifierValue;
              } else {
                item.setAlias(identifierValue);
              }
            } else {
              // Check for RecordAttribute (e.g., @rid, @type, @this)
              // Use toString to get the record attribute name
              final StringBuilder sb = new StringBuilder();
              suffix.toString(Collections.emptyMap(), sb);
              final String suffixStr = sb.toString();
              if (suffixStr != null && suffixStr.startsWith("@")) {
                item.recordAttr = suffixStr;
              }
            }
          }
        }
      }

      // If we couldn't extract anything, use the full expression as alias
      if (item.getAlias() == null && item.getRecordAttr() == null && item.modifier == null) {
        // Use toString to get the expression text
        final StringBuilder sb = new StringBuilder();
        expr.toString(Collections.emptyMap(), sb);
        item.setAlias(sb.toString());
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build ORDER BY item: " + e.getMessage(), e);
    }

    // Set ASC or DESC based on orderDirection
    // Supports ASC/DESC keywords, TRUE/FALSE boolean alternatives, and input parameters
    // TRUE = ASC (ascending, default), FALSE = DESC (descending)
    // Parameters are evaluated at runtime (true/ASC = ascending, false/DESC = descending)
    final SQLParser.OrderDirectionContext dirCtx = ctx.orderDirection();
    if (dirCtx != null) {
      if (dirCtx.inputParameter() != null) {
        // Direction specified via parameter - will be resolved at runtime
        item.setDirectionParameter((InputParameter) visit(dirCtx.inputParameter()));
        // Default to ASC, will be overridden at compare time if parameter value is false/DESC
        item.setType(OrderByItem.ASC);
      } else if (dirCtx.DESC() != null || dirCtx.FALSE() != null) {
        item.setType(OrderByItem.DESC);
      } else {
        item.setType(OrderByItem.ASC);
      }
    } else {
      item.setType(OrderByItem.ASC);
    }

    return item;
  }

  /**
   * Visit UNWIND clause.
   * Grammar: UNWIND expression (AS? identifier)?
   * The expression is what to unwind, the identifier is an optional alias.
   */
  @Override
  public Unwind visitUnwind(final SQLParser.UnwindContext ctx) {
    final Unwind unwind = new Unwind(-1);

    try {
      // The expression is what we're unwinding - it should be an identifier
      final Expression expr = (Expression) visit(ctx.expression());

      // Extract the identifier from the expression
      // The expression is typically a simple identifier like "iSeq"
      Identifier unwindField = null;

      if (expr != null && expr.mathExpression instanceof final BaseExpression baseExpr) {
        if (baseExpr.identifier != null) {
          // Access suffix field directly
          final Object suffix = baseExpr.identifier.suffix;

          if (suffix != null && suffix instanceof SuffixIdentifier) {
            // Access identifier field from SuffixIdentifier directly
            unwindField = ((SuffixIdentifier) suffix).identifier;
          }
        }
      }

      // If there's an AS clause, use that identifier, otherwise use the field itself
      if (ctx.identifier() != null) {
        final Identifier alias = (Identifier) visit(ctx.identifier());
        unwind.items.add(alias);
      } else if (unwindField != null) {
        unwind.items.add(unwindField);
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build UNWIND clause: " + e.getMessage(), e);
    }

    return unwind;
  }

  /**
   * Visit LET clause.
   */
  @Override
  public LetClause visitLetClause(final SQLParser.LetClauseContext ctx) {
    final LetClause letClause = new LetClause(-1);

    try {
      for (final SQLParser.LetItemContext itemCtx : ctx.letItem()) {
        final LetItem item = (LetItem) visit(itemCtx);
        letClause.items.add(item);
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build LET clause: " + e.getMessage(), e);
    }

    return letClause;
  }

  /**
   * Visit LET item (identifier = expression or identifier = (query)).
   */
  @Override
  public LetItem visitLetItem(final SQLParser.LetItemContext ctx) {
    final LetItem item = new LetItem(-1);

    // Set the variable name
    final Identifier varName = (Identifier) visit(ctx.identifier());
    item.setVarName(varName);

    // Check if we have a statement in parentheses within the expression
    // This happens when the grammar matches: identifier EQ expression
    // where expression → MathExprContext → mathExpression (BaseContext) → baseExpression (ParenthesizedExprContext)
    // → statement
    Statement statementFromExpr = null;
    if (ctx.expression() != null && ctx.expression() instanceof final SQLParser.MathExprContext mathExprCtx) {
      // Navigate the parse tree to find if this is a parenthesizedExpr with a statement
      final SQLParser.MathExpressionContext mathCtx = mathExprCtx.mathExpression();

      if (mathCtx != null && mathCtx instanceof final SQLParser.BaseContext baseCtx) {
        final SQLParser.BaseExpressionContext baseExprCtx = baseCtx.baseExpression();

        if (baseExprCtx != null && baseExprCtx instanceof final SQLParser.ParenthesizedStmtContext parenCtx) {
          if (parenCtx.statement() != null) {
            // Found a statement inside parentheses - visit it directly
            statementFromExpr = (Statement) visit(parenCtx.statement());
          }
        }
      }
    }

    // Set either expression or query
    if (statementFromExpr != null) {
      // Expression was actually a parenthesized statement - use it as a query
      item.setQuery(statementFromExpr);
    } else if (ctx.expression() != null) {
      // Regular expression
      final Expression expr = (Expression) visit(ctx.expression());
      item.setExpression(expr);
    } else if (ctx.statement() != null) {
      // Direct statement (though this alternative may never be reached due to grammar ambiguity)
      final Statement stmt = (Statement) visit(ctx.statement());
      item.setQuery(stmt);
    }

    return item;
  }

  // DML STATEMENT VISITORS

  /**
   * Visit INSERT statement.
   */
  @Override
  public InsertStatement visitInsertStmt(final SQLParser.InsertStmtContext ctx) {
    final InsertStatement stmt = new InsertStatement(-1);
    final SQLParser.InsertStatementContext insertCtx = ctx.insertStatement();

    // Target: identifier (BUCKET identifier)? | bucketIdentifier
    if (CollectionUtils.isNotEmpty(insertCtx.identifier())) {
      stmt.targetType = (Identifier) visit(insertCtx.identifier(0));
      // Check for BUCKET clause
      if (insertCtx.identifier().size() > 1) {
        stmt.targetBucketName = (Identifier) visit(insertCtx.identifier(1));
      }
    } else if (insertCtx.bucketIdentifier() != null) {
      // Convert BucketIdentifier to Bucket
      final BucketIdentifier bucketId = (BucketIdentifier) visit(insertCtx.bucketIdentifier());
      final Bucket bucket = new Bucket(-1);
      if (bucketId.bucketName != null) {
        bucket.bucketName = bucketId.bucketName.getStringValue();
      } else if (bucketId.bucketId != null) {
        bucket.bucketNumber = bucketId.bucketId.getValue().intValue();
      } else if (bucketId.inputParam != null) {
        bucket.inputParam = bucketId.inputParam;
      }
      stmt.targetBucket = bucket;
    }

    // Insert body (VALUES, SET, or CONTENT)
    if (insertCtx.insertBody() != null) {
      stmt.insertBody = (InsertBody) visit(insertCtx.insertBody());
    }

    // RETURN clause
    if (insertCtx.projection() != null) {
      stmt.returnStatement = (Projection) visit(insertCtx.projection());
    }

    // FROM SELECT clause
    if (insertCtx.selectStatement() != null) {
      final Object visitResult = visit(insertCtx.selectStatement());
      if (visitResult instanceof SelectStatement) {
        stmt.selectStatement = (SelectStatement) visitResult;
      } else if (visitResult instanceof FromClause) {
        // Handle case where parser returns FromClause - wrap it in a SELECT
        final SelectStatement select = new SelectStatement(-1);
        select.target = (FromClause) visitResult;
        stmt.selectStatement = select;
      } else {
        // Note: Complex SELECT statements (with WHERE, etc.) may not work without parentheses
        // This is a known grammar limitation - use INSERT INTO dst (SELECT...) instead
        throw new CommandSQLParsingException(
            "INSERT...SELECT parsing incomplete: parser returned " +
                (visitResult != null ? visitResult.getClass().getSimpleName() : "null") +
                " instead of SelectStatement. Use parentheses: INSERT INTO dst (SELECT...)"
        );
      }
      stmt.selectWithFrom = insertCtx.FROM() != null;
      stmt.selectInParentheses = insertCtx.LPAREN() != null;
    }

    // UNSAFE flag
    stmt.unsafe = insertCtx.UNSAFE() != null;

    return stmt;
  }

  /**
   * Visit INSERT body (VALUES, SET, or CONTENT).
   */
  @Override
  public InsertBody visitInsertBody(final SQLParser.InsertBodyContext ctx) {
    final InsertBody body = new InsertBody(-1);

    // VALUES clause: (field1, field2) VALUES (val1, val2), (val3, val4)
    if (ctx.VALUES() != null) {
      body.identifierList = new ArrayList<>();
      for (final SQLParser.IdentifierContext idCtx : ctx.identifier()) {
        body.identifierList.add((Identifier) visit(idCtx));
      }

      body.valueExpressions = new ArrayList<>();

      // The grammar: (id, id) VALUES (expr, expr) (, (expr, expr))*
      // All expressions are in ctx.expression() as a flat list
      // We need to group them by the number of fields per row
      final List<SQLParser.ExpressionContext> allExpressions = ctx.expression();
      final int fieldsPerRow = body.identifierList.size();

      if (fieldsPerRow > 0 && !allExpressions.isEmpty()) {
        // Split expressions into rows of size fieldsPerRow
        for (int i = 0; i < allExpressions.size(); i += fieldsPerRow) {
          List<Expression> currentRow = new ArrayList<>();
          for (int j = 0; j < fieldsPerRow && (i + j) < allExpressions.size(); j++) {
            currentRow.add((Expression) visit(allExpressions.get(i + j)));
          }
          if (!currentRow.isEmpty()) {
            body.valueExpressions.add(currentRow);
          }
        }
      }
    }
    // SET clause: SET field1 = val1, field2 = val2
    else if (ctx.SET() != null) {
      body.setExpressions = new ArrayList<>();
      for (final SQLParser.InsertSetItemContext setItemCtx : ctx.insertSetItem()) {
        final InsertSetExpression setExpr = (InsertSetExpression) visit(setItemCtx);
        body.setExpressions.add(setExpr);
      }
    }
    // CONTENT clause: CONTENT {...} or CONTENT [...] or CONTENT :param
    else if (ctx.CONTENT() != null) {
      if (ctx.json() != null) {
        body.contentJson = (Json) visit(ctx.json());
      } else if (ctx.jsonArray() != null) {
        body.contentArray = (JsonArray) visit(ctx.jsonArray());
      } else if (ctx.inputParameter() != null) {
        body.contentInputParam = (InputParameter) visit(ctx.inputParameter());
      }
    }

    return body;
  }

  /**
   * Visit INSERT SET item (field = value).
   */
  @Override
  public InsertSetExpression visitInsertSetItem(final SQLParser.InsertSetItemContext ctx) {
    final InsertSetExpression setExpr = new InsertSetExpression(-1);
    setExpr.left = (Identifier) visit(ctx.identifier());
    setExpr.right = (Expression) visit(ctx.expression());
    return setExpr;
  }

  /**
   * Visit UPDATE statement.
   */
  @Override
  public UpdateStatement visitUpdateStmt(final SQLParser.UpdateStmtContext ctx) {
    final UpdateStatement stmt = new UpdateStatement(-1);
    final SQLParser.UpdateStatementContext updateCtx = ctx.updateStatement();

    // Target
    stmt.target = (FromClause) visit(updateCtx.fromClause());

    // Update operations (SET, ADD, PUT, REMOVE, INCREMENT, MERGE, CONTENT)
    for (final SQLParser.UpdateOperationContext opCtx : updateCtx.updateOperation()) {
      final UpdateOperations ops = (UpdateOperations) visit(opCtx);
      stmt.operations.add(ops);
    }

    // UPSERT flag
    stmt.upsert = updateCtx.UPSERT() != null;

    // APPLY DEFAULTS flag
    stmt.applyDefaults = updateCtx.APPLY() != null && updateCtx.DEFAULTS() != null;

    // RETURN clause
    if (updateCtx.RETURN() != null) {
      stmt.returnBefore = updateCtx.BEFORE() != null;
      stmt.returnAfter = updateCtx.AFTER() != null;
      stmt.returnCount = updateCtx.COUNT() != null;
      if (updateCtx.projection() != null) {
        stmt.returnProjection = (Projection) visit(updateCtx.projection());
      }
    }

    // WHERE clause
    if (updateCtx.whereClause() != null) {
      stmt.whereClause = (WhereClause) visit(updateCtx.whereClause());
    }

    // LIMIT clause
    if (updateCtx.limit() != null) {
      stmt.limit = (Limit) visit(updateCtx.limit());
    }

    // TIMEOUT clause
    if (updateCtx.timeout() != null) {
      stmt.timeout = (Timeout) visit(updateCtx.timeout());
    }

    return stmt;
  }

  /**
   * Visit UPDATE operation (SET, ADD, PUT, REMOVE, INCREMENT, MERGE, CONTENT).
   */
  @Override
  public UpdateOperations visitUpdateOperation(final SQLParser.UpdateOperationContext ctx) {
    final UpdateOperations ops = new UpdateOperations(-1);

    if (ctx.SET() != null) {
      ops.type = UpdateOperations.TYPE_SET;
      for (final SQLParser.UpdateItemContext itemCtx : ctx.updateItem()) {
        ops.updateItems.add((UpdateItem) visit(itemCtx));
      }
    } else if (ctx.ADD() != null) {
      ops.type = UpdateOperations.TYPE_ADD;
      for (final SQLParser.UpdateItemContext itemCtx : ctx.updateItem()) {
        ops.updateItems.add((UpdateItem) visit(itemCtx));
      }
    } else if (ctx.PUT() != null) {
      ops.type = UpdateOperations.TYPE_PUT;
      for (final SQLParser.UpdatePutItemContext itemCtx : ctx.updatePutItem()) {
        ops.updatePutItems.add((UpdatePutItem) visit(itemCtx));
      }
    } else if (ctx.REMOVE() != null) {
      ops.type = UpdateOperations.TYPE_REMOVE;
      for (final SQLParser.UpdateRemoveItemContext itemCtx : ctx.updateRemoveItem()) {
        ops.updateRemoveItems.add((UpdateRemoveItem) visit(itemCtx));
      }
    } else if (ctx.INCREMENT() != null) {
      ops.type = UpdateOperations.TYPE_INCREMENT;
      for (final SQLParser.UpdateIncrementItemContext itemCtx : ctx.updateIncrementItem()) {
        ops.updateIncrementItems.add((UpdateIncrementItem) visit(itemCtx));
      }
    } else if (ctx.MERGE() != null) {
      ops.type = UpdateOperations.TYPE_MERGE;
      // MERGE uses json field for expression
      final Expression expr = (Expression) visit(ctx.expression());

      // Extract Json from expression
      // JSON literals can be in expr.json directly OR nested in mathExpression
      if (expr.json != null) {
        // Direct JSON literal from jsonLiteral alternative
        ops.json = expr.json;
      } else if (expr.mathExpression instanceof final BaseExpression baseExpr) {
        // JSON literal parsed as baseExpression mapLit alternative
        if (baseExpr.expression != null && baseExpr.expression.json != null) {
          ops.json = baseExpr.expression.json;
        }
      }
    } else if (ctx.CONTENT() != null) {
      ops.type = UpdateOperations.TYPE_CONTENT;
      // CONTENT accepts json, jsonArray, or inputParameter (same as INSERT)
      if (ctx.json() != null) {
        ops.json = (Json) visit(ctx.json());
      } else if (ctx.jsonArray() != null) {
        ops.jsonArray = (JsonArray) visit(ctx.jsonArray());
      } else if (ctx.inputParameter() != null) {
        ops.inputParam = (InputParameter) visit(ctx.inputParameter());
      }
    }

    return ops;
  }

  /**
   * Visit UPDATE item (identifier [modifier] operator expression).
   * Grammar: updateItem : identifier modifier? (EQ | PLUSASSIGN | MINUSASSIGN | STARASSIGN | SLASHASSIGN) expression
   */
  @Override
  public UpdateItem visitUpdateItem(final SQLParser.UpdateItemContext ctx) {
    final UpdateItem item = new UpdateItem(-1);

    // Left side: identifier
    item.left = (Identifier) visit(ctx.identifier());

    // Left modifier (optional)
    if (ctx.modifier() != null) {
      item.leftModifier = (Modifier) visit(ctx.modifier());
    }

    // Operator
    if (ctx.EQ() != null) {
      item.operator = UpdateItem.OPERATOR_EQ;
    } else if (ctx.PLUSASSIGN() != null) {
      item.operator = UpdateItem.OPERATOR_PLUSASSIGN;
    } else if (ctx.MINUSASSIGN() != null) {
      item.operator = UpdateItem.OPERATOR_MINUSASSIGN;
    } else if (ctx.STARASSIGN() != null) {
      item.operator = UpdateItem.OPERATOR_STARASSIGN;
    } else if (ctx.SLASHASSIGN() != null) {
      item.operator = UpdateItem.OPERATOR_SLASHASSIGN;
    }

    // Right side: expression
    item.right = (Expression) visit(ctx.expression());

    return item;
  }

  /**
   * Visit UPDATE REMOVE item (expression [= expression]).
   * Grammar: updateRemoveItem : expression (EQ expression)?
   */
  @Override
  public UpdateRemoveItem visitUpdateRemoveItem(final SQLParser.UpdateRemoveItemContext ctx) {
    final UpdateRemoveItem item = new UpdateRemoveItem(-1);

    // Left side: expression
    item.left = (Expression) visit(ctx.expression(0));

    // Right side: optional expression after EQ
    if (ctx.EQ() != null && ctx.expression().size() > 1) {
      item.right = (Expression) visit(ctx.expression(1));
    }

    return item;
  }

  /**
   * Visit DELETE statement.
   */
  @Override
  public DeleteStatement visitDeleteStmt(final SQLParser.DeleteStmtContext ctx) {
    final DeleteStatement stmt = new DeleteStatement(-1);
    final SQLParser.DeleteStatementContext deleteCtx = ctx.deleteStatement();

    // FROM clause
    stmt.fromClause = (FromClause) visit(deleteCtx.fromClause());

    // RETURN BEFORE flag
    stmt.returnBefore = deleteCtx.RETURN() != null && deleteCtx.BEFORE() != null;

    // WHERE clause
    if (deleteCtx.whereClause() != null) {
      stmt.whereClause = (WhereClause) visit(deleteCtx.whereClause());
    }

    // LIMIT clause
    if (deleteCtx.limit() != null) {
      stmt.limit = (Limit) visit(deleteCtx.limit());
    }

    // UNSAFE flag
    stmt.unsafe = deleteCtx.UNSAFE() != null;

    return stmt;
  }

  // TODO: Complete MOVE VERTEX visitor implementation - temporarily disabled
  // /**
  //  * Visit MOVE VERTEX statement.
  //  * Supports: MOVE VERTEX expr TO type:TypeName or TO TypeName or TO bucket:name
  //  */
  @Override
  public MoveVertexStatement visitMoveVertexStmt(final SQLParser.MoveVertexStmtContext ctx) {
    final MoveVertexStatement stmt = new MoveVertexStatement(-1);
    final SQLParser.MoveVertexStatementContext moveCtx = ctx.moveVertexStatement();

    // Source: expression (typically a subquery like "(SELECT FROM ...)")
    final Object sourceObj = visit(moveCtx.expression(0));
    final FromItem fromItem = new FromItem(-1);

    // The expression could be a subquery (SELECT statement) or other expression
    if (sourceObj instanceof SelectStatement) {
      fromItem.statement = (SelectStatement) sourceObj;
    } else if (sourceObj instanceof SubqueryExpression) {
      // Parenthesized SELECT statement wrapped in SubqueryExpression
      fromItem.statement = ((SubqueryExpression) sourceObj).getStatement();
    } else if (sourceObj instanceof final Expression expr) {
      // Expression that contains a subquery - try to extract the SelectStatement

      // Check if the mathExpression is a SubqueryExpression
      if (expr.mathExpression instanceof SubqueryExpression) {
        fromItem.statement = ((SubqueryExpression) expr.mathExpression).getStatement();
      } else {
        // Fallback: use the expression itself as identifier
        fromItem.identifier = new Identifier("(" + expr.toString(null) + ")");
      }
    } else {
      // For other expressions, throw error
      throw new CommandSQLParsingException("MOVE VERTEX source must be a SELECT subquery, got: " + sourceObj.getClass().getName());
    }
    stmt.source = fromItem;

    // Target: TYPE:identifier or BUCKET:identifier or identifier (bucket name)
    if (moveCtx.TYPE() != null) {
      // TO TYPE:typename
      stmt.targetType = (Identifier) visit(moveCtx.identifier());
    } else if (moveCtx.BUCKET() != null) {
      // TO BUCKET:bucketname
      final Identifier bucketId = (Identifier) visit(moveCtx.identifier());
      stmt.targetBucket = new Bucket(bucketId.getStringValue());
    } else {
      // TO bucketname (without BUCKET: prefix)
      final Identifier bucketId = (Identifier) visit(moveCtx.identifier());
      stmt.targetBucket = new Bucket(bucketId.getStringValue());
    }

    // SET clause (updateItems)
    if (moveCtx.SET() != null) {
      final UpdateOperations updateOps = new UpdateOperations(-1);
      updateOps.type = UpdateOperations.TYPE_SET;
      for (final SQLParser.UpdateItemContext itemCtx : moveCtx.updateItem()) {
        final UpdateItem item = (UpdateItem) visit(itemCtx);
        updateOps.updateItems.add(item);
      }
      stmt.updateOperations = updateOps;
    }

    // BATCH clause
    if (moveCtx.BATCH() != null) {
      final Batch batch = new Batch(-1);
      // BATCH expression - get the last expression (index 1 if there was a source expression)
      final int batchExprIndex = moveCtx.expression().size() - 1;
      batch.value = visit(moveCtx.expression(batchExprIndex));
      stmt.batch = batch;
    }

    return stmt;
  }

  // DDL STATEMENT VISITORS - CREATE

  /**
   * Visit bucket identifier (integer ID, bucket name, BUCKET:name/BUCKET:id syntax, or BUCKET:parameter).
   * Grammar: bucketIdentifier : INTEGER_LITERAL | identifier | BUCKET_IDENTIFIER | BUCKET_NUMBER_IDENTIFIER
   * | BUCKET_NAMED_PARAM | BUCKET_POSITIONAL_PARAM
   */
  @Override
  public BucketIdentifier visitBucketIdentifier(final SQLParser.BucketIdentifierContext ctx) {
    final BucketIdentifier bucketId = new BucketIdentifier(-1);

    if (ctx.INTEGER_LITERAL() != null) {
      // Bucket ID (integer)
      final PInteger pInt = new PInteger(-1);
      pInt.setValue(Integer.parseInt(ctx.INTEGER_LITERAL().getText()));
      bucketId.bucketId = pInt;
    } else if (ctx.identifier() != null) {
      // Bucket name (identifier)
      bucketId.bucketName = (Identifier) visit(ctx.identifier());
    } else if (ctx.BUCKET_IDENTIFIER() != null) {
      // BUCKET:name format
      final String text = ctx.BUCKET_IDENTIFIER().getText();
      final String bucketName = text.substring("bucket:".length());
      bucketId.bucketName = new Identifier(bucketName);
    } else if (ctx.BUCKET_NUMBER_IDENTIFIER() != null) {
      // BUCKET:123 format
      final String text = ctx.BUCKET_NUMBER_IDENTIFIER().getText();
      final String bucketNumberStr = text.substring("bucket:".length());
      final PInteger pInt = new PInteger(-1);
      pInt.setValue(Integer.parseInt(bucketNumberStr));
      bucketId.bucketId = pInt;
    } else if (ctx.BUCKET_NAMED_PARAM() != null) {
      // bucket::paramName format
      final String text = ctx.BUCKET_NAMED_PARAM().getText();
      final String paramName = text.substring("bucket::".length());
      final NamedParameter param = new NamedParameter(-1);
      param.paramName = paramName;
      param.paramNumber = positionalParamCounter++;
      bucketId.inputParam = param;
    } else if (ctx.BUCKET_POSITIONAL_PARAM() != null) {
      // bucket:? format
      final PositionalParameter param = new PositionalParameter(-1);
      param.paramNumber = positionalParamCounter++;
      bucketId.inputParam = param;
    }

    return bucketId;
  }

  /**
   * Visit CREATE DOCUMENT TYPE statement.
   */
  @Override
  public CreateDocumentTypeStatement visitCreateDocumentTypeStmt(final SQLParser.CreateDocumentTypeStmtContext ctx) {
    final CreateDocumentTypeStatement stmt = new CreateDocumentTypeStatement(-1);
    final SQLParser.CreateTypeBodyContext bodyCtx = ctx.createTypeBody();

    // Type name
    stmt.name = (Identifier) visit(bodyCtx.identifier(0));

    // IF NOT EXISTS
    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;

    // EXTENDS clause - supports multiple parent types (EXTENDS bar, baz)
    if (bodyCtx.EXTENDS() != null && bodyCtx.identifier().size() > 1) {
      stmt.supertypes = new ArrayList<>();
      // All identifiers after the first one (type name) are supertypes
      for (int i = 1; i < bodyCtx.identifier().size(); i++) {
        stmt.supertypes.add((Identifier) visit(bodyCtx.identifier(i)));
      }
    }

    // BUCKET clause (list of bucket identifiers)
    if (bodyCtx.BUCKET() != null && !bodyCtx.bucketIdentifier().isEmpty()) {
      stmt.buckets = new ArrayList<>();
      for (final SQLParser.BucketIdentifierContext bucketCtx : bodyCtx.bucketIdentifier()) {
        stmt.buckets.add((BucketIdentifier) visit(bucketCtx));
      }
    }

    // BUCKETS clause (total number)
    if (bodyCtx.BUCKETS() != null) {
      // Walk through all children to find INTEGER_LITERAL after BUCKETS token
      boolean foundBuckets = false;
      for (int i = 0; i < bodyCtx.getChildCount(); i++) {
        final ParseTree child = bodyCtx.getChild(i);
        if (foundBuckets && child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.INTEGER_LITERAL) {
            final PInteger pInt = new PInteger(-1);
            pInt.setValue(Integer.parseInt(termNode.getText()));
            stmt.totalBucketNo = pInt;
            break;
          }
        }
        if (child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.BUCKETS) {
            foundBuckets = true;
          }
        }
      }
    }

    // PAGESIZE clause
    if (bodyCtx.PAGESIZE() != null) {
      // Walk through all children to find INTEGER_LITERAL after PAGESIZE token
      boolean foundPageSize = false;
      for (int i = 0; i < bodyCtx.getChildCount(); i++) {
        final ParseTree child = bodyCtx.getChild(i);
        if (foundPageSize && child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.INTEGER_LITERAL) {
            final PInteger pInt = new PInteger(-1);
            pInt.setValue(Integer.parseInt(termNode.getText()));
            stmt.pageSize = pInt;
            break;
          }
        }
        if (child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.PAGESIZE) {
            foundPageSize = true;
          }
        }
      }
    }

    return stmt;
  }

  /**
   * Visit CREATE VERTEX TYPE statement.
   */
  @Override
  public CreateVertexTypeStatement visitCreateVertexTypeStmt(final SQLParser.CreateVertexTypeStmtContext ctx) {
    final CreateVertexTypeStatement stmt = new CreateVertexTypeStatement(-1);
    final SQLParser.CreateTypeBodyContext bodyCtx = ctx.createTypeBody();

    // Type name
    stmt.name = (Identifier) visit(bodyCtx.identifier(0));

    // IF NOT EXISTS
    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;

    // EXTENDS clause - supports multiple parent types (EXTENDS bar, baz)
    if (bodyCtx.EXTENDS() != null && bodyCtx.identifier().size() > 1) {
      stmt.supertypes = new ArrayList<>();
      // All identifiers after the first one (type name) are supertypes
      for (int i = 1; i < bodyCtx.identifier().size(); i++) {
        stmt.supertypes.add((Identifier) visit(bodyCtx.identifier(i)));
      }
    }

    // BUCKET clause (list of bucket identifiers)
    if (bodyCtx.BUCKET() != null && !bodyCtx.bucketIdentifier().isEmpty()) {
      stmt.buckets = new ArrayList<>();
      for (final SQLParser.BucketIdentifierContext bucketCtx : bodyCtx.bucketIdentifier()) {
        stmt.buckets.add((BucketIdentifier) visit(bucketCtx));
      }
    }

    // BUCKETS clause (total number)
    if (bodyCtx.BUCKETS() != null) {
      // Walk through all children to find INTEGER_LITERAL after BUCKETS token
      boolean foundBuckets = false;
      for (int i = 0; i < bodyCtx.getChildCount(); i++) {
        final ParseTree child = bodyCtx.getChild(i);
        if (foundBuckets && child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.INTEGER_LITERAL) {
            final PInteger pInt = new PInteger(-1);
            pInt.setValue(Integer.parseInt(termNode.getText()));
            stmt.totalBucketNo = pInt;
            break;
          }
        }
        if (child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.BUCKETS) {
            foundBuckets = true;
          }
        }
      }
    }

    // PAGESIZE clause
    if (bodyCtx.PAGESIZE() != null) {
      // Walk through all children to find INTEGER_LITERAL after PAGESIZE token
      boolean foundPageSize = false;
      for (int i = 0; i < bodyCtx.getChildCount(); i++) {
        final ParseTree child = bodyCtx.getChild(i);
        if (foundPageSize && child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.INTEGER_LITERAL) {
            final PInteger pInt = new PInteger(-1);
            pInt.setValue(Integer.parseInt(termNode.getText()));
            stmt.pageSize = pInt;
            break;
          }
        }
        if (child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.PAGESIZE) {
            foundPageSize = true;
          }
        }
      }
    }

    return stmt;
  }

  /**
   * Visit CREATE EDGE TYPE statement.
   */
  @Override
  public CreateEdgeTypeStatement visitCreateEdgeTypeStmt(final SQLParser.CreateEdgeTypeStmtContext ctx) {
    final CreateEdgeTypeStatement stmt = new CreateEdgeTypeStatement(-1);
    final SQLParser.CreateEdgeTypeBodyContext bodyCtx = ctx.createEdgeTypeBody();

    // Type name
    stmt.name = (Identifier) visit(bodyCtx.identifier(0));

    // IF NOT EXISTS
    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;

    // EXTENDS clause - supports multiple parent types (EXTENDS bar, baz)
    if (bodyCtx.EXTENDS() != null && bodyCtx.identifier().size() > 1) {
      stmt.supertypes = new ArrayList<>();
      // All identifiers after the first one (type name) are supertypes
      for (int i = 1; i < bodyCtx.identifier().size(); i++) {
        stmt.supertypes.add((Identifier) visit(bodyCtx.identifier(i)));
      }
    }

    // UNIDIRECTIONAL flag
    stmt.unidirectional = bodyCtx.UNIDIRECTIONAL() != null;

    // BUCKET clause (list of bucket identifiers)
    if (bodyCtx.BUCKET() != null && !bodyCtx.bucketIdentifier().isEmpty()) {
      stmt.buckets = new ArrayList<>();
      for (final SQLParser.BucketIdentifierContext bucketCtx : bodyCtx.bucketIdentifier()) {
        stmt.buckets.add((BucketIdentifier) visit(bucketCtx));
      }
    }

    // BUCKETS clause (total number)
    if (bodyCtx.BUCKETS() != null) {
      // Walk through all children to find INTEGER_LITERAL after BUCKETS token
      boolean foundBuckets = false;
      for (int i = 0; i < bodyCtx.getChildCount(); i++) {
        final ParseTree child = bodyCtx.getChild(i);
        if (foundBuckets && child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.INTEGER_LITERAL) {
            final PInteger pInt = new PInteger(-1);
            pInt.setValue(Integer.parseInt(termNode.getText()));
            stmt.totalBucketNo = pInt;
            break;
          }
        }
        if (child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.BUCKETS) {
            foundBuckets = true;
          }
        }
      }
    }

    // PAGESIZE clause
    if (bodyCtx.PAGESIZE() != null) {
      // Walk through all children to find INTEGER_LITERAL after PAGESIZE token
      boolean foundPageSize = false;
      for (int i = 0; i < bodyCtx.getChildCount(); i++) {
        final ParseTree child = bodyCtx.getChild(i);
        if (foundPageSize && child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.INTEGER_LITERAL) {
            final PInteger pInt = new PInteger(-1);
            pInt.setValue(Integer.parseInt(termNode.getText()));
            stmt.pageSize = pInt;
            break;
          }
        }
        if (child instanceof final TerminalNode termNode) {
          if (termNode.getSymbol().getType() == SQLParser.PAGESIZE) {
            foundPageSize = true;
          }
        }
      }
    }

    return stmt;
  }

  /**
   * Visit CREATE VERTEX (instance) statement.
   * Grammar: CREATE VERTEX identifier? (VALUES | SET | CONTENT)?
   */
  @Override
  public CreateVertexStatement visitCreateVertexStmt(final SQLParser.CreateVertexStmtContext ctx) {
    final CreateVertexStatement stmt = new CreateVertexStatement(-1);
    final SQLParser.CreateVertexBodyContext bodyCtx = ctx.createVertexBody();

    try {
      // Set targetType (vertex type identifier) - first identifier in the body
      if (bodyCtx.identifier() != null && !bodyCtx.identifier().isEmpty()) {
        // First identifier is the target type
        stmt.targetType = (Identifier) visit(bodyCtx.identifier(0));
      }

      // Create InsertBody if we have VALUES, SET, or CONTENT clauses
      if (bodyCtx.VALUES() != null || bodyCtx.SET() != null || bodyCtx.CONTENT() != null) {
        final InsertBody body = new InsertBody(-1);

        // Handle VALUES clause - (field1, field2) VALUES (val1, val2), (val3, val4)
        if (bodyCtx.VALUES() != null) {
          // Build identifier list from identifiers (skipping first if it's target type)
          final int startIdx =
              (bodyCtx.identifier() != null && !bodyCtx.identifier().isEmpty() && stmt.targetType != null) ? 1 : 0;
          if (bodyCtx.identifier() != null && bodyCtx.identifier().size() > startIdx) {
            body.identifierList = new ArrayList<>();
            for (int i = startIdx; i < bodyCtx.identifier().size(); i++) {
              body.identifierList.add((Identifier) visit(bodyCtx.identifier(i)));
            }
          }

          // Build value expressions list
          if (bodyCtx.expression() != null && !bodyCtx.expression().isEmpty()) {
            body.valueExpressions = new ArrayList<>();

            // Group expressions by VALUES rows
            // Each row has same number of expressions as identifierList
            final int colCount = body.identifierList != null ? body.identifierList.size() : 0;
            if (colCount > 0) {
              int exprIdx = 0;
              while (exprIdx < bodyCtx.expression().size()) {
                final List<Expression> row = new ArrayList<>();
                for (int i = 0; i < colCount && exprIdx < bodyCtx.expression().size(); i++) {
                  row.add((Expression) visit(bodyCtx.expression(exprIdx++)));
                }
                body.valueExpressions.add(row);
              }
            }
          }
        }
        // Handle SET clause - list of updateItems
        else if (bodyCtx.SET() != null && bodyCtx.updateItem() != null) {
          body.setExpressions = new ArrayList<>();
          for (final SQLParser.UpdateItemContext updateItemCtx : bodyCtx.updateItem()) {
            // Create InsertSetExpression from updateItem
            final InsertSetExpression setExpr = new InsertSetExpression();
            setExpr.left = (Identifier) visit(updateItemCtx.identifier());
            setExpr.right = (Expression) visit(updateItemCtx.expression());
            body.setExpressions.add(setExpr);
          }
        }
        // Handle CONTENT clause - direct json, jsonArray, or inputParameter (same as INSERT)
        else if (bodyCtx.CONTENT() != null) {
          if (bodyCtx.json() != null) {
            body.contentJson = (Json) visit(bodyCtx.json());
          } else if (bodyCtx.jsonArray() != null) {
            body.contentArray = (JsonArray) visit(bodyCtx.jsonArray());
          } else if (bodyCtx.inputParameter() != null) {
            body.contentInputParam = (InputParameter) visit(bodyCtx.inputParameter());
          }
        }

        stmt.insertBody = body;
      }

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build CREATE VERTEX statement: " + e.getMessage(), e);
    }

    return stmt;
  }

  /**
   * Visit CREATE EDGE (instance) statement.
   */
  @Override
  public CreateEdgeStatement visitCreateEdgeStmt(final SQLParser.CreateEdgeStmtContext ctx) {
    final CreateEdgeStatement stmt = new CreateEdgeStatement(-1);
    final SQLParser.CreateEdgeBodyContext bodyCtx = ctx.createEdgeBody();

    try {
      // Set targetType (edge type identifier)
      if (bodyCtx.identifier() != null) {
        final Identifier targetType = (Identifier) visit(bodyCtx.identifier());
        stmt.targetType = targetType;
      }

      // Set leftExpression (FROM clause)
      if (bodyCtx.fromItem() != null && bodyCtx.fromItem().size() > 0) {
        final FromItem fromItem = (FromItem) visit(bodyCtx.fromItem(0));
        // Convert FromItem to Expression
        final Expression leftExpr = new Expression(-1);
        if (fromItem.statement != null) {
          // Handle subquery (e.g., CREATE EDGE FROM (SELECT ...) TO ...)
          final ParenthesisExpression parenExpr = new ParenthesisExpression(-1);
          parenExpr.setStatement(fromItem.statement);
          leftExpr.mathExpression = parenExpr;
        } else if (fromItem.identifier != null) {
          leftExpr.mathExpression = new BaseExpression(fromItem.identifier);
        } else if (fromItem.rids != null) {
          // Handle RID, array of RIDs, or empty array
          if (fromItem.rids.isEmpty()) {
            final ArrayLiteralExpression arrayLit = new ArrayLiteralExpression(-1);
            arrayLit.items = new ArrayList<>();
            leftExpr.mathExpression = arrayLit;
          } else if (fromItem.rids.size() == 1) {
            leftExpr.rid = fromItem.rids.get(0);
          } else {
            // Multiple RIDs - create array literal
            final List<Expression> ridExprs = new ArrayList<>();
            for (final Rid rid : fromItem.rids) {
              final Expression ridExpr = new Expression(-1);
              ridExpr.rid = rid;
              ridExprs.add(ridExpr);
            }
            final ArrayLiteralExpression arrayLit = new ArrayLiteralExpression(-1);
            arrayLit.items = ridExprs;
            leftExpr.mathExpression = arrayLit;
          }
        } else if (fromItem.inputParam != null) {
          // Handle single input parameter (e.g., CREATE EDGE FROM :rid TO ...)
          final BaseExpression baseExpr = new BaseExpression(-1);
          baseExpr.inputParam = fromItem.inputParam;
          leftExpr.mathExpression = baseExpr;
        } else if (CollectionUtils.isNotEmpty(fromItem.inputParams)) {
          // Handle input parameters list (e.g., CREATE EDGE FROM [?, ?] TO ?)
          final List<Expression> paramExprs = new ArrayList<>();
          for (final InputParameter param : fromItem.inputParams) {
            final Expression paramExpr = new Expression(-1);
            final BaseExpression baseExpr = new BaseExpression(-1);
            baseExpr.inputParam = param;
            paramExpr.mathExpression = baseExpr;
            paramExprs.add(paramExpr);
          }
          final ArrayLiteralExpression arrayLit = new ArrayLiteralExpression(-1);
          arrayLit.items = paramExprs;
          leftExpr.mathExpression = arrayLit;
        }
        stmt.leftExpression = leftExpr;
      }

      // Set rightExpression (TO clause)
      if (bodyCtx.fromItem() != null && bodyCtx.fromItem().size() > 1) {
        final FromItem toItem = (FromItem) visit(bodyCtx.fromItem(1));
        // Convert FromItem to Expression
        final Expression rightExpr = new Expression(-1);
        if (toItem.statement != null) {
          // Handle subquery (e.g., CREATE EDGE FROM ... TO (SELECT ...))
          final ParenthesisExpression parenExpr = new ParenthesisExpression(-1);
          parenExpr.setStatement(toItem.statement);
          rightExpr.mathExpression = parenExpr;
        } else if (toItem.identifier != null) {
          rightExpr.mathExpression = new BaseExpression(toItem.identifier);
        } else if (toItem.rids != null) {
          // Handle RID, array of RIDs, or empty array
          if (toItem.rids.isEmpty()) {
            final ArrayLiteralExpression arrayLit = new ArrayLiteralExpression(-1);
            arrayLit.items = new ArrayList<>();
            rightExpr.mathExpression = arrayLit;
          } else if (toItem.rids.size() == 1) {
            rightExpr.rid = toItem.rids.get(0);
          } else {
            // Multiple RIDs - create array literal
            final List<Expression> ridExprs = new ArrayList<>();
            for (final Rid rid : toItem.rids) {
              final Expression ridExpr = new Expression(-1);
              ridExpr.rid = rid;
              ridExprs.add(ridExpr);
            }
            final ArrayLiteralExpression arrayLit = new ArrayLiteralExpression(-1);
            arrayLit.items = ridExprs;
            rightExpr.mathExpression = arrayLit;
          }
        } else if (toItem.inputParam != null) {
          // Handle single input parameter (e.g., CREATE EDGE FROM ... TO :rid)
          final BaseExpression baseExpr = new BaseExpression(-1);
          baseExpr.inputParam = toItem.inputParam;
          rightExpr.mathExpression = baseExpr;
        } else if (CollectionUtils.isNotEmpty(toItem.inputParams)) {
          // Handle input parameters list (e.g., CREATE EDGE FROM ? TO [?, ?])
          final List<Expression> paramExprs = new ArrayList<>();
          for (final InputParameter param : toItem.inputParams) {
            final Expression paramExpr = new Expression(-1);
            final BaseExpression baseExpr = new BaseExpression(-1);
            baseExpr.inputParam = param;
            paramExpr.mathExpression = baseExpr;
            paramExprs.add(paramExpr);
          }
          final ArrayLiteralExpression arrayLit = new ArrayLiteralExpression(-1);
          arrayLit.items = paramExprs;
          rightExpr.mathExpression = arrayLit;
        }
        stmt.rightExpression = rightExpr;
      }

      // Set body (SET clause) if present
      if (bodyCtx.SET() != null && bodyCtx.updateItem() != null) {
        final InsertBody body = new InsertBody(-1);
        body.setExpressions = new ArrayList<>();
        for (final SQLParser.UpdateItemContext updateItemCtx : bodyCtx.updateItem()) {
          // Manually create InsertSetExpression from updateItem
          final InsertSetExpression setExpr = new InsertSetExpression();
          setExpr.left = (Identifier) visit(updateItemCtx.identifier());
          setExpr.right = (Expression) visit(updateItemCtx.expression());
          body.setExpressions.add(setExpr);
        }
        stmt.body = body;
      }

      // Set CONTENT clause if present
      // Note: CREATE EDGE CONTENT takes an expression (unlike INSERT/CREATE VERTEX which take json|jsonArray directly)
      // The expression is typically an array literal like [{'x':0}] or a map literal like {'x':0}
      if (bodyCtx.CONTENT() != null && bodyCtx.expression() != null) {
        final InsertBody body = stmt.body != null ? stmt.body : new InsertBody(-1);
        final Expression contentExpr = (Expression) visit(bodyCtx.expression());

        // Try to extract json or jsonArray from the expression
        if (contentExpr.json != null) {
          // Direct JSON literal like {'x':0}
          body.contentJson = contentExpr.json;
        } else if (contentExpr.mathExpression instanceof final BaseExpression baseExpr) {
          // BaseExpression wrapping an array literal, map literal, or input parameter

          // Check if it contains an array literal
          if (baseExpr.expression != null && baseExpr.expression.mathExpression instanceof final ArrayLiteralExpression arrayLit) {

            // For CREATE EDGE, if array has single element, extract it as contentJson
            // If array has multiple elements, store as contentArray (executor will validate)
            if (arrayLit.items.size() == 1) {
              // Single element array [{'x':0}] - extract the json
              final Expression firstItem = arrayLit.items.get(0);
              Json itemJson = firstItem.json;

              // If json is null, try unwrapping from BaseExpression
              if (itemJson == null && firstItem.mathExpression instanceof final BaseExpression itemBaseExpr) {
                if (itemBaseExpr.expression != null && itemBaseExpr.expression.json != null) {
                  itemJson = itemBaseExpr.expression.json;
                }
              }

              if (itemJson != null) {
                body.contentJson = itemJson;
              }
            } else {
              // Multiple elements or non-json items - convert to JsonArray
              final JsonArray jsonArray = new JsonArray(-1);
              for (final Expression itemExpr : arrayLit.items) {
                Json itemJson = itemExpr.json;
                // If json is null, try unwrapping from BaseExpression
                if (itemJson == null && itemExpr.mathExpression instanceof final BaseExpression itemBaseExpr) {
                  if (itemBaseExpr.expression != null && itemBaseExpr.expression.json != null) {
                    itemJson = itemBaseExpr.expression.json;
                  }
                }
                if (itemJson != null) {
                  jsonArray.items.add(itemJson);
                }
              }
              body.contentArray = jsonArray;
            }
          } else if (baseExpr.inputParam != null) {
            body.contentInputParam = baseExpr.inputParam;
          }
        } else if (contentExpr.mathExpression instanceof final ArrayLiteralExpression arrayLit) {
          // Direct ArrayLiteralExpression (shouldn't happen with current grammar, but handle it)
          if (arrayLit.items.size() == 1 && arrayLit.items.get(0).json != null) {
            body.contentJson = arrayLit.items.get(0).json;
          } else {
            final JsonArray jsonArray = new JsonArray(-1);
            for (final Expression itemExpr : arrayLit.items) {
              if (itemExpr.json != null) {
                jsonArray.items.add(itemExpr.json);
              }
            }
            body.contentArray = jsonArray;
          }
        }

        stmt.body = body;
      }

      // Set IF NOT EXISTS flag
      stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;

      // Set unidirectional flag
      if (bodyCtx.UNIDIRECTIONAL() != null) {
        stmt.unidirectional = true;
      }

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build CREATE EDGE statement: " + e.getMessage(), e);
    }

    return stmt;
  }

  /**
   * Visit CREATE PROPERTY statement.
   */
  @Override
  public CreatePropertyStatement visitCreatePropertyStmt(final SQLParser.CreatePropertyStmtContext ctx) {
    final CreatePropertyStatement stmt = new CreatePropertyStatement(-1);
    final SQLParser.CreatePropertyBodyContext bodyCtx = ctx.createPropertyBody();

    // Type.property names (identifier DOT identifier)
    stmt.typeName = (Identifier) visit(bodyCtx.identifier(0));
    stmt.propertyName = (Identifier) visit(bodyCtx.identifier(1));

    // IF NOT EXISTS
    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;

    // Property type - get from propertyType rule
    final SQLParser.PropertyTypeContext propTypeCtx = bodyCtx.propertyType();
    if (propTypeCtx != null) {
      stmt.propertyType = (Identifier) visit(propTypeCtx.identifier(0));

      // OF clause (optional - for types like LIST OF INTEGER)
      if (propTypeCtx.OF() != null && propTypeCtx.identifier().size() > 1) {
        stmt.ofType = (Identifier) visit(propTypeCtx.identifier(1));
      }
    }

    // Attributes (MANDATORY, READONLY, etc.)
    if (bodyCtx.propertyAttributes() != null) {
      for (final SQLParser.PropertyAttributeContext attrCtx : bodyCtx.propertyAttributes().propertyAttribute()) {
        final CreatePropertyAttributeStatement attr = (CreatePropertyAttributeStatement) visit(attrCtx);
        stmt.attributes.add(attr);
      }
    }

    return stmt;
  }

  /**
   * Visit property attribute (e.g., MANDATORY, READONLY true, MAX 5, DEFAULT sysdate()).
   * Grammar: identifier expression?
   */
  @Override
  public CreatePropertyAttributeStatement visitPropertyAttribute(final SQLParser.PropertyAttributeContext ctx) {
    final CreatePropertyAttributeStatement attr = new CreatePropertyAttributeStatement(-1);

    // Attribute name
    attr.settingName = (Identifier) visit(ctx.identifier());

    // Attribute value (optional expression - if not present, it's a boolean flag set to true)
    if (ctx.expression() != null) {
      attr.settingValue = (Expression) visit(ctx.expression());
    }
    // If no value specified, leave settingValue as null (implies true for boolean flags)

    return attr;
  }

  /**
   * Visit CREATE INDEX statement.
   */
  @Override
  public CreateIndexStatement visitCreateIndexStmt(final SQLParser.CreateIndexStmtContext ctx) {
    final CreateIndexStatement stmt = new CreateIndexStatement(-1);
    final SQLParser.CreateIndexBodyContext bodyCtx = ctx.createIndexBody();

    // IF NOT EXISTS flag
    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;

    // Index name and type name
    // Grammar: identifier? (IF NOT EXISTS)? ON TYPE? identifier (properties) ...
    // The identifier BEFORE ON is the optional index name
    // The identifier AFTER ON is the type/table name
    // We need to check if the first identifier comes before the ON keyword

    int idxNamePos = -1;
    int typeNamePos = -1;
    int onTokenIndex = -1;

    // Find the ON token position in the token stream
    for (int i = 0; i < bodyCtx.getChildCount(); i++) {
      if (bodyCtx.getChild(i) instanceof final TerminalNode termNode) {
        if (termNode.getSymbol().getType() == SQLParser.ON) {
          onTokenIndex = i;
          break;
        }
      }
    }

    // Determine which identifiers are index name vs type name
    if (onTokenIndex > 0) {
      // Check if there's an identifier before ON
      boolean hasIndexName = false;
      for (int i = 0; i < onTokenIndex; i++) {
        if (bodyCtx.getChild(i) == bodyCtx.identifier(0)) {
          hasIndexName = true;
          break;
        }
      }

      if (hasIndexName && bodyCtx.identifier().size() >= 2) {
        stmt.name = (Identifier) visit(bodyCtx.identifier(0));
        stmt.typeName = (Identifier) visit(bodyCtx.identifier(1));
      } else if (bodyCtx.identifier().size() >= 1) {
        stmt.typeName = (Identifier) visit(bodyCtx.identifier(0));
      }
    } else {
      // Fallback: assume first identifier is type name if ON not found
      stmt.typeName = (Identifier) visit(bodyCtx.identifier(0));
    }

    // Index properties
    for (final SQLParser.IndexPropertyContext propCtx : bodyCtx.indexProperty()) {
      final CreateIndexStatement.Property prop = new CreateIndexStatement.Property();
      prop.name = (Identifier) visit(propCtx.identifier());

      // Handle BY KEY/VALUE/ITEM syntax
      if (propCtx.BY() != null) {
        if (propCtx.KEY() != null) {
          prop.byKey = true;
        } else if (propCtx.VALUE() != null) {
          prop.byValue = true;
        } else if (propCtx.ITEM() != null) {
          prop.byItem = true;
        }
      }

      stmt.propertyList.add(prop);
    }

    // Index type (UNIQUE, NOTUNIQUE, FULL_TEXT, LSM_VECTOR, or custom identifier)
    if (bodyCtx.indexType() != null) {
      if (bodyCtx.indexType().UNIQUE() != null) {
        stmt.type = new Identifier("UNIQUE");
      } else if (bodyCtx.indexType().NOTUNIQUE() != null) {
        stmt.type = new Identifier("NOTUNIQUE");
      } else if (bodyCtx.indexType().FULL_TEXT() != null) {
        stmt.type = new Identifier("FULL_TEXT");
      } else if (bodyCtx.indexType().identifier() != null) {
        // Handle custom index types like LSM_VECTOR
        stmt.type = (Identifier) visit(bodyCtx.indexType().identifier());
      }
    }

    // NULL_STRATEGY and ENGINE
    // Determine which identifiers are which based on presence of optional elements
    // Grammar: identifier? (IF NOT EXISTS)? ON TYPE? identifier ... (NULL_STRATEGY identifier)? (ENGINE identifier)?
    // If index name is present: identifier(0)=name, identifier(1)=table, identifier(2+)=NULL_STRATEGY/ENGINE
    // If index name is NOT present: identifier(0)=table, identifier(1+)=NULL_STRATEGY/ENGINE

    int extraIdIndex = stmt.name != null ? 2 : 1; // Start position for NULL_STRATEGY/ENGINE identifiers

    if (bodyCtx.NULL_STRATEGY() != null && bodyCtx.identifier().size() > extraIdIndex) {
      final Identifier nsId = (Identifier) visit(bodyCtx.identifier(extraIdIndex));
      stmt.nullStrategy = LSMTreeIndexAbstract.NULL_STRATEGY.valueOf(nsId.getValue().toUpperCase());
      extraIdIndex++; // Move to next identifier position for ENGINE
    }

    // METADATA
    if (bodyCtx.METADATA() != null && bodyCtx.json() != null) {
      stmt.metadata = (Json) visit(bodyCtx.json());
    }

    // ENGINE
    if (bodyCtx.ENGINE() != null && bodyCtx.identifier().size() > extraIdIndex) {
      stmt.engine = (Identifier) visit(bodyCtx.identifier(extraIdIndex));
    }

    return stmt;
  }

  /**
   * Visit CREATE BUCKET statement.
   * Grammar: CREATE BUCKET identifier (IF NOT EXISTS)?
   */
  @Override
  public CreateBucketStatement visitCreateBucketStmt(final SQLParser.CreateBucketStmtContext ctx) {
    final CreateBucketStatement stmt = new CreateBucketStatement(-1);
    final SQLParser.CreateBucketBodyContext bodyCtx = ctx.createBucketBody();

    // Bucket name
    stmt.name = (Identifier) visit(bodyCtx.identifier());

    // IF NOT EXISTS flag
    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;

    return stmt;
  }

  /**
   * Visit DEFINE FUNCTION statement.
   * Grammar: DEFINE FUNCTION identifier DOT identifier STRING_LITERAL (PARAMETERS LBRACKET parameterList RBRACKET)?
   * (LANGUAGE identifier)?
   */
  @Override
  public DefineFunctionStatement visitDefineFunctionStatement(final SQLParser.DefineFunctionStatementContext ctx) {
    final DefineFunctionStatement stmt = new DefineFunctionStatement(-1);

    // Library name (first identifier)
    stmt.libraryName = (Identifier) visit(ctx.identifier(0));

    // Function name (second identifier)
    stmt.functionName = (Identifier) visit(ctx.identifier(1));

    // Code (quoted string)
    stmt.codeQuoted = ctx.STRING_LITERAL().getText();

    // Code (unquoted - remove surrounding quotes)
    String codeText = ctx.STRING_LITERAL().getText();
    if (codeText.startsWith("\"") && codeText.endsWith("\"")) {
      stmt.code = codeText.substring(1, codeText.length() - 1);
    } else if (codeText.startsWith("'") && codeText.endsWith("'")) {
      stmt.code = codeText.substring(1, codeText.length() - 1);
    } else {
      stmt.code = codeText;
    }

    // Parameters (optional)
    if (ctx.parameterList() != null) {
      stmt.parameters = new ArrayList<>();
      for (final SQLParser.IdentifierContext idCtx : ctx.parameterList().identifier()) {
        stmt.parameters.add((Identifier) visit(idCtx));
      }
    }

    // Language (optional)
    if (ctx.LANGUAGE() != null) {
      // The identifier after LANGUAGE is at index 2 (after library and function names)
      stmt.language = (Identifier) visit(ctx.identifier(2));
    }

    return stmt;
  }

  /**
   * Visit DELETE FUNCTION statement.
   * Grammar: DELETE FUNCTION identifier DOT identifier
   */
  @Override
  public DeleteFunctionStatement visitDeleteFunctionStmt(final SQLParser.DeleteFunctionStmtContext ctx) {
    final DeleteFunctionStatement stmt = new DeleteFunctionStatement(-1);
    final SQLParser.DeleteFunctionStatementContext bodyCtx = ctx.deleteFunctionStatement();

    // Library name (first identifier)
    stmt.libraryName = (Identifier) visit(bodyCtx.identifier(0));

    // Function name (second identifier)
    stmt.functionName = (Identifier) visit(bodyCtx.identifier(1));

    return stmt;
  }

  /**
   * Visit REBUILD INDEX statement.
   * Grammar: REBUILD INDEX (identifier | STAR) (WITH identifier EQ expression (COMMA identifier EQ expression)*)?
   */
  @Override
  public RebuildIndexStatement visitRebuildIndexStatement(final SQLParser.RebuildIndexStatementContext ctx) {
    final RebuildIndexStatement stmt = new RebuildIndexStatement(-1);

    if (ctx.STAR() != null) {
      // REBUILD INDEX * - rebuild all indexes
      stmt.all = true;
    } else if (ctx.identifier().size() > 0) {
      // REBUILD INDEX indexName (the first identifier if not with WITH params, or the only identifier if WITH)
      // If WITH is present, identifier(0) is the index name, identifier(1+) are setting keys
      stmt.name = (Identifier) visit(ctx.identifier(0));
    }

    // Handle WITH settings
    if (ctx.WITH() != null) {
      // WITH identifier EQ expression (COMMA identifier EQ expression)*
      // identifier(0) is index name (already processed above)
      // identifier(1), identifier(2), ... are setting keys
      // expression(0), expression(1), ... are setting values
      final List<SQLParser.IdentifierContext> settingKeys = ctx.identifier().subList(1, ctx.identifier().size());
      final List<SQLParser.ExpressionContext> settingValues = ctx.expression();

      for (int i = 0; i < settingKeys.size() && i < settingValues.size(); i++) {
        final Expression key = new Expression((Identifier) visit(settingKeys.get(i)));
        final Expression value = (Expression) visit(settingValues.get(i));
        stmt.settings.put(key, value);
      }
    }

    return stmt;
  }

  // DDL STATEMENTS - ALTER

  /**
   * Visit ALTER TYPE statement.
   */
  @Override
  public AlterTypeStatement visitAlterTypeStmt(final SQLParser.AlterTypeStmtContext ctx) {
    final AlterTypeStatement stmt = new AlterTypeStatement(-1);
    final SQLParser.AlterTypeBodyContext bodyCtx = ctx.alterTypeBody();

    // Type name
    stmt.name = (Identifier) visit(bodyCtx.identifier());

    // Process all alter type items
    for (final SQLParser.AlterTypeItemContext itemCtx : bodyCtx.alterTypeItem()) {
      if (itemCtx.NAME() != null) {
        stmt.property = "name";
        stmt.identifierValue = (Identifier) visit(itemCtx.identifier(0));
      } else if (itemCtx.SUPERTYPE() != null) {
        stmt.property = "supertype";
        // Process SUPERTYPE with optional +/- prefixes for each identifier
        // Grammar: SUPERTYPE ((PLUS | MINUS)? identifier (COMMA (PLUS | MINUS)? identifier)*)
        boolean nextIsAdd = true; // Default is add if no +/- specified
        int identifierIndex = 0;
        for (int i = 0; i < itemCtx.getChildCount(); i++) {
          final var child = itemCtx.getChild(i);
          if (child instanceof final TerminalNode terminal) {
            if (terminal.getSymbol().getType() == SQLParser.PLUS) {
              nextIsAdd = true;
            } else if (terminal.getSymbol().getType() == SQLParser.MINUS) {
              nextIsAdd = false;
            }
          } else if (child instanceof SQLParser.IdentifierContext) {
            stmt.identifierListValue.add((Identifier) visit(itemCtx.identifier(identifierIndex++)));
            stmt.identifierListAddRemove.add(nextIsAdd);
            nextIsAdd = true; // Reset to default for next identifier
          }
        }
      } else if (itemCtx.BUCKETSELECTIONSTRATEGY() != null) {
        stmt.property = "bucketselectionstrategy";
        stmt.identifierValue = (Identifier) visit(itemCtx.identifier(0));
      } else if (itemCtx.BUCKET() != null) {
        stmt.property = "bucket";
        // Process BUCKET with +/- identifiers
        // Grammar: BUCKET ((PLUS | MINUS) identifier)+
        int identifierIndex = 0;
        for (int i = 0; i < itemCtx.getChildCount(); i++) {
          final var child = itemCtx.getChild(i);
          if (child instanceof final TerminalNode terminal) {
            if (terminal.getSymbol().getType() == SQLParser.PLUS) {
              stmt.identifierListAddRemove.add(true);
            } else if (terminal.getSymbol().getType() == SQLParser.MINUS) {
              stmt.identifierListAddRemove.add(false);
            }
          } else if (child instanceof SQLParser.IdentifierContext) {
            stmt.identifierListValue.add((Identifier) visit(itemCtx.identifier(identifierIndex++)));
          }
        }
      } else if (itemCtx.CUSTOM() != null) {
        stmt.customKey = (Identifier) visit(itemCtx.identifier(0));
        stmt.customValue = (Expression) visit(itemCtx.expression());
      } else if (itemCtx.ALIASES() != null) {
        stmt.property = "aliases";
        // Check if NULL (to clear aliases) or identifiers (to set aliases)
        if (itemCtx.NULL() != null) {
          // NULL means clear all aliases - leave identifierListValue empty
        } else {
          // Add all alias identifiers
          for (final SQLParser.IdentifierContext aliasCtx : itemCtx.identifier()) {
            stmt.identifierListValue.add((Identifier) visit(aliasCtx));
            stmt.identifierListAddRemove.add(true); // Always add for ALIASES
          }
        }
      }
    }

    return stmt;
  }

  /**
   * Visit ALTER PROPERTY statement.
   */
  @Override
  public AlterPropertyStatement visitAlterPropertyStmt(final SQLParser.AlterPropertyStmtContext ctx) {
    final AlterPropertyStatement stmt = new AlterPropertyStatement(-1);
    final SQLParser.AlterPropertyBodyContext bodyCtx = ctx.alterPropertyBody();

    // Type.property
    final List<SQLParser.IdentifierContext> identifiers = bodyCtx.identifier();
    stmt.typeName = (Identifier) visit(identifiers.get(0));
    stmt.propertyName = (Identifier) visit(identifiers.get(1));

    // Process all alter property items
    for (final SQLParser.AlterPropertyItemContext itemCtx : bodyCtx.alterPropertyItem()) {
      if (itemCtx.NAME() != null) {
        stmt.settingName = new Identifier("name");
        stmt.settingValue = (Expression) visit(itemCtx.identifier());
      } else if (itemCtx.TYPE() != null) {
        stmt.settingName = new Identifier("type");
        // Property type as expression
        final Identifier typeId = new Identifier(itemCtx.propertyType().getText());
        final BaseExpression baseExpr = new BaseExpression(-1);
        baseExpr.identifier = new BaseIdentifier(typeId);
        final Expression expr = new Expression(-1);
        expr.mathExpression = baseExpr;
        stmt.settingValue = expr;
      } else if (itemCtx.CUSTOM() != null) {
        stmt.customPropertyName = (Identifier) visit(itemCtx.identifier());
        stmt.customPropertyValue = (Expression) visit(itemCtx.expression());
      } else if (itemCtx.identifier() != null) {
        // Property attribute (MANDATORY, READONLY, REGEXP, etc.)
        stmt.settingName = (Identifier) visit(itemCtx.identifier());
        if (itemCtx.expression() != null) {
          stmt.settingValue = (Expression) visit(itemCtx.expression());
        }
      }
    }

    return stmt;
  }

  /**
   * Visit ALTER BUCKET statement.
   */
  @Override
  public AlterBucketStatement visitAlterBucketStmt(final SQLParser.AlterBucketStmtContext ctx) {
    final AlterBucketStatement stmt = new AlterBucketStatement(-1);
    final SQLParser.AlterBucketBodyContext bodyCtx = ctx.alterBucketBody();

    // Bucket name
    stmt.name = (Identifier) visit(bodyCtx.identifier());

    // Process all alter bucket items
    for (final SQLParser.AlterBucketItemContext itemCtx : bodyCtx.alterBucketItem()) {
      if (itemCtx.NAME() != null) {
        stmt.attributeName = new Identifier("name");
        stmt.attributeValue = (Expression) visit(itemCtx.identifier());
      } else if (itemCtx.CUSTOM() != null) {
        stmt.attributeName = (Identifier) visit(itemCtx.identifier());
        stmt.attributeValue = (Expression) visit(itemCtx.expression());
      }
    }

    return stmt;
  }

  /**
   * Visit ALTER DATABASE statement.
   */
  @Override
  public AlterDatabaseStatement visitAlterDatabaseStmt(final SQLParser.AlterDatabaseStmtContext ctx) {
    final AlterDatabaseStatement stmt = new AlterDatabaseStatement(-1);
    final SQLParser.AlterDatabaseBodyContext bodyCtx = ctx.alterDatabaseBody();

    // Process all alter database items
    for (final SQLParser.AlterDatabaseItemContext itemCtx : bodyCtx.alterDatabaseItem()) {
      stmt.settingName = (Identifier) visit(itemCtx.identifier());
      stmt.settingValue = (Expression) visit(itemCtx.expression());
      // Note: grammar allows multiple items, but statement only stores one
      // This matches JavaCC behavior - last item wins
    }

    return stmt;
  }

  // DDL STATEMENTS - DROP

  /**
   * Visit DROP TYPE statement.
   */
  @Override
  public DropTypeStatement visitDropTypeStmt(final SQLParser.DropTypeStmtContext ctx) {
    final DropTypeStatement stmt = new DropTypeStatement(-1);
    final SQLParser.DropTypeBodyContext bodyCtx = ctx.dropTypeBody();

    // Type name - can be either identifier or input parameter
    if (bodyCtx.identifier() != null) {
      stmt.name = (Identifier) visit(bodyCtx.identifier());
    } else if (bodyCtx.inputParameter() != null) {
      stmt.nameParam = (InputParameter) visit(bodyCtx.inputParameter());
    }

    // IF EXISTS
    stmt.ifExists = bodyCtx.IF() != null && bodyCtx.EXISTS() != null;

    // UNSAFE
    stmt.unsafe = bodyCtx.UNSAFE() != null;

    return stmt;
  }

  /**
   * Visit DROP PROPERTY statement.
   */
  @Override
  public DropPropertyStatement visitDropPropertyStmt(final SQLParser.DropPropertyStmtContext ctx) {
    final DropPropertyStatement stmt = new DropPropertyStatement(-1);
    final SQLParser.DropPropertyBodyContext bodyCtx = ctx.dropPropertyBody();

    // Type.property
    final List<SQLParser.IdentifierContext> identifiers = bodyCtx.identifier();
    stmt.typeName = (Identifier) visit(identifiers.get(0));
    stmt.propertyName = (Identifier) visit(identifiers.get(1));

    // IF EXISTS
    stmt.ifExists = bodyCtx.IF() != null && bodyCtx.EXISTS() != null;

    return stmt;
  }

  /**
   * Visit DROP INDEX statement.
   */
  @Override
  public DropIndexStatement visitDropIndexStmt(final SQLParser.DropIndexStmtContext ctx) {
    final DropIndexStatement stmt = new DropIndexStatement(-1);
    final SQLParser.DropIndexBodyContext bodyCtx = ctx.dropIndexBody();

    // Index name or STAR (*)
    if (bodyCtx.STAR() != null) {
      stmt.all = true;
    } else if (bodyCtx.identifier() != null) {
      stmt.name = (Identifier) visit(bodyCtx.identifier());
    }

    // IF EXISTS
    stmt.ifExists = bodyCtx.IF() != null && bodyCtx.EXISTS() != null;

    return stmt;
  }

  /**
   * Visit DROP BUCKET statement.
   */
  @Override
  public DropBucketStatement visitDropBucketStmt(final SQLParser.DropBucketStmtContext ctx) {
    final DropBucketStatement stmt = new DropBucketStatement(-1);
    final SQLParser.DropBucketBodyContext bodyCtx = ctx.dropBucketBody();

    // Bucket name
    stmt.name = (Identifier) visit(bodyCtx.identifier());

    // IF EXISTS
    stmt.ifExists = bodyCtx.IF() != null && bodyCtx.EXISTS() != null;

    return stmt;
  }

  // TRIGGER MANAGEMENT

  /**
   * Visit CREATE TRIGGER statement.
   */
  @Override
  public CreateTriggerStatement visitCreateTriggerStmt(final SQLParser.CreateTriggerStmtContext ctx) {
    final CreateTriggerStatement stmt = new CreateTriggerStatement(-1);
    final SQLParser.CreateTriggerBodyContext bodyCtx = ctx.createTriggerBody();

    // IF NOT EXISTS flag
    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;

    // Trigger name (first identifier)
    stmt.name = (Identifier) visit(bodyCtx.identifier(0));

    // Trigger timing (BEFORE or AFTER)
    stmt.timing = (Identifier) visit(bodyCtx.triggerTiming());

    // Trigger event (CREATE, READ, UPDATE, DELETE)
    stmt.event = (Identifier) visit(bodyCtx.triggerEvent());

    // Type name (second identifier - the one after ON)
    stmt.typeName = (Identifier) visit(bodyCtx.identifier(1));

    // Action type and code (SQL or JAVASCRIPT)
    final SQLParser.TriggerActionContext actionCtx = bodyCtx.triggerAction();
    final Identifier actionTypeId = (Identifier) visit(actionCtx.identifier());
    stmt.actionType = actionTypeId;

    // Extract string literal and remove quotes
    final String rawText = actionCtx.STRING_LITERAL().getText();
    stmt.actionCode = rawText.substring(1, rawText.length() - 1);

    return stmt;
  }

  /**
   * Visit DROP TRIGGER statement.
   */
  @Override
  public DropTriggerStatement visitDropTriggerStmt(final SQLParser.DropTriggerStmtContext ctx) {
    final DropTriggerStatement stmt = new DropTriggerStatement(-1);
    final SQLParser.DropTriggerBodyContext bodyCtx = ctx.dropTriggerBody();

    // Trigger name
    stmt.name = (Identifier) visit(bodyCtx.identifier());

    // IF EXISTS
    stmt.ifExists = bodyCtx.IF() != null && bodyCtx.EXISTS() != null;

    return stmt;
  }

  // =========================================================================
  // MATERIALIZED VIEW MANAGEMENT
  // =========================================================================

  @Override
  public CreateMaterializedViewStatement visitCreateMaterializedViewStmt(
      final SQLParser.CreateMaterializedViewStmtContext ctx) {
    final CreateMaterializedViewStatement stmt = new CreateMaterializedViewStatement(-1);
    final SQLParser.CreateMaterializedViewBodyContext bodyCtx = ctx.createMaterializedViewBody();

    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;
    stmt.name = (Identifier) visit(bodyCtx.identifier());
    stmt.selectStatement = (SelectStatement) visit(bodyCtx.selectStatement());

    if (bodyCtx.materializedViewRefreshClause() != null) {
      final SQLParser.MaterializedViewRefreshClauseContext refreshCtx =
          bodyCtx.materializedViewRefreshClause();
      if (refreshCtx.MANUAL() != null)
        stmt.refreshMode = "MANUAL";
      else if (refreshCtx.INCREMENTAL() != null)
        stmt.refreshMode = "INCREMENTAL";
      else if (refreshCtx.EVERY() != null) {
        stmt.refreshMode = "PERIODIC";
        stmt.refreshInterval = Integer.parseInt(refreshCtx.INTEGER_LITERAL().getText());
        final SQLParser.MaterializedViewTimeUnitContext unitCtx =
            refreshCtx.materializedViewTimeUnit();
        if (unitCtx.SECOND() != null)
          stmt.refreshUnit = "SECOND";
        else if (unitCtx.MINUTE() != null)
          stmt.refreshUnit = "MINUTE";
        else if (unitCtx.HOUR() != null)
          stmt.refreshUnit = "HOUR";
      }
    }

    if (bodyCtx.BUCKETS() != null)
      stmt.buckets = Integer.parseInt(bodyCtx.INTEGER_LITERAL().getText());

    return stmt;
  }

  @Override
  public CreateTimeSeriesTypeStatement visitCreateTimeSeriesTypeStmt(
      final SQLParser.CreateTimeSeriesTypeStmtContext ctx) {
    final CreateTimeSeriesTypeStatement stmt = new CreateTimeSeriesTypeStatement(-1);
    final SQLParser.CreateTimeSeriesTypeBodyContext bodyCtx = ctx.createTimeSeriesTypeBody();

    stmt.name = (Identifier) visit(bodyCtx.identifier(0));
    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;

    // TIMESTAMP column
    if (bodyCtx.TIMESTAMP() != null && bodyCtx.identifier().size() > 1)
      stmt.timestampColumn = (Identifier) visit(bodyCtx.identifier(1));

    // TAGS (name type, ...)
    if (bodyCtx.TAGS() != null) {
      for (final SQLParser.TsTagColumnDefContext colCtx : bodyCtx.tsTagColumnDef()) {
        final Identifier colName = (Identifier) visit(colCtx.identifier(0));
        final Identifier colType = (Identifier) visit(colCtx.identifier(1));
        stmt.tags.add(new CreateTimeSeriesTypeStatement.ColumnDef(colName, colType));
      }
    }

    // FIELDS (name type, ...)
    if (bodyCtx.FIELDS() != null) {
      for (final SQLParser.TsFieldColumnDefContext colCtx : bodyCtx.tsFieldColumnDef()) {
        final Identifier colName = (Identifier) visit(colCtx.identifier(0));
        final Identifier colType = (Identifier) visit(colCtx.identifier(1));
        stmt.fields.add(new CreateTimeSeriesTypeStatement.ColumnDef(colName, colType));
      }
    }

    // SHARDS count
    if (bodyCtx.SHARDS() != null) {
      for (int i = 0; i < bodyCtx.children.size(); i++) {
        if (bodyCtx.children.get(i) instanceof org.antlr.v4.runtime.tree.TerminalNode tn
            && tn.getSymbol().getType() == SQLParser.SHARDS) {
          // Next INTEGER_LITERAL
          for (int j = i + 1; j < bodyCtx.children.size(); j++) {
            if (bodyCtx.children.get(j) instanceof org.antlr.v4.runtime.tree.TerminalNode tn2
                && tn2.getSymbol().getType() == SQLParser.INTEGER_LITERAL) {
              stmt.shards = new PInteger(-1);
              stmt.shards.setValue(Integer.parseInt(tn2.getText()));
              break;
            }
          }
          break;
        }
      }
    }

    // RETENTION value with optional time unit
    if (bodyCtx.RETENTION() != null) {
      long retentionValue = 0;
      for (int i = 0; i < bodyCtx.children.size(); i++) {
        if (bodyCtx.children.get(i) instanceof org.antlr.v4.runtime.tree.TerminalNode tn
            && tn.getSymbol().getType() == SQLParser.RETENTION) {
          for (int j = i + 1; j < bodyCtx.children.size(); j++) {
            if (bodyCtx.children.get(j) instanceof org.antlr.v4.runtime.tree.TerminalNode tn2
                && tn2.getSymbol().getType() == SQLParser.INTEGER_LITERAL) {
              retentionValue = Long.parseLong(tn2.getText());
              break;
            }
          }
          break;
        }
      }

      // Determine time unit by looking at tokens after RETENTION + INTEGER_LITERAL
      long multiplier = 86400000L; // default: DAYS
      boolean foundRetention = false;
      boolean foundValue = false;
      for (int i = 0; i < bodyCtx.children.size(); i++) {
        if (bodyCtx.children.get(i) instanceof org.antlr.v4.runtime.tree.TerminalNode tn) {
          if (tn.getSymbol().getType() == SQLParser.RETENTION)
            foundRetention = true;
          else if (foundRetention && tn.getSymbol().getType() == SQLParser.INTEGER_LITERAL)
            foundValue = true;
          else if (foundRetention && foundValue) {
            if (tn.getSymbol().getType() == SQLParser.HOURS)
              multiplier = 3600000L;
            else if (tn.getSymbol().getType() == SQLParser.MINUTES)
              multiplier = 60000L;
            break;
          }
        }
      }

      stmt.retentionMs = retentionValue * multiplier;
    }

    // COMPACTION_INTERVAL value with optional time unit
    if (bodyCtx.COMPACTION_INTERVAL() != null) {
      long compactionValue = 0;
      for (int i = 0; i < bodyCtx.children.size(); i++) {
        if (bodyCtx.children.get(i) instanceof org.antlr.v4.runtime.tree.TerminalNode tn
            && tn.getSymbol().getType() == SQLParser.COMPACTION_INTERVAL) {
          for (int j = i + 1; j < bodyCtx.children.size(); j++) {
            if (bodyCtx.children.get(j) instanceof org.antlr.v4.runtime.tree.TerminalNode tn2
                && tn2.getSymbol().getType() == SQLParser.INTEGER_LITERAL) {
              compactionValue = Long.parseLong(tn2.getText());
              break;
            }
          }
          break;
        }
      }

      // Determine time unit (default: HOURS for compaction interval)
      long multiplier = 3600000L; // HOURS
      // Check for unit keywords AFTER the COMPACTION_INTERVAL token
      // We need to look at the remaining children after the integer literal
      boolean foundCompaction = false;
      for (int i = 0; i < bodyCtx.children.size(); i++) {
        if (bodyCtx.children.get(i) instanceof org.antlr.v4.runtime.tree.TerminalNode tn
            && tn.getSymbol().getType() == SQLParser.COMPACTION_INTERVAL)
          foundCompaction = true;
        else if (foundCompaction && bodyCtx.children.get(i) instanceof org.antlr.v4.runtime.tree.TerminalNode tn) {
          if (tn.getSymbol().getType() == SQLParser.DAYS) {
            multiplier = 86400000L;
            break;
          } else if (tn.getSymbol().getType() == SQLParser.HOURS) {
            multiplier = 3600000L;
            break;
          } else if (tn.getSymbol().getType() == SQLParser.MINUTES) {
            multiplier = 60000L;
            break;
          }
        }
      }

      stmt.compactionIntervalMs = compactionValue * multiplier;
    }

    return stmt;
  }

  @Override
  public AlterTimeSeriesTypeStatement visitAlterTimeSeriesTypeStmt(
      final SQLParser.AlterTimeSeriesTypeStmtContext ctx) {
    final AlterTimeSeriesTypeStatement stmt = new AlterTimeSeriesTypeStatement(-1);
    final SQLParser.AlterTimeSeriesTypeBodyContext bodyCtx = ctx.alterTimeSeriesTypeBody();

    stmt.name = (Identifier) visit(bodyCtx.identifier());

    if (bodyCtx.ADD() != null) {
      stmt.addPolicy = true;
      for (final SQLParser.DownsamplingTierClauseContext tierCtx : bodyCtx.downsamplingTierClause()) {
        final long afterValue = Long.parseLong(tierCtx.INTEGER_LITERAL(0).getText());
        final long afterMs = afterValue * parseTimeUnitMs(tierCtx.tsTimeUnit(0));

        final long granValue = Long.parseLong(tierCtx.INTEGER_LITERAL(1).getText());
        final long granMs = granValue * parseTimeUnitMs(tierCtx.tsTimeUnit(1));

        stmt.tiers.add(new DownsamplingTier(afterMs, granMs));
      }
      // Sort tiers by afterMs ascending
      stmt.tiers.sort((a, b) -> Long.compare(a.afterMs(), b.afterMs()));
    } else {
      stmt.addPolicy = false;
    }

    return stmt;
  }

  private static long parseTimeUnitMs(final SQLParser.TsTimeUnitContext unitCtx) {
    if (unitCtx.DAYS() != null)
      return 86400000L;
    if (unitCtx.HOURS() != null || unitCtx.HOUR() != null)
      return 3600000L;
    if (unitCtx.MINUTES() != null || unitCtx.MINUTE() != null)
      return 60000L;
    return 86400000L; // default to days
  }

  @Override
  public DropMaterializedViewStatement visitDropMaterializedViewStmt(
      final SQLParser.DropMaterializedViewStmtContext ctx) {
    final DropMaterializedViewStatement stmt = new DropMaterializedViewStatement(-1);
    final SQLParser.DropMaterializedViewBodyContext bodyCtx = ctx.dropMaterializedViewBody();
    stmt.name = (Identifier) visit(bodyCtx.identifier());
    stmt.ifExists = bodyCtx.IF() != null && bodyCtx.EXISTS() != null;
    return stmt;
  }

  @Override
  public RefreshMaterializedViewStatement visitRefreshMaterializedViewStmt(
      final SQLParser.RefreshMaterializedViewStmtContext ctx) {
    final RefreshMaterializedViewStatement stmt = new RefreshMaterializedViewStatement(-1);
    stmt.name = (Identifier) visit(ctx.refreshMaterializedViewBody().identifier());
    return stmt;
  }

  @Override
  public AlterMaterializedViewStatement visitAlterMaterializedViewStmt(
      final SQLParser.AlterMaterializedViewStmtContext ctx) {
    final AlterMaterializedViewStatement stmt = new AlterMaterializedViewStatement(-1);
    final SQLParser.AlterMaterializedViewBodyContext bodyCtx = ctx.alterMaterializedViewBody();
    stmt.name = (Identifier) visit(bodyCtx.identifier());

    final SQLParser.MaterializedViewRefreshClauseContext refreshCtx =
        bodyCtx.materializedViewRefreshClause();
    if (refreshCtx.MANUAL() != null)
      stmt.refreshMode = "MANUAL";
    else if (refreshCtx.INCREMENTAL() != null)
      stmt.refreshMode = "INCREMENTAL";
    else if (refreshCtx.EVERY() != null) {
      stmt.refreshMode = "PERIODIC";
      stmt.refreshInterval = Integer.parseInt(refreshCtx.INTEGER_LITERAL().getText());
      final SQLParser.MaterializedViewTimeUnitContext unitCtx =
          refreshCtx.materializedViewTimeUnit();
      if (unitCtx.SECOND() != null)
        stmt.refreshUnit = "SECOND";
      else if (unitCtx.MINUTE() != null)
        stmt.refreshUnit = "MINUTE";
      else if (unitCtx.HOUR() != null)
        stmt.refreshUnit = "HOUR";
    }

    return stmt;
  }

  // =========================================================================
  // CONTINUOUS AGGREGATE MANAGEMENT
  // =========================================================================

  @Override
  public CreateContinuousAggregateStatement visitCreateContinuousAggregateStmt(
      final SQLParser.CreateContinuousAggregateStmtContext ctx) {
    final CreateContinuousAggregateStatement stmt = new CreateContinuousAggregateStatement(-1);
    final SQLParser.CreateContinuousAggregateBodyContext bodyCtx = ctx.createContinuousAggregateBody();

    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;
    stmt.name = (Identifier) visit(bodyCtx.identifier());
    stmt.selectStatement = (SelectStatement) visit(bodyCtx.selectStatement());

    return stmt;
  }

  @Override
  public DropContinuousAggregateStatement visitDropContinuousAggregateStmt(
      final SQLParser.DropContinuousAggregateStmtContext ctx) {
    final DropContinuousAggregateStatement stmt = new DropContinuousAggregateStatement(-1);
    final SQLParser.DropContinuousAggregateBodyContext bodyCtx = ctx.dropContinuousAggregateBody();
    stmt.name = (Identifier) visit(bodyCtx.identifier());
    stmt.ifExists = bodyCtx.IF() != null && bodyCtx.EXISTS() != null;
    return stmt;
  }

  @Override
  public RefreshContinuousAggregateStatement visitRefreshContinuousAggregateStmt(
      final SQLParser.RefreshContinuousAggregateStmtContext ctx) {
    final RefreshContinuousAggregateStatement stmt = new RefreshContinuousAggregateStatement(-1);
    stmt.name = (Identifier) visit(ctx.refreshContinuousAggregateBody().identifier());
    return stmt;
  }

  /**
   * Visit trigger timing (BEFORE or AFTER).
   */
  @Override
  public Identifier visitTriggerTiming(final SQLParser.TriggerTimingContext ctx) {
    if (ctx.BEFORE() != null) {
      return new Identifier("BEFORE");
    } else if (ctx.AFTER() != null) {
      return new Identifier("AFTER");
    }
    return null;
  }

  /**
   * Visit trigger event (CREATE, READ, UPDATE, DELETE).
   */
  @Override
  public Identifier visitTriggerEvent(final SQLParser.TriggerEventContext ctx) {
    if (ctx.CREATE() != null) {
      return new Identifier("CREATE");
    } else if (ctx.READ() != null) {
      return new Identifier("READ");
    } else if (ctx.UPDATE() != null) {
      return new Identifier("UPDATE");
    } else if (ctx.DELETE() != null) {
      return new Identifier("DELETE");
    }
    return null;
  }

  // DDL STATEMENTS - TRUNCATE

  /**
   * Visit TRUNCATE TYPE statement.
   */
  @Override
  public TruncateTypeStatement visitTruncateTypeStmt(final SQLParser.TruncateTypeStmtContext ctx) {
    final TruncateTypeStatement stmt = new TruncateTypeStatement(-1);
    final SQLParser.TruncateTypeBodyContext bodyCtx = ctx.truncateTypeBody();

    // Type name
    stmt.typeName = (Identifier) visit(bodyCtx.identifier());

    // POLYMORPHIC - check if POLYMORPHIC keyword is present
    // Grammar uses (POLYMORPHIC | UNSAFE)* which returns a list, not a single token
    stmt.polymorphic = bodyCtx.POLYMORPHIC() != null && !bodyCtx.POLYMORPHIC().isEmpty();

    // UNSAFE - check if UNSAFE keyword is present
    stmt.unsafe = bodyCtx.UNSAFE() != null && !bodyCtx.UNSAFE().isEmpty();

    return stmt;
  }

  /**
   * Visit TRUNCATE BUCKET statement.
   */
  @Override
  public TruncateBucketStatement visitTruncateBucketStmt(final SQLParser.TruncateBucketStmtContext ctx) {
    final TruncateBucketStatement stmt = new TruncateBucketStatement(-1);
    final SQLParser.TruncateBucketBodyContext bodyCtx = ctx.truncateBucketBody();

    // Bucket name
    stmt.bucketName = (Identifier) visit(bodyCtx.identifier());

    // UNSAFE
    stmt.unsafe = bodyCtx.UNSAFE() != null;

    return stmt;
  }

  /**
   * Visit TRUNCATE RECORD statement.
   */
  @Override
  public TruncateRecordStatement visitTruncateRecordStmt(final SQLParser.TruncateRecordStmtContext ctx) {
    final TruncateRecordStatement stmt = new TruncateRecordStatement(-1);
    final SQLParser.TruncateRecordBodyContext bodyCtx = ctx.truncateRecordBody();

    // Record RIDs (one or more)
    stmt.records = new ArrayList<>();
    for (final SQLParser.RidContext ridCtx : bodyCtx.rid()) {
      final Rid rid = (Rid) visit(ridCtx);
      stmt.records.add(rid);
    }

    // Set single record if only one
    if (stmt.records.size() == 1) {
      stmt.record = stmt.records.get(0);
    }

    return stmt;
  }

  // CONTROL FLOW STATEMENTS

  /**
   * Visit LET statement.
   */
  @Override
  public LetStatement visitLetStmt(final SQLParser.LetStmtContext ctx) {
    final LetStatement stmt = new LetStatement(-1);
    final SQLParser.LetStatementContext letCtx = ctx.letStatement();

    // Process the first let item
    final SQLParser.LetItemContext firstItem = letCtx.letItem(0);
    stmt.variableName = (Identifier) visit(firstItem.identifier());

    if (firstItem.expression() != null) {
      stmt.expression = (Expression) visit(firstItem.expression());
    } else if (firstItem.statement() != null) {
      stmt.statement = (Statement) visit(firstItem.statement());
    }

    // Note: JavaCC LetStatement only stores one variable
    // If there are multiple let items, we would need to create multiple LetStatements
    // For now, matching JavaCC behavior and using only the first item

    return stmt;
  }

  /**
   * Visit SET GLOBAL statement.
   */
  @Override
  public SetGlobalStatement visitSetGlobalStmt(final SQLParser.SetGlobalStmtContext ctx) {
    final SetGlobalStatement stmt = new SetGlobalStatement(-1);
    final SQLParser.SetGlobalStatementContext setGlobalCtx = ctx.setGlobalStatement();

    stmt.variableName = (Identifier) visit(setGlobalCtx.identifier());
    stmt.expression = (Expression) visit(setGlobalCtx.expression());

    return stmt;
  }

  /**
   * Visit RETURN statement.
   */
  @Override
  public ReturnStatement visitReturnStmt(final SQLParser.ReturnStmtContext ctx) {
    final ReturnStatement stmt = new ReturnStatement(-1);
    final SQLParser.ReturnStatementContext returnCtx = ctx.returnStatement();

    if (returnCtx.expression() != null) {
      stmt.expression = (Expression) visit(returnCtx.expression());
    }

    return stmt;
  }

  /**
   * Visit scriptRegularStmt - delegates to the underlying regular statement.
   * This allows all regular SQL statements to work in script context.
   */
  @Override
  public Statement visitScriptRegularStmt(final SQLParser.ScriptRegularStmtContext ctx) {
    return (Statement) visit(ctx.statement());
  }

  /**
   * Visit FOREACH statement (script-only).
   * Grammar: FOREACH LPAREN identifier IN expression RPAREN LBRACE scriptStatement* RBRACE
   */
  @Override
  public ForEachBlock visitForeachStmt(final SQLParser.ForeachStmtContext ctx) {
    final ForEachBlock block = new ForEachBlock(-1);
    final SQLParser.ForeachStatementContext foreachCtx = ctx.foreachStatement();

    // Loop variable (the identifier after FOREACH and before IN)
    block.loopVariable = (Identifier) visit(foreachCtx.identifier());

    // Loop values (the expression after IN)
    block.loopValues = (Expression) visit(foreachCtx.expression());

    // Loop body statements (uses scriptStatement* to allow nested FOREACH/WHILE)
    for (final SQLParser.ScriptStatementContext stmtCtx : foreachCtx.scriptStatement()) {
      block.statements.add((Statement) visit(stmtCtx));
    }

    return block;
  }

  /**
   * Visit WHILE statement (script-only).
   * Grammar: WHILE LPAREN orBlock RPAREN LBRACE scriptStatement* RBRACE
   */
  @Override
  public WhileBlock visitWhileStmt(final SQLParser.WhileStmtContext ctx) {
    final WhileBlock block = new WhileBlock(-1);
    final SQLParser.WhileStatementContext whileCtx = ctx.whileStatement();

    // Condition (the orBlock produces a BooleanExpression)
    block.condition = (BooleanExpression) visit(whileCtx.orBlock());

    // Loop body statements (uses scriptStatement* to allow nested FOREACH/WHILE)
    for (final SQLParser.ScriptStatementContext stmtCtx : whileCtx.scriptStatement()) {
      block.statements.add((Statement) visit(stmtCtx));
    }

    return block;
  }

  /**
   * Visit BREAK statement (script-only).
   * Grammar: BREAK
   */
  @Override
  public BreakStatement visitBreakStmt(final SQLParser.BreakStmtContext ctx) {
    return new BreakStatement(-1);
  }

  /**
   * Visit function call as statement (script-only).
   * Grammar: functionCall
   * Wraps the FunctionCall in an ExpressionStatement for script execution.
   */
  @Override
  public ExpressionStatement visitFunctionStmt(final SQLParser.FunctionStmtContext ctx) {
    final ExpressionStatement stmt = new ExpressionStatement(-1);

    // Get the function call
    final FunctionCall funcCall = (FunctionCall) visit(ctx.functionCall());

    // Wrap it in an Expression (like visitFunctionCallExpr does)
    final BaseExpression baseExpr = new BaseExpression(-1);
    final LevelZeroIdentifier levelZero = new LevelZeroIdentifier(-1);
    levelZero.functionCall = funcCall;
    final BaseIdentifier baseId = new BaseIdentifier(-1);
    baseId.levelZero = levelZero;
    baseExpr.identifier = baseId;

    final Expression expr = new Expression(-1);
    expr.mathExpression = baseExpr;

    stmt.expression = expr;
    return stmt;
  }

  /**
   * Visit IF statement.
   * Grammar: IF LPAREN orBlock RPAREN LBRACE (scriptStatement SEMICOLON?)* RBRACE (ELSE LBRACE (scriptStatement
   * SEMICOLON?)* RBRACE)?
   */
  @Override
  public IfStatement visitIfStmt(final SQLParser.IfStmtContext ctx) {
    final IfStatement stmt = new IfStatement(-1);
    final SQLParser.IfStatementContext ifCtx = ctx.ifStatement();

    // Condition (the orBlock produces a BooleanExpression)
    stmt.expression = (BooleanExpression) visit(ifCtx.orBlock());

    // Get all script statements - first group is IF body, second group (if exists) is ELSE body
    final List<SQLParser.ScriptStatementContext> allStatements = ifCtx.scriptStatement();

    // Find the index where ELSE keyword appears (if it exists)
    int elseIndex = -1;
    if (ifCtx.ELSE() != null) {
      // Count statements until we hit the second LBRACE (which comes after ELSE)
      // The grammar generates scriptStatement* for both IF and ELSE blocks
      // We need to split them based on which block they belong to
      // This is tricky - let's use a different approach: count LBRACE tokens
      final int numBraces = ifCtx.LBRACE().size();
      if (numBraces == 2) {
        // We have both IF and ELSE blocks - need to determine the split
        // The safest way is to iterate and check each statement's start position
        // against the ELSE token position
        final int elseTokenIndex = ifCtx.ELSE().getSymbol().getTokenIndex();
        for (int i = 0; i < allStatements.size(); i++) {
          if (allStatements.get(i).start.getTokenIndex() > elseTokenIndex) {
            elseIndex = i;
            break;
          }
        }
      }
    }

    // Add statements to IF block (before ELSE) or all statements if no ELSE
    final int endIndex = (elseIndex != -1) ? elseIndex : allStatements.size();
    for (int i = 0; i < endIndex; i++) {
      stmt.statements.add((Statement) visit(allStatements.get(i)));
    }

    // Add statements to ELSE block (after ELSE)
    if (elseIndex != -1) {
      for (int i = elseIndex; i < allStatements.size(); i++) {
        stmt.elseStatements.add((Statement) visit(allStatements.get(i)));
      }
    }

    return stmt;
  }

  /**
   * Visit EXPLAIN statement.
   */
  @Override
  public ExplainStatement visitExplainStmt(final SQLParser.ExplainStmtContext ctx) {
    final ExplainStatement stmt = new ExplainStatement(-1);
    final SQLParser.ExplainStatementContext explainCtx = ctx.explainStatement();

    stmt.statement = (Statement) visit(explainCtx.statement());

    return stmt;
  }

  /**
   * Visit PROFILE statement.
   */
  @Override
  public ProfileStatement visitProfileStmt(final SQLParser.ProfileStmtContext ctx) {
    final ProfileStatement stmt = new ProfileStatement(-1);
    final SQLParser.ProfileStatementContext profileCtx = ctx.profileStatement();

    stmt.statement = (Statement) visit(profileCtx.statement());

    return stmt;
  }

  /**
   * Visit SLEEP statement.
   * Grammar: SLEEP expression
   */
  @Override
  public SleepStatement visitSleepStmt(final SQLParser.SleepStmtContext ctx) {
    final SleepStatement stmt = new SleepStatement(-1);
    final SQLParser.SleepStatementContext sleepCtx = ctx.sleepStatement();

    // EXPRESSION: duration in milliseconds
    stmt.expression = (Expression) visit(sleepCtx.expression());

    return stmt;
  }

  /**
   * Visit CONSOLE statement.
   * Grammar: CONSOLE DOT identifier expression
   */
  @Override
  public ConsoleStatement visitConsoleStmt(final SQLParser.ConsoleStmtContext ctx) {
    final ConsoleStatement stmt = new ConsoleStatement(-1);
    final SQLParser.ConsoleStatementContext consoleCtx = ctx.consoleStatement();

    // LOG LEVEL: log, output, error, warn, debug
    stmt.logLevel = (Identifier) visit(consoleCtx.identifier());

    // MESSAGE: expression to log
    stmt.message = (Expression) visit(consoleCtx.expression());

    return stmt;
  }

  /**
   * Visit LOCK statement.
   * Grammar: LOCK (TYPE | BUCKET) identifier (COMMA identifier)*
   */
  @Override
  public LockStatement visitLockStmt(final SQLParser.LockStmtContext ctx) {
    final LockStatement stmt = new LockStatement(-1);
    final SQLParser.LockStatementContext lockCtx = ctx.lockStatement();

    // MODE: TYPE or BUCKET
    if (lockCtx.TYPE() != null) {
      stmt.mode = "TYPE";
    } else if (lockCtx.BUCKET() != null) {
      stmt.mode = "BUCKET";
    }

    // IDENTIFIERS: List of types or buckets to lock
    stmt.identifiers = new ArrayList<>();
    for (final SQLParser.IdentifierContext idCtx : lockCtx.identifier()) {
      stmt.identifiers.add((Identifier) visit(idCtx));
    }

    return stmt;
  }

  // TRANSACTION STATEMENTS

  /**
   * Visit BEGIN statement.
   */
  @Override
  public BeginStatement visitBeginStmt(final SQLParser.BeginStmtContext ctx) {
    final BeginStatement stmt = new BeginStatement(-1);
    final SQLParser.BeginStatementContext beginCtx = ctx.beginStatement();

    // ISOLATION clause
    if (beginCtx.identifier() != null) {
      stmt.isolation = (Identifier) visit(beginCtx.identifier());
    }

    return stmt;
  }

  /**
   * Visit COMMIT statement.
   */
  @Override
  public CommitStatement visitCommitStmt(final SQLParser.CommitStmtContext ctx) {
    final CommitStatement stmt = new CommitStatement(-1);
    final SQLParser.CommitStatementContext commitCtx = ctx.commitStatement();

    // RETRY clause
    if (commitCtx.INTEGER_LITERAL() != null) {
      final PInteger retry = new PInteger(-1);
      retry.value = Integer.parseInt(commitCtx.INTEGER_LITERAL().getText());
      stmt.retry = retry;

      // ELSE clause
      if (commitCtx.ELSE() != null) {
        // ELSE {statements}
        if (commitCtx.LBRACE() != null) {
          for (final SQLParser.ScriptStatementContext stmtCtx : commitCtx.scriptStatement()) {
            final Statement elseStmt = (Statement) visit(stmtCtx);
            stmt.addElse(elseStmt);
          }
        }

        // FAIL or CONTINUE
        if (commitCtx.FAIL() != null) {
          stmt.elseFail = true;
        } else if (commitCtx.CONTINUE() != null) {
          stmt.elseFail = false;
        }
      }
    }

    return stmt;
  }

  /**
   * Visit ROLLBACK statement.
   */
  @Override
  public RollbackStatement visitRollbackStmt(final SQLParser.RollbackStmtContext ctx) {
    final RollbackStatement stmt = new RollbackStatement(-1);
    // ROLLBACK has no parameters - just need to reference the rollbackStatement context
    return stmt;
  }

  // DATABASE MANAGEMENT STATEMENTS

  /**
   * Visit CHECK DATABASE statement.
   * Grammar: CHECK DATABASE (TYPE ident (COMMA ident)*)? (BUCKET (ident|int) (COMMA (ident|int))*)? (FIX)? (COMPRESS)?
   */
  @Override
  public CheckDatabaseStatement visitCheckDatabaseStmt(final SQLParser.CheckDatabaseStmtContext ctx) {
    final CheckDatabaseStatement stmt = new CheckDatabaseStatement(-1);
    final SQLParser.CheckDatabaseStatementContext checkCtx = ctx.checkDatabaseStatement();

    // Parse TYPE clause - list of type identifiers
    if (checkCtx.TYPE() != null) {
      for (final SQLParser.IdentifierContext identCtx : checkCtx.identifier()) {
        stmt.types.add((Identifier) visit(identCtx));
      }
    }

    // Parse BUCKET clause - list of bucket identifiers or integers
    if (checkCtx.BUCKET() != null) {
      // Count how many identifiers we've consumed for TYPE
      final int typeIdentCount = checkCtx.TYPE() != null ? checkCtx.identifier().size() : 0;

      // BUCKET can be identifier or INTEGER_LITERAL
      // If we have INTEGER_LITERAL tokens, use them
      if (checkCtx.INTEGER_LITERAL() != null && !checkCtx.INTEGER_LITERAL().isEmpty()) {
        for (final TerminalNode intNode : checkCtx.INTEGER_LITERAL()) {
          final BucketIdentifier bucketId = new BucketIdentifier(-1);
          final PInteger pInt = new PInteger(-1);
          pInt.setValue(Integer.parseInt(intNode.getText()));
          bucketId.bucketId = pInt;
          stmt.buckets.add(bucketId);
        }
      }

      // Also check for bucket identifiers (non-TYPE identifiers)
      // All identifiers after TYPE count belong to BUCKET
      final List<SQLParser.IdentifierContext> allIdents = checkCtx.identifier();
      if (allIdents != null && allIdents.size() > typeIdentCount) {
        for (int i = typeIdentCount; i < allIdents.size(); i++) {
          final BucketIdentifier bucketId = new BucketIdentifier(-1);
          bucketId.bucketName = (Identifier) visit(allIdents.get(i));
          stmt.buckets.add(bucketId);
        }
      }
    }

    // Parse FIX flag
    if (checkCtx.FIX() != null) {
      stmt.fix = true;
    }

    // Parse COMPRESS flag
    if (checkCtx.COMPRESS() != null) {
      stmt.compress = true;
    }

    return stmt;
  }

  /**
   * Visit ALIGN DATABASE statement.
   * Grammar: ALIGN DATABASE
   */
  @Override
  public AlignDatabaseStatement visitAlignDatabaseStmt(final SQLParser.AlignDatabaseStmtContext ctx) {
    return new AlignDatabaseStatement(-1);
  }

  /**
   * Visit IMPORT DATABASE statement.
   * Grammar: IMPORT DATABASE STRING_LITERAL
   */
  @Override
  public ImportDatabaseStatement visitImportDatabaseStmt(final SQLParser.ImportDatabaseStmtContext ctx) {
    final ImportDatabaseStatement stmt = new ImportDatabaseStatement(-1);
    final SQLParser.ImportDatabaseStatementContext importCtx = ctx.importDatabaseStatement();

    // Parse URL (optional)
    if (importCtx.url() != null) {
      final Url url = (Url) visit(importCtx.url());
      stmt.url = url;
    }
    // If no URL, it's null - this is valid for CSV imports with vertices/edges settings

    // Parse WITH settings
    if (importCtx.settingList() != null) {
      for (final SQLParser.SettingContext settingCtx : importCtx.settingList().setting()) {
        final Identifier key = (Identifier) visit(settingCtx.identifier());
        final Expression value = (Expression) visit(settingCtx.expression());

        // Store as Expression with key/value
        // ImportDatabaseStatement uses .execute() on the value Expression
        final Expression keyExpr = new Expression(-1);
        keyExpr.value = key.getStringValue();

        stmt.settings.put(keyExpr, value);
      }
    }

    return stmt;
  }

  /**
   * Visit EXPORT DATABASE statement.
   * Grammar: EXPORT DATABASE STRING_LITERAL
   */
  @Override
  public ExportDatabaseStatement visitExportDatabaseStmt(final SQLParser.ExportDatabaseStmtContext ctx) {
    final ExportDatabaseStatement stmt = new ExportDatabaseStatement(-1);
    final SQLParser.ExportDatabaseStatementContext exportCtx = ctx.exportDatabaseStatement();

    // Parse URL
    if (exportCtx.url() != null) {
      final Url url = (Url) visit(exportCtx.url());
      stmt.url = url;
    }

    // Parse WITH settings
    if (exportCtx.settingList() != null) {
      for (final SQLParser.SettingContext settingCtx : exportCtx.settingList().setting()) {
        final Identifier key = (Identifier) visit(settingCtx.identifier());
        final Expression value = (Expression) visit(settingCtx.expression());

        // Store as Expression with key/value
        // ExportDatabaseStatement uses .execute() on the value Expression
        final Expression keyExpr = new Expression(-1);
        keyExpr.value = key.getStringValue();

        stmt.settings.put(keyExpr, value);
      }
    }

    return stmt;
  }

  /**
   * Visit url rule
   * Grammar: FILE_URL | HTTP_URL | HTTPS_URL | CLASSPATH_URL | STRING_LITERAL
   */
  @Override
  public Url visitUrl(final SQLParser.UrlContext ctx) {
    String urlString = null;

    if (ctx.FILE_URL() != null) {
      urlString = ctx.FILE_URL().getText();
    } else if (ctx.HTTP_URL() != null) {
      urlString = ctx.HTTP_URL().getText();
    } else if (ctx.HTTPS_URL() != null) {
      urlString = ctx.HTTPS_URL().getText();
    } else if (ctx.CLASSPATH_URL() != null) {
      urlString = ctx.CLASSPATH_URL().getText();
    } else if (ctx.STRING_LITERAL() != null) {
      urlString = removeQuotes(ctx.STRING_LITERAL().getText());
    }

    return new Url(urlString);
  }

  /**
   * Visit BACKUP DATABASE statement.
   * Grammar: BACKUP DATABASE url (WITH settingList)?
   */
  @Override
  public BackupDatabaseStatement visitBackupDatabaseStmt(final SQLParser.BackupDatabaseStmtContext ctx) {
    final BackupDatabaseStatement stmt = new BackupDatabaseStatement(-1);
    final SQLParser.BackupDatabaseStatementContext backupCtx = ctx.backupDatabaseStatement();

    // Parse URL
    if (backupCtx.url() != null) {
      final Url url = (Url) visit(backupCtx.url());
      stmt.url = url;
    }

    // Parse WITH settings
    if (backupCtx.settingList() != null) {
      for (final SQLParser.SettingContext settingCtx : backupCtx.settingList().setting()) {
        final Identifier key = (Identifier) visit(settingCtx.identifier());
        final Expression value = (Expression) visit(settingCtx.expression());

        // Store as Expression with key/value
        // BackupDatabaseStatement uses .execute() on the value Expression, so no need to extract .value
        final Expression keyExpr = new Expression(-1);
        keyExpr.value = key.getStringValue();

        stmt.settings.put(keyExpr, value);
      }
    }

    return stmt;
  }

  /**
   * Visit array single selector: [0] or [expression] or [:param] or [#rid]
   * <p>
   * Special handling: If expression is INTEGER_RANGE or ELLIPSIS_INTEGER_RANGE,
   * create ArrayRangeSelector instead (JavaCC compatibility).
   */
  @Override
  public Object visitArraySingleSelector(final SQLParser.ArraySingleSelectorContext ctx) {
    try {
      if (ctx.rid() != null) {
        // RID selector like [#10:5]
        final ArraySelector selector = new ArraySelector(-1);
        selector.rid = (Rid) visit(ctx.rid());
        return selector;

      } else if (ctx.inputParameter() != null) {
        // Parameter selector like [?] or [:name] or [$1]
        final ArraySelector selector = new ArraySelector(-1);
        selector.inputParam = (InputParameter) visit(ctx.inputParameter());
        return selector;

      } else if (ctx.expression() != null) {
        // Check if this is an INTEGER_RANGE or ELLIPSIS_INTEGER_RANGE
        // These should create ArrayRangeSelector, not ArraySelector
        final SQLParser.ExpressionContext exprCtx = ctx.expression();

        if (exprCtx instanceof final SQLParser.MathExprContext mathCtx) {
          final SQLParser.MathExpressionContext mathExprCtx = mathCtx.mathExpression();

          if (mathExprCtx instanceof final SQLParser.BaseContext baseCtx) {
            final SQLParser.BaseExpressionContext baseExprCtx = baseCtx.baseExpression();

            // Check for INTEGER_RANGE (0..3)
            if (baseExprCtx instanceof final SQLParser.IntegerRangeContext rangeCtx) {
              return createRangeSelectorFromToken(rangeCtx.INTEGER_RANGE().getText(), false);
            }

            // Check for ELLIPSIS_INTEGER_RANGE (0...3)
            if (baseExprCtx instanceof final SQLParser.EllipsisIntegerRangeContext rangeCtx) {
              return createRangeSelectorFromToken(rangeCtx.ELLIPSIS_INTEGER_RANGE().getText(), true);
            }
          }
        }

        // Regular expression selector like [i+1]
        final ArraySelector selector = new ArraySelector(-1);
        selector.expression = (Expression) visit(ctx.expression());
        return selector;
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build array selector: " + e.getMessage(), e);
    }

    return new ArraySelector(-1);
  }

  /**
   * Array multi selector visitor - handles [0,1,2] or [expr1,expr2]
   * Grammar: LBRACKET (expression | rid | inputParameter) (COMMA (expression | rid | inputParameter))+ RBRACKET
   */
  @Override
  public ArraySingleValuesSelector visitArrayMultiSelector(final SQLParser.ArrayMultiSelectorContext ctx) {
    try {
      final ArraySingleValuesSelector multiSelector = new ArraySingleValuesSelector(-1);

      // Add all selectors (first one plus the ones after commas)
      for (int i = 0; i < ctx.expression().size(); i++) {
        final ArraySelector selector = new ArraySelector(-1);

        if (ctx.expression(i) != null) {
          selector.expression = (Expression) visit(ctx.expression(i));
        } else if (ctx.rid(i) != null) {
          selector.rid = (Rid) visit(ctx.rid(i));
        } else if (ctx.inputParameter(i) != null) {
          selector.inputParam = (InputParameter) visit(ctx.inputParameter(i));
        }

        multiSelector.items.add(selector);
      }

      return multiSelector;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build multi-value array selector: " + e.getMessage(), e);
    }
  }

  /**
   * Array condition selector visitor - handles [whereClause].
   * Grammar: LBRACKET whereClause RBRACKET
   */
  @Override
  public Modifier visitArrayConditionSelector(final SQLParser.ArrayConditionSelectorContext ctx) {
    try {
      final WhereClause whereClause = (WhereClause) visit(ctx.whereClause());

      // Convert whereClause to OrBlock for the condition field
      final OrBlock orBlock = new OrBlock(-1);
      if (whereClause.baseExpression != null) {
        final AndBlock andBlock = new AndBlock(-1);
        andBlock.subBlocks.add(whereClause.baseExpression);
        orBlock.subBlocks.add(andBlock);
      }

      // Create Modifier with the condition
      final Modifier modifier = new Modifier(-1);
      modifier.squareBrackets = true;
      modifier.condition = orBlock;

      return modifier;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build array condition selector: " + e.getMessage(), e);
    }
  }

  /**
   * Array filter selector visitor - handles [=expression], [<expression], etc.
   * Grammar: LBRACKET comparisonOperator expression RBRACKET
   */
  @Override
  public Modifier visitArrayFilterSelector(final SQLParser.ArrayFilterSelectorContext ctx) {
    try {
      // Create RightBinaryCondition for filtering
      final RightBinaryCondition rightBinaryCondition = new RightBinaryCondition(-1);

      // Get and set the comparison operator
      rightBinaryCondition.operator = mapComparisonOperator(ctx.comparisonOperator());

      // Get and set the right expression
      rightBinaryCondition.right = (Expression) visit(ctx.expression());

      // Create Modifier with the filter condition
      final Modifier modifier = new Modifier(-1);
      modifier.squareBrackets = true;

      modifier.rightBinaryCondition = rightBinaryCondition;

      return modifier;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build array filter selector: " + e.getMessage(), e);
    }
  }

  /**
   * Array binary condition selector visitor - handles [expr LIKE expr], [expr IN expr], etc.
   * Grammar: LBRACKET expression comparisonOperator expression RBRACKET
   * <p>
   * Note: This is different from arrayFilterSelector which is [operator expr].
   * This handles the full binary condition [leftExpr operator rightExpr] for operators like LIKE.
   */
  @Override
  public Modifier visitArrayBinaryCondSelector(final SQLParser.ArrayBinaryCondSelectorContext ctx) {
    try {
      // Create a binary condition to filter elements
      final BinaryCondition condition = new BinaryCondition(-1);

      // Set left side - use special placeholder for "current element"
      // In array filter context, the left side will be substituted with each element
      condition.left = (Expression) visit(ctx.expression(0));

      // Set operator
      condition.operator = mapComparisonOperator(ctx.comparisonOperator());

      // Set right side
      condition.right = (Expression) visit(ctx.expression(1));

      // Wrap in WhereClause, then OrBlock for the condition field
      final WhereClause whereClause = new WhereClause(-1);
      try {
        whereClause.baseExpression = condition;
      } catch (final Exception e) {
        throw new CommandSQLParsingException("Failed to set baseExpression: " + e.getMessage(), e);
      }

      final OrBlock orBlock = new OrBlock(-1);
      try {
        final AndBlock andBlock = new AndBlock(-1);
        andBlock.subBlocks.add(condition);
        orBlock.subBlocks.add(andBlock);
      } catch (final Exception e) {
        throw new CommandSQLParsingException("Failed to build OrBlock: " + e.getMessage(), e);
      }

      // Create Modifier with the condition
      final Modifier modifier = new Modifier(-1);
      modifier.squareBrackets = true;

      modifier.condition = orBlock;

      return modifier;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build array binary condition selector: " + e.getMessage(), e);
    }
  }

  /**
   * Array LIKE selector visitor - handles [LIKE expression].
   * Grammar: LBRACKET LIKE expression RBRACKET
   */
  @Override
  public Modifier visitArrayLikeSelector(final SQLParser.ArrayLikeSelectorContext ctx) {
    try {
      final RightBinaryCondition rightBinaryCondition = new RightBinaryCondition(-1);

      rightBinaryCondition.operator = new LikeOperator(-1);

      rightBinaryCondition.right = (Expression) visit(ctx.expression());

      final Modifier modifier = new Modifier(-1);
      modifier.squareBrackets = true;

      modifier.rightBinaryCondition = rightBinaryCondition;

      return modifier;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build array LIKE selector: " + e.getMessage(), e);
    }
  }

  /**
   * Array ILIKE selector visitor - handles [ILIKE expression].
   * Grammar: LBRACKET ILIKE expression RBRACKET
   */
  @Override
  public Modifier visitArrayIlikeSelector(final SQLParser.ArrayIlikeSelectorContext ctx) {
    try {
      final RightBinaryCondition rightBinaryCondition = new RightBinaryCondition(-1);

      rightBinaryCondition.operator = new ILikeOperator(-1);

      rightBinaryCondition.right = (Expression) visit(ctx.expression());

      final Modifier modifier = new Modifier(-1);
      modifier.squareBrackets = true;

      modifier.rightBinaryCondition = rightBinaryCondition;

      return modifier;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build array ILIKE selector: " + e.getMessage(), e);
    }
  }

  /**
   * Array IN selector visitor - handles [IN expression].
   * Grammar: LBRACKET IN expression RBRACKET
   */
  @Override
  public Modifier visitArrayInSelector(final SQLParser.ArrayInSelectorContext ctx) {
    try {
      final RightBinaryCondition rightBinaryCondition = new RightBinaryCondition(-1);

      rightBinaryCondition.inOperator = new InOperator(-1);

      rightBinaryCondition.right = (Expression) visit(ctx.expression());

      final Modifier modifier = new Modifier(-1);
      modifier.squareBrackets = true;

      modifier.rightBinaryCondition = rightBinaryCondition;

      return modifier;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build array IN selector: " + e.getMessage(), e);
    }
  }

  /**
   * Create ArrayRangeSelector from INTEGER_RANGE or ELLIPSIS_INTEGER_RANGE token.
   * Splits the token text on ".." or "..." to extract from and to integers.
   */
  private ArrayRangeSelector createRangeSelectorFromToken(final String tokenText, final boolean inclusive) {
    final ArrayRangeSelector selector = new ArrayRangeSelector(-1);

    try {
      // Split on ".." or "..."
      final String[] parts = inclusive ? tokenText.split("\\.\\.\\.") : tokenText.split("\\.\\.");

      if (parts.length == 2) {
        final int from = Integer.parseInt(parts[0].trim());
        final int to = Integer.parseInt(parts[1].trim());

        // Set from and to fields
        selector.from = from;

        selector.to = to;

        // Set newRange flag
        selector.newRange = true;

        // Set included flag
        selector.included = inclusive;
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to parse range token: " + e.getMessage(), e);
    }

    return selector;
  }

  /**
   * Visit array range selector: [0..3] (exclusive)
   */
  @Override
  public ArrayRangeSelector visitArrayRangeSelector(final SQLParser.ArrayRangeSelectorContext ctx) {
    final ArrayRangeSelector selector = new ArrayRangeSelector(-1);

    try {
      // Set newRange flag (true for .. syntax)
      selector.newRange = true;

      // Set included flag (false for .. = exclusive)
      selector.included = false;

      // Handle FROM expression (optional)
      if (!ctx.expression().isEmpty() && ctx.expression(0) != null) {
        final Expression fromExpr = (Expression) visit(ctx.expression(0));
        final Integer fromInt = tryExtractIntegerLiteral(fromExpr);
        if (fromInt != null) {
          // Simple integer literal - set from field directly
          selector.from = fromInt;
        } else {
          // Complex expression - use fromSelector
          selector.fromSelector = createArrayNumberSelectorFromExpression(fromExpr);
        }
      }

      // Handle TO expression (optional)
      if (ctx.expression().size() >= 2 && ctx.expression(1) != null) {
        final Expression toExpr = (Expression) visit(ctx.expression(1));
        final Integer toInt = tryExtractIntegerLiteral(toExpr);
        if (toInt != null) {
          // Simple integer literal - set to field directly
          selector.to = toInt;
        } else {
          // Complex expression - use toSelector
          final ArrayNumberSelector toSelector = createArrayNumberSelectorFromExpression(toExpr);
          selector.toSelector = toSelector;
        }
      }

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build array range selector: " + e.getMessage(), e);
    }

    return selector;
  }

  /**
   * Visit array ellipsis selector: [0...3] (inclusive)
   */
  @Override
  public ArrayRangeSelector visitArrayEllipsisSelector(final SQLParser.ArrayEllipsisSelectorContext ctx) {
    final ArrayRangeSelector selector = new ArrayRangeSelector(-1);

    try {
      // Set newRange flag (true for ... syntax)
      selector.newRange = true;

      // Set included flag (true for ... = inclusive)
      selector.included = true;

      // Handle FROM expression (optional)
      if (!ctx.expression().isEmpty() && ctx.expression(0) != null) {
        final Expression fromExpr = (Expression) visit(ctx.expression(0));
        final Integer fromInt = tryExtractIntegerLiteral(fromExpr);
        if (fromInt != null) {
          // Simple integer literal - set from field directly
          selector.from = fromInt;
        } else {
          // Complex expression - use fromSelector
          selector.fromSelector = createArrayNumberSelectorFromExpression(fromExpr);
        }
      }

      // Handle TO expression (optional)
      if (ctx.expression().size() >= 2 && ctx.expression(1) != null) {
        final Expression toExpr = (Expression) visit(ctx.expression(1));
        final Integer toInt = tryExtractIntegerLiteral(toExpr);
        if (toInt != null) {
          // Simple integer literal - set to field directly
          selector.to = toInt;
        } else {
          // Complex expression - use toSelector
          final ArrayNumberSelector toSelector = createArrayNumberSelectorFromExpression(toExpr);
          selector.toSelector = toSelector;
        }
      }

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build array ellipsis selector: " + e.getMessage(), e);
    }

    return selector;
  }

  /**
   * Try to extract an integer literal from an expression.
   * Returns the integer value if it's a simple integer literal, null otherwise.
   */
  private Integer tryExtractIntegerLiteral(final Expression expr) {
    try {
      if (expr != null && expr.mathExpression instanceof final BaseExpression baseExpr) {
        if (baseExpr.number != null && baseExpr.number.getValue() != null) {
          return baseExpr.number.getValue().intValue();
        }
      }
    } catch (final Exception e) {
      // Not a simple integer literal
    }
    return null;
  }

  /**
   * Helper method to create ArrayNumberSelector from an already-visited expression.
   * Handles input parameters, simple integers, and complex expressions.
   */
  private ArrayNumberSelector createArrayNumberSelectorFromExpression(final Expression expr) {
    final ArrayNumberSelector selector = new ArrayNumberSelector(-1);

    try {
      // Check if the expression is an input parameter
      if (expr != null && expr.mathExpression instanceof final BaseExpression baseExpr) {

        if (baseExpr.inputParam != null) {
          // Input parameter like ? or :name or $1
          selector.inputValue = baseExpr.inputParam;
          return selector;
        }
      }

      // Use expressionValue for dynamic evaluation
      selector.expressionValue = expr.mathExpression;

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to create ArrayNumberSelector: " + e.getMessage(), e);
    }

    return selector;
  }

  /**
   * Helper method to remove quotes from string literals.
   * Handles both single and double quotes.
   */
  private String removeQuotes(final String str) {
    if (str == null || str.length() < 2) {
      return str;
    }

    if ((str.startsWith("\"") && str.endsWith("\"")) || (str.startsWith("'") && str.endsWith("'"))) {
      return str.substring(1, str.length() - 1);
    }

    return str;
  }

  /**
   * Helper method to create a BooleanExpression from a boolean literal value.
   * This is used for WHILE clauses like "while: (true)" or "while: (false)".
   */
  private BooleanExpression createBooleanLiteral(final Boolean boolValue) {
    // Create an anonymous BooleanExpression that returns the literal value
    final boolean val = boolValue != null && boolValue;  // Convert to primitive to avoid closure issues
    return new BooleanExpression(-1) {
      @Override
      public Boolean evaluate(final Identifiable currentRecord, final CommandContext ctx) {
        return val;
      }

      @Override
      public Boolean evaluate(final Result currentRecord, final CommandContext ctx) {
        return val;
      }

      @Override
      public void toString(final Map<String, Object> params, final StringBuilder builder) {
        builder.append(val);
      }

      @Override
      public BooleanExpression copy() {
        return createBooleanLiteral(val);
      }

      @Override
      public void extractSubQueries(final SubQueryCollector collector) {
        // No subqueries in a boolean literal
      }

      @Override
      public List<String> getMatchPatternInvolvedAliases() {
        // No aliases involved in a boolean literal
        return Collections.emptyList();
      }
    };
  }

}
