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

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.query.sql.grammar.SQLParserBaseVisitor;
import com.arcadedb.query.sql.parser.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;

/**
 * ANTLR4 visitor that builds ArcadeDB's internal AST from the SQL parse tree.
 * Transforms ANTLR's parse tree into Statement objects that are compatible
 * with the existing execution engine.
 *
 * This visitor produces identical AST structures to the JavaCC parser,
 * ensuring 100% backward compatibility with existing execution planners
 * and executor steps.
 */
public class SQLASTBuilder extends SQLParserBaseVisitor<Object> {

  private final Database database;

  public SQLASTBuilder(final Database database) {
    this.database = database;
  }

  // ============================================================================
  // ENTRY POINTS
  // ============================================================================

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
    for (final SQLParser.StatementContext stmtCtx : ctx.statement()) {
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

  // ============================================================================
  // QUERY STATEMENTS
  // ============================================================================

  /**
   * SELECT statement visitor.
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
   * Projection visitor.
   * Handles SELECT projections including DISTINCT and item lists.
   */
  @Override
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
    if (ctx.projectionItem() != null && !ctx.projectionItem().isEmpty()) {
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
    if (ctx.nestedProjection() != null)
      item.nestedProjection = (NestedProjection) visit(ctx.nestedProjection());

    // Alias (AS identifier)
    if (ctx.identifier() != null)
      item.alias = (Identifier) visit(ctx.identifier());

    return item;
  }

  /**
   * Nested projection visitor (e.g., :{field1, field2}).
   */
  @Override
  public NestedProjection visitNestedProjection(final SQLParser.NestedProjectionContext ctx) {
    final NestedProjection nestedProjection = new NestedProjection(-1);

    try {
      final java.lang.reflect.Field includeItemsField = NestedProjection.class.getDeclaredField("includeItems");
      includeItemsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      final List<NestedProjectionItem> includeItems = (List<NestedProjectionItem>) includeItemsField.get(nestedProjection);

      final java.lang.reflect.Field excludeItemsField = NestedProjection.class.getDeclaredField("excludeItems");
      excludeItemsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      final List<NestedProjectionItem> excludeItems = (List<NestedProjectionItem>) excludeItemsField.get(nestedProjection);

      final java.lang.reflect.Field starItemField = NestedProjection.class.getDeclaredField("starItem");
      starItemField.setAccessible(true);
      final java.lang.reflect.Field itemExcludeField = NestedProjectionItem.class.getDeclaredField("exclude");
      itemExcludeField.setAccessible(true);
      final java.lang.reflect.Field itemStarField = NestedProjectionItem.class.getDeclaredField("star");
      itemStarField.setAccessible(true);

      for (final SQLParser.NestedProjectionItemContext itemCtx : ctx.nestedProjectionItem()) {
        final NestedProjectionItem item = (NestedProjectionItem) visit(itemCtx);
        final boolean isExclude = (Boolean) itemExcludeField.get(item);
        final boolean isStar = (Boolean) itemStarField.get(item);

        if (isStar)
          starItemField.set(nestedProjection, item);
        else if (isExclude)
          excludeItems.add(item);
        else
          includeItems.add(item);
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build nested projection: " + e.getMessage(), e);
    }

    return nestedProjection;
  }

  /**
   * Nested projection item visitor.
   */
  @Override
  public NestedProjectionItem visitNestedProjectionItem(final SQLParser.NestedProjectionItemContext ctx) {
    final NestedProjectionItem item = new NestedProjectionItem(-1);

    try {
      // Get reflection fields
      final java.lang.reflect.Field starField = NestedProjectionItem.class.getDeclaredField("star");
      starField.setAccessible(true);
      final java.lang.reflect.Field excludeField = NestedProjectionItem.class.getDeclaredField("exclude");
      excludeField.setAccessible(true);
      final java.lang.reflect.Field expressionField = NestedProjectionItem.class.getDeclaredField("expression");
      expressionField.setAccessible(true);
      final java.lang.reflect.Field rightWildcardField = NestedProjectionItem.class.getDeclaredField("rightWildcard");
      rightWildcardField.setAccessible(true);
      final java.lang.reflect.Field expansionField = NestedProjectionItem.class.getDeclaredField("expansion");
      expansionField.setAccessible(true);
      final java.lang.reflect.Field aliasField = NestedProjectionItem.class.getDeclaredField("alias");
      aliasField.setAccessible(true);

      // STAR
      if (ctx.STAR() != null && ctx.expression() == null)
        starField.set(item, true);

      // BANG (exclude)
      if (ctx.BANG() != null)
        excludeField.set(item, true);

      // Expression
      if (ctx.expression() != null)
        expressionField.set(item, (Expression) visit(ctx.expression()));

      // Right wildcard (expression followed by *)
      if (ctx.expression() != null && ctx.STAR() != null)
        rightWildcardField.set(item, true);

      // Nested expansion
      if (ctx.nestedProjection() != null)
        expansionField.set(item, (NestedProjection) visit(ctx.nestedProjection()));

      // Alias
      if (ctx.identifier() != null)
        aliasField.set(item, (Identifier) visit(ctx.identifier()));

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build nested projection item: " + e.getMessage(), e);
    }

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
    if (ctx.identifier() != null && !ctx.identifier().isEmpty()) {
      fromItem.identifier = (Identifier) visit(ctx.identifier(0));
    }

    // Handle modifiers if present
    if (ctx.modifier() != null && !ctx.modifier().isEmpty()) {
      // For now, just handle the first modifier
      // TODO: Handle modifier chains properly
      fromItem.modifier = (Modifier) visit(ctx.modifier(0));
    }

    // TODO: Handle alias (second identifier after AS)
    // if (ctx.identifier().size() > 1) {
    //   alias = visit(ctx.identifier(1));
    // }

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
   * FROM single parameter visitor (e.g., FROM ?).
   */
  @Override
  public FromItem visitFromParam(final SQLParser.FromParamContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    fromItem.inputParams = new ArrayList<>();
    fromItem.inputParams.add((InputParameter) visit(ctx.inputParameter()));
    return fromItem;
  }

  /**
   * FROM bucket visitor (e.g., FROM bucket:users).
   */
  @Override
  public FromItem visitFromBucket(final SQLParser.FromBucketContext ctx) {
    final FromItem fromItem = new FromItem(-1);
    fromItem.bucket = (Bucket) visit(ctx.bucketIdentifier());
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

    // Handle modifiers if present
    if (ctx.modifier() != null && !ctx.modifier().isEmpty()) {
      fromItem.modifier = (Modifier) visit(ctx.modifier(0));
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
        final java.lang.reflect.Field exprField = IsNotNullCondition.class.getDeclaredField("expression");
        exprField.setAccessible(true);
        exprField.set(condition, expr);
        return condition;
      } else {
        // IS NULL
        final IsNullCondition condition = new IsNullCondition(-1);
        final java.lang.reflect.Field exprField = IsNullCondition.class.getDeclaredField("expression");
        exprField.setAccessible(true);
        exprField.set(condition, expr);
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

    condition.left = (Expression) visit(ctx.expression(0));
    condition.not = ctx.NOT() != null;

    // Right side can be:
    // 1. IN (expr1, expr2, ...) - parenthesized list
    // 2. IN [expr1, expr2, ...] - array literal
    // 3. IN ? or IN :param - input parameter (already an expression)

    if (ctx.LPAREN() != null) {
      // Form: IN (expr1, expr2, ...)
      // Create array of expressions
      final List<Expression> expressions = new ArrayList<>();
      for (int i = 1; i < ctx.expression().size(); i++) {
        expressions.add((Expression) visit(ctx.expression(i)));
      }
      condition.right = expressions;
    } else {
      // Form: IN expression (could be array literal, input parameter, etc.)
      final Expression rightExpr = (Expression) visit(ctx.expression(1));

      // Check if it's a math expression (array literal would be a BaseExpression with array)
      if (rightExpr.mathExpression != null) {
        condition.rightMathExpression = rightExpr.mathExpression;
      } else {
        condition.right = rightExpr;
      }
    }

    return condition;
  }

  /**
   * TRUE/FALSE/NULL condition literals - TODO: Implement when needed by tests.
   */
  @Override
  public BooleanExpression visitTrueCondition(final SQLParser.TrueConditionContext ctx) {
    throw new UnsupportedOperationException("TRUE literal condition not yet implemented");
  }

  @Override
  public BooleanExpression visitFalseCondition(final SQLParser.FalseConditionContext ctx) {
    throw new UnsupportedOperationException("FALSE literal condition not yet implemented");
  }

  @Override
  public BooleanExpression visitNullCondition(final SQLParser.NullConditionContext ctx) {
    throw new UnsupportedOperationException("NULL literal condition not yet implemented");
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
      final java.lang.reflect.Field firstField = BetweenCondition.class.getDeclaredField("first");
      firstField.setAccessible(true);
      firstField.set(condition, first);

      final java.lang.reflect.Field secondField = BetweenCondition.class.getDeclaredField("second");
      secondField.setAccessible(true);
      secondField.set(condition, second);

      final java.lang.reflect.Field thirdField = BetweenCondition.class.getDeclaredField("third");
      thirdField.setAccessible(true);
      thirdField.set(condition, third);

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
   * CONTAINS condition - checks if collection contains a value.
   * Grammar: expression CONTAINS expression
   */
  @Override
  public BooleanExpression visitContainsCondition(final SQLParser.ContainsConditionContext ctx) {
    final Expression left = (Expression) visit(ctx.expression(0));
    final Expression right = (Expression) visit(ctx.expression(1));

    try {
      final ContainsCondition condition = new ContainsCondition(-1);
      final java.lang.reflect.Field leftField = ContainsCondition.class.getDeclaredField("left");
      leftField.setAccessible(true);
      leftField.set(condition, left);

      final java.lang.reflect.Field rightField = ContainsCondition.class.getDeclaredField("right");
      rightField.setAccessible(true);
      rightField.set(condition, right);

      return condition;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build CONTAINS condition: " + e.getMessage(), e);
    }
  }

  /**
   * CONTAINSALL condition - checks if collection contains all specified values.
   */
  @Override
  public BooleanExpression visitContainsAllCondition(final SQLParser.ContainsAllConditionContext ctx) {
    final Expression left = (Expression) visit(ctx.expression(0));
    final Expression right = (Expression) visit(ctx.expression(1));

    try {
      final ContainsAllCondition condition = new ContainsAllCondition(-1);
      final java.lang.reflect.Field leftField = ContainsAllCondition.class.getDeclaredField("left");
      leftField.setAccessible(true);
      leftField.set(condition, left);

      final java.lang.reflect.Field rightField = ContainsAllCondition.class.getDeclaredField("right");
      rightField.setAccessible(true);
      rightField.set(condition, right);

      return condition;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build CONTAINSALL condition: " + e.getMessage(), e);
    }
  }

  /**
   * CONTAINSANY condition - checks if collection contains any of the specified values.
   */
  @Override
  public BooleanExpression visitContainsAnyCondition(final SQLParser.ContainsAnyConditionContext ctx) {
    final Expression left = (Expression) visit(ctx.expression(0));
    final Expression right = (Expression) visit(ctx.expression(1));

    try {
      final ContainsAnyCondition condition = new ContainsAnyCondition(-1);
      final java.lang.reflect.Field leftField = ContainsAnyCondition.class.getDeclaredField("left");
      leftField.setAccessible(true);
      leftField.set(condition, left);

      final java.lang.reflect.Field rightField = ContainsAnyCondition.class.getDeclaredField("right");
      rightField.setAccessible(true);
      rightField.set(condition, right);

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
      final java.lang.reflect.Field leftField = ContainsValueCondition.class.getDeclaredField("left");
      leftField.setAccessible(true);
      leftField.set(condition, left);

      final java.lang.reflect.Field exprField = ContainsValueCondition.class.getDeclaredField("expression");
      exprField.setAccessible(true);
      exprField.set(condition, right);

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
      final java.lang.reflect.Field leftField = ContainsTextCondition.class.getDeclaredField("left");
      leftField.setAccessible(true);
      leftField.set(condition, left);

      final java.lang.reflect.Field rightField = ContainsTextCondition.class.getDeclaredField("right");
      rightField.setAccessible(true);
      rightField.set(condition, right);

      return condition;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build CONTAINSTEXT condition: " + e.getMessage(), e);
    }
  }

  /**
   * LIKE condition (e.g., name LIKE 'John%').
   * Grammar: expression LIKE expression
   */
  @Override
  public BooleanExpression visitLikeCondition(final SQLParser.LikeConditionContext ctx) {
    final BinaryCondition condition = new BinaryCondition(-1);
    condition.left = (Expression) visit(ctx.expression(0));
    condition.right = (Expression) visit(ctx.expression(1));
    condition.operator = new LikeOperator(-1);
    return condition;
  }

  /**
   * ILIKE condition (case-insensitive LIKE).
   * Grammar: expression ILIKE expression
   */
  @Override
  public BooleanExpression visitIlikeCondition(final SQLParser.IlikeConditionContext ctx) {
    final BinaryCondition condition = new BinaryCondition(-1);
    condition.left = (Expression) visit(ctx.expression(0));
    condition.right = (Expression) visit(ctx.expression(1));
    condition.operator = new ILikeOperator(-1);
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
      final java.lang.reflect.Field exprField = MatchesCondition.class.getDeclaredField("expression");
      exprField.setAccessible(true);
      exprField.set(condition, leftExpr);

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
      final java.lang.reflect.Field leftField = InstanceofCondition.class.getDeclaredField("left");
      leftField.setAccessible(true);
      leftField.set(condition, left);

      if (ctx.identifier() != null) {
        // Right side is an identifier
        final Identifier right = (Identifier) visit(ctx.identifier());
        final java.lang.reflect.Field rightField = InstanceofCondition.class.getDeclaredField("right");
        rightField.setAccessible(true);
        rightField.set(condition, right);
      } else if (ctx.STRING_LITERAL() != null) {
        // Right side is a string literal
        final String rightString = ctx.STRING_LITERAL().getText();
        final java.lang.reflect.Field rightStringField = InstanceofCondition.class.getDeclaredField("rightString");
        rightStringField.setAccessible(true);
        rightStringField.set(condition, rightString);
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
        final java.lang.reflect.Field exprField = IsNotDefinedCondition.class.getDeclaredField("expression");
        exprField.setAccessible(true);
        exprField.set(condition, expr);
        return condition;
      } else {
        // IS DEFINED
        final IsDefinedCondition condition = new IsDefinedCondition(-1);
        final java.lang.reflect.Field exprField = IsDefinedCondition.class.getDeclaredField("expression");
        exprField.setAccessible(true);
        exprField.set(condition, expr);
        return condition;
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build IS DEFINED condition: " + e.getMessage(), e);
    }
  }

  // ============================================================================
  // EXPRESSION VISITORS
  // ============================================================================

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
   */
  @Override
  public Expression visitArrayConcat(final SQLParser.ArrayConcatContext ctx) {
    final Expression expr = new Expression(-1);
    // TODO: Implement ArrayConcatExpression properly
    // For now, just parse as math expression fallback
    return (Expression) visitChildren(ctx);
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
    // TODO: Implement unary operations when needed
    throw new UnsupportedOperationException("Unary operations not yet implemented");
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
        number.setValue(Integer.parseInt(text));
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
        number.value = Double.parseDouble(text);
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
    // Remove quotes and handle escape sequences
    final String unquoted = text.substring(1, text.length() - 1);
    final String decoded = unquoted
        .replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
        .replace("\\\\", "\\")
        .replace("\\'", "'")
        .replace("\\\"", "\"");

    baseExpr.string = "\"" + decoded + "\""; // BaseExpression expects quoted string

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
      final java.lang.reflect.Field funcCallField = LevelZeroIdentifier.class.getDeclaredField("functionCall");
      funcCallField.setAccessible(true);
      funcCallField.set(levelZero, funcCall);

      final BaseIdentifier baseId = new BaseIdentifier(-1);
      final java.lang.reflect.Field levelZeroField = BaseIdentifier.class.getDeclaredField("levelZero");
      levelZeroField.setAccessible(true);
      levelZeroField.set(baseId, levelZero);

      baseExpr.identifier = baseId;

    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build function call expression: " + e.getMessage(), e);
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
      // Function name (using reflection for protected field)
      final Identifier funcName = (Identifier) visit(ctx.identifier());
      final java.lang.reflect.Field nameField = FunctionCall.class.getDeclaredField("name");
      nameField.setAccessible(true);
      nameField.set(funcCall, funcName);

      // Parameters (using reflection for protected field)
      final java.lang.reflect.Field paramsField = FunctionCall.class.getDeclaredField("params");
      paramsField.setAccessible(true);

      if (ctx.STAR() != null) {
        // Handle COUNT(*), SUM(*), etc. - create a parameter representing *
        final List<Expression> params = new ArrayList<>();
        final Expression starExpr = new Expression(-1);
        final BaseExpression baseExpr = new BaseExpression(-1);

        // Create a special identifier for * using SuffixIdentifier with star flag
        final BaseIdentifier starId = new BaseIdentifier(-1);
        try {
          final java.lang.reflect.Field suffixField = BaseIdentifier.class.getDeclaredField("suffix");
          suffixField.setAccessible(true);
          final SuffixIdentifier suffix = new SuffixIdentifier(-1);
          final java.lang.reflect.Field starField = SuffixIdentifier.class.getDeclaredField("star");
          starField.setAccessible(true);
          starField.set(suffix, true);
          suffixField.set(starId, suffix);
        } catch (final Exception e) {
          throw new CommandSQLParsingException("Failed to create star identifier: " + e.getMessage(), e);
        }

        baseExpr.identifier = starId;
        starExpr.mathExpression = baseExpr;
        params.add(starExpr);
        paramsField.set(funcCall, params);
      } else if (ctx.expression() != null && !ctx.expression().isEmpty()) {
        // Regular parameters
        final List<Expression> params = new ArrayList<>();
        for (final SQLParser.ExpressionContext exprCtx : ctx.expression()) {
          params.add((Expression) visit(exprCtx));
        }
        paramsField.set(funcCall, params);
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
    if (ctx.identifier() != null && !ctx.identifier().isEmpty()) {
      final Identifier firstId = (Identifier) visit(ctx.identifier(0));

      // Use BaseIdentifier constructor that automatically creates SuffixIdentifier
      final BaseIdentifier baseId = new BaseIdentifier(firstId);
      baseExpr.identifier = baseId;

      // TODO: Handle method calls, array selectors, modifiers if present
      // For now, simple identifier chains work
    }

    return baseExpr;
  }

  /**
   * Parenthesized expression visitor.
   * Handles both (expression) and (statement) alternatives.
   */
  @Override
  public BaseExpression visitParenthesizedExpr(final SQLParser.ParenthesizedExprContext ctx) {
    final BaseExpression baseExpr = new BaseExpression(-1);

    if (ctx.expression() != null) {
      // Regular parenthesized expression
      baseExpr.expression = (Expression) visit(ctx.expression());
    } else if (ctx.statement() != null) {
      // Parenthesized statement (subquery)
      // This case is typically handled at a higher level (e.g., visitLetItem checks for this pattern)
      // For cases where it isn't handled higher up, we need to gracefully handle it here
      // NOTE: Statements in expressions are not directly supported in all contexts
      // The calling code should detect this pattern and handle it appropriately
      // For now, create an empty BaseExpression - this will be detected as a parsing issue if used
      baseExpr.expression = null;
    }

    return baseExpr;
  }

  // ============================================================================
  // HELPER METHODS
  // ============================================================================

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
      return new Identifier(ctx.getText());
    }
  }

  /**
   * Visit rid (record ID).
   */
  @Override
  public Rid visitRid(final SQLParser.RidContext ctx) {
    final Rid rid = new Rid(-1);

    // Check if it's an expression-based RID: {rid: expr} or {"@rid": expr}
    if (ctx.expression() != null) {
      rid.expression = (Expression) visit(ctx.expression());
      rid.legacy = false;
    } else {
      // Legacy format: #bucket:position or bucket:position
      final List<SQLParser.PIntegerContext> integers = ctx.pInteger();
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
      // Use paramNumber = 0 to indicate unindexed positional parameter
      final PositionalParameter param = new PositionalParameter(-1);
      try {
        final java.lang.reflect.Field paramNumberField = PositionalParameter.class.getDeclaredField("paramNumber");
        paramNumberField.setAccessible(true);
        paramNumberField.set(param, 0);
      } catch (final Exception e) {
        throw new CommandSQLParsingException("Failed to set paramNumber: " + e.getMessage(), e);
      }
      return param;
    } else if (ctx.identifier() != null) {
      // Named parameter: :name
      final Identifier id = (Identifier) visit(ctx.identifier());
      final NamedParameter param = new NamedParameter(-1);
      try {
        final java.lang.reflect.Field paramNameField = NamedParameter.class.getDeclaredField("paramName");
        paramNameField.setAccessible(true);
        paramNameField.set(param, id.getValue());
        final java.lang.reflect.Field paramNumberField = NamedParameter.class.getDeclaredField("paramNumber");
        paramNumberField.setAccessible(true);
        paramNumberField.set(param, -1);
      } catch (final Exception e) {
        throw new CommandSQLParsingException("Failed to set paramName: " + e.getMessage(), e);
      }
      return param;
    } else if (ctx.INTEGER_LITERAL() != null) {
      // Positional parameter: $1, $2, etc.
      final int paramNum = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
      final PositionalParameter param = new PositionalParameter(-1);
      try {
        final java.lang.reflect.Field paramNumberField = PositionalParameter.class.getDeclaredField("paramNumber");
        paramNumberField.setAccessible(true);
        paramNumberField.set(param, paramNum);
      } catch (final Exception e) {
        throw new CommandSQLParsingException("Failed to set paramNumber: " + e.getMessage(), e);
      }
      return param;
    } else if (ctx.COLON() != null && ctx.INTEGER_LITERAL() != null) {
      // Named parameter: :1, :2, etc. (numeric named params)
      final int paramNum = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
      final NamedParameter param = new NamedParameter(-1);
      try {
        final java.lang.reflect.Field paramNameField = NamedParameter.class.getDeclaredField("paramName");
        paramNameField.setAccessible(true);
        paramNameField.set(param, String.valueOf(paramNum));
        final java.lang.reflect.Field paramNumberField = NamedParameter.class.getDeclaredField("paramNumber");
        paramNumberField.setAccessible(true);
        paramNumberField.set(param, paramNum);
      } catch (final Exception e) {
        throw new CommandSQLParsingException("Failed to set paramName: " + e.getMessage(), e);
      }
      return param;
    }

    throw new CommandSQLParsingException("Unknown input parameter format");
  }

  /**
   * Visit bucket identifier (bucket:name or bucket number).
   */
  @Override
  public Bucket visitBucketIdentifier(final SQLParser.BucketIdentifierContext ctx) {
    final Bucket bucket = new Bucket(-1);

    if (ctx.BUCKET_IDENTIFIER() != null) {
      // bucket:name format
      final String text = ctx.BUCKET_IDENTIFIER().getText();
      bucket.bucketName = text.substring("bucket:".length());
    } else if (ctx.BUCKET_NUMBER_IDENTIFIER() != null) {
      // bucket number format
      final String text = ctx.BUCKET_NUMBER_IDENTIFIER().getText();
      final String bucketNumberStr = text.substring("bucket:".length());
      bucket.bucketNumber = Integer.parseInt(bucketNumberStr);
    }

    return bucket;
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
   * Visit LIMIT clause.
   */
  @Override
  public Limit visitLimit(final SQLParser.LimitContext ctx) {
    final Limit limit = new Limit(-1);

    // The limit expression can be a number or an input parameter
    final Expression expr = (Expression) visit(ctx.expression());

    // Extract the number or parameter from the expression
    if (expr.mathExpression instanceof BaseExpression) {
      final BaseExpression baseExpr = (BaseExpression) expr.mathExpression;
      if (baseExpr.number instanceof PInteger) {
        limit.num = (PInteger) baseExpr.number;
      } else if (baseExpr.inputParam != null) {
        limit.inputParam = baseExpr.inputParam;
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
      if (expr.mathExpression instanceof BaseExpression) {
        final BaseExpression baseExpr = (BaseExpression) expr.mathExpression;
        final java.lang.reflect.Field numField = Skip.class.getDeclaredField("num");
        numField.setAccessible(true);
        final java.lang.reflect.Field inputParamField = Skip.class.getDeclaredField("inputParam");
        inputParamField.setAccessible(true);

        if (baseExpr.number instanceof PInteger) {
          numField.set(skip, (PInteger) baseExpr.number);
        } else if (baseExpr.inputParam != null) {
          inputParamField.set(skip, baseExpr.inputParam);
        }
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build SKIP clause: " + e.getMessage(), e);
    }

    return skip;
  }

  /**
   * Visit GROUP BY clause.
   */
  @Override
  public GroupBy visitGroupBy(final SQLParser.GroupByContext ctx) {
    final GroupBy groupBy = new GroupBy(-1);

    // GROUP BY has a list of expressions
    try {
      final java.lang.reflect.Field itemsField = GroupBy.class.getDeclaredField("items");
      itemsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      final List<Expression> items = (List<Expression>) itemsField.get(groupBy);

      for (final SQLParser.ExpressionContext exprCtx : ctx.expression()) {
        final Expression expr = (Expression) visit(exprCtx);
        items.add(expr);
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
      if (expr.mathExpression instanceof BaseExpression) {
        final BaseExpression baseExpr = (BaseExpression) expr.mathExpression;

        // Use reflection to access protected fields
        final java.lang.reflect.Field suffixField = BaseIdentifier.class.getDeclaredField("suffix");
        suffixField.setAccessible(true);
        final SuffixIdentifier suffix = (SuffixIdentifier) suffixField.get(baseExpr.identifier);

        if (suffix != null) {
          final java.lang.reflect.Field identifierField = SuffixIdentifier.class.getDeclaredField("identifier");
          identifierField.setAccessible(true);
          final Identifier id = (Identifier) identifierField.get(suffix);

          if (id != null) {
            // Simple identifier case
            item.setAlias(id.getValue());
          }
        }
      }

      // If we couldn't extract a simple alias, create a modifier
      if (item.getAlias() == null) {
        final Modifier modifier = new Modifier(-1);
        // Use the expression by wrapping it in a method call or similar
        // For now, just store in the recordAttr field as a fallback
        final java.lang.reflect.Field modifierField = OrderByItem.class.getDeclaredField("modifier");
        modifierField.setAccessible(true);
        modifierField.set(item, modifier);
      }
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Failed to build ORDER BY item: " + e.getMessage(), e);
    }

    // Set ASC or DESC
    if (ctx.DESC() != null) {
      item.setType(OrderByItem.DESC);
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
      final java.lang.reflect.Field itemsField = Unwind.class.getDeclaredField("items");
      itemsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      final List<Identifier> items = (List<Identifier>) itemsField.get(unwind);

      // The expression is what we're unwinding - it should be an identifier
      final Expression expr = (Expression) visit(ctx.expression());

      // Extract the identifier from the expression
      // The expression is typically a simple identifier like "iSeq"
      Identifier unwindField = null;

      if (expr != null && expr.mathExpression instanceof BaseExpression) {
        final BaseExpression baseExpr = (BaseExpression) expr.mathExpression;
        if (baseExpr.identifier != null) {
          // Access protected suffix field using reflection
          final java.lang.reflect.Field suffixField = BaseIdentifier.class.getDeclaredField("suffix");
          suffixField.setAccessible(true);
          final Object suffix = suffixField.get(baseExpr.identifier);

          if (suffix != null) {
            // Access protected identifier field from SuffixIdentifier
            final java.lang.reflect.Field identifierField = suffix.getClass().getDeclaredField("identifier");
            identifierField.setAccessible(true);
            unwindField = (Identifier) identifierField.get(suffix);
          }
        }
      }

      // If there's an AS clause, use that identifier, otherwise use the field itself
      if (ctx.identifier() != null) {
        final Identifier alias = (Identifier) visit(ctx.identifier());
        items.add(alias);
      } else if (unwindField != null) {
        items.add(unwindField);
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
      final java.lang.reflect.Field itemsField = LetClause.class.getDeclaredField("items");
      itemsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      final List<LetItem> items = (List<LetItem>) itemsField.get(letClause);

      for (final SQLParser.LetItemContext itemCtx : ctx.letItem()) {
        final LetItem item = (LetItem) visit(itemCtx);
        items.add(item);
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
    // where expression → MathExprContext → mathExpression (BaseContext) → baseExpression (ParenthesizedExprContext) → statement
    Statement statementFromExpr = null;
    if (ctx.expression() != null && ctx.expression() instanceof SQLParser.MathExprContext) {
      // Navigate the parse tree to find if this is a parenthesizedExpr with a statement
      final SQLParser.MathExprContext mathExprCtx = (SQLParser.MathExprContext) ctx.expression();
      final SQLParser.MathExpressionContext mathCtx = mathExprCtx.mathExpression();

      if (mathCtx != null && mathCtx instanceof SQLParser.BaseContext) {
        final SQLParser.BaseContext baseCtx = (SQLParser.BaseContext) mathCtx;
        final SQLParser.BaseExpressionContext baseExprCtx = baseCtx.baseExpression();

        if (baseExprCtx != null && baseExprCtx instanceof SQLParser.ParenthesizedExprContext) {
          final SQLParser.ParenthesizedExprContext parenCtx = (SQLParser.ParenthesizedExprContext) baseExprCtx;
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

  // ============================================================================
  // DML STATEMENT VISITORS
  // ============================================================================

  /**
   * Visit INSERT statement.
   */
  @Override
  public InsertStatement visitInsertStmt(final SQLParser.InsertStmtContext ctx) {
    final InsertStatement stmt = new InsertStatement(-1);
    final SQLParser.InsertStatementContext insertCtx = ctx.insertStatement();

    // Target: identifier (BUCKET identifier)? | bucketIdentifier
    if (insertCtx.identifier() != null && !insertCtx.identifier().isEmpty()) {
      stmt.targetType = (Identifier) visit(insertCtx.identifier(0));
      // Check for BUCKET clause
      if (insertCtx.identifier().size() > 1) {
        stmt.targetBucketName = (Identifier) visit(insertCtx.identifier(1));
      }
    } else if (insertCtx.bucketIdentifier() != null) {
      stmt.targetBucket = (Bucket) visit(insertCtx.bucketIdentifier());
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
      stmt.selectStatement = (SelectStatement) visit(insertCtx.selectStatement());
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
    // CONTENT clause: CONTENT {...} or CONTENT [...]
    else if (ctx.CONTENT() != null) {
      if (ctx.json() != null) {
        body.contentJson = (Json) visit(ctx.json());
      } else if (ctx.jsonArray() != null) {
        body.contentArray = (JsonArray) visit(ctx.jsonArray());
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
      // TODO: Need to convert expression to Json if it's a json literal
      // For now, just store as json field
      if (expr.json != null) {
        ops.json = expr.json;
      }
    } else if (ctx.CONTENT() != null) {
      ops.type = UpdateOperations.TYPE_CONTENT;
      // CONTENT also uses json field
      final Expression expr = (Expression) visit(ctx.expression());
      if (expr.json != null) {
        ops.json = expr.json;
      }
    }

    return ops;
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

  // ============================================================================
  // DDL STATEMENT VISITORS - CREATE
  // ============================================================================

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

    // EXTENDS clause
    if (bodyCtx.EXTENDS() != null && bodyCtx.identifier().size() > 1) {
      stmt.supertypes = new ArrayList<>();
      stmt.supertypes.add((Identifier) visit(bodyCtx.identifier(1)));
    }

    // BUCKET clause (list of bucket numbers)
    if (bodyCtx.BUCKET() != null && bodyCtx.INTEGER_LITERAL() != null) {
      stmt.buckets = new ArrayList<>();
      for (final org.antlr.v4.runtime.tree.TerminalNode bucketNode : bodyCtx.INTEGER_LITERAL()) {
        final BucketIdentifier bucketIdentifier = new BucketIdentifier(-1);
        final PInteger pInt = new PInteger(-1);
        pInt.setValue(Integer.parseInt(bucketNode.getText()));
        bucketIdentifier.bucketId = pInt;
        stmt.buckets.add(bucketIdentifier);
      }
    }

    // BUCKETS clause (total number)
    if (bodyCtx.BUCKETS() != null && bodyCtx.INTEGER_LITERAL() != null) {
      final PInteger pInt = new PInteger(-1);
      pInt.setValue(Integer.parseInt(bodyCtx.INTEGER_LITERAL(0).getText()));
      stmt.totalBucketNo = pInt;
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

    // EXTENDS clause
    if (bodyCtx.EXTENDS() != null && bodyCtx.identifier().size() > 1) {
      stmt.supertypes = new ArrayList<>();
      stmt.supertypes.add((Identifier) visit(bodyCtx.identifier(1)));
    }

    // BUCKET clause
    if (bodyCtx.BUCKET() != null && bodyCtx.INTEGER_LITERAL() != null) {
      stmt.buckets = new ArrayList<>();
      for (final org.antlr.v4.runtime.tree.TerminalNode bucketNode : bodyCtx.INTEGER_LITERAL()) {
        final BucketIdentifier bucketIdentifier = new BucketIdentifier(-1);
        final PInteger pInt = new PInteger(-1);
        pInt.setValue(Integer.parseInt(bucketNode.getText()));
        bucketIdentifier.bucketId = pInt;
        stmt.buckets.add(bucketIdentifier);
      }
    }

    // BUCKETS clause
    if (bodyCtx.BUCKETS() != null && bodyCtx.INTEGER_LITERAL() != null) {
      final PInteger pInt = new PInteger(-1);
      pInt.setValue(Integer.parseInt(bodyCtx.INTEGER_LITERAL(0).getText()));
      stmt.totalBucketNo = pInt;
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

    // EXTENDS clause
    if (bodyCtx.EXTENDS() != null && bodyCtx.identifier().size() > 1) {
      stmt.supertypes = new ArrayList<>();
      stmt.supertypes.add((Identifier) visit(bodyCtx.identifier(1)));
    }

    // UNIDIRECTIONAL flag
    stmt.unidirectional = bodyCtx.UNIDIRECTIONAL() != null;

    // BUCKET clause
    if (bodyCtx.BUCKET() != null && bodyCtx.INTEGER_LITERAL() != null) {
      stmt.buckets = new ArrayList<>();
      for (final org.antlr.v4.runtime.tree.TerminalNode bucketNode : bodyCtx.INTEGER_LITERAL()) {
        final BucketIdentifier bucketIdentifier = new BucketIdentifier(-1);
        final PInteger pInt = new PInteger(-1);
        pInt.setValue(Integer.parseInt(bucketNode.getText()));
        bucketIdentifier.bucketId = pInt;
        stmt.buckets.add(bucketIdentifier);
      }
    }

    // BUCKETS clause
    if (bodyCtx.BUCKETS() != null && bodyCtx.INTEGER_LITERAL() != null) {
      final PInteger pInt = new PInteger(-1);
      pInt.setValue(Integer.parseInt(bodyCtx.INTEGER_LITERAL(0).getText()));
      stmt.totalBucketNo = pInt;
    }

    return stmt;
  }

  /**
   * Visit CREATE VERTEX (instance) statement - TODO: Full implementation pending.
   * For now returns a basic statement to prevent null pointer.
   */
  @Override
  public CreateVertexStatement visitCreateVertexStmt(final SQLParser.CreateVertexStmtContext ctx) {
    // Return basic statement - full implementation requires access to protected fields
    // TODO: Use reflection or find proper API to set fields
    final CreateVertexStatement stmt = new CreateVertexStatement(-1);
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
        final java.lang.reflect.Field targetTypeField = CreateEdgeStatement.class.getDeclaredField("targetType");
        targetTypeField.setAccessible(true);
        targetTypeField.set(stmt, targetType);
      }

      // Set leftExpression (FROM clause)
      if (bodyCtx.fromItem() != null && bodyCtx.fromItem().size() > 0) {
        final FromItem fromItem = (FromItem) visit(bodyCtx.fromItem(0));
        // Convert FromItem to Expression
        final Expression leftExpr = new Expression(-1);
        if (fromItem.identifier != null) {
          leftExpr.mathExpression = new BaseExpression(fromItem.identifier);
        } else if (fromItem.rids != null && !fromItem.rids.isEmpty()) {
          leftExpr.rid = fromItem.rids.get(0);
        }
        final java.lang.reflect.Field leftExprField = CreateEdgeStatement.class.getDeclaredField("leftExpression");
        leftExprField.setAccessible(true);
        leftExprField.set(stmt, leftExpr);
      }

      // Set rightExpression (TO clause)
      if (bodyCtx.fromItem() != null && bodyCtx.fromItem().size() > 1) {
        final FromItem toItem = (FromItem) visit(bodyCtx.fromItem(1));
        // Convert FromItem to Expression
        final Expression rightExpr = new Expression(-1);
        if (toItem.identifier != null) {
          rightExpr.mathExpression = new BaseExpression(toItem.identifier);
        } else if (toItem.rids != null && !toItem.rids.isEmpty()) {
          rightExpr.rid = toItem.rids.get(0);
        }
        final java.lang.reflect.Field rightExprField = CreateEdgeStatement.class.getDeclaredField("rightExpression");
        rightExprField.setAccessible(true);
        rightExprField.set(stmt, rightExpr);
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
        final java.lang.reflect.Field bodyField = CreateEdgeStatement.class.getDeclaredField("body");
        bodyField.setAccessible(true);
        bodyField.set(stmt, body);
      }

      // Set unidirectional flag
      if (bodyCtx.UNIDIRECTIONAL() != null) {
        final java.lang.reflect.Field unidirectionalField = CreateEdgeStatement.class.getDeclaredField("unidirectional");
        unidirectionalField.setAccessible(true);
        unidirectionalField.set(stmt, true);
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

    // Property type
    stmt.propertyType = (Identifier) visit(bodyCtx.propertyType().identifier());

    // IF NOT EXISTS
    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;

    return stmt;
  }

  /**
   * Visit CREATE INDEX statement.
   */
  @Override
  public CreateIndexStatement visitCreateIndexStmt(final SQLParser.CreateIndexStmtContext ctx) {
    final CreateIndexStatement stmt = new CreateIndexStatement(-1);
    final SQLParser.CreateIndexBodyContext bodyCtx = ctx.createIndexBody();

    // Index name (optional - can be null for unnamed indexes)
    if (bodyCtx.identifier().size() > 1) {
      stmt.name = (Identifier) visit(bodyCtx.identifier(0));
      stmt.typeName = (Identifier) visit(bodyCtx.identifier(1));
    } else {
      stmt.typeName = (Identifier) visit(bodyCtx.identifier(0));
    }

    // Index properties
    for (final SQLParser.IndexPropertyContext propCtx : bodyCtx.indexProperty()) {
      final CreateIndexStatement.Property prop = new CreateIndexStatement.Property();
      prop.name = (Identifier) visit(propCtx.identifier());
      // TODO: Handle BY KEY/VALUE if needed
      stmt.propertyList.add(prop);
    }

    // Index type (UNIQUE, NOTUNIQUE, FULL_TEXT)
    if (bodyCtx.indexType() != null) {
      if (bodyCtx.indexType().UNIQUE() != null) {
        stmt.type = new Identifier("UNIQUE");
      } else if (bodyCtx.indexType().NOTUNIQUE() != null) {
        stmt.type = new Identifier("NOTUNIQUE");
      } else if (bodyCtx.indexType().FULL_TEXT() != null) {
        stmt.type = new Identifier("FULL_TEXT");
      }
    }

    // NULL_STRATEGY
    if (bodyCtx.NULL_STRATEGY() != null && bodyCtx.identifier().size() > 2) {
      // The null strategy identifier is the last one
      final Identifier nsId = (Identifier) visit(bodyCtx.identifier(bodyCtx.identifier().size() - 1));
      stmt.nullStrategy = LSMTreeIndexAbstract.NULL_STRATEGY.valueOf(nsId.getValue().toUpperCase());
    }

    // METADATA
    if (bodyCtx.METADATA() != null && bodyCtx.json() != null) {
      stmt.metadata = (Json) visit(bodyCtx.json());
    }

    // ENGINE
    if (bodyCtx.ENGINE() != null && bodyCtx.identifier().size() > 2) {
      // The engine identifier is the last one
      stmt.engine = (Identifier) visit(bodyCtx.identifier(bodyCtx.identifier().size() - 1));
    }

    return stmt;
  }

  // ============================================================================
  // DDL STATEMENTS - ALTER
  // ============================================================================

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
        stmt.identifierValue = (Identifier) visit(itemCtx.identifier());
      } else if (itemCtx.SUPERTYPE() != null) {
        stmt.property = "supertype";
        stmt.identifierListValue.add((Identifier) visit(itemCtx.identifier()));
        stmt.identifierListAddRemove.add(true); // Always add for SUPERTYPE
      } else if (itemCtx.CUSTOM() != null) {
        stmt.customKey = (Identifier) visit(itemCtx.identifier());
        stmt.customValue = (Expression) visit(itemCtx.expression());
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

  // ============================================================================
  // DDL STATEMENTS - DROP
  // ============================================================================

  /**
   * Visit DROP TYPE statement.
   */
  @Override
  public DropTypeStatement visitDropTypeStmt(final SQLParser.DropTypeStmtContext ctx) {
    final DropTypeStatement stmt = new DropTypeStatement(-1);
    final SQLParser.DropTypeBodyContext bodyCtx = ctx.dropTypeBody();

    // Type name
    stmt.name = (Identifier) visit(bodyCtx.identifier());

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

    // Index name
    stmt.name = (Identifier) visit(bodyCtx.identifier());

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

  // ============================================================================
  // DDL STATEMENTS - TRUNCATE
  // ============================================================================

  /**
   * Visit TRUNCATE TYPE statement.
   */
  @Override
  public TruncateTypeStatement visitTruncateTypeStmt(final SQLParser.TruncateTypeStmtContext ctx) {
    final TruncateTypeStatement stmt = new TruncateTypeStatement(-1);
    final SQLParser.TruncateTypeBodyContext bodyCtx = ctx.truncateTypeBody();

    // Type name
    stmt.typeName = (Identifier) visit(bodyCtx.identifier());

    // UNSAFE
    stmt.unsafe = bodyCtx.UNSAFE() != null;

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

  // ============================================================================
  // CONTROL FLOW STATEMENTS
  // ============================================================================

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

  // ============================================================================
  // TRANSACTION STATEMENTS
  // ============================================================================

  /**
   * Visit BEGIN statement.
   */
  @Override
  public BeginStatement visitBeginStmt(final SQLParser.BeginStmtContext ctx) {
    final BeginStatement stmt = new BeginStatement(-1);
    // BEGIN has no parameters in the grammar (just the keyword)
    return stmt;
  }

  /**
   * Visit COMMIT statement.
   */
  @Override
  public CommitStatement visitCommitStmt(final SQLParser.CommitStmtContext ctx) {
    final CommitStatement stmt = new CommitStatement(-1);
    // COMMIT has no parameters in the grammar (just the keyword)
    return stmt;
  }

  /**
   * Visit ROLLBACK statement.
   */
  @Override
  public RollbackStatement visitRollbackStmt(final SQLParser.RollbackStmtContext ctx) {
    final RollbackStatement stmt = new RollbackStatement(-1);
    // ROLLBACK has no parameters in the grammar (just the keyword)
    return stmt;
  }

  // ============================================================================
  // DATABASE MANAGEMENT STATEMENTS
  // ============================================================================

  /**
   * Visit CHECK DATABASE statement.
   */
  @Override
  public CheckDatabaseStatement visitCheckDatabaseStmt(final SQLParser.CheckDatabaseStmtContext ctx) {
    final CheckDatabaseStatement stmt = new CheckDatabaseStatement(-1);
    // CHECK DATABASE has no parameters in the simple grammar
    // Extended syntax with FIX, COMPRESS, TYPE, BUCKET is TODO
    return stmt;
  }

  /**
   * Visit ALIGN DATABASE statement - TODO: Implement when needed.
   */
  @Override
  public Statement visitAlignDatabaseStmt(final SQLParser.AlignDatabaseStmtContext ctx) {
    throw new UnsupportedOperationException("ALIGN DATABASE statement not yet implemented");
  }

  /**
   * Visit IMPORT DATABASE statement - TODO: Implement when needed.
   */
  @Override
  public Statement visitImportDatabaseStmt(final SQLParser.ImportDatabaseStmtContext ctx) {
    throw new UnsupportedOperationException("IMPORT DATABASE statement not yet implemented");
  }

  /**
   * Visit EXPORT DATABASE statement - TODO: Implement when needed.
   */
  @Override
  public Statement visitExportDatabaseStmt(final SQLParser.ExportDatabaseStmtContext ctx) {
    throw new UnsupportedOperationException("EXPORT DATABASE statement not yet implemented");
  }

  /**
   * Visit BACKUP DATABASE statement - TODO: Implement when needed.
   */
  @Override
  public Statement visitBackupDatabaseStmt(final SQLParser.BackupDatabaseStmtContext ctx) {
    throw new UnsupportedOperationException("BACKUP DATABASE statement not yet implemented");
  }

}
