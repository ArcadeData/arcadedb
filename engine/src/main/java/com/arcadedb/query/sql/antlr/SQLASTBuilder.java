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
import com.arcadedb.utility.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

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

  private final Database database;
  private       int      positionalParamCounter = 0;

  public SQLASTBuilder(final Database database) {
    this.database = database;
  }

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

  // QUERY STATEMENTS

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

    @SuppressWarnings("unchecked") final List<NestedProjectionItem> includeItems = (List<NestedProjectionItem>) nestedProjection.includeItems;
    @SuppressWarnings("unchecked") final List<NestedProjectionItem> excludeItems = (List<NestedProjectionItem>) nestedProjection.excludeItems;

    for (final SQLParser.NestedProjectionItemContext itemCtx : ctx.nestedProjectionItem()) {
      final NestedProjectionItem item = (NestedProjectionItem) visit(itemCtx);
      final boolean isExclude = (Boolean) item.exclude;
      final boolean isStar = (Boolean) item.star;

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
        if (leftBaseCtx instanceof SQLParser.ParenthesizedExprContext) {
          final SQLParser.ParenthesizedExprContext leftParenCtx = (SQLParser.ParenthesizedExprContext) leftBaseCtx;
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
              if (rightExpr.mathExpression instanceof BaseExpression) {
                final BaseExpression baseExpr = (BaseExpression) rightExpr.mathExpression;
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
        if (rightExpr.mathExpression instanceof BaseExpression) {
          final BaseExpression baseExpr = (BaseExpression) rightExpr.mathExpression;
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
          if (baseCtx instanceof SQLParser.ParenthesizedExprContext) {
            final SQLParser.ParenthesizedExprContext parenCtx = (SQLParser.ParenthesizedExprContext) baseCtx;
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
      if (rightExpr.mathExpression instanceof BaseExpression) {
        final BaseExpression baseExpr = (BaseExpression) rightExpr.mathExpression;
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
   * In SQL, NULL in a boolean context is typically falsy.
   */
  @Override
  public BooleanExpression visitNullCondition(final SQLParser.NullConditionContext ctx) {
    // NULL in boolean context evaluates to FALSE
    return BooleanExpression.FALSE;
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
            if (baseCtx instanceof SQLParser.ParenthesizedExprContext) {
              final SQLParser.ParenthesizedExprContext parenCtx = (SQLParser.ParenthesizedExprContext) baseCtx;
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
        condition.right = right;
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
   */
  @Override
  public Expression visitArrayConcat(final SQLParser.ArrayConcatContext ctx) {
    final ArrayConcatExpression concatExpr = new ArrayConcatExpression(-1);

    // Visit left side
    final Expression leftExpr = (Expression) visit(ctx.expression(0));
    final ArrayConcatExpressionElement leftElement = new ArrayConcatExpressionElement(-1);
    // Copy all fields from Expression to ArrayConcatExpressionElement
    copyExpressionFields(leftExpr, leftElement);
    concatExpr.getChildExpressions().add(leftElement);

    // Visit right side
    final Expression rightExpr = (Expression) visit(ctx.expression(1));
    final ArrayConcatExpressionElement rightElement = new ArrayConcatExpressionElement(-1);
    copyExpressionFields(rightExpr, rightElement);
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
      levelZero.functionCall = funcCall;

      final BaseIdentifier baseId = new BaseIdentifier(-1);
      baseId.levelZero = levelZero;

      baseExpr.identifier = baseId;

      // Handle method calls, array selectors, and modifiers on function call
      // Grammar: functionCall: identifier LPAREN ... RPAREN methodCall* arraySelector* modifier*
      final SQLParser.FunctionCallContext funcCtx = ctx.functionCall();

      Modifier firstModifier = null;
      Modifier currentModifier = null;


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
   * Array literal visitor - handles [expr1, expr2, ...] syntax.
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
      final Identifier firstId = (Identifier) visit(ctx.identifier(0));

      // Use BaseIdentifier constructor that automatically creates SuffixIdentifier
      final BaseIdentifier baseId = new BaseIdentifier(firstId);
      baseExpr.identifier = baseId;

      // Build modifier chain from additional identifiers, methodCalls, arraySelectors and modifiers
      Modifier firstModifier = null;
      Modifier currentModifier = null;

      try {

        // Process additional identifiers (DOT identifier)* - e.g., "custom.label"
        if (ctx.identifier().size() > 1) {
          for (int i = 1; i < ctx.identifier().size(); i++) {
            final Modifier modifier = new Modifier(-1);
            final Identifier id = (Identifier) visit(ctx.identifier(i));
            final SuffixIdentifier suffix = new SuffixIdentifier(id);

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
   * Visit modifier (DOT identifier or array selector).
   * Grammar: modifier: DOT identifier | arraySelector
   */
  @Override
  public Modifier visitModifier(final SQLParser.ModifierContext ctx) {
    final Modifier modifier = new Modifier(-1);

    try {
      if (ctx.identifier() != null) {
        // DOT identifier modifier
        final Identifier id = (Identifier) visit(ctx.identifier());
        final SuffixIdentifier suffix = new SuffixIdentifier(id);
        modifier.suffix = suffix;
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
      param.paramNumber = -1;
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

        if (baseExpr.number instanceof PInteger) {
          skip.num = (PInteger) baseExpr.number;
        } else if (baseExpr.inputParam != null) {
          skip.inputParam = baseExpr.inputParam;
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
      if (expr.mathExpression instanceof BaseExpression) {
        final BaseExpression baseExpr = (BaseExpression) expr.mathExpression;

        // Use direct field access
        final SuffixIdentifier suffix = (SuffixIdentifier) baseExpr.identifier.suffix;

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
          }
        }
      }

      // If we couldn't extract a simple alias or recordAttr, create a modifier
      if (item.getAlias() == null && item.getRecordAttr() == null) {
        final Modifier modifier = new Modifier(-1);
        // Use the expression by wrapping it in a method call or similar
        // For now, just store in the recordAttr field as a fallback
        item.modifier = modifier;
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
      // The expression is what we're unwinding - it should be an identifier
      final Expression expr = (Expression) visit(ctx.expression());

      // Extract the identifier from the expression
      // The expression is typically a simple identifier like "iSeq"
      Identifier unwindField = null;

      if (expr != null && expr.mathExpression instanceof BaseExpression) {
        final BaseExpression baseExpr = (BaseExpression) expr.mathExpression;
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

      // Extract Json from expression
      // JSON literals can be in expr.json directly OR nested in mathExpression
      if (expr.json != null) {
        // Direct JSON literal from jsonLiteral alternative
        ops.json = expr.json;
      } else if (expr.mathExpression instanceof BaseExpression) {
        // JSON literal parsed as baseExpression mapLit alternative
        final BaseExpression baseExpr = (BaseExpression) expr.mathExpression;
        if (baseExpr.expression != null && baseExpr.expression.json != null) {
          ops.json = baseExpr.expression.json;
        }
      }
    } else if (ctx.CONTENT() != null) {
      ops.type = UpdateOperations.TYPE_CONTENT;
      // CONTENT also uses json field
      final Expression expr = (Expression) visit(ctx.expression());

      // Extract Json from expression (same logic as MERGE)
      if (expr.json != null) {
        // Direct JSON literal from jsonLiteral alternative
        ops.json = expr.json;
      } else if (expr.mathExpression instanceof BaseExpression) {
        // JSON literal parsed as baseExpression mapLit alternative
        final BaseExpression baseExpr = (BaseExpression) expr.mathExpression;
        if (baseExpr.expression != null && baseExpr.expression.json != null) {
          ops.json = baseExpr.expression.json;
        }
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

  // DDL STATEMENT VISITORS - CREATE

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
          final int startIdx = (bodyCtx.identifier() != null && !bodyCtx.identifier().isEmpty() && stmt.targetType != null) ? 1 : 0;
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
        // Handle CONTENT clause - direct json or jsonArray (same as INSERT)
        else if (bodyCtx.CONTENT() != null) {
          if (bodyCtx.json() != null) {
            body.contentJson = (Json) visit(bodyCtx.json());
          } else if (bodyCtx.jsonArray() != null) {
            body.contentArray = (JsonArray) visit(bodyCtx.jsonArray());
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
        if (fromItem.identifier != null) {
          leftExpr.mathExpression = new BaseExpression(fromItem.identifier);
        } else if (CollectionUtils.isNotEmpty(fromItem.rids)) {
          leftExpr.rid = fromItem.rids.get(0);
        }
        stmt.leftExpression = leftExpr;
      }

      // Set rightExpression (TO clause)
      if (bodyCtx.fromItem() != null && bodyCtx.fromItem().size() > 1) {
        final FromItem toItem = (FromItem) visit(bodyCtx.fromItem(1));
        // Convert FromItem to Expression
        final Expression rightExpr = new Expression(-1);
        if (toItem.identifier != null) {
          rightExpr.mathExpression = new BaseExpression(toItem.identifier);
        } else if (CollectionUtils.isNotEmpty(toItem.rids)) {
          rightExpr.rid = toItem.rids.get(0);
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

      // Handle BY KEY/VALUE syntax
      if (propCtx.BY() != null) {
        if (propCtx.KEY() != null) {
          prop.byKey = true;
        } else if (propCtx.VALUE() != null) {
          prop.byValue = true;
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

  // DDL STATEMENTS - DROP

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

  // TRANSACTION STATEMENTS

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
        for (final org.antlr.v4.runtime.tree.TerminalNode intNode : checkCtx.INTEGER_LITERAL()) {
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

    // Parse URL from STRING_LITERAL
    if (importCtx.STRING_LITERAL() != null) {
      final String urlString = removeQuotes(importCtx.STRING_LITERAL().getText());
      stmt.url = new Url(urlString);
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

    // Parse URL from STRING_LITERAL
    if (exportCtx.STRING_LITERAL() != null) {
      final String urlString = removeQuotes(exportCtx.STRING_LITERAL().getText());
      stmt.url = new Url(urlString);
    }

    return stmt;
  }

  /**
   * Visit BACKUP DATABASE statement.
   * Grammar: BACKUP DATABASE STRING_LITERAL
   */
  @Override
  public BackupDatabaseStatement visitBackupDatabaseStmt(final SQLParser.BackupDatabaseStmtContext ctx) {
    final BackupDatabaseStatement stmt = new BackupDatabaseStatement(-1);
    final SQLParser.BackupDatabaseStatementContext backupCtx = ctx.backupDatabaseStatement();

    // Parse URL from STRING_LITERAL
    if (backupCtx.STRING_LITERAL() != null) {
      final String urlString = removeQuotes(backupCtx.STRING_LITERAL().getText());
      stmt.url = new Url(urlString);
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
      if (expr != null && expr.mathExpression instanceof BaseExpression) {
        final BaseExpression baseExpr = (BaseExpression) expr.mathExpression;
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
      if (expr != null && expr.mathExpression instanceof BaseExpression) {
        final BaseExpression baseExpr = (BaseExpression) expr.mathExpression;

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

}
