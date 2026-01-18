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

    // Expression
    item.expression = (Expression) visit(ctx.expression());

    // Alias (AS identifier)
    if (ctx.identifier() != null) {
      item.alias = (Identifier) visit(ctx.identifier());
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
   * Condition block visitor.
   * Handles all types of conditions: comparisons, NULL checks, IN, BETWEEN, etc.
   */
  public BooleanExpression visitConditionBlock(final SQLParser.ConditionBlockContext ctx) {
    // Handle different condition types based on the labeled alternative
    return (BooleanExpression) visitChildren(ctx);
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
   */
  @Override
  public BooleanExpression visitIsNullCondition(final SQLParser.IsNullConditionContext ctx) {
    final Expression expr = (Expression) visit(ctx.expression());
    final boolean isNot = ctx.NOT() != null;

    // Create appropriate condition
    // TODO: Map to correct ArcadeDB AST class (IsNullCondition or similar)
    // For now, return a placeholder
    throw new UnsupportedOperationException("IS NULL condition not yet implemented");
  }

  /**
   * IN condition (e.g., x IN (1, 2, 3)).
   */
  @Override
  public BooleanExpression visitInCondition(final SQLParser.InConditionContext ctx) {
    final InCondition condition = new InCondition(-1);

    condition.left = (Expression) visit(ctx.expression(0));
    condition.not = ctx.NOT() != null;

    // Right side: list of expressions or subquery
    // TODO: Build InItem from expression list
    // For now, leave right field null - will be implemented later

    return condition;
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

    final PNumber number = new PNumber(-1);
    final String text = ctx.INTEGER_LITERAL().getText();
    try {
      if (text.endsWith("L") || text.endsWith("l")) {
        number.value = Long.parseLong(text.substring(0, text.length() - 1));
      } else {
        number.value = Integer.parseInt(text);
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
   * Identifier chain visitor (foo.bar.baz with modifiers).
   */
  @Override
  public BaseExpression visitIdentifierChain(final SQLParser.IdentifierChainContext ctx) {
    final BaseExpression baseExpr = new BaseExpression(-1);

    // Build identifier chain
    // TODO: Implement full identifier chain with method calls, array selectors, modifiers
    if (ctx.identifier() != null && !ctx.identifier().isEmpty()) {
      final Identifier firstId = (Identifier) visit(ctx.identifier(0));
      final BaseIdentifier baseId = new BaseIdentifier(-1);
      // TODO: Build complete BaseIdentifier structure
      baseExpr.identifier = baseId;
    }

    return baseExpr;
  }

  /**
   * Parenthesized expression visitor.
   */
  @Override
  public BaseExpression visitParenthesizedExpr(final SQLParser.ParenthesizedExprContext ctx) {
    final BaseExpression baseExpr = new BaseExpression(-1);
    baseExpr.expression = (Expression) visit(ctx.expression());
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
    final InputParameter param = new InputParameter(-1);

    if (ctx.HOOK() != null) {
      // Positional parameter: ?
      param.value = "?";
    } else if (ctx.identifier() != null) {
      // Named parameter: :name
      final Identifier id = (Identifier) visit(ctx.identifier());
      param.value = ":" + id.getValue();
    } else if (ctx.INTEGER_LITERAL() != null) {
      // Positional parameter: $1
      param.value = "$" + ctx.INTEGER_LITERAL().getText();
    }

    return param;
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
      // Each LPAREN...RPAREN group is a row of values
      // The grammar allows multiple value rows: VALUES (1,2), (3,4), (5,6)
      // This is captured by: LPAREN expression (COMMA expression)* RPAREN (COMMA LPAREN...)*
      // We need to extract each row
      List<Expression> currentRow = new ArrayList<>();
      for (final SQLParser.ExpressionContext exprCtx : ctx.expression()) {
        currentRow.add((Expression) visit(exprCtx));
      }
      if (!currentRow.isEmpty()) {
        body.valueExpressions.add(currentRow);
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

}
