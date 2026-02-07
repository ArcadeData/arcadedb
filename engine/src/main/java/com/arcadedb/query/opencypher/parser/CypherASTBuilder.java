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

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.*;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import com.arcadedb.query.opencypher.grammar.Cypher25ParserBaseVisitor;
import com.arcadedb.query.opencypher.rewriter.*;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * ANTLR4 visitor that builds our internal AST from the Cypher parse tree.
 * Transforms ANTLR's parse tree into CypherStatement objects.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class CypherASTBuilder extends Cypher25ParserBaseVisitor<Object> {

  // Delegate expression parsing to a dedicated builder
  private final CypherExpressionBuilder expressionBuilder = new CypherExpressionBuilder();

  // AST rewriter for query canonicalization (applied after parsing)
  private static final ExpressionRewriter AST_REWRITER = new CompositeRewriter(
      new ComparisonNormalizer(),  // Normalize comparisons for better optimizer matching
      new ConstantFolder(),        // Fold constant expressions at parse time
      new BooleanSimplifier()      // Simplify boolean expressions
  );

  @Override
  public CypherStatement visitStatement(final Cypher25Parser.StatementContext ctx) {
    // For now, focus on queryWithLocalDefinitions (the most common case)
    if (ctx.queryWithLocalDefinitions() != null) {
      return (CypherStatement) visit(ctx.queryWithLocalDefinitions());
    }
    throw new CommandParsingException("Command statements not yet supported");
  }

  @Override
  public CypherStatement visitQueryWithLocalDefinitions(final Cypher25Parser.QueryWithLocalDefinitionsContext ctx) {
    return (CypherStatement) visit(ctx.nextStatement());
  }

  @Override
  public CypherStatement visitNextStatement(final Cypher25Parser.NextStatementContext ctx) {
    // For now, support single regularQuery (no NEXT support yet)
    if (ctx.regularQuery().size() > 1) {
      throw new CommandParsingException("NEXT statements not yet supported");
    }
    return (CypherStatement) visit(ctx.regularQuery(0));
  }

  @Override
  public CypherStatement visitUnion(final Cypher25Parser.UnionContext ctx) {
    final List<Cypher25Parser.SingleQueryContext> singleQueries = ctx.singleQuery();

    // Single query - no UNION
    if (singleQueries.size() == 1)
      return (CypherStatement) visit(singleQueries.get(0));

    // Multiple queries - parse as UNION
    final List<CypherStatement> queries = new ArrayList<>();
    final List<Boolean> unionAllFlags = new ArrayList<>();

    // Parse each singleQuery
    for (final Cypher25Parser.SingleQueryContext sqCtx : singleQueries)
      queries.add((CypherStatement) visit(sqCtx));

    // Determine if each UNION is ALL or DISTINCT
    // Grammar: singleQuery (UNION (ALL | DISTINCT)? singleQuery)*
    // We have N queries and N-1 UNION tokens
    final List<TerminalNode> unionTokens = ctx.UNION();
    final List<TerminalNode> allTokens = ctx.ALL();
    final List<TerminalNode> distinctTokens = ctx.DISTINCT();

    // Build a simple flag for each union: default is DISTINCT (false), unless ALL is present
    // We need to determine which ALL/DISTINCT tokens correspond to which UNION
    // Since ANTLR gives us tokens in document order, we can match them by position
    for (int i = 0; i < unionTokens.size(); i++) {
      final int unionStart = unionTokens.get(i).getSymbol().getStartIndex();
      final int nextQueryStart = singleQueries.get(i + 1).getStart().getStartIndex();

      // Check if there's an ALL token between this UNION and the next query
      boolean isAll = false;
      for (final TerminalNode allToken : allTokens) {
        final int allPos = allToken.getSymbol().getStartIndex();
        if (allPos > unionStart && allPos < nextQueryStart) {
          isAll = true;
          break;
        }
      }

      // DISTINCT is the default, so we only need to check for ALL
      unionAllFlags.add(isAll);
    }

    return new UnionStatement(queries, unionAllFlags);
  }

  @Override
  public CypherStatement visitSingleQuery(final Cypher25Parser.SingleQueryContext ctx) {
    // Use builder pattern with dispatch table to eliminate cascading if/else statements
    final StatementBuilder builder = new StatementBuilder();
    final ClauseDispatcher dispatcher = new ClauseDispatcher();

    // Process all clauses using dispatch pattern
    for (final Cypher25Parser.ClauseContext clauseCtx : ctx.clause())
      dispatcher.dispatch(clauseCtx, builder, this);

    final CypherStatement statement = builder.build();

    // NOTE: AST rewriting infrastructure is in place (AST_REWRITER defined above)
    // Full integration would require walking the statement tree and rewriting all expressions.
    // This is deferred to avoid breaking existing functionality.
    // The rewriter can be applied to individual expressions as needed:
    //   Expression rewritten = (Expression) AST_REWRITER.rewrite(originalExpression);

    return statement;
  }

  @Override
  public MatchClause visitMatchClause(final Cypher25Parser.MatchClauseContext ctx) {
    final List<PathPattern> pathPatterns = visitPatternList(ctx.patternList());
    final boolean optional = ctx.OPTIONAL() != null;

    // Extract WHERE clause if present and scoped to this MATCH
    WhereClause whereClause = null;
    if (ctx.whereClause() != null) {
      whereClause = visitWhereClause(ctx.whereClause());
    }

    return new MatchClause(pathPatterns, optional, whereClause);
  }

  @Override
  public CreateClause visitCreateClause(final Cypher25Parser.CreateClauseContext ctx) {
    final List<PathPattern> pathPatterns = visitPatternList(ctx.patternList());
    return new CreateClause(pathPatterns);
  }

  @Override
  public SetClause visitSetClause(final Cypher25Parser.SetClauseContext ctx) {
    final List<SetClause.SetItem> items = new ArrayList<>();

    for (final Cypher25Parser.SetItemContext itemCtx : ctx.setItem()) {
      if (itemCtx instanceof Cypher25Parser.SetPropContext propCtx) {
        final String propExpr = propCtx.propertyExpression().getText();
        final Expression valueExpr = expressionBuilder.parseExpression(propCtx.expression());
        if (propExpr.contains(".")) {
          final String[] parts = propExpr.split("\\.", 2);
          items.add(new SetClause.SetItem(parts[0], parts[1], valueExpr));
        }
      } else if (itemCtx instanceof Cypher25Parser.SetPropsContext propsCtx) {
        // SET n = {map} — replace all properties
        final String variable = propsCtx.variable().getText();
        final Expression valueExpr = expressionBuilder.parseExpression(propsCtx.expression());
        items.add(new SetClause.SetItem(variable, valueExpr, SetClause.SetType.REPLACE_MAP));
      } else if (itemCtx instanceof Cypher25Parser.AddPropContext addCtx) {
        // SET n += {map} — merge properties
        final String variable = addCtx.variable().getText();
        final Expression valueExpr = expressionBuilder.parseExpression(addCtx.expression());
        items.add(new SetClause.SetItem(variable, valueExpr, SetClause.SetType.MERGE_MAP));
      } else if (itemCtx instanceof Cypher25Parser.SetLabelsContext labelsCtx) {
        // SET n:Label — add labels
        final String variable = labelsCtx.variable().getText();
        final String labelsText = labelsCtx.nodeLabels().getText();
        final String cleanText = labelsText.replaceAll("^:+", "");
        final String[] parts = cleanText.split("[:&|]+");
        final List<String> labelList = new ArrayList<>();
        for (final String part : parts)
          if (!part.isEmpty())
            labelList.add(stripBackticks(part));
        items.add(new SetClause.SetItem(variable, labelList));
      } else if (itemCtx instanceof Cypher25Parser.SetLabelsIsContext labelsIsCtx) {
        // SET n IS Label — add labels (IS syntax)
        final String variable = labelsIsCtx.variable().getText();
        final String labelsText = labelsIsCtx.nodeLabelsIs().getText();
        final String cleanText = labelsText.replaceAll("^\\s*IS\\s+", "").replaceAll("^:+", "");
        final String[] parts = cleanText.split("[:&|]+");
        final List<String> labelList = new ArrayList<>();
        for (final String part : parts)
          if (!part.isEmpty())
            labelList.add(stripBackticks(part));
        items.add(new SetClause.SetItem(variable, labelList));
      }
    }

    return new SetClause(items);
  }

  @Override
  public DeleteClause visitDeleteClause(final Cypher25Parser.DeleteClauseContext ctx) {
    final boolean detach = ctx.DETACH() != null;
    final List<String> variables = ctx.expression().stream()
        .map(expr -> expr.getText())
        .collect(Collectors.toList());

    return new DeleteClause(variables, detach);
  }

  public RemoveClause visitRemoveClause(final Cypher25Parser.RemoveClauseContext ctx) {
    final List<RemoveClause.RemoveItem> items = new ArrayList<>();

    for (final Cypher25Parser.RemoveItemContext itemCtx : ctx.removeItem()) {
      if (itemCtx instanceof Cypher25Parser.RemovePropContext) {
        // REMOVE n.property
        final Cypher25Parser.RemovePropContext propCtx = (Cypher25Parser.RemovePropContext) itemCtx;
        final String propExpr = propCtx.propertyExpression().getText();

        // Parse property expression: variable.property
        if (propExpr.contains(".")) {
          final String[] parts = propExpr.split("\\.", 2);
          items.add(new RemoveClause.RemoveItem(parts[0], parts[1]));
        }
      } else if (itemCtx instanceof Cypher25Parser.RemoveLabelsContext) {
        // REMOVE n:Label (not yet fully implemented)
        final Cypher25Parser.RemoveLabelsContext labelsCtx = (Cypher25Parser.RemoveLabelsContext) itemCtx;
        final String variable = stripBackticks(labelsCtx.variable().getText());
        final List<String> labels = new ArrayList<>();
        // TODO: Extract labels from nodeLabels context
        items.add(new RemoveClause.RemoveItem(variable, labels));
      }
      // TODO: Handle RemoveDynamicProp and RemoveLabelsIs
    }

    return new RemoveClause(items);
  }

  @Override
  public MergeClause visitMergeClause(final Cypher25Parser.MergeClauseContext ctx) {
    final PathPattern pathPattern = visitPattern(ctx.pattern());

    // Parse ON CREATE SET and ON MATCH SET actions
    SetClause onCreateSet = null;
    SetClause onMatchSet = null;

    for (final Cypher25Parser.MergeActionContext actionCtx : ctx.mergeAction()) {
      final SetClause setClause = visitSetClause(actionCtx.setClause());

      if (actionCtx.CREATE() != null) {
        // ON CREATE SET
        onCreateSet = setClause;
      } else if (actionCtx.MATCH() != null) {
        // ON MATCH SET
        onMatchSet = setClause;
      }
    }

    return new MergeClause(pathPattern, onCreateSet, onMatchSet);
  }

  @Override
  public CallClause visitCallClause(final Cypher25Parser.CallClauseContext ctx) {
    // Grammar: OPTIONAL? CALL procedureName (LPAREN (procedureArgument (COMMA procedureArgument)*)? RPAREN)?
    //          (YIELD (TIMES | procedureResultItem (COMMA procedureResultItem)* whereClause?))?
    final boolean optional = ctx.OPTIONAL() != null;

    // Parse procedure name: namespace symbolicNameString
    final Cypher25Parser.ProcedureNameContext nameCtx = ctx.procedureName();
    final StringBuilder procedureName = new StringBuilder();
    if (nameCtx.namespace() != null) {
      // Namespace is (symbolicNameString DOT)*
      for (final Cypher25Parser.SymbolicNameStringContext nsCtx : nameCtx.namespace().symbolicNameString()) {
        procedureName.append(stripBackticks(nsCtx.getText())).append(".");
      }
    }
    procedureName.append(stripBackticks(nameCtx.symbolicNameString().getText()));

    // Parse arguments
    final List<Expression> arguments = new ArrayList<>();
    for (final Cypher25Parser.ProcedureArgumentContext argCtx : ctx.procedureArgument()) {
      arguments.add(expressionBuilder.parseExpression(argCtx.expression()));
    }

    // Parse YIELD items
    List<CallClause.YieldItem> yieldItems = null;
    if (ctx.YIELD() != null) {
      yieldItems = new ArrayList<>();
      if (ctx.TIMES() != null) {
        // YIELD * - empty list means all fields
        // yieldItems remains empty
      } else {
        for (final Cypher25Parser.ProcedureResultItemContext itemCtx : ctx.procedureResultItem()) {
          final String fieldName = itemCtx.yieldItemName.getText();
          final String alias = itemCtx.yieldItemAlias != null ? itemCtx.yieldItemAlias.getText() : null;
          yieldItems.add(new CallClause.YieldItem(fieldName, alias));
        }
      }
    }

    // Parse YIELD WHERE clause
    WhereClause yieldWhere = null;
    if (ctx.whereClause() != null) {
      yieldWhere = visitWhereClause(ctx.whereClause());
    }

    return new CallClause(procedureName.toString(), arguments, yieldItems, yieldWhere, optional);
  }

  public SubqueryClause visitSubqueryClause(final Cypher25Parser.SubqueryClauseContext ctx) {
    final boolean optional = ctx.OPTIONAL() != null;

    // Parse scope variables: CALL (x, y) { ... }
    List<String> scopeVariables = null;
    if (ctx.subqueryScope() != null) {
      final Cypher25Parser.SubqueryScopeContext scopeCtx = ctx.subqueryScope();
      if (scopeCtx.TIMES() != null) {
        // CALL (*) { ... } - import all variables
        scopeVariables = List.of("*");
      } else if (scopeCtx.variable() != null && !scopeCtx.variable().isEmpty()) {
        scopeVariables = new ArrayList<>();
        for (final Cypher25Parser.VariableContext varCtx : scopeCtx.variable())
          scopeVariables.add(stripBackticks(varCtx.getText()));
      }
    }

    // Parse the inner query
    final CypherStatement innerStatement = (CypherStatement) visit(ctx.queryWithLocalDefinitions());

    return new SubqueryClause(innerStatement, scopeVariables, optional);
  }

  @Override
  public WithClause visitWithClause(final Cypher25Parser.WithClauseContext ctx) {
    // Grammar: WITH returnBody whereClause?
    final Cypher25Parser.ReturnBodyContext body = ctx.returnBody();

    // Parse return items
    final List<ReturnClause.ReturnItem> items = new ArrayList<>();
    if (body.returnItems().TIMES() != null) {
      // WITH *
      items.add(new ReturnClause.ReturnItem(new VariableExpression("*"), "*"));
    } else {
      for (final Cypher25Parser.ReturnItemContext itemCtx : body.returnItems().returnItem()) {
        final Expression expr = expressionBuilder.parseExpression(itemCtx.expression());
        final String alias = itemCtx.variable() != null ? stripBackticks(itemCtx.variable().getText()) : null;
        items.add(new ReturnClause.ReturnItem(expr, alias));
      }
    }

    // Parse DISTINCT flag
    final boolean distinct = body.DISTINCT() != null;

    // Parse WHERE clause (applied after projection)
    WhereClause whereClause = null;
    if (ctx.whereClause() != null) {
      whereClause = visitWhereClause(ctx.whereClause());
    }

    // Parse ORDER BY, SKIP, LIMIT from returnBody
    OrderByClause orderByClause = null;
    if (body.orderBy() != null) {
      orderByClause = visitOrderBy(body.orderBy());
    }

    Integer skip = null;
    if (body.skip() != null) {
      skip = visitSkip(body.skip());
    }

    Integer limit = null;
    if (body.limit() != null) {
      limit = visitLimit(body.limit());
    }

    return new WithClause(items, distinct, whereClause, orderByClause, skip, limit);
  }

  @Override
  public UnwindClause visitUnwindClause(final Cypher25Parser.UnwindClauseContext ctx) {
    // Grammar: UNWIND expression AS variable
    final Expression listExpression = expressionBuilder.parseExpression(ctx.expression());
    final String variable = stripBackticks(ctx.variable().getText());
    return new UnwindClause(listExpression, variable);
  }

  public ForeachClause visitForeachClause(final Cypher25Parser.ForeachClauseContext ctx) {
    // Grammar: FOREACH LPAREN variable IN expression BAR clause+ RPAREN
    final String variable = stripBackticks(ctx.variable().getText());
    final Expression listExpression = expressionBuilder.parseExpression(ctx.expression());

    // Parse inner clauses (CREATE, SET, DELETE, MERGE, FOREACH)
    final List<ClauseEntry> innerClauses = new ArrayList<>();
    int innerOrder = 0;
    for (final Cypher25Parser.ClauseContext innerClauseCtx : ctx.clause()) {
      if (innerClauseCtx.createClause() != null) {
        final CreateClause create = visitCreateClause(innerClauseCtx.createClause());
        innerClauses.add(new ClauseEntry(ClauseEntry.ClauseType.CREATE, create, innerOrder++));
      } else if (innerClauseCtx.setClause() != null) {
        final SetClause set = visitSetClause(innerClauseCtx.setClause());
        innerClauses.add(new ClauseEntry(ClauseEntry.ClauseType.SET, set, innerOrder++));
      } else if (innerClauseCtx.deleteClause() != null) {
        final DeleteClause delete = visitDeleteClause(innerClauseCtx.deleteClause());
        innerClauses.add(new ClauseEntry(ClauseEntry.ClauseType.DELETE, delete, innerOrder++));
      } else if (innerClauseCtx.mergeClause() != null) {
        final MergeClause merge = visitMergeClause(innerClauseCtx.mergeClause());
        innerClauses.add(new ClauseEntry(ClauseEntry.ClauseType.MERGE, merge, innerOrder++));
      } else if (innerClauseCtx.removeClause() != null) {
        final RemoveClause remove = visitRemoveClause(innerClauseCtx.removeClause());
        innerClauses.add(new ClauseEntry(ClauseEntry.ClauseType.REMOVE, remove, innerOrder++));
      } else if (innerClauseCtx.foreachClause() != null) {
        final ForeachClause nested = visitForeachClause(innerClauseCtx.foreachClause());
        innerClauses.add(new ClauseEntry(ClauseEntry.ClauseType.FOREACH, nested, innerOrder++));
      }
    }

    return new ForeachClause(variable, listExpression, innerClauses);
  }

  @Override
  public ReturnClause visitReturnClause(final Cypher25Parser.ReturnClauseContext ctx) {
    final Cypher25Parser.ReturnBodyContext body = ctx.returnBody();
    final List<ReturnClause.ReturnItem> items = new ArrayList<>();

    if (body.returnItems().TIMES() != null) {
      // RETURN *
      items.add(new ReturnClause.ReturnItem(new VariableExpression("*"), "*"));
    } else {
      for (final Cypher25Parser.ReturnItemContext itemCtx : body.returnItems().returnItem()) {
        final Expression expr = expressionBuilder.parseExpression(itemCtx.expression());
        final String alias = itemCtx.variable() != null ? stripBackticks(itemCtx.variable().getText()) : null;
        items.add(new ReturnClause.ReturnItem(expr, alias));
      }
    }

    final boolean distinct = body.DISTINCT() != null;
    return new ReturnClause(items, distinct);
  }

  @Override
  public OrderByClause visitOrderBy(final Cypher25Parser.OrderByContext ctx) {
    final List<OrderByClause.OrderByItem> items = new ArrayList<>();

    for (final Cypher25Parser.OrderItemContext itemCtx : ctx.orderItem()) {
      final String expression = itemCtx.expression().getText();
      final boolean ascending = itemCtx.descToken() == null;
      final Expression exprAST = expressionBuilder.parseExpression(itemCtx.expression());

      items.add(new OrderByClause.OrderByItem(expression, ascending, exprAST));
    }

    return new OrderByClause(items);
  }

  @Override
  public Integer visitSkip(final Cypher25Parser.SkipContext ctx) {
    return Integer.parseInt(ctx.expression().getText());
  }

  @Override
  public Integer visitLimit(final Cypher25Parser.LimitContext ctx) {
    return Integer.parseInt(ctx.expression().getText());
  }

  public WhereClause visitWhereClause(final Cypher25Parser.WhereClauseContext ctx) {
    // Parse the WHERE condition as a boolean expression
    final BooleanExpression condition = parseBooleanExpression(ctx.expression());
    return new WhereClause(condition);
  }

  /**
   * Parse an expression context into a BooleanExpression for WHERE clauses.
   * Handles logical operators (AND, OR, NOT), comparisons, IS NULL, IN, regex, etc.
   */
  private BooleanExpression parseBooleanExpression(final Cypher25Parser.ExpressionContext ctx) {
    // Traverse the expression tree to find boolean operations
    // The grammar has: expression: expression11 (OR expression11)*

    // Parse all expression11 children and combine with OR if needed
    final List<Cypher25Parser.Expression11Context> expr11List = ctx.expression11();

    if (expr11List.size() == 1) {
      // No OR operator, just delegate to expression11
      return parseBooleanFromExpression11(expr11List.get(0));
    } else if (expr11List.size() > 1) {
      // Multiple expression11 connected with OR
      BooleanExpression result = parseBooleanFromExpression11(expr11List.get(0));
      for (int i = 1; i < expr11List.size(); i++) {
        final BooleanExpression right = parseBooleanFromExpression11(expr11List.get(i));
        result = new LogicalExpression(LogicalExpression.Operator.OR, result, right);
      }
      return result;
    }

    // Fallback: create a comparison from the expression text (legacy mode)
    // This handles cases we haven't explicitly parsed yet
    return createFallbackComparison(ctx);
  }

  private Cypher25Parser.Expression11Context findExpression11(final ParseTree node) {
    if (node instanceof Cypher25Parser.Expression11Context) {
      return (Cypher25Parser.Expression11Context) node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.Expression11Context found = findExpression11(node.getChild(i));
      if (found != null) return found;
    }
    return null;
  }

  private Cypher25Parser.ParenthesizedExpressionContext findParenthesizedExpressionRecursive(final ParseTree node) {
    if (node instanceof Cypher25Parser.ParenthesizedExpressionContext) {
      return (Cypher25Parser.ParenthesizedExpressionContext) node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ParenthesizedExpressionContext found =
          findParenthesizedExpressionRecursive(node.getChild(i));
      if (found != null) return found;
    }
    return null;
  }

  /**
   * Recursively find a PatternExpressionContext in the parse tree.
   * Pattern expressions are used for pattern predicates in WHERE clauses.
   */
  private Cypher25Parser.PatternExpressionContext findPatternExpressionRecursive(final ParseTree node) {
    if (node instanceof Cypher25Parser.PatternExpressionContext) {
      return (Cypher25Parser.PatternExpressionContext) node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.PatternExpressionContext found = findPatternExpressionRecursive(node.getChild(i));
      if (found != null) return found;
    }
    return null;
  }

  /**
   * Visit a PatternExpressionContext and convert it to a PathPattern.
   */
  @Override
  public PathPattern visitPatternExpression(final Cypher25Parser.PatternExpressionContext ctx) {
    // patternExpression: pathPatternNonEmpty
    final Cypher25Parser.PathPatternNonEmptyContext pathCtx = ctx.pathPatternNonEmpty();
    if (pathCtx != null) {
      return visitPathPatternNonEmpty(pathCtx);
    }
    throw new CommandParsingException("Pattern expression must contain a path pattern");
  }

  /**
   * Visit a PathPatternNonEmptyContext and convert it to a PathPattern.
   */
  @Override
  public PathPattern visitPathPatternNonEmpty(final Cypher25Parser.PathPatternNonEmptyContext ctx) {
    // pathPatternNonEmpty: nodePattern (relationshipPattern nodePattern)*
    final List<NodePattern> nodes = new ArrayList<>();
    final List<RelationshipPattern> relationships = new ArrayList<>();

    // Parse first node
    if (ctx.nodePattern() != null && !ctx.nodePattern().isEmpty()) {
      nodes.add(visitNodePattern(ctx.nodePattern(0)));
    }

    // Parse relationships and subsequent nodes
    if (ctx.relationshipPattern() != null) {
      for (int i = 0; i < ctx.relationshipPattern().size(); i++) {
        relationships.add(visitRelationshipPattern(ctx.relationshipPattern(i)));
        if (i + 1 < ctx.nodePattern().size()) {
          nodes.add(visitNodePattern(ctx.nodePattern(i + 1)));
        }
      }
    }

    return new PathPattern(nodes, relationships, null);
  }

  private BooleanExpression parseBooleanFromExpression11(final Cypher25Parser.Expression11Context ctx) {
    // expression11: expression10 (XOR expression10)*
    final List<Cypher25Parser.Expression10Context> expr10List = ctx.expression10();

    if (expr10List.size() == 1)
      return parseBooleanFromExpression10(expr10List.get(0));

    // Multiple expression10 connected with XOR
    BooleanExpression result = parseBooleanFromExpression10(expr10List.get(0));
    for (int i = 1; i < expr10List.size(); i++) {
      final BooleanExpression right = parseBooleanFromExpression10(expr10List.get(i));
      result = new LogicalExpression(LogicalExpression.Operator.XOR, result, right);
    }
    return result;
  }

  private BooleanExpression parseBooleanFromExpression10(final Cypher25Parser.Expression10Context ctx) {
    // expression10: expression9 (AND expression9)*
    final List<Cypher25Parser.Expression9Context> expr9List = ctx.expression9();

    if (expr9List.size() == 1) {
      // No AND operator, just delegate
      return parseBooleanFromExpression9(expr9List.get(0));
    } else if (expr9List.size() > 1) {
      // Multiple expression9 connected with AND
      BooleanExpression result = parseBooleanFromExpression9(expr9List.get(0));
      for (int i = 1; i < expr9List.size(); i++) {
        final BooleanExpression right = parseBooleanFromExpression9(expr9List.get(i));
        result = new LogicalExpression(LogicalExpression.Operator.AND, result, right);
      }
      return result;
    }

    return createFallbackComparison(ctx);
  }

  private BooleanExpression parseBooleanFromExpression9(final Cypher25Parser.Expression9Context ctx) {
    // expression9: NOT* expression8
    // Check for NOT
    final boolean hasNot = ctx.NOT() != null && !ctx.NOT().isEmpty();
    final BooleanExpression inner = parseBooleanFromExpression8(ctx.expression8());

    return hasNot ? new LogicalExpression(LogicalExpression.Operator.NOT, inner) : inner;
  }

  private BooleanExpression parseBooleanFromExpression8(final Cypher25Parser.Expression8Context ctx) {
    // expression8 handles comparisons (=, !=, <, >, <=, >=)
    // expression8: expression7 ((EQ | NEQ | LT | GT | LE | GE) expression7)*

    // Try to find comparison operator
    final Cypher25Parser.Expression7Context expr7 = ctx.expression7(0);

    // Check for comparison operators in expression8
    if (ctx.expression7().size() > 1) {
      // Found a comparison, get the operator
      for (int i = 1; i < ctx.getChildCount(); i++) {
        if (ctx.getChild(i) instanceof TerminalNode) {
          final TerminalNode terminal = (TerminalNode) ctx.getChild(i);
          final int type = terminal.getSymbol().getType();

          ComparisonExpression.Operator op = null;
          if (type == Cypher25Parser.EQ) op = ComparisonExpression.Operator.EQUALS;
          else if (type == Cypher25Parser.NEQ || type == Cypher25Parser.INVALID_NEQ)
            op = ComparisonExpression.Operator.NOT_EQUALS;
          else if (type == Cypher25Parser.LT) op = ComparisonExpression.Operator.LESS_THAN;
          else if (type == Cypher25Parser.GT) op = ComparisonExpression.Operator.GREATER_THAN;
          else if (type == Cypher25Parser.LE) op = ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
          else if (type == Cypher25Parser.GE) op = ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;

          if (op != null) {
            final Expression left = expressionBuilder.parseExpressionFromText(ctx.expression7(0));
            final Expression right = expressionBuilder.parseExpressionFromText(ctx.expression7(1));
            return new ComparisonExpression(left, op, right);
          }
        }
      }
    }

    // Delegate to expression7 which handles IS NULL, IN, REGEX, etc.
    return parseBooleanFromExpression7(expr7);
  }

  private BooleanExpression parseBooleanFromExpression7(final Cypher25Parser.Expression7Context ctx) {
    // expression7: expression6 comparisonExpression6?
    final Cypher25Parser.ComparisonExpression6Context compCtx = ctx.comparisonExpression6();

    // Check if expression6 contains an EXISTS expression
    // This handles cases like: WHERE EXISTS { (p)-[:WORKS_AT]->(:Company) }
    final Cypher25Parser.Expression6Context expr6 = ctx.expression6();
    final Cypher25Parser.ExistsExpressionContext existsExpr = expressionBuilder.findExistsExpressionRecursive(expr6);
    if (existsExpr != null && compCtx == null) {
      // Parse the EXISTS expression and wrap it as a boolean expression
      final ExistsExpression exists = expressionBuilder.parseExistsExpression(existsExpr);
      // ExistsExpression implements Expression, we need to wrap it to return as BooleanExpression
      // Create an adapter that evaluates the EXISTS expression as a boolean
      return new BooleanExpression() {
        @Override
        public boolean evaluate(final Result result,
                                final CommandContext context) {
          final Object value = exists.evaluate(result, context);
          return value instanceof Boolean && (Boolean) value;
        }

        @Override
        public String getText() {
          return exists.getText();
        }
      };
    }

    // Check if expression6 contains a pattern expression (pattern predicate)
    // This handles cases like: WHERE (n)-[:KNOWS]->()
    final Cypher25Parser.PatternExpressionContext patternExpr = findPatternExpressionRecursive(expr6);
    if (patternExpr != null && compCtx == null) {
      // Parse the pattern and create a pattern predicate expression
      final PathPattern pathPattern = visitPatternExpression(patternExpr);
      return new PatternPredicateExpression(pathPattern, false);
    }

    // Check if expression6 contains a parenthesized expression
    // This handles cases like: (p.age < 26 OR p.age > 35)
    final Cypher25Parser.ParenthesizedExpressionContext parenExpr = findParenthesizedExpressionRecursive(expr6);
    if (parenExpr != null && compCtx == null) {
      // Recursively parse the inner expression
      return parseBooleanExpression(parenExpr.expression());
    }

    if (compCtx != null) {
      final Expression leftExpr = expressionBuilder.parseExpressionFromText(ctx.expression6());

      // Check which alternative of comparisonExpression6 was matched
      // NullComparison: IS NOT? NULL
      if (compCtx instanceof Cypher25Parser.NullComparisonContext) {
        final Cypher25Parser.NullComparisonContext nullCtx = (Cypher25Parser.NullComparisonContext) compCtx;
        final boolean isNot = nullCtx.NOT() != null;
        return new IsNullExpression(leftExpr, isNot);
      }

      // StringAndListComparison: (REGEQ | STARTS WITH | ENDS WITH | CONTAINS | IN) expression6
      if (compCtx instanceof Cypher25Parser.StringAndListComparisonContext) {
        final Cypher25Parser.StringAndListComparisonContext strListCtx =
            (Cypher25Parser.StringAndListComparisonContext) compCtx;

        // Check for IN operator
        if (strListCtx.IN() != null) {
          // Parse the list from expression6
          final List<Expression> listItems = expressionBuilder.parseListExpression(strListCtx.expression6());
          final boolean isNot = false; // IN doesn't have NOT variant in this grammar rule
          return new InExpression(leftExpr, listItems, isNot);
        }

        // Check for REGEX (=~)
        if (strListCtx.REGEQ() != null) {
          final Expression pattern = expressionBuilder.parseExpressionFromText(strListCtx.expression6());
          return new RegexExpression(leftExpr, pattern);
        }

        // Check for STARTS WITH
        if (strListCtx.STARTS() != null && strListCtx.WITH() != null) {
          final Expression pattern = expressionBuilder.parseExpressionFromText(strListCtx.expression6());
          return new StringMatchExpression(leftExpr, pattern, StringMatchExpression.MatchType.STARTS_WITH);
        }

        // Check for ENDS WITH
        if (strListCtx.ENDS() != null && strListCtx.WITH() != null) {
          final Expression pattern = expressionBuilder.parseExpressionFromText(strListCtx.expression6());
          return new StringMatchExpression(leftExpr, pattern, StringMatchExpression.MatchType.ENDS_WITH);
        }

        // Check for CONTAINS
        if (strListCtx.CONTAINS() != null) {
          final Expression pattern = expressionBuilder.parseExpressionFromText(strListCtx.expression6());
          return new StringMatchExpression(leftExpr, pattern, StringMatchExpression.MatchType.CONTAINS);
        }

        // Fallback for any unhandled operators
        return createFallbackComparison(ctx);
      }

      // LabelComparison: labelExpression (e.g., n:Person, n:Person|Developer)
      if (compCtx instanceof Cypher25Parser.LabelComparisonContext) {
        final Cypher25Parser.LabelComparisonContext labelCtx = (Cypher25Parser.LabelComparisonContext) compCtx;
        return parseLabelCheckExpression(leftExpr, labelCtx.labelExpression(), ctx.getText());
      }

      // Other comparison types (TypeComparison, NormalFormComparison)
      // Fall back to text-based parsing for now
      return createFallbackComparison(ctx);
    }

    // If no special comparison, treat as a simple expression that should evaluate to boolean
    // This is a fallback for cases we haven't handled yet
    return createFallbackComparison(ctx);
  }

  private BooleanExpression createFallbackComparison(final ParseTree ctx) {
    // Legacy fallback: parse simple comparisons from text
    // This handles cases we haven't explicitly parsed yet
    final String text = ctx.getText();

    // Try to parse as "variable.property operator value"
    final Pattern pattern = Pattern.compile("(\\w+)\\.(\\w+)\\s*([><=!]+)\\s*(\\w+|'[^']*'|\"[^\"]*\"|\\d+(?:\\.\\d+)" +
        "?)");
    final Matcher matcher = pattern.matcher(text);

    if (matcher.find()) {
      final String variable = matcher.group(1);
      final String property = matcher.group(2);
      final String operatorStr = matcher.group(3);
      final String valueStr = matcher.group(4);

      final Expression left = new PropertyAccessExpression(variable, property);
      final Expression right = new LiteralExpression(parseValueString(valueStr), valueStr);
      final ComparisonExpression.Operator op = ComparisonExpression.Operator.fromString(operatorStr);

      return new ComparisonExpression(left, op, right);
    }

    // Ultimate fallback: create a dummy true expression
    // This should rarely happen in practice
    return new ComparisonExpression(
        new LiteralExpression(true, "true"),
        ComparisonExpression.Operator.EQUALS,
        new LiteralExpression(true, "true")
    );
  }

  /**
   * Parse a label check expression from WHERE clause (e.g., n:Person, n:Person|Developer).
   * The grammar for labelExpression is:
   * labelExpression : (COLON | IS) labelExpression4
   * labelExpression4 : labelExpression3 (BAR COLON? labelExpression3)*  # OR
   * labelExpression3 : labelExpression2 ((AMPERSAND | COLON) labelExpression2)*  # AND
   * labelExpression2 : EXCLAMATION_MARK* labelExpression1
   * labelExpression1 : symbolicNameString  # label name
   */
  private LabelCheckExpression parseLabelCheckExpression(final Expression variableExpr,
                                                         final Cypher25Parser.LabelExpressionContext labelExprCtx,
                                                         final String text) {
    // Extract labels and operator from the label expression
    final List<String> labels = new ArrayList<>();
    LabelCheckExpression.LabelOperator operator = LabelCheckExpression.LabelOperator.AND;

    // Get the text and parse the labels - remove leading : or IS
    final String labelText = labelExprCtx.getText();
    String cleanText = labelText;
    if (cleanText.startsWith(":"))
      cleanText = cleanText.substring(1);

    // Check if it contains OR operator (|)
    if (cleanText.contains("|")) {
      operator = LabelCheckExpression.LabelOperator.OR;
      // Split by | (may have optional : after |)
      final String[] parts = cleanText.split("\\|:?");
      for (final String part : parts) {
        final String label = part.trim();
        if (!label.isEmpty())
          labels.add(label);
      }
    } else if (cleanText.contains("&") || cleanText.contains(":")) {
      // AND operator (& or :)
      operator = LabelCheckExpression.LabelOperator.AND;
      final String[] parts = cleanText.split("[&:]");
      for (final String part : parts) {
        final String label = part.trim();
        if (!label.isEmpty())
          labels.add(label);
      }
    } else {
      // Single label
      labels.add(cleanText.trim());
    }

    return new LabelCheckExpression(variableExpr, labels, operator, text);
  }

  private Object parseValueString(final String value) {
    // Delegate to ParserUtils for value parsing
    return ParserUtils.parseValueString(value);
  }

  public List<PathPattern> visitPatternList(final Cypher25Parser.PatternListContext ctx) {
    final List<PathPattern> pathPatterns = new ArrayList<>();

    for (final Cypher25Parser.PatternContext patternCtx : ctx.pattern()) {
      final PathPattern pathPattern = visitPattern(patternCtx);
      pathPatterns.add(pathPattern);
    }

    return pathPatterns;
  }

  public PathPattern visitPattern(final Cypher25Parser.PatternContext ctx) {
    // Pattern: (variable =)? pathPatternPrefix? anonymousPattern
    // Extract path variable if present (e.g., p = (a)-[r]->(b))

    String pathVariable = null;
    if (ctx.variable() != null) {
      pathVariable = stripBackticks(ctx.variable().getText());
    }

    // Visit the anonymous pattern to get the base path
    final PathPattern basePath = visitAnonymousPattern(ctx.anonymousPattern());

    // If there's a path variable, create a new PathPattern/ShortestPathPattern with it
    // IMPORTANT: Preserve the ShortestPathPattern type if the base path is one
    if (pathVariable != null) {
      if (basePath instanceof ShortestPathPattern) {
        // Preserve ShortestPathPattern type
        final ShortestPathPattern shortestBase = (ShortestPathPattern) basePath;
        return new ShortestPathPattern(basePath.getNodes(), basePath.getRelationships(), pathVariable,
            shortestBase.isAllPaths());
      }
      return new PathPattern(basePath.getNodes(), basePath.getRelationships(), pathVariable);
    }

    return basePath;
  }

  public PathPattern visitAnonymousPattern(final Cypher25Parser.AnonymousPatternContext ctx) {
    if (ctx.patternElement() != null) {
      return visitPatternElement(ctx.patternElement());
    }
    if (ctx.shortestPathPattern() != null) {
      return visitShortestPathPattern(ctx.shortestPathPattern());
    }
    throw new CommandParsingException("Unknown anonymous pattern type");
  }

  /**
   * Visit a shortestPathPattern and convert it to a ShortestPathPattern.
   * Grammar: (SHORTEST_PATH | ALL_SHORTEST_PATHS) LPAREN patternElement RPAREN
   */
  public PathPattern visitShortestPathPattern(final Cypher25Parser.ShortestPathPatternContext ctx) {
    // Parse the inner pattern element
    final PathPattern innerPattern = visitPatternElement(ctx.patternElement());

    // Determine if this is shortestPath or allShortestPaths
    final boolean isAllPaths = ctx.ALL_SHORTEST_PATHS() != null;

    // Create a ShortestPathPattern (extends PathPattern with shortest path flag)
    return new ShortestPathPattern(innerPattern.getNodes(), innerPattern.getRelationships(),
        innerPattern.getPathVariable(), isAllPaths);
  }

  public PathPattern visitPatternElement(final Cypher25Parser.PatternElementContext ctx) {
    // patternElement: (nodePattern (relationshipPattern quantifier? nodePattern)* | parenthesizedPath)+

    final List<NodePattern> nodes = new ArrayList<>();
    final List<RelationshipPattern> relationships = new ArrayList<>();

    // First node
    if (!ctx.nodePattern().isEmpty()) {
      nodes.add(visitNodePattern(ctx.nodePattern(0)));

      // Relationships and subsequent nodes
      for (int i = 0; i < ctx.relationshipPattern().size(); i++) {
        relationships.add(visitRelationshipPattern(ctx.relationshipPattern(i)));
        if (i + 1 < ctx.nodePattern().size()) {
          nodes.add(visitNodePattern(ctx.nodePattern(i + 1)));
        }
      }
    }

    // Build PathPattern
    if (relationships.isEmpty()) {
      // Single node pattern
      return new PathPattern(nodes.get(0));
    } else {
      // Path with relationships
      return new PathPattern(nodes, relationships);
    }
  }

  public NodePattern visitNodePattern(final Cypher25Parser.NodePatternContext ctx) {
    String variable = null;
    List<String> labels = null;
    Map<String, Object> properties = null;

    // Variable
    if (ctx.variable() != null) {
      variable = stripBackticks(ctx.variable().getText());
    }

    // Label expression
    if (ctx.labelExpression() != null) {
      labels = extractLabels(ctx.labelExpression());
    }

    // Properties
    if (ctx.properties() != null) {
      properties = visitProperties(ctx.properties());
    }

    return new NodePattern(variable, labels, properties);
  }

  public RelationshipPattern visitRelationshipPattern(final Cypher25Parser.RelationshipPatternContext ctx) {
    String variable = null;
    List<String> types = null;
    Map<String, Object> properties = null;
    Integer minHops = null;
    Integer maxHops = null;

    // Variable
    if (ctx.variable() != null) {
      variable = stripBackticks(ctx.variable().getText());
    }

    // Label expression (relationship types)
    if (ctx.labelExpression() != null) {
      types = extractLabels(ctx.labelExpression());
    }

    // Properties
    if (ctx.properties() != null) {
      properties = visitProperties(ctx.properties());
    }

    // Path length (variable-length relationships)
    if (ctx.pathLength() != null) {
      final Cypher25Parser.PathLengthContext pathLen = ctx.pathLength();
      if (pathLen.from != null) {
        minHops = Integer.parseInt(pathLen.from.getText());
      }
      if (pathLen.to != null) {
        maxHops = Integer.parseInt(pathLen.to.getText());
      }
      if (pathLen.single != null) {
        minHops = maxHops = Integer.parseInt(pathLen.single.getText());
      }
    }

    // Direction
    final Direction direction;
    if (ctx.leftArrow() != null && ctx.rightArrow() != null) {
      direction = Direction.BOTH;
    } else if (ctx.leftArrow() != null) {
      direction = Direction.IN;
    } else if (ctx.rightArrow() != null) {
      direction = Direction.OUT;
    } else {
      direction = Direction.BOTH;
    }

    return new RelationshipPattern(variable, types, direction, properties, minHops, maxHops);
  }

  public Map<String, Object> visitProperties(final Cypher25Parser.PropertiesContext ctx) {
    if (ctx.map() != null) {
      return visitMap(ctx.map());
    }
    // TODO: Handle parameter case
    return null;
  }

  public Map<String, Object> visitMap(final Cypher25Parser.MapContext ctx) {
    final Map<String, Object> map = new HashMap<>();

    final List<Cypher25Parser.PropertyKeyNameContext> keys = ctx.propertyKeyName();
    final List<Cypher25Parser.ExpressionContext> values = ctx.expression();

    for (int i = 0; i < keys.size() && i < values.size(); i++) {
      final String key = stripBackticks(keys.get(i).getText());
      // Parse as Expression
      final Expression expr = expressionBuilder.parseExpression(values.get(i));

      // For literal expressions, extract the value immediately for backward compatibility
      // For dynamic expressions (property access, variables, etc.), keep as Expression for runtime evaluation
      final Object value;
      if (expr instanceof LiteralExpression) {
        Object literalValue = ((LiteralExpression) expr).getValue();
        // Convert Long to Integer for backward compatibility with tests
        if (literalValue instanceof Long) {
          final long longValue = (Long) literalValue;
          if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE)
            literalValue = (int) longValue;
        }
        value = literalValue;
      } else if (expr instanceof ParameterExpression) {
        // Convert to ParameterReference for backward compatibility
        final ParameterExpression paramExpr = (ParameterExpression) expr;
        value = new ParameterReference(paramExpr.getParameterName());
      } else if (expr instanceof ListExpression) {
        // Evaluate list literals immediately
        value = expr.evaluate(null, null);
      } else {
        // Keep dynamic expressions as Expression objects for runtime evaluation
        value = expr;
      }

      map.put(key, value);
    }

    return map;
  }

  private List<String> extractLabels(final Cypher25Parser.LabelExpressionContext ctx) {
    // Delegate to ParserUtils for grammar-based label extraction
    return ParserUtils.extractLabels(ctx);
  }

  /**
   * Strips backticks from an escaped symbolic name.
   * Delegates to ParserUtils for implementation.
   *
   * @param name the name potentially wrapped in backticks
   * @return the name without backticks
   */
  static String stripBackticks(final String name) {
    return ParserUtils.stripBackticks(name);
  }

  private Object evaluateExpression(final Cypher25Parser.ExpressionContext ctx) {
    // Check for list literals first
    final Cypher25Parser.ListLiteralContext listCtx = expressionBuilder.findListLiteralRecursive(ctx);
    if (listCtx != null) {
      // Parse list literal into actual Java List
      final List<Object> list = new ArrayList<>();
      if (listCtx.expression() != null) {
        for (final Cypher25Parser.ExpressionContext exprCtx : listCtx.expression()) {
          list.add(evaluateExpression(exprCtx));
        }
      }
      return list;
    }

    // Simple expression evaluation
    final String text = ctx.getText();

    // String literal - strip quotes and decode escape sequences
    if (text.startsWith("'") && text.endsWith("'")) {
      return decodeStringLiteral(text.substring(1, text.length() - 1));
    }
    if (text.startsWith("\"") && text.endsWith("\"")) {
      return decodeStringLiteral(text.substring(1, text.length() - 1));
    }

    // Null
    if (text.equalsIgnoreCase("null")) {
      return null;
    }

    // Boolean
    if (text.equalsIgnoreCase("true")) {
      return Boolean.TRUE;
    }
    if (text.equalsIgnoreCase("false")) {
      return Boolean.FALSE;
    }

    // Parameter reference: $paramName (unquoted, starts with $)
    // This is different from a string literal like '$50' which was already handled above
    if (text.startsWith("$") && text.length() > 1) {
      final String paramName = text.substring(1);
      // Return a marker that will be resolved at execution time
      return new ParameterReference(paramName);
    }

    // Number
    try {
      if (text.contains(".")) {
        return Double.parseDouble(text);
      } else {
        return Integer.parseInt(text);
      }
    } catch (final NumberFormatException e) {
      // Keep as string
      return text;
    }
  }

  /**
   * Marker class for unresolved parameter references.
   * Used when parsing property values that reference parameters.
   */
  public static class ParameterReference {
    private final String name;

    public ParameterReference(final String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return "$" + name;
    }
  }

  /**
   * Decodes escape sequences in a string literal.
   * Delegates to ParserUtils for implementation.
   *
   * @param input the string with escape sequences (without surrounding quotes)
   * @return the decoded string
   */
  static String decodeStringLiteral(final String input) {
    return ParserUtils.decodeStringLiteral(input);
  }
}
