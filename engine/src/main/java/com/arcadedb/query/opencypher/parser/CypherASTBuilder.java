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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * ANTLR4 visitor that builds our internal AST from the Cypher parse tree.
 * Transforms ANTLR's parse tree into CypherStatement objects.
 */
public class CypherASTBuilder extends Cypher25ParserBaseVisitor<Object> {

  // Delegate expression parsing to a dedicated builder
  private final CypherExpressionBuilder expressionBuilder = new CypherExpressionBuilder();

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
    // Process all clauses in the query
    final List<MatchClause> matchClauses = new ArrayList<>();
    CreateClause createClause = null;
    SetClause setClause = null;
    DeleteClause deleteClause = null;
    MergeClause mergeClause = null;
    final List<UnwindClause> unwindClauses = new ArrayList<>();
    final List<WithClause> withClauses = new ArrayList<>();
    final List<CallClause> callClauses = new ArrayList<>();
    final List<ClauseEntry> clausesInOrder = new ArrayList<>();
    WhereClause whereClause = null;
    ReturnClause returnClause = null;
    OrderByClause orderByClause = null;
    Integer skip = null;
    Integer limit = null;

    int clauseOrder = 0;
    for (final Cypher25Parser.ClauseContext clauseCtx : ctx.clause()) {
      if (clauseCtx.matchClause() != null) {
        final MatchClause match = visitMatchClause(clauseCtx.matchClause());
        matchClauses.add(match);
        clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.MATCH, match, clauseOrder++));

        // WHERE clause is now scoped to the MatchClause itself, not extracted at statement level
      } else if (clauseCtx.createClause() != null) {
        createClause = visitCreateClause(clauseCtx.createClause());
        clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.CREATE, createClause, clauseOrder++));
      } else if (clauseCtx.setClause() != null) {
        setClause = visitSetClause(clauseCtx.setClause());
        clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.SET, setClause, clauseOrder++));
      } else if (clauseCtx.deleteClause() != null) {
        deleteClause = visitDeleteClause(clauseCtx.deleteClause());
        clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.DELETE, deleteClause, clauseOrder++));
      } else if (clauseCtx.mergeClause() != null) {
        mergeClause = visitMergeClause(clauseCtx.mergeClause());
        clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.MERGE, mergeClause, clauseOrder++));
      } else if (clauseCtx.unwindClause() != null) {
        final UnwindClause unwind = visitUnwindClause(clauseCtx.unwindClause());
        unwindClauses.add(unwind);
        clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.UNWIND, unwind, clauseOrder++));
      } else if (clauseCtx.withClause() != null) {
        final WithClause with = visitWithClause(clauseCtx.withClause());
        withClauses.add(with);
        clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.WITH, with, clauseOrder++));
      } else if (clauseCtx.returnClause() != null) {
        // RETURN clause with embedded ORDER BY, SKIP, LIMIT
        final Cypher25Parser.ReturnBodyContext body = clauseCtx.returnClause().returnBody();
        returnClause = visitReturnClause(clauseCtx.returnClause());
        clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.RETURN, returnClause, clauseOrder++));

        // Extract ORDER BY, SKIP, LIMIT from returnBody
        if (body.orderBy() != null) {
          orderByClause = visitOrderBy(body.orderBy());
        }
        if (body.skip() != null) {
          skip = visitSkip(body.skip());
        }
        if (body.limit() != null) {
          limit = visitLimit(body.limit());
        }
      } else if (clauseCtx.orderBySkipLimitClause() != null) {
        // Standalone ORDER BY, SKIP, LIMIT clause
        final Cypher25Parser.OrderBySkipLimitClauseContext orderBySkipLimit = clauseCtx.orderBySkipLimitClause();
        if (orderBySkipLimit.orderBy() != null) {
          orderByClause = visitOrderBy(orderBySkipLimit.orderBy());
        }
        if (orderBySkipLimit.skip() != null) {
          skip = visitSkip(orderBySkipLimit.skip());
        }
        if (orderBySkipLimit.limit() != null) {
          limit = visitLimit(orderBySkipLimit.limit());
        }
      } else if (clauseCtx.callClause() != null) {
        final CallClause call = visitCallClause(clauseCtx.callClause());
        callClauses.add(call);
        clausesInOrder.add(new ClauseEntry(ClauseEntry.ClauseType.CALL, call, clauseOrder++));
      }
    }

    // Extract WHERE clause from MATCH if present
    if (!matchClauses.isEmpty() && matchClauses.get(0) != null) {
      // WHERE is embedded in matchClause in the grammar
      // We'll handle it when visiting matchClause
    }

    // Determine if query has write operations
    final boolean hasCreate = createClause != null;
    final boolean hasMerge = mergeClause != null;
    final boolean hasDelete = deleteClause != null;

    return new SimpleCypherStatement(
        "", // Original query string (we'll set this later)
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
        clausesInOrder,
        hasCreate,
        hasMerge,
        hasDelete
    );
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
      if (itemCtx instanceof Cypher25Parser.SetPropContext) {
        final Cypher25Parser.SetPropContext propCtx = (Cypher25Parser.SetPropContext) itemCtx;
        final String propExpr = propCtx.propertyExpression().getText();
        final Expression valueExpr = expressionBuilder.parseExpression(propCtx.expression());

        // Parse property expression: variable.property
        if (propExpr.contains(".")) {
          final String[] parts = propExpr.split("\\.", 2);
          items.add(new SetClause.SetItem(parts[0], parts[1], valueExpr));
        }
      }
      // TODO: Handle other SetItem types (SetProps, AddProp, SetLabels, etc.)
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
        procedureName.append(nsCtx.getText()).append(".");
      }
    }
    procedureName.append(nameCtx.symbolicNameString().getText());

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
        final String alias = itemCtx.variable() != null ? itemCtx.variable().getText() : null;
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
    final String variable = ctx.variable().getText();
    return new UnwindClause(listExpression, variable);
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
        final String alias = itemCtx.variable() != null ? itemCtx.variable().getText() : null;
        items.add(new ReturnClause.ReturnItem(expr, alias));
      }
    }

    return new ReturnClause(items, true);
  }

  @Override
  public OrderByClause visitOrderBy(final Cypher25Parser.OrderByContext ctx) {
    final List<OrderByClause.OrderByItem> items = new ArrayList<>();

    for (final Cypher25Parser.OrderItemContext itemCtx : ctx.orderItem()) {
      final String expression = itemCtx.expression().getText();
      final boolean ascending = itemCtx.descToken() == null; // DESC means not ascending

      items.add(new OrderByClause.OrderByItem(expression, ascending));
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
      final Cypher25Parser.ParenthesizedExpressionContext found = findParenthesizedExpressionRecursive(node.getChild(i));
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
    // Note: XOR is rare, for now we just delegate to expression10 if there's only one
    // TODO: Implement XOR operator support if needed
    final List<Cypher25Parser.Expression10Context> expr10List = ctx.expression10();

    if (expr10List.size() == 1) {
      // No XOR operator, just delegate
      return parseBooleanFromExpression10(expr10List.get(0));
    }

    // For now, if multiple expression10 with XOR, use fallback
    // TODO: Implement proper XOR support
    return createFallbackComparison(ctx);
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

      // Other comparison types (TypeComparison, NormalFormComparison, LabelComparison)
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
    final Pattern pattern = Pattern.compile("(\\w+)\\.(\\w+)\\s*([><=!]+)\\s*(\\w+|'[^']*'|\"[^\"]*\"|\\d+(?:\\.\\d+)?)");
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

  private Object parseValueString(String value) {
    // Remove quotes from strings
    if (value.startsWith("'") && value.endsWith("'")) {
      return value.substring(1, value.length() - 1);
    } else if (value.startsWith("\"") && value.endsWith("\"")) {
      return value.substring(1, value.length() - 1);
    }

    // Try to parse as number
    try {
      if (value.contains(".")) {
        return Double.parseDouble(value);
      } else {
        return Long.parseLong(value);
      }
    } catch (final NumberFormatException e) {
      return value;
    }
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
      pathVariable = ctx.variable().getText();
    }

    // Visit the anonymous pattern to get the base path
    final PathPattern basePath = visitAnonymousPattern(ctx.anonymousPattern());

    // If there's a path variable, create a new PathPattern with it
    if (pathVariable != null) {
      return new PathPattern(basePath.getNodes(), basePath.getRelationships(), pathVariable);
    }

    return basePath;
  }

  public PathPattern visitAnonymousPattern(final Cypher25Parser.AnonymousPatternContext ctx) {
    if (ctx.patternElement() != null) {
      return visitPatternElement(ctx.patternElement());
    }
    throw new CommandParsingException("shortestPathPattern not yet supported");
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
      variable = ctx.variable().getText();
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
      variable = ctx.variable().getText();
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
      final String key = keys.get(i).getText();
      final Object value = evaluateExpression(values.get(i));
      map.put(key, value);
    }

    return map;
  }

  private List<String> extractLabels(final Cypher25Parser.LabelExpressionContext ctx) {
    // For now, simple label extraction
    // Label expression can be complex (OR, AND, etc.), but we'll start with simple cases
    final String text = ctx.getText();

    // Remove leading colon(s) and split by |
    final String cleanText = text.replaceAll("^:+", "");
    return Arrays.asList(cleanText.split("\\|"));
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
   * Handles: \n (newline), \t (tab), \r (carriage return), \\ (backslash), \' (single quote), \" (double quote)
   *
   * @param input the string with escape sequences (without surrounding quotes)
   * @return the decoded string
   */
  static String decodeStringLiteral(final String input) {
    if (input == null || input.isEmpty()) {
      return input;
    }

    // Quick check: if no backslash, return as-is to avoid allocation
    if (input.indexOf('\\') == -1) {
      return input;
    }

    final StringBuilder result = new StringBuilder(input.length());
    boolean escaped = false;

    for (int i = 0; i < input.length(); i++) {
      final char c = input.charAt(i);

      if (escaped) {
        escaped = false;
        switch (c) {
          case 'n':
            result.append('\n');
            break;
          case 't':
            result.append('\t');
            break;
          case 'r':
            result.append('\r');
            break;
          case 'b':
            result.append('\b');
            break;
          case 'f':
            result.append('\f');
            break;
          case '\\':
            result.append('\\');
            break;
          case '\'':
            result.append('\'');
            break;
          case '"':
            result.append('"');
            break;
          case '0':
            result.append('\0');
            break;
          default:
            // For unrecognized escape sequences, keep the character as-is
            result.append(c);
            break;
        }
      } else if (c == '\\') {
        escaped = true;
      } else {
        result.append(c);
      }
    }

    // Handle trailing backslash (keep it as-is)
    if (escaped) {
      result.append('\\');
    }

    return result.toString();
  }
}
