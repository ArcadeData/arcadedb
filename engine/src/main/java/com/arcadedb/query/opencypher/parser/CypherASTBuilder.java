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
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    final List<org.antlr.v4.runtime.tree.TerminalNode> unionTokens = ctx.UNION();
    final List<org.antlr.v4.runtime.tree.TerminalNode> allTokens = ctx.ALL();
    final List<org.antlr.v4.runtime.tree.TerminalNode> distinctTokens = ctx.DISTINCT();

    // Build a simple flag for each union: default is DISTINCT (false), unless ALL is present
    // We need to determine which ALL/DISTINCT tokens correspond to which UNION
    // Since ANTLR gives us tokens in document order, we can match them by position
    for (int i = 0; i < unionTokens.size(); i++) {
      final int unionStart = unionTokens.get(i).getSymbol().getStartIndex();
      final int nextQueryStart = singleQueries.get(i + 1).getStart().getStartIndex();

      // Check if there's an ALL token between this UNION and the next query
      boolean isAll = false;
      for (final org.antlr.v4.runtime.tree.TerminalNode allToken : allTokens) {
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

  private Cypher25Parser.Expression11Context findExpression11(final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node instanceof Cypher25Parser.Expression11Context) {
      return (Cypher25Parser.Expression11Context) node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.Expression11Context found = findExpression11(node.getChild(i));
      if (found != null) return found;
    }
    return null;
  }

  private Cypher25Parser.ParenthesizedExpressionContext findParenthesizedExpressionRecursive(final org.antlr.v4.runtime.tree.ParseTree node) {
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
  private Cypher25Parser.PatternExpressionContext findPatternExpressionRecursive(final org.antlr.v4.runtime.tree.ParseTree node) {
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
        if (ctx.getChild(i) instanceof org.antlr.v4.runtime.tree.TerminalNode) {
          final org.antlr.v4.runtime.tree.TerminalNode terminal = (org.antlr.v4.runtime.tree.TerminalNode) ctx.getChild(i);
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
        public boolean evaluate(final com.arcadedb.query.sql.executor.Result result,
                               final com.arcadedb.query.sql.executor.CommandContext context) {
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

  private BooleanExpression createFallbackComparison(final org.antlr.v4.runtime.tree.ParseTree ctx) {
    // Legacy fallback: parse simple comparisons from text
    // This handles cases we haven't explicitly parsed yet
    final String text = ctx.getText();

    // Try to parse as "variable.property operator value"
    final java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(\\w+)\\.(\\w+)\\s*([><=!]+)\\s*(\\w+|'[^']*'|\"[^\"]*\"|\\d+(?:\\.\\d+)?)");
    final java.util.regex.Matcher matcher = pattern.matcher(text);

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

    // String literal - strip quotes
    if (text.startsWith("'") && text.endsWith("'")) {
      return text.substring(1, text.length() - 1);
    }
    if (text.startsWith("\"") && text.endsWith("\"")) {
      return text.substring(1, text.length() - 1);
    }

    // Boolean
    if (text.equalsIgnoreCase("true")) {
      return Boolean.TRUE;
    }
    if (text.equalsIgnoreCase("false")) {
      return Boolean.FALSE;
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
}

  /**
   * Parse a list expression into a list of Expression items.
   * Handles list literals like [1, 2, 3] or ['Alice', 'Bob'].
   */
  private List<Expression> parseListExpression(final org.antlr.v4.runtime.tree.ParseTree node) {
    final List<Expression> items = new ArrayList<>();

    // Try to find a listLiteral context
    final Cypher25Parser.ListLiteralContext listCtx = findListLiteral(node);
    if (listCtx != null) {
      // Parse each expression in the list
      for (final Cypher25Parser.ExpressionContext exprCtx : listCtx.expression()) {
        items.add(parseExpression(exprCtx));
      }
      return items;
    }

    // Fallback: parse as text and try to extract items
    // This handles simple cases like [1,2,3] or ['a','b','c']
    final String text = node.getText();
    if (text.startsWith("[") && text.endsWith("]")) {
      final String content = text.substring(1, text.length() - 1);
      if (!content.isEmpty()) {
        final String[] parts = content.split(",");
        for (final String part : parts) {
          items.add(parseExpressionText(part.trim()));
        }
      }
    } else {
      // Not a list literal, treat as single item
      items.add(parseExpressionFromText(node));
    }

    return items;
  }

  /**
   * Recursively find a ListLiteralContext in the parse tree.
   */
  private Cypher25Parser.ListLiteralContext findListLiteral(final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node instanceof Cypher25Parser.ListLiteralContext) {
      return (Cypher25Parser.ListLiteralContext) node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ListLiteralContext found = findListLiteral(node.getChild(i));
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Parse expression text into an Expression AST node.
   * Shared logic for parsing expressions from text.
   */
  private Expression parseExpressionText(final String text) {
    // Check for parameter: $paramName or $1
    if (text.startsWith("$")) {
      final String parameterName = text.substring(1); // Remove the $ prefix
      return new ParameterExpression(parameterName, text);
    }

    // Try to parse as literal FIRST (before checking for dots)
    // This prevents string literals like 'A.*' from being parsed as property access
    final Object literalValue = tryParseLiteral(text);
    if (literalValue != null) {
      return new LiteralExpression(literalValue, text);
    }

    // Check for comparison expressions: expr1 op expr2
    // This handles cases like ID(a) = row.source_id, x > 5, etc.
    // We need to handle this before function calls because comparisons like ID(a)=x
    // contain parentheses but should be parsed as comparisons
    final String[] comparisonOps = {"!=", "<=", ">=", "<>", "=", "<", ">"};
    for (final String op : comparisonOps) {
      // Find the operator, but be careful with operators inside parentheses
      final int opIndex = findOperatorOutsideParentheses(text, op);
      if (opIndex > 0 && opIndex < text.length() - op.length()) {
        final String leftText = text.substring(0, opIndex).trim();
        final String rightText = text.substring(opIndex + op.length()).trim();
        if (!leftText.isEmpty() && !rightText.isEmpty()) {
          final Expression left = parseExpressionText(leftText);
          final Expression right = parseExpressionText(rightText);
          final ComparisonExpression.Operator compOp = op.equals("<>") ?
              ComparisonExpression.Operator.NOT_EQUALS :
              ComparisonExpression.Operator.fromString(op);
          // Return a BooleanExpression wrapped as Expression
          return new ComparisonExpressionWrapper(left, compOp, right);
        }
      }
    }

    // Check for function call: functionName(args)
    // This handles cases like ID(n), count(n), etc. in WHERE clauses
    if (text.contains("(") && text.endsWith(")")) {
      final int openParen = text.indexOf('(');
      final String functionName = text.substring(0, openParen).trim();

      // Make sure it's a valid function name (letters, numbers, underscores)
      if (functionName.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
        final String argsText = text.substring(openParen + 1, text.length() - 1).trim();
        final List<Expression> args = new ArrayList<>();

        if (!argsText.isEmpty()) {
          // Parse arguments (simple split by comma - doesn't handle nested calls)
          // For nested calls, we'd need the full parse tree
          final String[] argParts = argsText.split(",");
          for (final String argPart : argParts) {
            args.add(parseExpressionText(argPart.trim()));
          }
        }

        return new FunctionCallExpression(functionName, args, false);
      }
    }

    // Check for property access: variable.property
    if (text.contains(".") && !text.contains("(")) {
      final String[] parts = text.split("\\.", 2);
      return new PropertyAccessExpression(parts[0], parts[1]);
    }

    // Simple variable
    return new VariableExpression(text);
  }

  /**
   * Parse an expression into an Expression AST node.
   * Handles variables, property access, function calls, list literals, and literals.
   */
  private Expression parseExpression(final Cypher25Parser.ExpressionContext ctx) {
    final String text = ctx.getText();

    // Check for count(*) special case - has its own grammar rule
    final Cypher25Parser.CountStarContext countStarCtx = findCountStarRecursive(ctx);
    if (countStarCtx != null) {
      // count(*) is treated as count(asterisk) where asterisk evaluates to a non-null marker
      final List<Expression> args = new ArrayList<>();
      args.add(new com.arcadedb.query.opencypher.ast.StarExpression());
      return new FunctionCallExpression("count", args, false);
    }

    // Check for EXISTS expression
    final Cypher25Parser.ExistsExpressionContext existsCtx = findExistsExpressionRecursive(ctx);
    if (existsCtx != null) {
      return parseExistsExpression(existsCtx);
    }

    // Check for CASE expressions (check both forms)
    final Cypher25Parser.CaseExpressionContext caseCtx = findCaseExpressionRecursive(ctx);
    if (caseCtx != null) {
      return parseCaseExpression(caseCtx);
    }

    final Cypher25Parser.ExtendedCaseExpressionContext extCaseCtx = findExtendedCaseExpressionRecursive(ctx);
    if (extCaseCtx != null) {
      return parseExtendedCaseExpression(extCaseCtx);
    }

    // Check for list comprehensions BEFORE comparison expressions
    // List comprehensions can contain comparisons in WHERE clause, e.g., [x IN list WHERE x > 2 | x * 10]
    // If we check comparisons first, the inner comparison gets matched instead
    final Cypher25Parser.ListComprehensionContext listCompCtx = findListComprehensionRecursive(ctx);
    if (listCompCtx != null) {
      return parseListComprehension(listCompCtx);
    }

    // Check for comparison expressions (before function invocations)
    // This is critical for expressions like ID(a) = row.source_id
    // where we need to recognize the comparison operator at the top level
    final Cypher25Parser.Expression8Context expr8Ctx = findExpression8Recursive(ctx);
    if (expr8Ctx != null) {
      // Check if this Expression8 actually contains a comparison (has 2+ expression7 children)
      if (expr8Ctx.expression7().size() > 1) {
        return parseComparisonFromExpression8(expr8Ctx);
      }
    }

    // Check for function invocations BEFORE list literals
    // (tail([1,2,3]) should be parsed as a function call, not as a list literal)
    final Cypher25Parser.FunctionInvocationContext funcCtx = findFunctionInvocationRecursive(ctx);
    if (funcCtx != null) {
      return parseFunctionInvocation(funcCtx);
    }

    // Check for list literals (after list comprehensions to avoid matching inner lists)
    final Cypher25Parser.ListLiteralContext listCtx = findListLiteralRecursive(ctx);
    if (listCtx != null) {
      return parseListLiteral(listCtx);
    }

    // Check for map projections BEFORE arithmetic expressions
    // Map projections have syntax: n{.name, .age} and can contain arithmetic inside
    final Cypher25Parser.MapProjectionContext mapProjCtx = findMapProjectionRecursive(ctx);
    if (mapProjCtx != null) {
      return parseMapProjection(mapProjCtx);
    }

    // Check for map literals BEFORE arithmetic expressions
    // Map literals can contain arithmetic expressions inside, e.g., {doubled: n.age * 2}
    final Cypher25Parser.MapContext mapCtx = findMapRecursive(ctx);
    if (mapCtx != null) {
      return parseMapLiteralExpression(mapCtx);
    }

    // Check for arithmetic expressions (+ - * / % ^)
    // Must be checked BEFORE falling back to text parsing
    final Cypher25Parser.Expression6Context arith6Ctx = findArithmeticExpression6Recursive(ctx);
    if (arith6Ctx != null) {
      return parseArithmeticExpression6(arith6Ctx);
    }
    final Cypher25Parser.Expression5Context arith5Ctx = findArithmeticExpression5Recursive(ctx);
    if (arith5Ctx != null) {
      return parseArithmeticExpression5(arith5Ctx);
    }

    // Check for IS NULL / IS NOT NULL expressions
    final Cypher25Parser.NullComparisonContext nullCtx = findNullComparisonRecursive(ctx);
    if (nullCtx != null) {
      return parseIsNullExpression(nullCtx);
    }

    // Use the shared text parsing logic
    return parseExpressionText(text);
  }

  /**
   * Find an operator outside of parentheses in the given text.
   * This ensures we don't match operators inside function calls like ID(a).
   */
  private int findOperatorOutsideParentheses(final String text, final String op) {
    int parenDepth = 0;
    int bracketDepth = 0;
    boolean inString = false;
    char stringChar = 0;

    for (int i = 0; i <= text.length() - op.length(); i++) {
      final char c = text.charAt(i);

      // Track string literals
      if ((c == '\'' || c == '"') && (i == 0 || text.charAt(i - 1) != '\\')) {
        if (!inString) {
          inString = true;
          stringChar = c;
        } else if (c == stringChar) {
          inString = false;
        }
        continue;
      }

      if (inString) {
        continue;
      }

      // Track parentheses
      if (c == '(') {
        parenDepth++;
        continue;
      }
      if (c == ')') {
        parenDepth--;
        continue;
      }
      // Track brackets
      if (c == '[') {
        bracketDepth++;
        continue;
      }
      if (c == ']') {
        bracketDepth--;
        continue;
      }

      // Only match operator at top level
      if (parenDepth == 0 && bracketDepth == 0) {
        if (text.substring(i).startsWith(op)) {
          return i;
        }
      }
    }
    return -1;
  }

  /**
   * Try to parse text as a literal value (number, string, boolean, null).
   */
  private Object tryParseLiteral(final String text) {
    // Null
    if ("null".equalsIgnoreCase(text)) {
      return null; // Return null as a marker that we found a literal
    }

    // Boolean
    if ("true".equalsIgnoreCase(text)) {
      return Boolean.TRUE;
    }
    if ("false".equalsIgnoreCase(text)) {
      return Boolean.FALSE;
    }

    // String (quoted)
    if (text.startsWith("'") && text.endsWith("'") && text.length() >= 2) {
      return text.substring(1, text.length() - 1);
    }
    if (text.startsWith("\"") && text.endsWith("\"") && text.length() >= 2) {
      return text.substring(1, text.length() - 1);
    }

    // Number
    try {
      // Handle negative numbers
      if (text.startsWith("-") || text.startsWith("+") || Character.isDigit(text.charAt(0))) {
        if (text.contains(".")) {
          return Double.parseDouble(text);
        } else {
          return Long.parseLong(text);
        }
      }
    } catch (final NumberFormatException e) {
      // Not a number
    }

    return null; // Not a literal
  }

  /**
   * Recursively find countStar context in the parse tree.
   * count(*) has special grammar handling as CountStarContext.
   */
  private Cypher25Parser.CountStarContext findCountStarRecursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null) {
      return null;
    }

    // Check if this node is count(*)
    if (node instanceof Cypher25Parser.CountStarContext) {
      return (Cypher25Parser.CountStarContext) node;
    }

    // Recursively search all children
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.CountStarContext found = findCountStarRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Recursively find list literal context in the parse tree.
   */
  private Cypher25Parser.ListLiteralContext findListLiteralRecursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null) {
      return null;
    }

    // Check if this node is a list literal
    if (node instanceof Cypher25Parser.ListLiteralContext) {
      return (Cypher25Parser.ListLiteralContext) node;
    }

    // Recursively search all children
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ListLiteralContext found = findListLiteralRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Parse a list literal context into a ListExpression.
   * Example: [1, 2, 3] or ['a', 'b', 'c']
   */
  private com.arcadedb.query.opencypher.ast.ListExpression parseListLiteral(
      final Cypher25Parser.ListLiteralContext ctx) {
    final List<Expression> elements = new ArrayList<>();

    // Parse each expression in the list
    if (ctx.expression() != null) {
      for (final Cypher25Parser.ExpressionContext exprCtx : ctx.expression()) {
        elements.add(parseExpression(exprCtx));
      }
    }

    return new com.arcadedb.query.opencypher.ast.ListExpression(elements, ctx.getText());
  }

  /**
   * Recursively find function invocation in the parse tree using depth-first search.
   */
  private Cypher25Parser.FunctionInvocationContext findFunctionInvocationRecursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.FunctionInvocationContext) {
      return (Cypher25Parser.FunctionInvocationContext) node;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.FunctionInvocationContext found = findFunctionInvocationRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Parse a function invocation into a FunctionCallExpression.
   */
  private FunctionCallExpression parseFunctionInvocation(final Cypher25Parser.FunctionInvocationContext ctx) {
    final String functionName = ctx.functionName().getText();
    final boolean distinct = ctx.DISTINCT() != null;
    final List<Expression> arguments = new ArrayList<>();

    // Parse arguments
    for (final Cypher25Parser.FunctionArgumentContext argCtx : ctx.functionArgument()) {
      final String argText = argCtx.expression().getText();

      // Special handling for asterisk (though count(*) is typically handled by CountStarContext)
      if ("*".equals(argText)) {
        arguments.add(new com.arcadedb.query.opencypher.ast.StarExpression());
      } else {
        arguments.add(parseExpression(argCtx.expression()));
      }
    }

    return new FunctionCallExpression(functionName, arguments, distinct);
  }

  /**
   * Recursively find EXISTS expression in the parse tree.
   */
  private Cypher25Parser.ExistsExpressionContext findExistsExpressionRecursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.ExistsExpressionContext) {
      return (Cypher25Parser.ExistsExpressionContext) node;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ExistsExpressionContext found = findExistsExpressionRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Parse an EXISTS expression.
   * Examples: EXISTS { MATCH (n)-[:KNOWS]->(m) }, EXISTS { (n)-[:KNOWS]->() WHERE n.age > 18 }
   */
  private com.arcadedb.query.opencypher.ast.ExistsExpression parseExistsExpression(
      final Cypher25Parser.ExistsExpressionContext ctx) {
    // Extract the subquery text between { and }
    // For now, we'll extract it as text and execute it as a separate query
    final String fullText = ctx.getText();
    final String text = fullText;

    // Remove "EXISTS{" prefix and "}" suffix
    String subquery = fullText.substring(7, fullText.length() - 1); // Remove "EXISTS{" and "}"

    // If it's a pattern without MATCH, add MATCH prefix
    if (!subquery.trim().toUpperCase().startsWith("MATCH")
        && !subquery.trim().toUpperCase().startsWith("WITH")
        && !subquery.trim().toUpperCase().startsWith("RETURN")) {
      subquery = "MATCH " + subquery + " RETURN true";
    }

    return new com.arcadedb.query.opencypher.ast.ExistsExpression(subquery, text);
  }

  /**
   * Recursively find CASE expression in the parse tree.
   */
  private Cypher25Parser.CaseExpressionContext findCaseExpressionRecursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.CaseExpressionContext) {
      return (Cypher25Parser.CaseExpressionContext) node;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.CaseExpressionContext found = findCaseExpressionRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Recursively find extended CASE expression in the parse tree.
   */
  private Cypher25Parser.ExtendedCaseExpressionContext findExtendedCaseExpressionRecursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.ExtendedCaseExpressionContext) {
      return (Cypher25Parser.ExtendedCaseExpressionContext) node;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ExtendedCaseExpressionContext found = findExtendedCaseExpressionRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Parse a simple CASE expression (no case value).
   * Example: CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END
   */
  private com.arcadedb.query.opencypher.ast.CaseExpression parseCaseExpression(
      final Cypher25Parser.CaseExpressionContext ctx) {
    final List<com.arcadedb.query.opencypher.ast.CaseAlternative> alternatives = new ArrayList<>();

    // Parse each WHEN...THEN alternative
    for (final Cypher25Parser.CaseAlternativeContext altCtx : ctx.caseAlternative()) {
      final Expression whenExpr = parseExpression(altCtx.expression(0)); // First expression is WHEN
      final Expression thenExpr = parseExpression(altCtx.expression(1)); // Second expression is THEN
      alternatives.add(new com.arcadedb.query.opencypher.ast.CaseAlternative(whenExpr, thenExpr));
    }

    // Parse optional ELSE clause
    Expression elseExpr = null;
    if (ctx.expression() != null) {
      elseExpr = parseExpression(ctx.expression());
    }

    return new com.arcadedb.query.opencypher.ast.CaseExpression(alternatives, elseExpr, ctx.getText());
  }

  /**
   * Parse an extended CASE expression (with case value).
   * Example: CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END
   */
  private com.arcadedb.query.opencypher.ast.CaseExpression parseExtendedCaseExpression(
      final Cypher25Parser.ExtendedCaseExpressionContext ctx) {
    // Parse the case expression (the value being tested)
    final Expression caseExpr = parseExpression(ctx.expression(0));

    final List<com.arcadedb.query.opencypher.ast.CaseAlternative> alternatives = new ArrayList<>();

    // Parse each WHEN...THEN alternative
    for (final Cypher25Parser.ExtendedCaseAlternativeContext altCtx : ctx.extendedCaseAlternative()) {
      // In extended form, WHEN contains value(s) to match against
      // Use the first extendedWhen's text to create an expression
      final String whenText = altCtx.extendedWhen(0).getText();
      final Expression whenExpr = parseExpressionText(whenText);
      final Expression thenExpr = parseExpression(altCtx.expression());
      alternatives.add(new com.arcadedb.query.opencypher.ast.CaseAlternative(whenExpr, thenExpr));
    }

    // Parse optional ELSE clause
    Expression elseExpr = null;
    if (ctx.elseExp != null) {
      elseExpr = parseExpression(ctx.elseExp);
    }

    return new com.arcadedb.query.opencypher.ast.CaseExpression(caseExpr, alternatives, elseExpr, ctx.getText());
  }

  /**
   * Recursively find Expression8 context (handles comparison operators).
   */
  private Cypher25Parser.Expression8Context findExpression8Recursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.Expression8Context) {
      // Only return if it contains a comparison operator
      final Cypher25Parser.Expression8Context expr8 = (Cypher25Parser.Expression8Context) node;
      if (expr8.expression7().size() > 1) {
        return expr8;
      }
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.Expression8Context found = findExpression8Recursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Parse a comparison expression from Expression8Context.
   * Handles: <, >, <=, >=, =, !=
   */
  private Expression parseComparisonFromExpression8(final Cypher25Parser.Expression8Context ctx) {
    // Expression8 has multiple expression7 children with comparison operators between them
    if (ctx.expression7().size() > 1) {
      // Found a comparison, get the operator
      for (int i = 1; i < ctx.getChildCount(); i++) {
        if (ctx.getChild(i) instanceof org.antlr.v4.runtime.tree.TerminalNode) {
          final org.antlr.v4.runtime.tree.TerminalNode terminal = (org.antlr.v4.runtime.tree.TerminalNode) ctx.getChild(i);
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
            final String leftText = ctx.expression7(0).getText();
            final String rightText = ctx.expression7(1).getText();
            final Expression left = parseExpressionText(leftText);
            final Expression right = parseExpressionText(rightText);
            final ComparisonExpression comparison = new ComparisonExpression(left, op, right);
            // Wrap BooleanExpression in an Expression adapter
            return new com.arcadedb.query.opencypher.ast.BooleanWrapperExpression(comparison);
          }
        }
      }
    }

    // No comparison found, parse as text
    return parseExpressionText(ctx.getText());
  }

  /**
   * Recursively find NullComparison context (IS NULL / IS NOT NULL).
   */
  private Cypher25Parser.NullComparisonContext findNullComparisonRecursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null) {
      return null;
    }

    if (node instanceof Cypher25Parser.NullComparisonContext) {
      return (Cypher25Parser.NullComparisonContext) node;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.NullComparisonContext found = findNullComparisonRecursive(node.getChild(i));
      if (found != null) {
        return found;
      }
    }

    return null;
  }

  /**
   * Parse IS NULL / IS NOT NULL expression and wrap it for use in Expression contexts.
   */
  private Expression parseIsNullExpression(final Cypher25Parser.NullComparisonContext ctx) {
    // Get the parent expression7 context to find the left side
    org.antlr.v4.runtime.tree.ParseTree parent = ctx.getParent();
    if (parent instanceof Cypher25Parser.ComparisonExpression6Context) {
      parent = parent.getParent(); // Get expression7
    }

    if (parent instanceof Cypher25Parser.Expression7Context) {
      final Cypher25Parser.Expression7Context expr7 = (Cypher25Parser.Expression7Context) parent;
      final String leftText = expr7.expression6().getText();
      final Expression leftExpr = parseExpressionText(leftText);
      final boolean isNot = ctx.NOT() != null;
      final IsNullExpression isNullExpr = new IsNullExpression(leftExpr, isNot);
      // Wrap BooleanExpression as Expression
      return new com.arcadedb.query.opencypher.ast.BooleanWrapperExpression(isNullExpr);
    }

    // Fallback: parse as text
    return parseExpressionText(ctx.getText());
  }

  // ============================================================================
  // Arithmetic Expression Parsing
  // ============================================================================

  /**
   * Recursively find Expression6Context with arithmetic operators (+ - ||)
   */
  private Cypher25Parser.Expression6Context findArithmeticExpression6Recursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.Expression6Context) {
      final Cypher25Parser.Expression6Context ctx = (Cypher25Parser.Expression6Context) node;
      // Only return if it has multiple expression5 children (i.e., has operators)
      if (ctx.expression5().size() > 1)
        return ctx;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.Expression6Context found = findArithmeticExpression6Recursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Recursively find Expression5Context with arithmetic operators (* / %)
   */
  private Cypher25Parser.Expression5Context findArithmeticExpression5Recursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.Expression5Context) {
      final Cypher25Parser.Expression5Context ctx = (Cypher25Parser.Expression5Context) node;
      // Only return if it has multiple expression4 children (i.e., has operators)
      if (ctx.expression4().size() > 1)
        return ctx;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.Expression5Context found = findArithmeticExpression5Recursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Parse an arithmetic expression from Expression6Context (handles + - ||)
   */
  private Expression parseArithmeticExpression6(final Cypher25Parser.Expression6Context ctx) {
    final List<Cypher25Parser.Expression5Context> operands = ctx.expression5();
    if (operands.size() == 1)
      return parseArithmeticExpression5(operands.get(0));

    // Build left-associative expression tree
    Expression result = parseArithmeticExpression5(operands.get(0));

    int operandIndex = 1;
    for (int i = 0; i < ctx.getChildCount() && operandIndex < operands.size(); i++) {
      if (ctx.getChild(i) instanceof org.antlr.v4.runtime.tree.TerminalNode) {
        final org.antlr.v4.runtime.tree.TerminalNode terminal = (org.antlr.v4.runtime.tree.TerminalNode) ctx.getChild(i);
        final int type = terminal.getSymbol().getType();

        ArithmeticExpression.Operator op = null;
        if (type == Cypher25Parser.PLUS)
          op = ArithmeticExpression.Operator.ADD;
        else if (type == Cypher25Parser.MINUS)
          op = ArithmeticExpression.Operator.SUBTRACT;

        if (op != null) {
          final Expression right = parseArithmeticExpression5(operands.get(operandIndex));
          result = new ArithmeticExpression(result, op, right, ctx.getText());
          operandIndex++;
        }
      }
    }

    return result;
  }

  /**
   * Parse an arithmetic expression from Expression5Context (handles * / %)
   */
  private Expression parseArithmeticExpression5(final Cypher25Parser.Expression5Context ctx) {
    final List<Cypher25Parser.Expression4Context> operands = ctx.expression4();
    if (operands.size() == 1)
      return parseArithmeticExpression4(operands.get(0));

    // Build left-associative expression tree
    Expression result = parseArithmeticExpression4(operands.get(0));

    int operandIndex = 1;
    for (int i = 0; i < ctx.getChildCount() && operandIndex < operands.size(); i++) {
      if (ctx.getChild(i) instanceof org.antlr.v4.runtime.tree.TerminalNode) {
        final org.antlr.v4.runtime.tree.TerminalNode terminal = (org.antlr.v4.runtime.tree.TerminalNode) ctx.getChild(i);
        final int type = terminal.getSymbol().getType();

        ArithmeticExpression.Operator op = null;
        if (type == Cypher25Parser.TIMES)
          op = ArithmeticExpression.Operator.MULTIPLY;
        else if (type == Cypher25Parser.DIVIDE)
          op = ArithmeticExpression.Operator.DIVIDE;
        else if (type == Cypher25Parser.PERCENT)
          op = ArithmeticExpression.Operator.MODULO;

        if (op != null) {
          final Expression right = parseArithmeticExpression4(operands.get(operandIndex));
          result = new ArithmeticExpression(result, op, right, ctx.getText());
          operandIndex++;
        }
      }
    }

    return result;
  }

  /**
   * Parse an arithmetic expression from Expression4Context (handles ^/POW)
   */
  private Expression parseArithmeticExpression4(final Cypher25Parser.Expression4Context ctx) {
    final List<Cypher25Parser.Expression3Context> operands = ctx.expression3();
    if (operands.size() == 1)
      return parseArithmeticExpression3(operands.get(0));

    // Build right-associative expression tree for power (a ^ b ^ c = a ^ (b ^ c))
    Expression result = parseArithmeticExpression3(operands.get(operands.size() - 1));
    for (int i = operands.size() - 2; i >= 0; i--) {
      final Expression left = parseArithmeticExpression3(operands.get(i));
      result = new ArithmeticExpression(left, ArithmeticExpression.Operator.POWER, result, ctx.getText());
    }

    return result;
  }

  /**
   * Parse an arithmetic expression from Expression3Context (handles unary + -)
   */
  private Expression parseArithmeticExpression3(final Cypher25Parser.Expression3Context ctx) {
    // Check for unary plus/minus
    if (ctx.getChildCount() > 1) {
      final org.antlr.v4.runtime.tree.ParseTree firstChild = ctx.getChild(0);
      if (firstChild instanceof org.antlr.v4.runtime.tree.TerminalNode) {
        final org.antlr.v4.runtime.tree.TerminalNode terminal = (org.antlr.v4.runtime.tree.TerminalNode) firstChild;
        final int type = terminal.getSymbol().getType();
        if (type == Cypher25Parser.MINUS) {
          // Unary minus: -expression
          final Expression inner = parseExpressionFromText(ctx.expression2());
          return new ArithmeticExpression(new LiteralExpression(0L, "0"), ArithmeticExpression.Operator.SUBTRACT, inner,
              ctx.getText());
        }
        // Unary plus is a no-op
        if (type == Cypher25Parser.PLUS)
          return parseExpressionFromText(ctx.expression2());
      }
    }

    return parseExpressionFromText(ctx.expression2());
  }

  // ============================================================================
  // Map Literal Parsing
  // ============================================================================

  /**
   * Recursively find MapContext in the parse tree (for map literals like {name: 'Alice'})
   */
  private Cypher25Parser.MapContext findMapRecursive(final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.MapContext)
      return (Cypher25Parser.MapContext) node;

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.MapContext found = findMapRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Parse a map literal into a MapExpression.
   * Example: {name: 'Alice', age: 30}
   */
  private MapExpression parseMapLiteralExpression(final Cypher25Parser.MapContext ctx) {
    final Map<String, Expression> entries = new java.util.LinkedHashMap<>();

    final List<Cypher25Parser.PropertyKeyNameContext> keys = ctx.propertyKeyName();
    final List<Cypher25Parser.ExpressionContext> values = ctx.expression();

    for (int i = 0; i < keys.size() && i < values.size(); i++) {
      final String key = keys.get(i).getText();
      final Expression valueExpr = parseExpression(values.get(i));
      entries.put(key, valueExpr);
    }

    return new MapExpression(entries, ctx.getText());
  }

  // ============================================================================
  // List Comprehension Parsing
  // ============================================================================

  /**
   * Recursively find ListComprehensionContext in the parse tree.
   */
  private Cypher25Parser.ListComprehensionContext findListComprehensionRecursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.ListComprehensionContext)
      return (Cypher25Parser.ListComprehensionContext) node;

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ListComprehensionContext found = findListComprehensionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Parse a list comprehension into a ListComprehensionExpression.
   * Syntax: [variable IN listExpression WHERE filterExpression | mapExpression]
   * Examples: [x IN [1,2,3] | x * 2], [x IN list WHERE x > 5 | x.name]
   */
  private ListComprehensionExpression parseListComprehension(final Cypher25Parser.ListComprehensionContext ctx) {
    final String variable = ctx.variable().getText();

    // The main expression after IN - it's the first expression in the list
    final List<Cypher25Parser.ExpressionContext> expressions = ctx.expression();
    final Expression listExpression = parseExpression(expressions.get(0));

    // Optional WHERE clause
    Expression whereExpression = null;
    if (ctx.whereExp != null)
      whereExpression = parseExpression(ctx.whereExp);

    // Optional mapping expression after |
    Expression mapExpression = null;
    if (ctx.barExp != null)
      mapExpression = parseExpression(ctx.barExp);

    return new ListComprehensionExpression(variable, listExpression, whereExpression, mapExpression, ctx.getText());
  }

  // ============================================================================
  // Map Projection Parsing
  // ============================================================================

  /**
   * Recursively find MapProjectionContext in the parse tree.
   */
  private Cypher25Parser.MapProjectionContext findMapProjectionRecursive(
      final org.antlr.v4.runtime.tree.ParseTree node) {
    if (node == null)
      return null;

    if (node instanceof Cypher25Parser.MapProjectionContext)
      return (Cypher25Parser.MapProjectionContext) node;

    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.MapProjectionContext found = findMapProjectionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }

    return null;
  }

  /**
   * Parse a map projection into a MapProjectionExpression.
   * Syntax: variable{.property1, .property2, key: expression, .*}
   * Examples: n{.name, .age}, n{.*, totalAge: n.age * 2}
   */
  private MapProjectionExpression parseMapProjection(final Cypher25Parser.MapProjectionContext ctx) {
    final String variableName = ctx.variable().getText();
    final List<MapProjectionExpression.ProjectionElement> elements = new ArrayList<>();

    for (final Cypher25Parser.MapProjectionElementContext elemCtx : ctx.mapProjectionElement()) {
      if (elemCtx.propertyKeyName() != null && elemCtx.expression() != null) {
        // key: expression
        final String key = elemCtx.propertyKeyName().getText();
        final Expression expr = parseExpression(elemCtx.expression());
        elements.add(new MapProjectionExpression.ProjectionElement(key, expr));
      } else if (elemCtx.property() != null) {
        // .propertyName
        final String propName = elemCtx.property().propertyKeyName().getText();
        elements.add(new MapProjectionExpression.ProjectionElement(propName));
      } else if (elemCtx.variable() != null) {
        // variable (include another variable's value)
        final String varName = elemCtx.variable().getText();
        elements.add(new MapProjectionExpression.ProjectionElement(varName, new VariableExpression(varName)));
      } else if (elemCtx.DOT() != null && elemCtx.TIMES() != null) {
        // .* (all properties)
        elements.add(new MapProjectionExpression.ProjectionElement(true));
      }
    }

    return new MapProjectionExpression(variableName, elements, ctx.getText());
  }
}
