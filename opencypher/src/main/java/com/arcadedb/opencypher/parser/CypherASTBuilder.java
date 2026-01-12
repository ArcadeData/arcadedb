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
package com.arcadedb.opencypher.parser;

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.opencypher.ast.*;
import com.arcadedb.opencypher.grammar.Cypher25Parser;
import com.arcadedb.opencypher.grammar.Cypher25ParserBaseVisitor;
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
    // For now, support single singleQuery (no UNION support yet)
    if (ctx.singleQuery().size() > 1) {
      throw new CommandParsingException("UNION not yet supported");
    }
    return (CypherStatement) visit(ctx.singleQuery(0));
  }

  @Override
  public CypherStatement visitSingleQuery(final Cypher25Parser.SingleQueryContext ctx) {
    // Process all clauses in the query
    final List<MatchClause> matchClauses = new ArrayList<>();
    CreateClause createClause = null;
    SetClause setClause = null;
    DeleteClause deleteClause = null;
    MergeClause mergeClause = null;
    WhereClause whereClause = null;
    ReturnClause returnClause = null;
    OrderByClause orderByClause = null;
    Integer skip = null;
    Integer limit = null;

    for (final Cypher25Parser.ClauseContext clauseCtx : ctx.clause()) {
      if (clauseCtx.matchClause() != null) {
        final MatchClause match = visitMatchClause(clauseCtx.matchClause());
        matchClauses.add(match);

        // Extract WHERE clause from MATCH if present
        if (clauseCtx.matchClause().whereClause() != null) {
          whereClause = visitWhereClause(clauseCtx.matchClause().whereClause());
        }
      } else if (clauseCtx.createClause() != null) {
        createClause = visitCreateClause(clauseCtx.createClause());
      } else if (clauseCtx.setClause() != null) {
        setClause = visitSetClause(clauseCtx.setClause());
      } else if (clauseCtx.deleteClause() != null) {
        deleteClause = visitDeleteClause(clauseCtx.deleteClause());
      } else if (clauseCtx.mergeClause() != null) {
        mergeClause = visitMergeClause(clauseCtx.mergeClause());
      } else if (clauseCtx.returnClause() != null) {
        // RETURN clause with embedded ORDER BY, SKIP, LIMIT
        final Cypher25Parser.ReturnBodyContext body = clauseCtx.returnClause().returnBody();
        returnClause = visitReturnClause(clauseCtx.returnClause());

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
        hasCreate,
        hasMerge,
        hasDelete
    );
  }

  @Override
  public MatchClause visitMatchClause(final Cypher25Parser.MatchClauseContext ctx) {
    final List<PathPattern> pathPatterns = visitPatternList(ctx.patternList());
    final boolean optional = ctx.OPTIONAL() != null;

    // TODO: Handle WHERE clause embedded in MATCH
    // For now, WHERE is handled separately at statement level

    return new MatchClause(pathPatterns, optional);
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
        final String valueExpr = propCtx.expression().getText();

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
    return new MergeClause(pathPattern);
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
        final Expression expr = parseExpression(itemCtx.expression());
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
    // Extract the WHERE condition expression
    final String condition = ctx.expression().getText();
    return new WhereClause(condition);
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
    // For now, focus on simple patterns without path prefix

    return visitAnonymousPattern(ctx.anonymousPattern());
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
    // Simple expression evaluation
    final String text = ctx.getText();

    // String literal
    if (text.startsWith("'") && text.endsWith("'")) {
      return text; // Keep quotes for later processing
    }
    if (text.startsWith("\"") && text.endsWith("\"")) {
      return text; // Keep quotes for later processing
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

  /**
   * Parse an expression into an Expression AST node.
   * Handles variables, property access, function calls, and literals.
   */
  private Expression parseExpression(final Cypher25Parser.ExpressionContext ctx) {
    final String text = ctx.getText();

    // Check for count(*) special case - has its own grammar rule
    final Cypher25Parser.CountStarContext countStarCtx = findCountStarRecursive(ctx);
    if (countStarCtx != null) {
      // count(*) is treated as count(asterisk) where asterisk evaluates to a non-null marker
      final List<Expression> args = new ArrayList<>();
      args.add(new com.arcadedb.opencypher.ast.StarExpression());
      return new FunctionCallExpression("count", args, false);
    }

    // Recursively search for function invocations
    final Cypher25Parser.FunctionInvocationContext funcCtx = findFunctionInvocationRecursive(ctx);
    if (funcCtx != null) {
      return parseFunctionInvocation(funcCtx);
    }

    // Check for property access: variable.property
    if (text.contains(".") && !text.contains("(")) {
      final String[] parts = text.split("\\.", 2);
      return new PropertyAccessExpression(parts[0], parts[1]);
    }

    // Try to parse as literal
    final Object literalValue = tryParseLiteral(text);
    if (literalValue != null) {
      return new LiteralExpression(literalValue, text);
    }

    // Simple variable
    return new VariableExpression(text);
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
        arguments.add(new com.arcadedb.opencypher.ast.StarExpression());
      } else {
        arguments.add(parseExpression(argCtx.expression()));
      }
    }

    return new FunctionCallExpression(functionName, arguments, distinct);
  }
}
