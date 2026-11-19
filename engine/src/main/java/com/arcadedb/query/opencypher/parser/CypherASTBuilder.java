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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.CallClause;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.ConditionalStatement;
import com.arcadedb.query.opencypher.ast.CreateClause;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FinishClause;
import com.arcadedb.query.opencypher.ast.ForeachClause;
import com.arcadedb.query.opencypher.ast.LabelPredicate;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LoadCSVClause;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PathMode;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.RemoveClause;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SearchCallClause;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.opencypher.ast.SubqueryClause;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import com.arcadedb.query.opencypher.grammar.Cypher25ParserBaseVisitor;
import com.arcadedb.query.opencypher.rewriter.BooleanSimplifier;
import com.arcadedb.query.opencypher.rewriter.ComparisonNormalizer;
import com.arcadedb.query.opencypher.rewriter.CompositeRewriter;
import com.arcadedb.query.opencypher.rewriter.ConstantFolder;
import com.arcadedb.query.opencypher.rewriter.ExpressionRewriter;
import com.arcadedb.query.opencypher.rewriter.InlineNodeWhereHoister;
import com.arcadedb.query.opencypher.rewriter.LabelPredicateHoister;
import com.arcadedb.query.opencypher.rewriter.ProjectedOrderByNormalizer;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * ANTLR4 visitor that builds our internal AST from the Cypher parse tree.
 * Transforms ANTLR's parse tree into CypherStatement objects.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class CypherASTBuilder extends Cypher25ParserBaseVisitor<Object> {

  // Delegate expression parsing to a dedicated builder
  private final CypherExpressionBuilder expressionBuilder = new CypherExpressionBuilder();

  // The statements that are commands rather than queries: schema, users, transactions, session settings
  private final CypherCommandBuilder commands = new CypherCommandBuilder(expressionBuilder);

  // The boolean expression of a WHERE, descended through OR, XOR, AND, NOT and the comparisons
  private final BooleanExpressionBuilder booleans = new BooleanExpressionBuilder(this, expressionBuilder);

  // A parenthesized path pattern, spliced into the pattern around it or lowered to a variable-length relationship
  private final ParenthesizedPathLowering parenthesizedPaths = new ParenthesizedPathLowering(this, booleans);

  // The write clause (CREATE or MERGE) whose pattern is being visited, null in every other position. A label
  // expression beyond a plain conjunction says which labels a node MAY have, which only a read can answer.
  private String writeClause;

  // AST rewriter for query canonicalization (applied after parsing)
  private static final ExpressionRewriter AST_REWRITER = new CompositeRewriter(
      new ComparisonNormalizer(),  // Normalize comparisons for better optimizer matching
      new ConstantFolder(),        // Fold constant expressions at parse time
      new BooleanSimplifier()      // Simplify boolean expressions
  );

  @Override
  public CypherStatement visitStatement(final Cypher25Parser.StatementContext ctx) {
    if (ctx.queryWithLocalDefinitions() != null)
      return (CypherStatement) visit(ctx.queryWithLocalDefinitions());
    if (ctx.command() != null)
      return commands.handleCommand(ctx.command());
    throw new CommandParsingException("Unsupported statement type");
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

  /**
   * True when the conditional query IS the statement: grammar-wise a {@code when} sits in a regularQuery, inside a
   * nextStatement, inside a queryWithLocalDefinitions that is the statement's own rather than a subquery's body.
   */
  private static boolean isWholeStatement(final Cypher25Parser.WhenContext ctx) {
    ParserRuleContext node = ctx;
    for (final Class<?> expected : new Class<?>[] { Cypher25Parser.RegularQueryContext.class,
        Cypher25Parser.NextStatementContext.class, Cypher25Parser.QueryWithLocalDefinitionsContext.class }) {
      node = node.getParent();
      if (!expected.isInstance(node))
        return false;
    }
    return node.getParent() instanceof Cypher25Parser.StatementContext;
  }

  /**
   * Cypher 25 conditional query {@code WHEN cond THEN query [WHEN ...] [ELSE query]} (issue #9940). Without this visit the
   * default one returned the LAST branch, so the conditions were never read. The branch is chosen at execution time, when
   * the parameters the conditions read are known, by {@code OpenCypherQueryEngine}; nested in a subquery the condition could
   * read the outer row, which no subquery runner evaluates, so there it is refused.
   */
  @Override
  public CypherStatement visitWhen(final Cypher25Parser.WhenContext ctx) {
    if (!isWholeStatement(ctx))
      throw new FeatureNotImplementedException("a conditional query (WHEN ... THEN ... ELSE ...) is only "
          + "supported as a whole statement, not inside a subquery");

    final List<Expression> conditions = new ArrayList<>(ctx.whenBranch().size());
    final List<CypherStatement> branches = new ArrayList<>(ctx.whenBranch().size());
    for (final Cypher25Parser.WhenBranchContext branch : ctx.whenBranch()) {
      final Expression condition = expressionBuilder.parseExpression(branch.expression());
      final CypherReferencedVariables referenced = CypherReferencedVariables.of(condition);
      // Fail closed: a condition whose variables cannot all be listed (a subquery body, a pattern) would otherwise be
      // evaluated against a row that does not exist and quietly read as null
      if (!referenced.isComplete())
        throw new FeatureNotImplementedException("the condition of a conditional query must be an expression "
            + "over parameters; a subquery or a pattern is not supported there");
      if (!referenced.getNames().isEmpty())
        throw new CommandParsingException("UndefinedVariable: the condition of a conditional query can read parameters only, not '"
            + referenced.getNames().iterator().next() + "'");
      conditions.add(condition);
      branches.add((CypherStatement) visit(branch.singleQuery()));
    }
    final CypherStatement elseBranch = ctx.elseBranch() != null ? (CypherStatement) visit(ctx.elseBranch().singleQuery()) : null;
    return new ConditionalStatement(conditions, branches, elseBranch);
  }

  /**
   * GQL {@code LET x = expr [, y = expr]} (issue #9940): binds each variable next to the incoming ones, row by row, which is an
   * implicit {@code WITH *, expr AS x} per item. One WITH per item, in order, so an item can read the ones before it. An
   * aggregation would turn that WITH into a grouping, which a LET never is, so it is refused.
   */
  public List<WithClause> visitLetClause(final Cypher25Parser.LetClauseContext ctx) {
    final List<WithClause> clauses = new ArrayList<>(ctx.letItem().size());
    for (final Cypher25Parser.LetItemContext item : ctx.letItem()) {
      if (booleans.findPatternExpressionRecursive(item.expression()) != null)
        throw new CommandParsingException("UnexpectedSyntax: Pattern expressions are not allowed in LET");
      final Expression expression = expressionBuilder.parseExpression(item.expression());
      final String variable = stripBackticks(item.variable().getText());
      if (expression.containsAggregation())
        throw new CommandParsingException("InvalidAggregation: LET " + variable
            + " cannot aggregate, a LET binds one value per row; aggregate with WITH instead");

      final ReturnClause.ReturnItem letItem = new ReturnClause.ReturnItem(expression, variable);
      letItem.setExpressionText(item.expression().getText());
      final List<ReturnClause.ReturnItem> items = new ArrayList<>(2);
      items.add(ReturnClause.ReturnItem.star());
      items.add(letItem);
      clauses.add(WithClause.letBinding(items));
    }
    return clauses;
  }

  /**
   * The graph a {@code USE} clause names (issue #9940). Only a graph named by its name is resolved; the graph functions
   * ({@code graph.byName()}, {@code graph.byElementId()}) belong to composite databases and are refused. Whether the name is
   * the current database is checked by {@link Cypher25AntlrParser}, which knows the database.
   */
  static String useGraphName(final Cypher25Parser.UseClauseContext ctx) {
    Cypher25Parser.GraphReferenceContext reference = ctx.graphReference();
    while (reference.graphReference() != null)
      reference = reference.graphReference();
    if (reference.symbolicAliasName() == null)
      throw new FeatureNotImplementedException("USE " + getOriginalText(reference)
          + " is not supported; a query can only USE the current database, by its name");

    final StringBuilder name = new StringBuilder();
    for (final Cypher25Parser.SymbolicNameStringContext part : reference.symbolicAliasName().symbolicNameString()) {
      if (!name.isEmpty())
        name.append('.');
      name.append(stripBackticks(part.getText()));
    }
    return name.toString();
  }

  @Override
  public CypherStatement visitSingleQuery(final Cypher25Parser.SingleQueryContext ctx) {
    // `[USE graph] { query }`: the braces hold a whole query. Visiting only clause() turned it into an empty statement
    // that ran nothing and returned no row (issue #9940)
    if (ctx.queryWithLocalDefinitions() != null) {
      if (ctx.useClause() != null)
        useGraphName(ctx.useClause());
      return visitQueryWithLocalDefinitions(ctx.queryWithLocalDefinitions());
    }

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
    checkMatchMode(ctx.matchMode());
    final List<PathPattern> pathPatterns = visitPatternList(ctx.patternList());
    validateNoParameterProperties(pathPatterns, "MATCH");
    final boolean optional = ctx.OPTIONAL() != null;

    // Extract WHERE clause if present and scoped to this MATCH
    WhereClause whereClause = null;
    if (ctx.whereClause() != null) {
      whereClause = visitWhereClause(ctx.whereClause());
    }

    return newMatchClause(pathPatterns, optional, whereClause);
  }

  /**
   * Builds the Cypher 25 {@code SEARCH} subclause of a {@code MATCH} (issue #10776) as the {@link SearchCallClause} that runs
   * in front of it. Before this the clause was parsed and dropped: the {@code MATCH} answered every node of the label, and
   * a {@code SCORE AS} variable was undefined.
   * <p>
   * The binding variable must be a node variable of the {@code MATCH}'s own pattern, as in Neo4j. An {@code OPTIONAL MATCH}
   * with a {@code SEARCH} is refused rather than answered: the search runs in front of the {@code MATCH}, so an empty search
   * would drop the row an {@code OPTIONAL MATCH} keeps.
   *
   * @param ctx   the {@code SEARCH} subclause
   * @param match the {@code MATCH} it belongs to, as {@link #visitMatchClause} built it
   */
  public SearchCallClause buildSearchClause(final Cypher25Parser.SearchClauseContext ctx, final MatchClause match) {
    if (match.isOptional())
      throw new FeatureNotImplementedException("SEARCH is not supported in an OPTIONAL MATCH");

    final String variable = stripBackticks(ctx.variable().getText());
    if (!isNodeVariableOf(match, variable))
      throw new CommandSemanticException(
          "SEARCH: the binding variable '" + variable + "' must be a node variable of the pattern of its MATCH");

    final Cypher25Parser.SymbolicNameOrStringParameterContext indexCtx = ctx.indexSpecificationClause()
        .symbolicNameOrStringParameter();
    final Expression indexName = indexCtx.parameter() != null ?
        expressionBuilder.parseExpressionText(indexCtx.getText()) :
        new LiteralExpression(stripBackticks(indexCtx.getText()), indexCtx.getText());

    final Expression queryVector = expressionBuilder.parseExpression(ctx.forClause().expression());
    final WhereClause where = ctx.whereClause() != null ? visitWhereClause(ctx.whereClause()) : null;
    final Expression limit = visitLimit(ctx.limit());
    final String scoreVariable = ctx.scoreClause() != null ? stripBackticks(ctx.scoreClause().variable().getText()) : null;
    if (variable.equals(scoreVariable))
      throw new CommandSemanticException("SEARCH: the score variable '" + scoreVariable + "' is the binding variable");

    return new SearchCallClause(variable, indexName, queryVector, where, limit, scoreVariable);
  }

  /**
   * Whether {@code variable} names a node of one of the {@code MATCH}'s path patterns. A node inside a quantified group
   * is not one: there the variable is a group variable, a list, which a vector search cannot bind.
   */
  private static boolean isNodeVariableOf(final MatchClause match, final String variable) {
    for (final PathPattern path : match.getPathPatterns())
      for (final NodePattern node : path.getNodes())
        if (variable.equals(node.getVariable()))
          return true;
    return false;
  }

  /**
   * GQL match modes (issue #9940). {@code DIFFERENT RELATIONSHIPS} - in any of its spellings - is what every MATCH does
   * anyway, relationship isomorphism within the clause, so it is accepted. {@code REPEATABLE ELEMENTS} asks for
   * homomorphic matching, which no planner path implements: it is refused rather than answered isomorphically.
   *
   * @param ctx the match mode, or null when the clause has none
   */
  static void checkMatchMode(final Cypher25Parser.MatchModeContext ctx) {
    if (ctx != null && ctx.REPEATABLE() != null)
      throw new FeatureNotImplementedException("the " + spaced(ctx)
          + " match mode is not supported; relationships are always matched with DIFFERENT RELATIONSHIPS semantics");
  }

  /** The keywords of a parse subtree separated by single spaces, upper case, to name a construct in an error. */
  static String spaced(final ParseTree ctx) {
    if (ctx instanceof TerminalNode)
      return ctx.getText().toUpperCase(Locale.ROOT);
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ctx.getChildCount(); i++) {
      final String part = spaced(ctx.getChild(i));
      if (part.isEmpty())
        continue;
      if (!sb.isEmpty())
        sb.append(' ');
      sb.append(part);
    }
    return sb.toString();
  }

  /**
   * Builds a {@code MATCH} clause with the normalizations every one of them gets. Assembling a
   * {@link MatchClause} straight from its constructor instead skips them, which is only invisible while nothing
   * executes the result: the bare-pattern body of an {@code EXISTS { }} / {@code COUNT { }} did exactly that and, once
   * issue #5656 made that AST the thing that runs, a node inline {@code WHERE} inside it stopped being applied. One
   * factory, so a new normalization cannot reach one caller and miss the other.
   */
  static MatchClause newMatchClause(final List<PathPattern> pathPatterns, final boolean optional,
      final WhereClause whereClause) {
    // Normalize a node inline "(n WHERE ...)" predicate into the clause WHERE so that every planner
    // path enforces it, exactly like the equivalent "MATCH (n) WHERE ..." spelling (issue #5464)
    final MatchClause normalized = InlineNodeWhereHoister.hoist(new MatchClause(pathPatterns, optional, whereClause));

    // Move a "WHERE n:Label" conjunct into the node pattern so that the planner can resolve the node
    // with a label scan instead of iterating every vertex type in the database (issue #5363)
    return LabelPredicateHoister.hoist(normalized);
  }

  @Override
  public CreateClause visitCreateClause(final Cypher25Parser.CreateClauseContext ctx) {
    final String previous = writeClause;
    writeClause = "CREATE";
    final List<PathPattern> pathPatterns;
    try {
      pathPatterns = visitPatternList(ctx.patternList());
    } finally {
      writeClause = previous;
    }
    return new CreateClause(pathPatterns);
  }

  /**
   * Visits a GQL INSERT clause and produces a {@link CreateClause}.
   * INSERT is synonymous with CREATE per ISO/IEC 39075:2024 (issue #3365 section 1.1).
   * The grammar uses stricter sub-rules (no path length, single label per relationship,
   * map-only properties) which we honor by walking the dedicated insertPattern subtree
   * rather than reusing the generic CREATE pattern visitor.
   */
  public CreateClause visitInsertClause(final Cypher25Parser.InsertClauseContext ctx) {
    final List<PathPattern> pathPatterns = new ArrayList<>();
    for (final Cypher25Parser.InsertPatternContext patternCtx : ctx.insertPatternList().insertPattern())
      pathPatterns.add(visitInsertPattern(patternCtx));
    return new CreateClause(pathPatterns);
  }

  public PathPattern visitInsertPattern(final Cypher25Parser.InsertPatternContext ctx) {
    // Grammar: (symbolicNameString EQ)? insertNodePattern (insertRelationshipPattern insertNodePattern)*
    final String pathVariable;
    if (ctx.symbolicNameString() != null)
      pathVariable = stripBackticks(ctx.symbolicNameString().getText());
    else
      pathVariable = null;

    final List<NodePattern> nodes = new ArrayList<>();
    final List<RelationshipPattern> relationships = new ArrayList<>();

    final List<Cypher25Parser.InsertNodePatternContext> nodeCtxs = ctx.insertNodePattern();
    final List<Cypher25Parser.InsertRelationshipPatternContext> relCtxs = ctx.insertRelationshipPattern();

    for (int i = 0; i < nodeCtxs.size(); i++) {
      nodes.add(visitInsertNodePattern(nodeCtxs.get(i)));
      if (i < relCtxs.size())
        relationships.add(visitInsertRelationshipPattern(relCtxs.get(i)));
    }

    return new PathPattern(nodes, relationships, pathVariable);
  }

  public NodePattern visitInsertNodePattern(final Cypher25Parser.InsertNodePatternContext ctx) {
    // Grammar: LPAREN WHERE expression RPAREN
    //        | LPAREN variable? insertNodeLabelExpression? map? RPAREN
    if (ctx.WHERE() != null)
      throw new CommandParsingException("UnexpectedSyntax: WHERE is not allowed in INSERT node patterns");

    final String variable = ctx.variable() != null ? stripBackticks(ctx.variable().getText()) : null;

    List<String> labels = null;
    if (ctx.insertNodeLabelExpression() != null) {
      labels = new ArrayList<>();
      for (final Cypher25Parser.SymbolicNameStringContext nameCtx : ctx.insertNodeLabelExpression().symbolicNameString())
        labels.add(stripBackticks(nameCtx.getText()));
    }

    final Map<String, Object> properties = ctx.map() != null ? visitMap(ctx.map()) : null;

    return new NodePattern(variable, labels, null, properties, null, false);
  }

  public RelationshipPattern visitInsertRelationshipPattern(final Cypher25Parser.InsertRelationshipPatternContext ctx) {
    // Grammar: leftArrow? arrowLine ( LBRACKET WHERE expression RBRACKET | LBRACKET variable? insertRelationshipLabelExpression map? RBRACKET ) arrowLine rightArrow?
    if (ctx.WHERE() != null)
      throw new CommandParsingException("UnexpectedSyntax: WHERE is not allowed in INSERT relationship patterns");

    final String variable = ctx.variable() != null ? stripBackticks(ctx.variable().getText()) : null;

    List<String> types = null;
    if (ctx.insertRelationshipLabelExpression() != null)
      types = List.of(stripBackticks(ctx.insertRelationshipLabelExpression().symbolicNameString().getText()));

    final Map<String, Object> properties = ctx.map() != null ? visitMap(ctx.map()) : null;

    final Direction direction;
    if (ctx.leftArrow() != null && ctx.rightArrow() != null)
      direction = Direction.BOTH;
    else if (ctx.leftArrow() != null)
      direction = Direction.IN;
    else if (ctx.rightArrow() != null)
      direction = Direction.OUT;
    else
      direction = Direction.BOTH;

    return new RelationshipPattern(variable, types, direction, properties, null, null, null, null);
  }

  @Override
  public SetClause visitSetClause(final Cypher25Parser.SetClauseContext ctx) {
    final List<SetClause.SetItem> items = new ArrayList<>();

    for (final Cypher25Parser.SetItemContext itemCtx : ctx.setItem()) {
      if (itemCtx instanceof Cypher25Parser.SetPropContext propCtx) {
        // Pattern expressions (e.g., (n)-[:REL]->()) are not allowed in SET values
        if (booleans.findPatternExpressionRecursive(propCtx.expression()) != null)
          throw new CommandParsingException("UnexpectedSyntax: Pattern expressions are not allowed in SET values");
        final Cypher25Parser.PropertyExpressionContext propExprCtx = propCtx.propertyExpression();
        final Expression valueExpr = expressionBuilder.parseExpression(propCtx.expression());
        // Build the property name from all property accessors (supports chained access like a.b.c)
        final StringBuilder propertyName = new StringBuilder();
        for (final Cypher25Parser.PropertyContext prop : propExprCtx.property())
          if (propertyName.length() > 0)
            propertyName.append('.').append(stripBackticks(prop.propertyKeyName().getText()));
          else
            propertyName.append(stripBackticks(prop.propertyKeyName().getText()));
        // Check if the base expression is a simple variable or a complex expression (e.g., CASE)
        final Cypher25Parser.Expression1Context baseExpr1 = propExprCtx.expression1();
        if (baseExpr1.variable() != null) {
          // Simple variable: SET n.prop = value
          items.add(new SetClause.SetItem(baseExpr1.variable().getText(), propertyName.toString(), valueExpr));
        } else if (baseExpr1.parenthesizedExpression() != null) {
          // Check if the parenthesized expression contains just a variable
          final String innerText = baseExpr1.parenthesizedExpression().expression().getText().trim();
          // Try to parse as a complex expression (e.g., CASE WHEN ... THEN t END)
          final Expression targetExpr = expressionBuilder.parseExpression(baseExpr1.parenthesizedExpression().expression());
          if (targetExpr instanceof VariableExpression) {
            items.add(new SetClause.SetItem(((VariableExpression) targetExpr).getVariableName(), propertyName.toString(), valueExpr));
          } else {
            items.add(new SetClause.SetItem(targetExpr, propertyName.toString(), valueExpr));
          }
        } else {
          // Other expression types as base - parse as expression
          final Expression targetExpr = expressionBuilder.parseExpressionFromText(baseExpr1);
          items.add(new SetClause.SetItem(targetExpr, propertyName.toString(), valueExpr));
        }
      } else if (itemCtx instanceof Cypher25Parser.SetDynamicPropContext dynCtx) {
        // SET n[keyExpr] = value — dynamic property assignment (issue #5141)
        if (booleans.findPatternExpressionRecursive(dynCtx.expression()) != null)
          throw new CommandParsingException("UnexpectedSyntax: Pattern expressions are not allowed in SET values");
        final Cypher25Parser.DynamicPropertyExpressionContext dynPropExpr = dynCtx.dynamicPropertyExpression();
        final Expression keyExpr = expressionBuilder.parseExpression(dynPropExpr.dynamicProperty().expression());
        final Expression valueExpr = expressionBuilder.parseExpression(dynCtx.expression());
        final Cypher25Parser.Expression1Context baseExpr1 = dynPropExpr.expression1();
        if (baseExpr1.variable() != null) {
          items.add(new SetClause.SetItem(baseExpr1.variable().getText(), null, keyExpr, valueExpr));
        } else {
          final Expression targetExpr = expressionBuilder.parseExpressionFromText(baseExpr1);
          items.add(new SetClause.SetItem(null, targetExpr, keyExpr, valueExpr));
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
        // SET n:Label / SET n:$(expr) — add labels
        final List<String> labelList = new ArrayList<>();
        final List<Expression> labelExpressions = new ArrayList<>();
        collectNodeLabels(labelsCtx.nodeLabels(), labelList, labelExpressions);
        items.add(new SetClause.SetItem(stripBackticks(labelsCtx.variable().getText()), labelList, labelExpressions));
      } else if (itemCtx instanceof Cypher25Parser.SetLabelsIsContext labelsIsCtx) {
        // SET n IS Label / SET n IS $(expr) — add labels (IS syntax)
        final List<String> labelList = new ArrayList<>();
        final List<Expression> labelExpressions = new ArrayList<>();
        collectNodeLabelsIs(labelsIsCtx.nodeLabelsIs(), labelList, labelExpressions);
        items.add(new SetClause.SetItem(stripBackticks(labelsIsCtx.variable().getText()), labelList, labelExpressions));
      }
    }

    return new SetClause(items);
  }

  @Override
  public DeleteClause visitDeleteClause(final Cypher25Parser.DeleteClauseContext ctx) {
    final boolean detach = ctx.DETACH() != null;
    final List<String> variables = new ArrayList<>(ctx.expression().size());
    final List<Expression> expressions = new ArrayList<>(ctx.expression().size());
    for (final Cypher25Parser.ExpressionContext exprCtx : ctx.expression()) {
      variables.add(exprCtx.getText());
      expressions.add(expressionBuilder.parseExpression(exprCtx));
    }

    return new DeleteClause(variables, expressions, detach);
  }

  /**
   * Splits a {@code nodeLabels} subtree - the label list of a {@code SET n:...} or {@code REMOVE n:...} item - into
   * the statically written label names and the Cypher 25 {@code $(expression)} labels that have to be resolved per
   * row.
   * <p>
   * Both clauses used to reduce this whole subtree to {@code getText()} and split the result on {@code [:&|]},
   * which turned the source text of a dynamic label into a label of its own: {@code SET n:$(node.labels)} created a
   * vertex type literally named {@code $(node.labels)} and attached it to the node, corrupting schema and data with
   * no error at write time (issue #7059). {@code REMOVE} had the mirror-image defect - it read {@code labelType()}
   * alone, so a dynamic label was dropped from the AST and the clause silently did nothing.
   * <p>
   * Static and dynamic labels are collected into two lists rather than one interleaved sequence because labels are
   * a set: the composite type name is built from the sorted union either way.
   */
  private void collectNodeLabels(final Cypher25Parser.NodeLabelsContext ctx, final List<String> labels,
      final List<Expression> labelExpressions) {
    for (final Cypher25Parser.LabelTypeContext labelType : ctx.labelType())
      labels.add(stripBackticks(labelType.symbolicNameString().getText()));
    for (final Cypher25Parser.DynamicLabelTypeContext dynamicLabel : ctx.dynamicLabelType())
      labelExpressions.add(expressionBuilder.parseExpression(dynamicLabel.dynamicExpression().expression()));
  }

  /**
   * The {@code IS} spelling of {@link #collectNodeLabels}: {@code SET n IS Label:Other} / {@code SET n IS $(expr)}.
   * The grammar puts the first label outside the repetition ({@code IS (symbolicNameString | dynamicExpression)
   * (labelType | dynamicLabelType)*}), so it is read separately.
   */
  private void collectNodeLabelsIs(final Cypher25Parser.NodeLabelsIsContext ctx, final List<String> labels,
      final List<Expression> labelExpressions) {
    if (ctx.symbolicNameString() != null)
      labels.add(stripBackticks(ctx.symbolicNameString().getText()));
    if (ctx.dynamicExpression() != null)
      labelExpressions.add(expressionBuilder.parseExpression(ctx.dynamicExpression().expression()));
    for (final Cypher25Parser.LabelTypeContext labelType : ctx.labelType())
      labels.add(stripBackticks(labelType.symbolicNameString().getText()));
    for (final Cypher25Parser.DynamicLabelTypeContext dynamicLabel : ctx.dynamicLabelType())
      labelExpressions.add(expressionBuilder.parseExpression(dynamicLabel.dynamicExpression().expression()));
  }

  public RemoveClause visitRemoveClause(final Cypher25Parser.RemoveClauseContext ctx) {
    final List<RemoveClause.RemoveItem> items = new ArrayList<>();

    for (final Cypher25Parser.RemoveItemContext itemCtx : ctx.removeItem()) {
      if (itemCtx instanceof Cypher25Parser.RemovePropContext propCtx) {
        // REMOVE n.property / REMOVE (expr).property. The base is read from the parse tree, as SET reads it: splitting
        // the source text on its first '.' named a variable "(head(ns))" for REMOVE (head(ns)).s, which no row binds, so
        // the clause silently did nothing (issue #11754)
        final Cypher25Parser.PropertyExpressionContext propExprCtx = propCtx.propertyExpression();
        final StringBuilder propertyName = new StringBuilder();
        for (final Cypher25Parser.PropertyContext prop : propExprCtx.property()) {
          if (!propertyName.isEmpty())
            propertyName.append('.');
          propertyName.append(stripBackticks(prop.propertyKeyName().getText()));
        }
        final Cypher25Parser.Expression1Context baseExpr1 = propExprCtx.expression1();
        final Expression target = removeTarget(baseExpr1);
        if (target == null)
          items.add(new RemoveClause.RemoveItem(stripBackticks(baseExpr1.variable().getText()), propertyName.toString()));
        else if (target instanceof VariableExpression variable)
          items.add(new RemoveClause.RemoveItem(variable.getVariableName(), propertyName.toString()));
        else
          items.add(new RemoveClause.RemoveItem(null, target, propertyName.toString(), null));
      } else if (itemCtx instanceof Cypher25Parser.RemoveDynamicPropContext dynCtx) {
        // REMOVE n[keyExpr] — dynamic property removal (issue #5141), or REMOVE (expr)[keyExpr] (issue #11754)
        final Cypher25Parser.DynamicPropertyExpressionContext dynPropExpr = dynCtx.dynamicPropertyExpression();
        final Cypher25Parser.Expression1Context baseExpr1 = dynPropExpr.expression1();
        final Expression keyExpr = expressionBuilder.parseExpression(dynPropExpr.dynamicProperty().expression());
        final Expression target = removeTarget(baseExpr1);
        if (target == null)
          items.add(new RemoveClause.RemoveItem(stripBackticks(baseExpr1.variable().getText()), keyExpr));
        else if (target instanceof VariableExpression variable)
          items.add(new RemoveClause.RemoveItem(variable.getVariableName(), keyExpr));
        else
          items.add(new RemoveClause.RemoveItem(null, target, null, keyExpr));
      } else if (itemCtx instanceof Cypher25Parser.RemoveLabelsContext labelsCtx) {
        // REMOVE n:Label / REMOVE n:$(expr)
        final List<String> labels = new ArrayList<>();
        final List<Expression> labelExpressions = new ArrayList<>();
        collectNodeLabels(labelsCtx.nodeLabels(), labels, labelExpressions);
        items.add(new RemoveClause.RemoveItem(stripBackticks(labelsCtx.variable().getText()), labels, labelExpressions));
      } else if (itemCtx instanceof Cypher25Parser.RemoveLabelsIsContext labelsIsCtx) {
        // REMOVE n IS Label / REMOVE n IS $(expr) — the IS spelling was previously dropped on the floor, so the
        // whole clause silently did nothing.
        final List<String> labels = new ArrayList<>();
        final List<Expression> labelExpressions = new ArrayList<>();
        collectNodeLabelsIs(labelsIsCtx.nodeLabelsIs(), labels, labelExpressions);
        items.add(
            new RemoveClause.RemoveItem(stripBackticks(labelsIsCtx.variable().getText()), labels, labelExpressions));
      }
    }

    return new RemoveClause(items);
  }

  /**
   * The target of a REMOVE property item, read the way {@link #visitSetClause} reads the target of a SET property item:
   * null for a plain variable ({@code REMOVE n.p}), otherwise the expression - a {@link VariableExpression} when a
   * parenthesised one is only a variable ({@code REMOVE (n).p}).
   */
  private Expression removeTarget(final Cypher25Parser.Expression1Context baseExpr1) {
    if (baseExpr1.variable() != null)
      return null;
    if (baseExpr1.parenthesizedExpression() != null)
      return expressionBuilder.parseExpression(baseExpr1.parenthesizedExpression().expression());
    return expressionBuilder.parseExpressionFromText(baseExpr1);
  }

  @Override
  public MergeClause visitMergeClause(final Cypher25Parser.MergeClauseContext ctx) {
    final String previous = writeClause;
    writeClause = "MERGE";
    final PathPattern pathPattern;
    try {
      pathPattern = visitPattern(ctx.pattern());
    } finally {
      writeClause = previous;
    }
    validateNoParameterProperties(List.of(pathPattern), "MERGE");

    // Parse ON CREATE SET and ON MATCH SET actions: each repetition stays a SET of its own, applied in order
    final List<SetClause> onCreateActions = new ArrayList<>(1);
    final List<SetClause> onMatchActions = new ArrayList<>(1);

    for (final Cypher25Parser.MergeActionContext actionCtx : ctx.mergeAction()) {
      final SetClause setClause = visitSetClause(actionCtx.setClause());

      if (actionCtx.CREATE() != null)
        onCreateActions.add(setClause);
      else if (actionCtx.MATCH() != null)
        onMatchActions.add(setClause);
    }

    return new MergeClause(pathPattern, onCreateActions, onMatchActions);
  }

  @Override
  public CallClause visitCallClause(final Cypher25Parser.CallClauseContext ctx) {
    // Grammar: OPTIONAL? CALL procedureName (LPAREN (procedureArgument (COMMA procedureArgument)*)? RPAREN)?
    //          (YIELD (TIMES | procedureResultItem (COMMA procedureResultItem)* whereClause?))?
    final boolean optional = ctx.OPTIONAL() != null;

    // Parse procedure name: namespace symbolicNameString
    final Cypher25Parser.ProcedureNameContext nameCtx = ctx.procedureName();
    final String procedureName = ParserUtils.qualifiedName(nameCtx.namespace(), nameCtx.symbolicNameString());

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

    return new CallClause(procedureName, arguments, yieldItems, yieldWhere, optional);
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

    // Parse IN TRANSACTIONS parameters
    boolean inTransactions = false;
    Expression batchSize = null;
    SubqueryClause.OnError onError = SubqueryClause.OnError.FAIL;
    String reportStatusVariable = null;
    final Cypher25Parser.SubqueryInTransactionsParametersContext txParams = ctx.subqueryInTransactionsParameters();
    if (txParams != null) {
      inTransactions = true;
      if (txParams.subqueryInTransactionsBatchParameters() != null && !txParams.subqueryInTransactionsBatchParameters().isEmpty()) {
        final Cypher25Parser.SubqueryInTransactionsBatchParametersContext batchCtx =
            txParams.subqueryInTransactionsBatchParameters(0);
        if (batchCtx.expression() != null)
          batchSize = expressionBuilder.parseExpression(batchCtx.expression());
      }
      // ON ERROR CONTINUE | BREAK | FAIL (issue #11595)
      if (txParams.subqueryInTransactionsErrorParameters() != null && !txParams.subqueryInTransactionsErrorParameters().isEmpty()) {
        final Cypher25Parser.SubqueryInTransactionsErrorParametersContext errorCtx =
            txParams.subqueryInTransactionsErrorParameters(0);
        // ON ERROR RETRY runs a batch that failed with a transient error again before its THEN mode applies. The retries are not
        // implemented, and the THEN mode alone would skip under THEN CONTINUE, at the first transient error, a batch the statement
        // asked to be retried: refused rather than answered with what it did not ask for
        if (errorCtx.RETRY() != null)
          throw new FeatureNotImplementedException(
              "ON ERROR RETRY in CALL { ... } IN TRANSACTIONS is not supported: use ON ERROR CONTINUE, BREAK or FAIL");
        if (errorCtx.CONTINUE() != null)
          onError = SubqueryClause.OnError.CONTINUE;
        else if (errorCtx.BREAK() != null)
          onError = SubqueryClause.OnError.BREAK;
      }
      if (txParams.subqueryInTransactionsReportParameters() != null && !txParams.subqueryInTransactionsReportParameters().isEmpty()) {
        reportStatusVariable = stripBackticks(txParams.subqueryInTransactionsReportParameters(0).variable().getText());
        if (onError == SubqueryClause.OnError.FAIL)
          throw new CommandParsingException(
              "REPORT STATUS can only be used when specifying ON ERROR CONTINUE or ON ERROR BREAK");
      }
    }

    // A nested IN TRANSACTIONS fails as ON ERROR FAIL does, rolling back every level, the caller's transaction included: an outer
    // CONTINUE or BREAK could not take that back, and would carry on without the caller's writes
    if (onError != SubqueryClause.OnError.FAIL && innerStatement.hasSubqueryInTransactions())
      throw new CommandParsingException(
          "A CALL { ... } IN TRANSACTIONS with ON ERROR CONTINUE or BREAK cannot contain another CALL { ... } IN TRANSACTIONS");

    return new SubqueryClause(innerStatement, scopeVariables, optional, inTransactions, batchSize, onError, reportStatusVariable);
  }

  @Override
  public WithClause visitWithClause(final Cypher25Parser.WithClauseContext ctx) {
    // Grammar: WITH returnBody whereClause?
    final Cypher25Parser.ReturnBodyContext body = ctx.returnBody();

    // Parse return items
    final List<ReturnClause.ReturnItem> items = new ArrayList<>();
    if (body.returnItems().TIMES() != null)
      // WITH * — pass all variables through
      items.add(ReturnClause.ReturnItem.star());
    // Always process explicit return items (handles both "WITH expr" and "WITH *, expr AS alias")
    for (final Cypher25Parser.ReturnItemContext itemCtx : body.returnItems().returnItem()) {
      // Pattern expressions (e.g., (n)-[]->()) are not allowed in WITH projections
      if (booleans.findPatternExpressionRecursive(itemCtx.expression()) != null)
        throw new CommandParsingException("UnexpectedSyntax: Pattern expressions are not allowed in WITH projections");
      final Expression expr = expressionBuilder.parseExpression(itemCtx.expression());
      final String alias = itemCtx.variable() != null ? stripBackticks(itemCtx.variable().getText()) : null;
      final ReturnClause.ReturnItem item = new ReturnClause.ReturnItem(expr, alias);
      if (alias == null)
        item.setOriginalText(getOriginalText(itemCtx.expression()));
      // Canonical text, recorded for aliased items too, so ORDER BY can tell whether it repeats
      // this projected expression (issue #5283)
      item.setExpressionText(itemCtx.expression().getText());
      items.add(item);
    }

    // Parse DISTINCT flag
    final boolean distinct = body.DISTINCT() != null;

    boolean aggregating = false;
    for (final ReturnClause.ReturnItem item : items)
      if (!item.isStar() && item.getExpression().containsAggregation()) {
        aggregating = true;
        break;
      }

    // Parse WHERE clause (applied after projection). A WHERE that repeats a grouping key reads the projected column
    // (issue #12245), as ORDER BY does below
    // The rewrite runs on the predicate as written, before the canonicalization that may reorder it (1 < n.id becomes
    // n.id > 1), so it still matches a projected expression written the same way
    WhereClause whereClause = null;
    if (ctx.whereClause() != null) {
      final BooleanExpression parsed = ProjectedOrderByNormalizer.normalizeWhere(booleans.parseBooleanExpression(ctx.whereClause().expression()),
          items, distinct || aggregating);
      whereClause = new WhereClause((BooleanExpression) AST_REWRITER.rewrite(parsed));
    }

    // Parse ORDER BY, SKIP, LIMIT from returnBody
    OrderByClause orderByClause = null;
    if (body.orderBy() != null)
      // An ORDER BY item that repeats a projected expression sorts on the projected column
      // (#5283 for DISTINCT, #5286 for aggregation)
      orderByClause = ProjectedOrderByNormalizer.normalize(visitOrderBy(body.orderBy()), items, distinct || aggregating);

    Expression skip = null;
    if (body.skip() != null) {
      skip = visitSkip(body.skip());
    }

    Expression limit = null;
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

  /**
   * Visits a GQL {@code FOR variable IN expression} iteration clause and produces an
   * {@link UnwindClause}. FOR is synonymous with UNWIND per ISO/IEC 39075:2024 optional
   * feature GQ10 (issue #3365 section 1.2).
   */
  public UnwindClause visitForUnwindClause(final Cypher25Parser.ForUnwindClauseContext ctx) {
    // Grammar: FOR variable IN expression
    final String variable = stripBackticks(ctx.variable().getText());
    final Expression listExpression = expressionBuilder.parseExpression(ctx.expression());
    return new UnwindClause(listExpression, variable);
  }

  /**
   * Visits a GQL FINISH clause (issue #3365 section 1.3). The clause is a marker that
   * suppresses any rows the planner would otherwise emit; mutual exclusion with RETURN is
   * enforced in {@link StatementBuilder}.
   */
  @Override
  public FinishClause visitFinishClause(final Cypher25Parser.FinishClauseContext ctx) {
    return FinishClause.INSTANCE;
  }

  public LoadCSVClause visitLoadCSVClause(final Cypher25Parser.LoadCSVClauseContext ctx) {
    // Grammar: LOAD CSV (WITH HEADERS)? FROM expression AS variable (FIELDTERMINATOR stringLiteral)?
    final boolean withHeaders = ctx.HEADERS() != null;
    final Expression urlExpression = expressionBuilder.parseExpression(ctx.expression());
    final String variable = stripBackticks(ctx.variable().getText());
    String fieldTerminator = ",";
    if (ctx.FIELDTERMINATOR() != null) {
      final String raw = ctx.stringLiteral().getText();
      fieldTerminator = decodeStringLiteral(raw.substring(1, raw.length() - 1));
    }
    return new LoadCSVClause(urlExpression, variable, withHeaders, fieldTerminator);
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

    if (body.returnItems().TIMES() != null)
      // RETURN * - the variables in scope, followed by the explicit items of "RETURN *, expr AS alias" (issue #11556)
      items.add(ReturnClause.ReturnItem.star());
    for (final Cypher25Parser.ReturnItemContext itemCtx : body.returnItems().returnItem()) {
      // Bare pattern expressions (e.g., (n)-[]->()) are not allowed in RETURN projections.
      // A pattern nested inside a function-style exists(pattern) predicate is allowed: it evaluates
      // to a boolean, not a projected path (issue #4993).
      final Cypher25Parser.PatternExpressionContext patternInReturn = booleans.findPatternExpressionRecursive(itemCtx.expression());
      if (patternInReturn != null && !booleans.isArgumentOfExistsFunction(patternInReturn))
        throw new CommandParsingException("UnexpectedSyntax: Pattern expressions are not allowed in RETURN projections");
      final Expression expr = expressionBuilder.parseExpression(itemCtx.expression());
      final String alias = itemCtx.variable() != null ? stripBackticks(itemCtx.variable().getText()) : null;
      final ReturnClause.ReturnItem item = new ReturnClause.ReturnItem(expr, alias);
      // When no alias, preserve original text from the query (whitespace and case)
      if (alias == null)
        item.setOriginalText(getOriginalText(itemCtx.expression()));
      // Canonical text, recorded for aliased items too, so ORDER BY can tell whether it repeats
      // this projected expression (issue #5283)
      item.setExpressionText(itemCtx.expression().getText());
      items.add(item);
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
  public Expression visitSkip(final Cypher25Parser.SkipContext ctx) {
    return expressionBuilder.parseExpression(ctx.expression());
  }

  @Override
  public Expression visitLimit(final Cypher25Parser.LimitContext ctx) {
    return expressionBuilder.parseExpression(ctx.expression());
  }

  public WhereClause visitWhereClause(final Cypher25Parser.WhereClauseContext ctx) {
    // Parse the WHERE condition as a boolean expression, check its operands, then simplify boolean constants
    final BooleanExpression parsed = booleans.parseBooleanExpression(ctx.expression());
    BooleanOperandValidator.validateConditionOperands(parsed);
    final BooleanExpression condition = (BooleanExpression) AST_REWRITER.rewrite(parsed);
    return new WhereClause(condition);
  }

  /**
   * Visits a GQL standalone {@code FILTER (WHERE)? expression} clause (issue #6574, ISO/IEC
   * 39075:2024 GQL). Semantically equivalent to a leading WHERE: parses the predicate as a
   * boolean expression, simplifying boolean constants the same way {@link #visitWhereClause}
   * does.
   */
  public WhereClause visitFilterClause(final Cypher25Parser.FilterClauseContext ctx) {
    // Grammar: FILTER WHERE? expression
    final BooleanExpression parsed = booleans.parseBooleanExpression(ctx.expression());
    BooleanOperandValidator.validateConditionOperands(parsed);
    final BooleanExpression condition = (BooleanExpression) AST_REWRITER.rewrite(parsed);
    return new WhereClause(condition);
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
    if (ctx.variable() != null)
      pathVariable = stripBackticks(ctx.variable().getText());

    // Extract path mode (WALK, TRAIL, ACYCLIC) from pathPatternPrefix if present.
    // The grammar has labeled alternatives (AllPath, AnyShortestPath, etc.) that each may contain pathMode().
    // Use getRuleContext on the base class to find PathModeContext regardless of which alternative matched.
    PathMode pathMode = null;
    if (ctx.pathPatternPrefix() != null) {
      // Only ALL [mode] [PATHS] - every matching path, which is what a pattern returns anyway - is executed. A
      // selective prefix (ANY SHORTEST, ALL SHORTEST, SHORTEST k [GROUPS], ANY [k]) is refused: dropping it, as the
      // parser used to, returned every path instead of the ones it selects (issue #9940).
      if (!(ctx.pathPatternPrefix() instanceof Cypher25Parser.AllPathContext))
        throw new FeatureNotImplementedException("the " + spaced(ctx.pathPatternPrefix())
            + " path search prefix is not supported; use shortestPath() or allShortestPaths() to select shortest paths");
      final Cypher25Parser.PathModeContext pm = ctx.pathPatternPrefix().getRuleContext(
          Cypher25Parser.PathModeContext.class, 0);
      if (pm != null) {
        if (pm.WALK() != null) pathMode = PathMode.WALK;
        else if (pm.TRAIL() != null) pathMode = PathMode.TRAIL;
        else if (pm.ACYCLIC() != null) pathMode = PathMode.ACYCLIC;
      }
    }

    // Visit the anonymous pattern to get the base path
    final PathPattern basePath = visitAnonymousPattern(ctx.anonymousPattern());

    // If there's a path variable or path mode, create a new PathPattern with them
    if (pathVariable != null || pathMode != null) {
      if (basePath instanceof ShortestPathPattern) {
        final ShortestPathPattern shortestBase = (ShortestPathPattern) basePath;
        return new ShortestPathPattern(basePath.getNodes(), basePath.getRelationships(),
            pathVariable != null ? pathVariable : basePath.getPathVariable(), shortestBase.isAllPaths());
      }
      return new PathPattern(basePath.getNodes(), basePath.getRelationships(),
          pathVariable != null ? pathVariable : basePath.getPathVariable(), pathMode);
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

    // Walk children in order so that each optional quantifier can be paired with
    // the relationship pattern it follows (grammar: relationshipPattern quantifier? nodePattern).
    final int childCount = ctx.getChildCount();
    for (int i = 0; i < childCount; i++) {
      final ParseTree child = ctx.getChild(i);
      if (child instanceof Cypher25Parser.NodePatternContext) {
        final NodePattern node = visitNodePattern((Cypher25Parser.NodePatternContext) child);
        // A node pattern written right after an unquantified parenthesized path is that path's last node (GQL
        // juxtaposition): the two are one node, not two consecutive ones
        if (i > 0 && ParenthesizedPathLowering.isUnquantifiedParenthesizedPath(ctx.getChild(i - 1)))
          nodes.set(nodes.size() - 1, ParenthesizedPathLowering.mergeJuxtaposedNodes(nodes.getLast(), node));
        else
          nodes.add(node);
      } else if (child instanceof Cypher25Parser.RelationshipPatternContext) {
        relationships.add(visitRelationshipPattern((Cypher25Parser.RelationshipPatternContext) child));
      } else if (child instanceof Cypher25Parser.QuantifierContext) {
        if (relationships.isEmpty())
          throw new CommandParsingException("InvalidSyntax: Quantifier has no preceding relationship");
        final int last = relationships.size() - 1;
        relationships.set(last, parenthesizedPaths.applyQuantifier(relationships.get(last), (Cypher25Parser.QuantifierContext) child));
      } else if (ParenthesizedPathLowering.isUnquantifiedParenthesizedPath(child)) {
        parenthesizedPaths.spliceParenthesizedPath((Cypher25Parser.ParenthesizedPathContext) child, nodes, relationships);
      } else if (child instanceof Cypher25Parser.ParenthesizedPathContext) {
        // GQL Quantified Path Pattern (issue #3365 sections 1.4 and 1.5).
        // Phase A: lower a single-relationship grouped pattern to a variable-length
        // relationship that the existing executor already handles.
        final boolean nextIsOuterNode = (i + 1) < childCount
            && ctx.getChild(i + 1) instanceof Cypher25Parser.NodePatternContext;
        parenthesizedPaths.absorbParenthesizedPath((Cypher25Parser.ParenthesizedPathContext) child, nodes, relationships, nextIsOuterNode);
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
    List<Expression> dynamicLabels = null;
    Map<String, Object> properties = null;
    String propertiesParameterName = null;
    boolean labelDisjunction = false;

    // Variable
    if (ctx.variable() != null) {
      variable = stripBackticks(ctx.variable().getText());
    }

    // A negation, the wildcard or a mix of '&' and '|': the pattern keeps the labels every match must carry, so the
    // planner can still pick a label scan, and the whole expression is checked as a predicate on the node (#8992).
    BooleanExpression labelCheck = null;
    final LabelPredicate labelPredicate = ParserUtils.buildLabelPredicate(ctx.labelExpression(), expressionBuilder::parseExpression);
    if (labelPredicate != null) {
      rejectLabelExpressionInWriteClause(ctx.labelExpression());
      labels = labelPredicate.requiredLabels();
      if (variable == null)
        variable = ParserUtils.newLabelExpressionVariable();
      labelCheck = ParserUtils.labelCheckOn(variable, labelPredicate, ctx.labelExpression());
    } else if (ctx.labelExpression() != null) {
      // Label expression (static labels + Cypher 25 dynamic $(expression) labels)
      labels = extractLabels(ctx.labelExpression());
      labelDisjunction = ParserUtils.isLabelDisjunction(ctx.labelExpression());
      dynamicLabels = ParserUtils.parseDynamicLabels(ctx.labelExpression(), expressionBuilder::parseExpression);
      // A read pattern also checks the dynamic labels as a predicate on the node, which every consumer evaluates; the steps
      // that read getDynamicLabels() (the anchor scan, the variable-length target) keep using them to narrow what they
      // walk. A write clause resolves them itself (issue #11776)
      if (dynamicLabels != null && writeClause == null) {
        if (variable == null)
          variable = ParserUtils.newLabelExpressionVariable();
        labelCheck = ParserUtils.dynamicLabelCheckOn(variable, labels, dynamicLabels, ctx.labelExpression());
        // (n:A|$(expr)): the static labels alone are not a filter, so they join the dynamic list. Every consumer that
        // reads the static list (hop target, pattern comprehension) would otherwise reject what $(expr) admits
        if (labelDisjunction && labels != null && !labels.isEmpty()) {
          dynamicLabels = ParserUtils.staticLabelsFirst(labels, dynamicLabels);
          labels = null;
        }
      }
    }

    // Properties
    if (ctx.properties() != null) {
      final Cypher25Parser.PropertiesContext propsCtx = ctx.properties();
      if (propsCtx.parameter() != null)
        propertiesParameterName = stripBackticks(propsCtx.parameter().parameterName().getText());
      else
        properties = visitProperties(propsCtx);
    }

    // Inline WHERE predicate (e.g., (n:Person WHERE n.age > 18)). Before issue #5464 this was parsed
    // by the grammar but silently discarded, so the predicate never filtered anything.
    BooleanExpression whereExpression = null;
    if (ctx.expression() != null)
      whereExpression = booleans.parseBooleanExpression(ctx.expression());
    whereExpression = ParserUtils.andLabelCheck(labelCheck, whereExpression);

    return new NodePattern(variable, labels, dynamicLabels, properties, propertiesParameterName, labelDisjunction,
        whereExpression);
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

    // Label expression (relationship types). A negation, the wildcard or a mix of '&' and '|' is checked as a
    // predicate on the relationship, keeping as its type filter only a type every match must have (#8992). On a
    // variable-length relationship the predicate rides the inline WHERE, which every expansion evaluates per hop, so
    // each relationship of the path is checked (#9117).
    BooleanExpression labelCheck = null;
    final LabelPredicate labelPredicate = ParserUtils.buildRelationshipTypePredicate(ctx.labelExpression(), expressionBuilder::parseExpression);
    if (labelPredicate != null) {
      rejectLabelExpressionInWriteClause(ctx.labelExpression());
      final List<String> required = labelPredicate.requiredLabels();
      types = required.isEmpty() ? null : required;
      if (variable == null)
        variable = ParserUtils.newLabelExpressionVariable();
      labelCheck = ParserUtils.labelCheckOn(variable, labelPredicate, ctx.labelExpression());
    } else if (ctx.labelExpression() != null) {
      types = extractLabels(ctx.labelExpression());
      // [r:$(expr)] in a read pattern: no static type filter can say it, so the whole type list is checked per relationship
      // through its inline predicate, which every hop kind evaluates. A write clause keeps refusing it (issue #11776)
      final List<Expression> dynamicTypes = writeClause == null ?
          ParserUtils.parseDynamicLabels(ctx.labelExpression(), expressionBuilder::parseExpression) : null;
      if (dynamicTypes != null) {
        if (variable == null)
          variable = ParserUtils.newLabelExpressionVariable();
        labelCheck = ParserUtils.dynamicLabelCheckOn(variable, types, dynamicTypes, ctx.labelExpression());
        types = null;
      }
    }

    // Properties
    String propertiesParameterName = null;
    if (ctx.properties() != null) {
      final Cypher25Parser.PropertiesContext propsCtx = ctx.properties();
      if (propsCtx.parameter() != null)
        propertiesParameterName = stripBackticks(propsCtx.parameter().parameterName().getText());
      else
        properties = visitProperties(propsCtx);
    }

    // Path length (variable-length relationships), read through the one place that knows what [*] means.
    if (ctx.pathLength() != null) {
      final ParserUtils.PathLength pathLength = ParserUtils.parsePathLength(ctx.pathLength());
      minHops = pathLength.minHops();
      maxHops = pathLength.maxHops();
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

    // Inline WHERE predicate (e.g., [r:KNOWS WHERE r.since < 2019])
    BooleanExpression whereExpression = null;
    if (ctx.expression() != null)
      whereExpression = booleans.parseBooleanExpression(ctx.expression());
    whereExpression = ParserUtils.andLabelCheck(labelCheck, whereExpression);

    return new RelationshipPattern(variable, types, direction, properties, propertiesParameterName, minHops, maxHops,
        whereExpression);
  }

  private void rejectLabelExpressionInWriteClause(final Cypher25Parser.LabelExpressionContext ctx) {
    if (writeClause != null)
      throw new CommandParsingException("UnexpectedSyntax: the label expression '" + ctx.getText() + "' is not allowed in "
          + writeClause + ", only in MATCH and in expressions: it says which labels an element MAY have, which is not "
          + "something a write can act on");
  }

  public Map<String, Object> visitProperties(final Cypher25Parser.PropertiesContext ctx) {
    if (ctx.map() != null) {
      return visitMap(ctx.map());
    }
    // Parameter used as node/relationship predicate (e.g., MATCH (n $param)) is invalid
    throw new CommandParsingException("InvalidParameterUse: Parameters cannot be used as predicates in patterns");
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
        // Evaluate list literals immediately, but only if all elements are simple literals
        if (isStaticListExpression((ListExpression) expr))
          value = expr.evaluate(null, null);
        else
          value = expr; // Keep as Expression for runtime evaluation (e.g., [date({...})])
      } else {
        // Keep dynamic expressions as Expression objects for runtime evaluation
        value = expr;
      }

      map.put(key, value);
    }

    return map;
  }

  private static boolean isStaticListExpression(final ListExpression listExpr) {
    for (final Expression element : listExpr.getElements()) {
      if (!(element instanceof LiteralExpression))
        return false;
    }
    return true;
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
   *
   * @return the name without backticks
   */
  static String stripBackticks(final String name) {
    return ParserUtils.stripBackticks(name);
  }

  /**
   * Gets the original text from the input stream for a parse tree context,
   * preserving whitespace and case from the query.
   */
  static String getOriginalText(final ParserRuleContext ctx) {
    if (ctx.getStart() == null || ctx.getStop() == null)
      return ctx.getText();
    return ctx.getStart().getInputStream().getText(
        Interval.of(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex()));
  }

  private Object evaluateExpression(final Cypher25Parser.ExpressionContext ctx) {
    // Check for list literals first
    final Cypher25Parser.ListLiteralContext listCtx = ParseTreeSearch.findListLiteralRecursive(ctx);
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
    if ("null".equalsIgnoreCase(text)) {
      return null;
    }

    // Boolean
    if ("true".equalsIgnoreCase(text)) {
      return Boolean.TRUE;
    }
    if ("false".equalsIgnoreCase(text)) {
      return Boolean.FALSE;
    }

    // Parameter reference: $paramName (unquoted, starts with $)
    // This is different from a string literal like '$50' which was already handled above
    if (text.startsWith("$") && text.length() > 1) {
      final String paramName = stripBackticks(text.substring(1));
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
   * Validates that no bare parameter properties are used in patterns.
   * Per the OpenCypher specification, bare parameter properties (e.g., $props) are only valid
   * in CREATE patterns, not in MATCH or MERGE.
   */
  private static void validateNoParameterProperties(final List<PathPattern> patterns, final String clause) {
    for (final PathPattern path : patterns) {
      for (int i = 0; i < path.getNodeCount(); i++) {
        final NodePattern node = path.getNode(i);
        if (node.getPropertiesParameterName() != null)
          throw new CommandParsingException("InvalidParameterUse: Parameters cannot be used as predicates in " + clause + " patterns");
      }
      for (int i = 0; i < path.getRelationshipCount(); i++) {
        final RelationshipPattern rel = path.getRelationship(i);
        if (rel.getPropertiesParameterName() != null)
          throw new CommandParsingException("InvalidParameterUse: Parameters cannot be used as predicates in " + clause + " patterns");
      }
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
   *
   * @return the decoded string
   */
  static String decodeStringLiteral(final String input) {
    return ParserUtils.decodeStringLiteral(input);
  }

  static boolean isBooleanOrNullKeyword(final String text) {
    return "true".equalsIgnoreCase(text) || "false".equalsIgnoreCase(text) || "null".equalsIgnoreCase(text);
  }
}
