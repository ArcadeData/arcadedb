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

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.literal.LiteralParameterizer;
import com.arcadedb.query.literal.LiteralScan;
import com.arcadedb.query.sql.grammar.SQLLexer;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.CreateEdgeStatement;
import com.arcadedb.query.sql.parser.CreateVertexStatement;
import com.arcadedb.query.sql.parser.DeleteStatement;
import com.arcadedb.query.sql.parser.InsertStatement;
import com.arcadedb.query.sql.parser.MatchStatement;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.TraverseStatement;
import com.arcadedb.query.sql.parser.UpdateStatement;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.math.BigDecimal;

/**
 * Extracts the literals of a SQL statement into generated {@code :name} parameters before the statement and plan cache lookups,
 * so {@code SELECT FROM Person WHERE id = 12345} and {@code ... id = 67890} share one parsed statement and one plan (issue
 * #8307).
 * <p>
 * Only queries and record writes are parameterized: SELECT, MATCH, TRAVERSE, INSERT, UPDATE, DELETE, CREATE VERTEX and CREATE
 * EDGE. In them a number or a non-empty string written as an expression becomes a parameter, except where a parameter would not
 * mean what the literal meant:
 * <ul>
 *   <li>in a projection without an alias: the column is named after the text of the expression;</li>
 *   <li>in GROUP BY and ORDER BY: their items are matched against the projections by their text;</li>
 *   <li>in SKIP, LIMIT, TIMEOUT and BATCH: the planner folds or sizes on the value, and ignores a TIMEOUT parameter;</li>
 *   <li>in the FROM target: a parameter there may name a type;</li>
 *   <li>in a MATCH filter other than {@code where:} and {@code while:}, and in a MATCH traversal method
 *       ({@code .out('Friend')}): the planner reads them while it builds the pattern;</li>
 *   <li>in a condition that refers to no record ({@code 1 = 1}): the planner folds it to always true or always false;</li>
 *   <li>a string followed by a method call ({@code 'a'.toUpperCase()}).</li>
 * </ul>
 * Booleans, null and the empty string are never extracted. A value keeps the Java type the parser gives the literal, and the
 * generated name records its class, so an integer, a decimal and a string never share a plan. A bound that the key type of an
 * index cannot hold exactly is handled at run time like any other parameter (issue #8970).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLLiteralParameterizer extends LiteralParameterizer<Statement> {
  private static final BaseErrorListener FAILING_LISTENER = new BaseErrorListener() {
    @Override
    public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol, final int line,
        final int charPositionInLine, final String msg, final RecognitionException e) {
      throw LexerError.INSTANCE;
    }
  };

  private final SQLAntlrParser parser;

  public SQLLiteralParameterizer(final SQLAntlrParser parser, final int size) {
    super(size);
    this.parser = parser;
  }

  @Override
  protected LiteralScan scan(final String text) {
    final SQLLexer lexer = new SQLLexer(CharStreams.fromString(text));
    lexer.removeErrorListeners();
    lexer.addErrorListener(FAILING_LISTENER);

    final LiteralScan scan = new LiteralScan(text);
    int previousType = Token.INVALID_TYPE;
    int previousStart = -1;
    try {
      for (Token token = lexer.nextToken(); token.getType() != Token.EOF; token = lexer.nextToken()) {
        if (token.getChannel() != Token.DEFAULT_CHANNEL)
          continue;
        final int type = token.getType();
        if (isCandidate(token))
          scan.add(type, token.getStartIndex(), token.getStopIndex() + 1, previousType == SQLLexer.MINUS ? previousStart : -1);
        previousType = type;
        previousStart = token.getStartIndex();
      }
    } catch (final LexerError e) {
      // the parser reports the error with its own message
      return null;
    }
    return scan;
  }

  private static boolean isCandidate(final Token token) {
    return switch (token.getType()) {
      case SQLLexer.INTEGER_LITERAL, SQLLexer.FLOATING_POINT_LITERAL -> true;
      // an empty string stays: it carries no value worth a shared entry
      case SQLLexer.STRING_LITERAL -> token.getStopIndex() - token.getStartIndex() > 1;
      default -> false;
    };
  }

  /**
   * Parses a statement, raising a {@link CommandSQLParsingException} for any failure.
   */
  @Override
  public Statement parse(final String text) {
    return parseWithTree(text).statement();
  }

  @Override
  protected Classified<Statement> parseAndClassify(final String text, final LiteralScan scan) {
    final SQLAntlrParser.ParsedTree parsed = parseWithTree(text);
    final byte[] policy = new byte[scan.size()];
    if (isParameterizable(parsed.statement()))
      classify(parsed.tree(), scan, policy, false, false);
    return new Classified<>(parsed.statement(), policy);
  }

  private SQLAntlrParser.ParsedTree parseWithTree(final String text) {
    try {
      return parser.parseWithTree(text);
    } catch (final CommandSQLParsingException e) {
      throw e;
    } catch (final StackOverflowError e) {
      // the Error carries no message (#9050): say what happened
      throw new CommandSQLParsingException(
          "The statement is nested or chained too deeply to be parsed (the thread stack overflowed)", e, text);
    } catch (final Throwable e) {
      throw new CommandSQLParsingException(e.getMessage(), e, text);
    }
  }

  private static boolean isParameterizable(final Statement statement) {
    return statement instanceof SelectStatement || statement instanceof MatchStatement || statement instanceof TraverseStatement
        || statement instanceof InsertStatement || statement instanceof UpdateStatement || statement instanceof DeleteStatement
        || statement instanceof CreateVertexStatement || statement instanceof CreateEdgeStatement;
  }

  /**
   * @param keep   every literal below is kept
   * @param inFrom below a FROM target, where literals are kept up to a nested statement, whose own clauses decide for its
   *               literals
   */
  private static void classify(final ParseTree node, final LiteralScan scan, final byte[] policy, final boolean keep,
      final boolean inFrom) {
    if (node instanceof TerminalNode terminal) {
      if (!keep && !inFrom) {
        final int index = scan.indexOfStart(terminal.getSymbol().getStartIndex());
        if (index >= 0)
          policy[index] = decide(terminal, scan, index);
      }
      return;
    }

    final boolean keepChildren = keep || keepsLiterals(node);
    final boolean childrenInFrom = !isStatement(node) && (inFrom || node instanceof SQLParser.FromClauseContext);
    final boolean sizedByBatch = node instanceof SQLParser.UpdateStatementContext || node instanceof SQLParser.DeleteStatementContext;
    for (int i = 0; i < node.getChildCount(); i++) {
      final ParseTree child = node.getChild(i);
      // BATCH <expression> is the only expression written directly under UPDATE and DELETE
      classify(child, scan, policy, keepChildren || (sizedByBatch && child instanceof SQLParser.ExpressionContext), childrenInFrom);
    }
  }

  private static boolean isStatement(final ParseTree node) {
    return node instanceof SQLParser.SelectStatementContext || node instanceof SQLParser.MatchStatementContext
        || node instanceof SQLParser.TraverseStatementContext;
  }

  private static boolean keepsLiterals(final ParseTree node) {
    if (node instanceof SQLParser.ProjectionItemContext item)
      return item.identifier() == null;
    if (node instanceof SQLParser.MatchReturnItemContext item)
      return item.identifier() == null;
    if (node instanceof SQLParser.MatchFilterItemContext item)
      return item.matchFilterItemKey() == null || (item.matchFilterItemKey().WHERE() == null
          && item.matchFilterItemKey().WHILE() == null);
    if (node instanceof SQLParser.ConditionBlockContext condition)
      return !refersToData(condition);
    return node instanceof SQLParser.GroupByContext || node instanceof SQLParser.OrderByContext
        || node instanceof SQLParser.SkipContext || node instanceof SQLParser.LimitContext
        || node instanceof SQLParser.TimeoutContext || node instanceof SQLParser.TraverseProjectionItemContext
        || node instanceof SQLParser.MatchMethodCallContext;
  }

  /**
   * Whether the subtree reads anything besides literals: a field, a parameter, a variable or a subquery. A function call is
   * not one by itself - the planner folds {@code abs(-1) = 999} too - and one that reads the record implicitly only keeps
   * literals that could have been extracted, which is always safe.
   */
  private static boolean refersToData(final ParseTree node) {
    if (node instanceof SQLParser.IdentifierChainContext || node instanceof SQLParser.InputParamContext
        || node instanceof SQLParser.ThisLiteralContext || node instanceof SQLParser.ParenthesizedStmtContext)
      return true;
    for (int i = 0; i < node.getChildCount(); i++)
      if (refersToData(node.getChild(i)))
        return true;
    return false;
  }

  private static byte decide(final TerminalNode terminal, final LiteralScan scan, final int index) {
    final ParseTree literal = terminal.getParent();
    if (literal instanceof SQLParser.StringLiteralContext string) {
      // a method call on the literal is applied to the literal, not to a parameter it could be swapped for
      return string.STRING_LITERAL() != null && string.modifier().isEmpty() ? STRIP : 0;
    }
    if (!(literal instanceof SQLParser.IntegerLiteralContext) && !(literal instanceof SQLParser.FloatLiteralContext))
      return 0;

    // a minus applied directly to a number is folded into it by the parser (SQLASTBuilder.visitUnary): it moves into the value
    if (literal.getParent() instanceof SQLParser.BaseContext base && base.getParent() instanceof SQLParser.UnaryContext unary
        && unary.MINUS() != null) {
      if (scan.getMinusStart(index) != unary.MINUS().getSymbol().getStartIndex())
        return 0;
      return STRIP | WITH_MINUS;
    }
    return STRIP;
  }

  @Override
  protected Object literalValue(final String text, final int start, final int end, final boolean withMinus,
      final boolean languageFlag) {
    if (text.charAt(start) == '\'' || text.charAt(start) == '"')
      // as BaseExpression evaluates a string literal: without its quotes, un-escaped
      return BaseExpression.decode(text.substring(start + 1, end - 1));

    final String token = text.substring(start, end);
    final Number magnitude;
    if (isFloatingPoint(token))
      magnitude = SQLASTBuilder.parseFloatingPointLiteral(token);
    else {
      try {
        magnitude = SQLASTBuilder.parseIntegerLiteral(token);
      } catch (final NumberFormatException e) {
        if (!withMinus)
          throw e;
        // -9223372036854775808: only the folded literal fits a long (SQLASTBuilder.tryFoldLongMinValueLiteral)
        final String digits = token.endsWith("L") || token.endsWith("l") ? token.substring(0, token.length() - 1) : token;
        return Long.parseLong("-" + digits);
      }
    }
    if (!withMinus)
      return magnitude;
    final Number negated = SQLASTBuilder.negateLiteral(magnitude);
    if (negated == null)
      throw new CommandSQLParsingException("Cannot negate " + token);
    return negated;
  }

  /** Whether an unsigned number token is a FLOATING_POINT_LITERAL rather than an INTEGER_LITERAL. */
  private static boolean isFloatingPoint(final String token) {
    final boolean hex = token.length() > 2 && token.charAt(0) == '0' && (token.charAt(1) == 'x' || token.charAt(1) == 'X');
    for (int i = 0; i < token.length(); i++) {
      final char c = token.charAt(i);
      if (c == '.' || (hex ? c == 'p' || c == 'P' : c == 'e' || c == 'E' || c == 'f' || c == 'F' || c == 'd' || c == 'D'))
        return true;
    }
    return false;
  }

  @Override
  protected char kind(final Object value) {
    if (value instanceof Integer || value instanceof Long)
      return 'i';
    if (value instanceof Double)
      return 'd';
    if (value instanceof Float)
      return 'f';
    if (value instanceof BigDecimal)
      return 'x';
    if (value instanceof String)
      return 's';
    return 'o';
  }

  @Override
  protected void appendParameter(final StringBuilder key, final String name) {
    // `{a:5}` must not become `{a::name}`
    if (!key.isEmpty() && key.charAt(key.length() - 1) == ':')
      key.append(' ');
    key.append(':').append(name);
  }

  /** Thrown by the scanning lexer on the first error; preallocated, it carries no stack trace. */
  private static final class LexerError extends RuntimeException {
    private static final LexerError INSTANCE = new LexerError();

    private LexerError() {
      super(null, null, false, false);
    }
  }
}
