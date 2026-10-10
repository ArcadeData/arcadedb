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
import com.arcadedb.query.literal.LiteralParameterizer;
import com.arcadedb.query.literal.LiteralScan;
import com.arcadedb.query.opencypher.ast.CypherAdminStatement;
import com.arcadedb.query.opencypher.ast.CypherDDLStatement;
import com.arcadedb.query.opencypher.ast.CypherSessionStatement;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.CypherTransactionStatement;
import com.arcadedb.query.opencypher.grammar.Cypher25Lexer;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser.ParsedQuery;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * Extracts the literals of an OpenCypher query into generated parameters before the statement and plan cache lookups, so
 * {@code MATCH (p:Person) WHERE p.id = 12345} and {@code ... p.id = 67890} share one parsed statement and one plan (issue #8307).
 * <p>
 * A number or a non-empty string written as an expression becomes a parameter, except where a parameter would not mean what
 * the literal meant:
 * <ul>
 *   <li>in a RETURN or WITH item without an alias: the column is named after the text of the expression;</li>
 *   <li>in ORDER BY: an item is matched to a projected expression by its text, and a kept projection keeps its literal;</li>
 *   <li>in SKIP, LIMIT and the batch size of CALL ... IN TRANSACTIONS: the parser rejects a negative or fractional literal,
 *       and the planner sizes a top-k on the value;</li>
 *   <li>in a procedure argument: a procedure may classify itself as reading or writing from its literal arguments
 *       ({@code apoc.do.when}), which the cached statement would then answer for every value;</li>
 *   <li>in {@code round()}: the rounding mode is validated by value when the query is parsed;</li>
 *   <li>inside an inline pattern map, unless it is the whole value of a property: the parser evaluates a list of literals there
 *       once, at parse time;</li>
 *   <li>anywhere in a schema, administration, session or transaction statement.</li>
 * </ul>
 * Booleans, null and the empty string are never extracted: they carry no cardinality worth a shared entry, and the planner reads
 * them specially. The whole value of an inline pattern property becomes an {@link Integer} when it fits, as the parser makes it.
 * Positions the grammar does not spell as an expression literal (variable-length bounds, quantifiers, SHORTEST k, names) are
 * never touched.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherLiteralParameterizer extends LiteralParameterizer<ParsedQuery> {
  private static final BaseErrorListener FAILING_LISTENER = new BaseErrorListener() {
    @Override
    public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol, final int line,
        final int charPositionInLine, final String msg, final RecognitionException e) {
      throw LexerError.INSTANCE;
    }
  };

  private final Cypher25AntlrParser parser;

  public CypherLiteralParameterizer(final Cypher25AntlrParser parser, final int size) {
    super(size);
    this.parser = parser;
  }

  @Override
  protected LiteralScan scan(final String text) {
    final Cypher25Lexer lexer = new Cypher25Lexer(CharStreams.fromString(text));
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
          scan.add(type, token.getStartIndex(), token.getStopIndex() + 1, previousType == Cypher25Lexer.MINUS ? previousStart : -1);
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
      case Cypher25Lexer.UNSIGNED_DECIMAL_INTEGER, Cypher25Lexer.DECIMAL_DOUBLE, Cypher25Lexer.UNSIGNED_HEX_INTEGER,
           Cypher25Lexer.UNSIGNED_OCTAL_INTEGER -> true;
      // an empty string stays: STARTS WITH '' and friends are read specially
      case Cypher25Lexer.STRING_LITERAL1, Cypher25Lexer.STRING_LITERAL2 -> token.getStopIndex() - token.getStartIndex() > 1;
      default -> false;
    };
  }

  @Override
  protected ParsedQuery parse(final String text) {
    return parser.parseQueryWithTree(text).parsed();
  }

  @Override
  protected Classified<ParsedQuery> parseAndClassify(final String text, final LiteralScan scan) {
    final Cypher25AntlrParser.ParsedTree parsed = parser.parseQueryWithTree(text);
    final byte[] policy = new byte[scan.size()];
    if (isParameterizable(parsed.parsed().statement()))
      classify(parsed.tree(), scan, policy, false, false);
    return new Classified<>(parsed.parsed(), policy);
  }

  private static boolean isParameterizable(final CypherStatement statement) {
    return !(statement instanceof CypherDDLStatement) && !(statement instanceof CypherAdminStatement)
        && !(statement instanceof CypherSessionStatement) && !(statement instanceof CypherTransactionStatement);
  }

  private static void classify(final ParseTree node, final LiteralScan scan, final byte[] policy, final boolean keep,
      final boolean inPatternProperties) {
    if (node instanceof TerminalNode terminal) {
      if (!keep) {
        final int index = scan.indexOfStart(terminal.getSymbol().getStartIndex());
        if (index >= 0)
          policy[index] = decide(terminal, scan, index, inPatternProperties);
      }
      return;
    }

    final boolean keepChildren = keep || keepsLiterals(node);
    final boolean childrenInPatternProperties = inPatternProperties || isPatternPropertyMap(node);
    for (int i = 0; i < node.getChildCount(); i++)
      classify(node.getChild(i), scan, policy, keepChildren, childrenInPatternProperties);
  }

  private static boolean keepsLiterals(final ParseTree node) {
    if (node instanceof Cypher25Parser.ReturnItemContext item)
      return item.variable() == null;
    if (node instanceof Cypher25Parser.FunctionInvocationContext function)
      return "round".equalsIgnoreCase(function.functionName().symbolicNameString().getText());
    return node instanceof Cypher25Parser.OrderItemContext || node instanceof Cypher25Parser.SkipContext
        || node instanceof Cypher25Parser.LimitContext || node instanceof Cypher25Parser.ProcedureArgumentContext
        || node instanceof Cypher25Parser.SubqueryInTransactionsParametersContext;
  }

  private static byte decide(final TerminalNode terminal, final LiteralScan scan, final int index,
      final boolean inPatternProperties) {
    final ParseTree literal;
    byte action = STRIP;
    if (terminal.getParent() instanceof Cypher25Parser.NumberLiteralContext number) {
      if (!(number.getParent() instanceof Cypher25Parser.NumericLiteralContext))
        return 0;
      if (number.MINUS() != null) {
        // the sign is part of the literal (only -9223372036854775808 can be written that way): it moves into the value
        if (scan.getMinusStart(index) != number.MINUS().getSymbol().getStartIndex())
          return 0;
        action |= WITH_MINUS;
      }
      literal = number.getParent();
    } else if (terminal.getParent() instanceof Cypher25Parser.StringLiteralContext string) {
      if (!(string.getParent() instanceof Cypher25Parser.StringsLiteralContext))
        return 0;
      literal = string.getParent();
    } else
      return 0;

    if (!(literal.getParent() instanceof Cypher25Parser.Expression1Context expression))
      return 0;

    if (inPatternProperties) {
      if (!isWholePatternPropertyValue(expression))
        return 0;
      action |= LANGUAGE;
    }
    return action;
  }

  /**
   * True for the inline property map of a node or relationship pattern, including the patterns of INSERT, which the parser
   * reads the same way ({@code CypherASTBuilder.visitMap}).
   */
  private static boolean isPatternPropertyMap(final ParseTree node) {
    return node instanceof Cypher25Parser.PropertiesContext || (node instanceof Cypher25Parser.MapContext
        && (node.getParent() instanceof Cypher25Parser.InsertNodePatternContext
        || node.getParent() instanceof Cypher25Parser.InsertRelationshipPatternContext));
  }

  /** True when the expression is the entire value of a property of an inline node or relationship pattern map. */
  private static boolean isWholePatternPropertyValue(final ParserRuleContext expression) {
    ParserRuleContext current = expression;
    while (current.getParent() != null && !(current instanceof Cypher25Parser.ExpressionContext)) {
      if (current.getParent().getChildCount() != 1)
        return false;
      current = current.getParent();
    }
    return current instanceof Cypher25Parser.ExpressionContext && current.getParent() instanceof Cypher25Parser.MapContext map
        && (map.getParent() instanceof Cypher25Parser.PropertiesContext || isPatternPropertyMap(map));
  }

  @Override
  protected Object literalValue(final String text, final int start, final int end, final boolean withMinus,
      final boolean patternProperty) {
    final String token = text.substring(start, end);
    final Object value = CypherExpressionBuilder.tryParseLiteral(withMinus ? "-" + token : token);
    if (value == null)
      throw new CommandParsingException("Not a literal: " + token);
    // the parser stores an inline pattern property that fits an int as an Integer (CypherASTBuilder.visitMap)
    if (patternProperty && value instanceof Long l && l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE)
      return l.intValue();
    return value;
  }

  @Override
  protected char kind(final Object value) {
    if (value instanceof Long)
      return 'i';
    if (value instanceof Integer)
      return 'n';
    if (value instanceof Double)
      return 'd';
    if (value instanceof String)
      return 's';
    return 'o';
  }

  @Override
  protected void appendParameter(final StringBuilder key, final String name) {
    key.append('$').append(name);
  }

  /** Thrown by the scanning lexer on the first error; preallocated, it carries no stack trace. */
  private static final class LexerError extends RuntimeException {
    private static final LexerError INSTANCE = new LexerError();

    private LexerError() {
      super(null, null, false, false);
    }
  }
}
