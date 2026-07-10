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
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.grammar.Cypher25Lexer;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Cypher parser using ANTLR4 with the official Cypher 2.5 grammar.
 * This replaces the regex-based parser with a proper grammar-based implementation.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class Cypher25AntlrParser {
  /**
   * Parses a Cypher query string into a CypherStatement AST.
   *
   * @param query the Cypher query string
   * @return parsed CypherStatement
   * @throws CommandParsingException if query cannot be parsed
   */
  public CypherStatement parse(final String query) {
    if (query == null || query.trim().isEmpty())
      throw new CommandParsingException("Query cannot be empty");

    try {
      final Cypher25Lexer lexer = new Cypher25Lexer(CharStreams.fromString(query));
      final CommonTokenStream tokens = new CommonTokenStream(lexer);

      // Reject deprecated/legacy Cypher syntax with an actionable hint before the generic ANTLR parse,
      // which would otherwise produce a cryptic "Unexpected input" error (issue #4141). Reuses this same
      // token stream, so no second lex. The catch below re-throws CommandParsingException as-is.
      checkDeprecatedSyntax(query, tokens);

      final Cypher25Parser parser = new Cypher25Parser(tokens);

      // Custom error handling
      parser.removeErrorListeners();
      parser.addErrorListener(new CypherErrorListener());

      // Parse the statement
      final Cypher25Parser.StatementContext statementContext = parser.statement();

      // Ensure all input was consumed (no trailing tokens)
      final Token nextToken = parser.getTokenStream().LT(1);
      if (nextToken.getType() != Token.EOF) {
        // Build helpful error message with line number and context
        final int line = nextToken.getLine();
        final int column = nextToken.getCharPositionInLine();
        final String[] lines = query.split("\n");
        final String errorLine = line > 0 && line <= lines.length ? lines[line - 1] : "";

        final StringBuilder errorMsg = new StringBuilder();
        errorMsg.append(String.format("Unexpected input '%s' at line %d, column %d\n",
            nextToken.getText(), line, column));

        if (!errorLine.isEmpty()) {
          errorMsg.append("  ").append(errorLine.trim()).append("\n");
          errorMsg.append("  ").append(" ".repeat(Math.max(0, column))).append("^");
        }

        throw new CommandParsingException(errorMsg.toString());
      }

      // Build AST using visitor
      final CypherASTBuilder astBuilder = new CypherASTBuilder();
      final CypherStatement statement = astBuilder.visitStatement(statementContext);

      CypherSemanticValidator.validate(statement);

      return statement;

    } catch (final CommandParsingException e) {
      // semantic-validation and explicit parse errors already carry a clear, actionable message
      // (e.g. AmbiguousAggregationExpression, UndefinedVariable, "Unexpected input ...") - surface it
      throw e;
    } catch (final Exception e) {
      throw new CommandParsingException("Failed to parse Cypher query: " + query, e);
    }
  }

  // A legacy named parameter name (the required leading letter/underscore also excludes purely numeric
  // braces, so quantified-path-pattern quantifiers '{n}'/'{n,m}' are never mistaken for one).
  private static final Pattern PARAM_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  // Operator/punctuation tokens after which a '{...}' begins a value, never a map projection (which must
  // attach to a variable). Restricting the legacy-'{name}' check to these positions guarantees we never
  // flag a real map projection - including 'type{name}', where the projected-on variable is a keyword
  // token (NAME, TYPE, ... are legal variable names) and so is indistinguishable from a clause keyword.
  // The cost is only a false negative (a legacy param after a keyword like IN/RETURN yields the generic
  // parser error instead of the hint), which is harmless. RBRACKET ('a[i]{p}') and COLON ('{k: {p}}')
  // are included as value positions even though those shapes are otherwise invalid anyway.
  private static final Set<Integer> VALUE_PREFIX_TOKENS = Set.of(
      Cypher25Lexer.EQ, Cypher25Lexer.NEQ, Cypher25Lexer.INVALID_NEQ, Cypher25Lexer.LT, Cypher25Lexer.GT,
      Cypher25Lexer.LE, Cypher25Lexer.GE, Cypher25Lexer.PLUS, Cypher25Lexer.MINUS, Cypher25Lexer.TIMES,
      Cypher25Lexer.DIVIDE, Cypher25Lexer.POW, Cypher25Lexer.PERCENT, Cypher25Lexer.PLUSEQUAL, Cypher25Lexer.DOUBLEBAR,
      Cypher25Lexer.COMMA, Cypher25Lexer.LPAREN, Cypher25Lexer.LBRACKET, Cypher25Lexer.RBRACKET, Cypher25Lexer.COLON);

  /**
   * Rejects deprecated syntax removed under ISO/IEC 39075 (GQL) / Cypher 25 with a hint at the supported
   * replacement: {@code PERIODIC COMMIT} (use {@code CALL { ... } IN TRANSACTIONS}) and legacy
   * {@code {param}} (use {@code $param}). Scans {@code tokens} on the default channel, so occurrences
   * inside string literals or comments are never matched.
   */
  private static void checkDeprecatedSyntax(final String query, final CommonTokenStream tokens) {
    // Allocation-free pre-filter: neither deprecated form is possible without a '{' or the word PERIODIC,
    // so the token walk is skipped for the vast majority of queries.
    if (query.indexOf('{') < 0 && !containsIgnoreCase(query, "PERIODIC"))
      return;

    tokens.fill(); // the parser reuses this same fully-buffered stream

    // Sliding window of the three most recent default-channel tokens: t1 is the immediate predecessor of
    // the current token, t2 the one before it, t3 the one before that. No intermediate list is allocated.
    Token t3 = null, t2 = null, t1 = null;
    for (final Token tok : tokens.getTokens()) {
      if (tok.getChannel() != Token.DEFAULT_CHANNEL || tok.getType() == Token.EOF)
        continue;

      if (t1 != null && "PERIODIC".equalsIgnoreCase(t1.getText()) && "COMMIT".equalsIgnoreCase(tok.getText()))
        throw new CommandParsingException(
            "Deprecated syntax: 'PERIODIC COMMIT' is no longer supported. Use 'CALL { ... } IN TRANSACTIONS [OF n ROWS]' instead");

      // legacy '{name}': t2='{', t1=name, tok='}', preceded (t3) by a value operator/punctuation
      if (tok.getType() == Cypher25Lexer.RCURLY
          && t2 != null && t2.getType() == Cypher25Lexer.LCURLY
          && t1 != null && PARAM_NAME.matcher(t1.getText()).matches()
          && t3 != null && VALUE_PREFIX_TOKENS.contains(t3.getType())) {
        final String name = t1.getText();
        throw new CommandParsingException(
            "Deprecated syntax: legacy parameter '{" + name + "}' is no longer supported. Use '$" + name + "' instead");
      }

      t3 = t2;
      t2 = t1;
      t1 = tok;
    }
  }

  // Allocation-free case-insensitive substring search (avoids String.toUpperCase() on the parse hot path).
  private static boolean containsIgnoreCase(final String s, final String sub) {
    final int max = s.length() - sub.length();
    for (int i = 0; i <= max; i++)
      if (s.regionMatches(true, i, sub, 0, sub.length()))
        return true;
    return false;
  }
}
