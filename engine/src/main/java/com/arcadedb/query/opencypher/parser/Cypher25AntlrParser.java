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

import java.util.ArrayList;
import java.util.List;
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

    // Reject deprecated/legacy Cypher syntax with a clear, actionable message before the generic
    // ANTLR parse (which would otherwise produce a cryptic "Unexpected input" error). Done on its
    // own token pass so the message is not swallowed by the wrapping catch below (issue #4141).
    checkDeprecatedSyntax(query);

    try {
      // Create lexer
      final Cypher25Lexer lexer = new Cypher25Lexer(CharStreams.fromString(query));

      // Create token stream
      final CommonTokenStream tokens = new CommonTokenStream(lexer);

      // Create parser
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

  // Keywords that legitimately introduce a brace-block expression ('EXISTS { ... }', 'COUNT { ... }',
  // 'COLLECT { ... }', 'CALL { ... }'), so a following '{' must not be mistaken for a legacy parameter.
  private static final Set<String> BRACE_BLOCK_KEYWORDS = Set.of("EXISTS", "COUNT", "COLLECT", "CALL");

  // A legacy named parameter ('{name}'). Matched on token TEXT rather than type because Cypher 25 lexes
  // many plain words (NAME, TYPE, ...) as keyword tokens that are still legal as a parameter name.
  // Purely numeric braces are intentionally excluded: '{n}' / '{n,m}' are quantified-path-pattern
  // quantifiers, not legacy positional parameters.
  private static final Pattern PARAM_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  /**
   * Detects deprecated/legacy Cypher syntax removed under ISO/IEC 39075 (GQL) / Cypher 25 and throws a
   * {@link CommandParsingException} pointing at the supported replacement. Scans the lexer token stream
   * (default channel only) so matches inside string literals or comments are never flagged.
   * <p>
   * Detected:
   * <ul>
   *   <li>{@code PERIODIC COMMIT} -&gt; use {@code CALL { ... } IN TRANSACTIONS}</li>
   *   <li>legacy {@code {param}} parameters -&gt; use {@code $param}</li>
   * </ul>
   */
  private static void checkDeprecatedSyntax(final String query) {
    final Cypher25Lexer lexer = new Cypher25Lexer(CharStreams.fromString(query));
    lexer.removeErrorListeners(); // the authoritative lex/parse pass runs later; tolerate lex issues here

    final CommonTokenStream stream = new CommonTokenStream(lexer);
    stream.fill();

    final List<Token> tokens = new ArrayList<>();
    for (final Token t : stream.getTokens())
      if (t.getChannel() == Token.DEFAULT_CHANNEL && t.getType() != Token.EOF)
        tokens.add(t);

    for (int i = 0; i < tokens.size(); i++) {
      final Token t = tokens.get(i);

      // PERIODIC COMMIT (two consecutive word tokens; the keywords are not in the grammar). Text match
      // naturally excludes string literals, whose token text retains the surrounding quotes.
      if ("PERIODIC".equalsIgnoreCase(t.getText())
          && i + 1 < tokens.size() && "COMMIT".equalsIgnoreCase(tokens.get(i + 1).getText()))
        throw new CommandParsingException(
            "Deprecated syntax: 'PERIODIC COMMIT' is no longer supported. Use 'CALL { ... } IN TRANSACTIONS [OF n ROWS]' instead");

      // Legacy parameter '{name}': exactly one name token between braces, that is neither a map projection
      // ('var {name}') nor a brace-block expression ('EXISTS/COUNT/COLLECT/CALL { ... }'). A map literal
      // would require a ':' inside, so it can never match this single-token shape.
      if (t.getType() == Cypher25Lexer.LCURLY
          && i + 2 < tokens.size() && tokens.get(i + 2).getType() == Cypher25Lexer.RCURLY
          && PARAM_NAME.matcher(tokens.get(i + 1).getText()).matches()) {
        final Token prev = i > 0 ? tokens.get(i - 1) : null;
        final boolean mapProjection = prev != null && isNameToken(prev);
        final boolean braceBlock = prev != null && BRACE_BLOCK_KEYWORDS.contains(prev.getText().toUpperCase());
        if (!mapProjection && !braceBlock) {
          final String name = tokens.get(i + 1).getText();
          throw new CommandParsingException(
              "Deprecated syntax: legacy parameter '{" + name + "}' is no longer supported. Use '$" + name + "' instead");
        }
      }
    }
  }

  // A map projection target ('var {...}') is a user-defined variable, which lexes as one of these name
  // token types (not an arbitrary keyword), so this reliably tells a projection apart from a clause
  // keyword or operator preceding a legacy parameter.
  private static boolean isNameToken(final Token t) {
    return t.getType() == Cypher25Lexer.IDENTIFIER
        || t.getType() == Cypher25Lexer.EXTENDED_IDENTIFIER
        || t.getType() == Cypher25Lexer.ESCAPED_SYMBOLIC_NAME;
  }
}
