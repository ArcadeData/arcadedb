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

/**
 * Cypher parser using ANTLR4 with the official Cypher 2.5 grammar.
 * This replaces the regex-based parser with a proper grammar-based implementation.
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
        throw new CommandParsingException(
            String.format("Unexpected input '%s' at position %d", nextToken.getText(),
                nextToken.getCharPositionInLine()));
      }

      // Build AST using visitor
      final CypherASTBuilder astBuilder = new CypherASTBuilder();
      final CypherStatement statement = astBuilder.visitStatement(statementContext);

      CypherSemanticValidator.validate(statement);

      return statement;

    } catch (final Exception e) {
      throw new CommandParsingException("Failed to parse Cypher query: " + query, e);
    }
  }
}
