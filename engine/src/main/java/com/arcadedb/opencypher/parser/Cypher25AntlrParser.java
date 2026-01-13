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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.opencypher.ast.CypherStatement;
import com.arcadedb.opencypher.grammar.Cypher25Lexer;
import com.arcadedb.opencypher.grammar.Cypher25Parser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.misc.ParseCancellationException;

/**
 * Cypher parser using ANTLR4 with the official Cypher 2.5 grammar.
 * This replaces the regex-based parser with a proper grammar-based implementation.
 */
public class Cypher25AntlrParser {
  private final DatabaseInternal database;

  public Cypher25AntlrParser(final DatabaseInternal database) {
    this.database = database;
  }

  /**
   * Parses a Cypher query string into a CypherStatement AST.
   *
   * @param query the Cypher query string
   * @return parsed CypherStatement
   * @throws CommandParsingException if query cannot be parsed
   */
  public CypherStatement parse(final String query) {
    if (query == null || query.trim().isEmpty()) {
      throw new CommandParsingException("Query cannot be empty");
    }

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

      // Build AST using visitor
      final CypherASTBuilder astBuilder = new CypherASTBuilder();
      final CypherStatement statement = astBuilder.visitStatement(statementContext);

      return statement;

    } catch (final RecognitionException | ParseCancellationException e) {
      throw new CommandParsingException("Failed to parse Cypher query: " + query, e);
    } catch (final Exception e) {
      throw new CommandParsingException("Failed to parse Cypher query: " + query, e);
    }
  }
}
