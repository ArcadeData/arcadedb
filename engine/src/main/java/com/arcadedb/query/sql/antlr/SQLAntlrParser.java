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

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.grammar.SQLLexer;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.WhereClause;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.List;

/**
 * ANTLR4-based SQL parser for ArcadeDB.
 *
 * This class wraps the ANTLR-generated lexer and parser, providing a clean API
 * for parsing SQL statements into ArcadeDB's internal AST representation.
 *
 * It produces identical AST structures to the JavaCC-based SqlParser, ensuring
 * 100% backward compatibility with existing code.
 *
 * Usage:
 * <pre>
 *   SQLAntlrParser parser = new SQLAntlrParser(database);
 *   Statement stmt = parser.parse("SELECT * FROM User WHERE name = 'John'");
 * </pre>
 */
public class SQLAntlrParser {

  private final Database database;

  /**
   * Create a new SQL parser.
   *
   * @param database The database context (may be null for syntax-only parsing)
   */
  public SQLAntlrParser(final Database database) {
    this.database = database;
  }

  /**
   * Parse a single SQL statement.
   *
   * @param sqlText The SQL text to parse
   * @return The parsed Statement object
   * @throws CommandSQLParsingException if the SQL is invalid
   */
  public Statement parse(final String sqlText) throws CommandSQLParsingException {
    try {
      // Create lexer
      final CharStream input = CharStreams.fromString(sqlText);
      final SQLLexer lexer = new SQLLexer(input);

      // Create token stream
      final CommonTokenStream tokens = new CommonTokenStream(lexer);

      // Create parser
      final SQLParser parser = new SQLParser(tokens);

      // Remove default error listeners (they print to stderr)
      parser.removeErrorListeners();
      lexer.removeErrorListeners();

      // Add our custom error listener
      final SQLErrorListener errorListener = new SQLErrorListener(sqlText);
      parser.addErrorListener(errorListener);
      lexer.addErrorListener(errorListener);

      // Parse the SQL
      final SQLParser.ParseContext parseTree = parser.parse();

      // Build AST
      final SQLASTBuilder builder = new SQLASTBuilder(database);
      final Statement stmt = (Statement) builder.visit(parseTree);

      // Set original statement text (used for caching and error messages)
      if (stmt != null) {
        stmt.originalStatementAsString = sqlText;
      }

      return stmt;

    } catch (final CommandSQLParsingException e) {
      // Re-throw parsing exceptions as-is
      throw e;
    } catch (final Exception e) {
      // Wrap other exceptions
      throw new CommandSQLParsingException("Error parsing SQL: " + e.getMessage(), e);
    }
  }

  /**
   * Parse multiple SQL statements (SQL script).
   *
   * @param sqlScript The SQL script containing multiple statements
   * @return List of parsed Statement objects
   * @throws CommandSQLParsingException if any statement is invalid
   */
  public List<Statement> parseScript(final String sqlScript) throws CommandSQLParsingException {
    try {
      // Create lexer
      final CharStream input = CharStreams.fromString(sqlScript);
      final SQLLexer lexer = new SQLLexer(input);

      // Create token stream
      final CommonTokenStream tokens = new CommonTokenStream(lexer);

      // Create parser
      final SQLParser parser = new SQLParser(tokens);

      // Remove default error listeners
      parser.removeErrorListeners();
      lexer.removeErrorListeners();

      // Add our custom error listener
      final SQLErrorListener errorListener = new SQLErrorListener(sqlScript);
      parser.addErrorListener(errorListener);
      lexer.addErrorListener(errorListener);

      // Parse the script
      final SQLParser.ParseScriptContext parseTree = parser.parseScript();

      // Build AST
      final SQLASTBuilder builder = new SQLASTBuilder(database);
      final List<Statement> statements = (List<Statement>) builder.visit(parseTree);

      // Set original statement text for each statement
      if (statements != null) {
        for (final Statement stmt : statements) {
          // For now, set the entire script as the original text
          // TODO: Track individual statement positions
          stmt.originalStatementAsString = sqlScript;
        }
      }

      return statements;

    } catch (final CommandSQLParsingException e) {
      throw e;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Error parsing SQL script: " + e.getMessage(), e);
    }
  }

  /**
   * Parse a standalone expression (for use in programmatic contexts).
   *
   * @param exprText The expression text to parse
   * @return The parsed Expression object
   * @throws CommandSQLParsingException if the expression is invalid
   */
  public Expression parseExpression(final String exprText) throws CommandSQLParsingException {
    try {
      // Create lexer
      final CharStream input = CharStreams.fromString(exprText);
      final SQLLexer lexer = new SQLLexer(input);

      // Create token stream
      final CommonTokenStream tokens = new CommonTokenStream(lexer);

      // Create parser
      final SQLParser parser = new SQLParser(tokens);

      // Remove default error listeners
      parser.removeErrorListeners();
      lexer.removeErrorListeners();

      // Add our custom error listener
      final SQLErrorListener errorListener = new SQLErrorListener(exprText);
      parser.addErrorListener(errorListener);
      lexer.addErrorListener(errorListener);

      // Parse the expression
      final SQLParser.ParseExpressionContext parseTree = parser.parseExpression();

      // Build AST
      final SQLASTBuilder builder = new SQLASTBuilder(database);
      final Expression expr = (Expression) builder.visit(parseTree);

      return expr;

    } catch (final CommandSQLParsingException e) {
      throw e;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Error parsing expression: " + e.getMessage(), e);
    }
  }

  /**
   * Parse a WHERE clause condition (for use in programmatic contexts).
   *
   * @param conditionText The condition text to parse
   * @return The parsed WhereClause object
   * @throws CommandSQLParsingException if the condition is invalid
   */
  public WhereClause parseCondition(final String conditionText) throws CommandSQLParsingException {
    try {
      // Create lexer
      final CharStream input = CharStreams.fromString(conditionText);
      final SQLLexer lexer = new SQLLexer(input);

      // Create token stream
      final CommonTokenStream tokens = new CommonTokenStream(lexer);

      // Create parser
      final SQLParser parser = new SQLParser(tokens);

      // Remove default error listeners
      parser.removeErrorListeners();
      lexer.removeErrorListeners();

      // Add our custom error listener
      final SQLErrorListener errorListener = new SQLErrorListener(conditionText);
      parser.addErrorListener(errorListener);
      lexer.addErrorListener(errorListener);

      // Parse the condition
      final SQLParser.ParseConditionContext parseTree = parser.parseCondition();

      // Build AST
      final SQLASTBuilder builder = new SQLASTBuilder(database);
      final WhereClause whereClause = (WhereClause) builder.visit(parseTree);

      return whereClause;

    } catch (final CommandSQLParsingException e) {
      throw e;
    } catch (final Exception e) {
      throw new CommandSQLParsingException("Error parsing condition: " + e.getMessage(), e);
    }
  }

  /**
   * Get the database associated with this parser.
   *
   * @return The database, or null if none was provided
   */
  public Database getDatabase() {
    return database;
  }
}
