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
import com.arcadedb.query.opencypher.ast.CypherAdminStatement;
import com.arcadedb.query.opencypher.ast.CypherDDLStatement;
import com.arcadedb.query.opencypher.ast.CypherSessionStatement;
import com.arcadedb.query.opencypher.ast.CypherShowRoutinesStatement;
import com.arcadedb.query.opencypher.ast.CypherShowSchemaStatement;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.CypherTransactionStatement;
import com.arcadedb.query.opencypher.ast.CypherTypeName;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Builds the statements that are commands rather than queries: CREATE and DROP of an index or a constraint, SHOW of
 * the schema and of the running transactions, TERMINATE TRANSACTIONS, the user commands and the session settings.
 * <p>
 * Split out of {@link CypherASTBuilder}, the visitor that builds a statement from its parse tree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CypherCommandBuilder {
  private final CypherExpressionBuilder expressionBuilder;

  CypherCommandBuilder(final CypherExpressionBuilder expressionBuilder) {
    this.expressionBuilder = expressionBuilder;
  }

  CypherStatement handleCommand(final Cypher25Parser.CommandContext ctx) {
    if (ctx.createCommand() != null)
      return handleCreateCommand(ctx.createCommand());
    if (ctx.dropCommand() != null)
      return handleDropCommand(ctx.dropCommand());
    if (ctx.showCommand() != null)
      return handleShowCommand(ctx.showCommand());
    if (ctx.terminateCommand() != null)
      return handleTerminateTransactions(ctx.terminateCommand().terminateTransactions());
    if (ctx.alterCommand() != null)
      return handleAlterCommand(ctx.alterCommand());
    if (ctx.transactionCommand() != null)
      return handleTransactionCommand(ctx.transactionCommand());
    if (ctx.sessionCommand() != null)
      return handleSessionCommand(ctx.sessionCommand());
    throw new CommandParsingException("Unsupported command type");
  }

  private CypherSessionStatement handleSessionCommand(final Cypher25Parser.SessionCommandContext ctx) {
    if (ctx.SET() != null) {
      final String parameterName = CypherASTBuilder.stripBackticks(ctx.parameter().parameterName().getText());
      final Expression value = expressionBuilder.parseExpression(ctx.expression());
      return new CypherSessionStatement(CypherSessionStatement.Kind.SET, parameterName, value);
    }
    if (ctx.RESET() != null)
      return new CypherSessionStatement(CypherSessionStatement.Kind.RESET);
    if (ctx.CLOSE() != null)
      return new CypherSessionStatement(CypherSessionStatement.Kind.CLOSE);
    throw new CommandParsingException("Unsupported session command");
  }

  private CypherTransactionStatement handleTransactionCommand(final Cypher25Parser.TransactionCommandContext ctx) {
    if (ctx.START() != null) {
      final String isolationLevel = ctx.symbolicNameString() != null ?
          CypherASTBuilder.stripBackticks(ctx.symbolicNameString().getText()) : null;
      return new CypherTransactionStatement(CypherTransactionStatement.Kind.BEGIN, isolationLevel);
    }
    if (ctx.COMMIT() != null)
      return new CypherTransactionStatement(CypherTransactionStatement.Kind.COMMIT);
    if (ctx.ROLLBACK() != null)
      return new CypherTransactionStatement(CypherTransactionStatement.Kind.ROLLBACK);
    throw new CommandParsingException("Unsupported transaction command");
  }

  private CypherStatement handleCreateCommand(final Cypher25Parser.CreateCommandContext ctx) {
    if (ctx.createConstraint() != null)
      return handleCreateConstraint(ctx.createConstraint());
    if (ctx.createIndex() != null)
      return handleCreateIndex(ctx.createIndex());
    if (ctx.createUser() != null)
      return handleCreateUser(ctx.createUser());
    throw new CommandParsingException("Only CREATE CONSTRAINT, CREATE INDEX and CREATE USER are currently supported");
  }

  private CypherStatement handleDropCommand(final Cypher25Parser.DropCommandContext ctx) {
    if (ctx.dropConstraint() != null)
      return handleDropConstraint(ctx.dropConstraint());
    if (ctx.dropIndex() != null)
      return handleDropIndex(ctx.dropIndex());
    if (ctx.dropUser() != null)
      return handleDropUser(ctx.dropUser());
    throw new CommandParsingException("Only DROP CONSTRAINT, DROP INDEX and DROP USER are currently supported");
  }

  private CypherStatement handleShowCommand(final Cypher25Parser.ShowCommandContext ctx) {
    if (ctx.showConstraintCommand() != null)
      return handleShowConstraints(ctx.showConstraintCommand());
    if (ctx.showIndexCommand() != null)
      return handleShowIndexes(ctx.showIndexCommand());
    if (ctx.showUsers() != null)
      return new CypherAdminStatement(CypherAdminStatement.Kind.SHOW_USERS, null, null, false, false);
    if (ctx.showCurrentUser() != null)
      return new CypherAdminStatement(CypherAdminStatement.Kind.SHOW_CURRENT_USER, null, null, false, false);
    if (ctx.showTransactions() != null)
      return handleShowTransactions(ctx.showTransactions());
    if (ctx.showProcedures() != null)
      return new CypherShowRoutinesStatement(CypherShowRoutinesStatement.Kind.PROCEDURES, null);
    if (ctx.showFunctions() != null)
      return handleShowFunctions(ctx.showFunctions());
    throw new CommandParsingException("Only SHOW USERS, SHOW CURRENT USER, SHOW TRANSACTIONS, SHOW CONSTRAINTS, SHOW INDEXES, "
        + "SHOW PROCEDURES and SHOW FUNCTIONS are currently supported");
  }

  /** {@code SHOW [ALL | BUILT IN | USER DEFINED] FUNCTIONS} (#13541). */
  private CypherShowRoutinesStatement handleShowFunctions(final Cypher25Parser.ShowFunctionsContext ctx) {
    final Cypher25Parser.ShowFunctionsTypeContext type = ctx.showFunctionsType();
    final CypherShowRoutinesStatement.FunctionFilter filter = type == null || type.ALL() != null ?
        CypherShowRoutinesStatement.FunctionFilter.ALL :
        type.BUILT() != null ?
            CypherShowRoutinesStatement.FunctionFilter.BUILT_IN :
            CypherShowRoutinesStatement.FunctionFilter.USER_DEFINED;
    return new CypherShowRoutinesStatement(CypherShowRoutinesStatement.Kind.FUNCTIONS, filter);
  }

  /** {@code SHOW [ALL | [NODE | RELATIONSHIP] (UNIQUE | EXIST | KEY | PROPERTY TYPE)] CONSTRAINTS} (#9706). */
  private CypherShowSchemaStatement handleShowConstraints(final Cypher25Parser.ShowConstraintCommandContext ctx) {
    final CypherShowSchemaStatement.ConstraintFilter filter;
    Cypher25Parser.ShowConstraintEntityContext entity = null;
    final Cypher25Parser.ShowConstraintsEndContext end;
    switch (ctx) {
    case Cypher25Parser.ShowConstraintUniqueContext unique -> {
      filter = CypherShowSchemaStatement.ConstraintFilter.UNIQUENESS;
      entity = unique.showConstraintEntity();
      end = unique.showConstraintsEnd();
    }
    case Cypher25Parser.ShowConstraintExistContext exist -> {
      filter = CypherShowSchemaStatement.ConstraintFilter.EXISTENCE;
      entity = exist.showConstraintEntity();
      end = exist.showConstraintsEnd();
    }
    case Cypher25Parser.ShowConstraintKeyContext key -> {
      filter = CypherShowSchemaStatement.ConstraintFilter.KEY;
      entity = key.showConstraintEntity();
      end = key.showConstraintsEnd();
    }
    case Cypher25Parser.ShowConstraintPropTypeContext propType -> {
      filter = CypherShowSchemaStatement.ConstraintFilter.PROPERTY_TYPE;
      entity = propType.showConstraintEntity();
      end = propType.showConstraintsEnd();
    }
    case Cypher25Parser.ShowConstraintAllContext all -> {
      filter = CypherShowSchemaStatement.ConstraintFilter.ALL;
      end = all.showConstraintsEnd();
    }
    default -> throw new CommandParsingException("Unsupported SHOW CONSTRAINTS form: " + ctx.getText());
    }
    if (end.composableCommandClauses() != null)
      throw new CommandParsingException("SHOW CONSTRAINTS cannot be followed by another command");

    final CypherShowSchemaStatement.EntityFilter entityFilter = entity == null ?
        CypherShowSchemaStatement.EntityFilter.ALL :
        entity instanceof Cypher25Parser.NodeEntityContext ?
            CypherShowSchemaStatement.EntityFilter.NODE :
            CypherShowSchemaStatement.EntityFilter.RELATIONSHIP;
    return new CypherShowSchemaStatement(CypherShowSchemaStatement.Kind.CONSTRAINTS, filter, entityFilter, null);
  }

  /** {@code SHOW [ALL | RANGE | TEXT | POINT | VECTOR | FULLTEXT | LOOKUP] INDEXES} (#9706). */
  private CypherShowSchemaStatement handleShowIndexes(final Cypher25Parser.ShowIndexCommandContext ctx) {
    if (ctx.showIndexesEnd().composableCommandClauses() != null)
      throw new CommandParsingException("SHOW INDEXES cannot be followed by another command");
    final Cypher25Parser.ShowIndexTypeContext type = ctx.showIndexType();
    final String indexType = type == null || type.ALL() != null ? null : type.getText().toUpperCase(Locale.ROOT);
    return new CypherShowSchemaStatement(CypherShowSchemaStatement.Kind.INDEXES, CypherShowSchemaStatement.ConstraintFilter.ALL,
        CypherShowSchemaStatement.EntityFilter.ALL, indexType);
  }

  /**
   * {@code SHOW TRANSACTIONS [ids] [YIELD ...|WHERE ...] [TERMINATE TRANSACTIONS ...]} (issue #9689). The tail is applied
   * to the rows at execution, from the statement text, as on every other {@code SHOW}.
   */
  private CypherAdminStatement handleShowTransactions(final Cypher25Parser.ShowTransactionsContext ctx) {
    final Cypher25Parser.NamesAndClausesContext clauses = ctx.namesAndClauses();
    CypherAdminStatement composedTerminate = null;
    if (clauses.composableCommandClauses() != null) {
      if (clauses.composableCommandClauses().terminateCommand() == null)
        throw new CommandParsingException("SHOW TRANSACTIONS can only be followed by TERMINATE TRANSACTIONS");
      composedTerminate = handleTerminateTransactions(clauses.composableCommandClauses().terminateCommand().terminateTransactions());
    }
    final Cypher25Parser.StringsOrExpressionContext ids = clauses.stringsOrExpression();
    return new CypherAdminStatement(CypherAdminStatement.Kind.SHOW_TRANSACTIONS, ids != null ? transactionIdLiterals(ids) : null,
        ids != null ? transactionIdExpression(ids) : null, composedTerminate);
  }

  /** {@code TERMINATE TRANSACTIONS ids [YIELD ...|WHERE ...]} (issue #9689). */
  private CypherAdminStatement handleTerminateTransactions(final Cypher25Parser.TerminateTransactionsContext ctx) {
    if (ctx.composableCommandClauses() != null)
      throw new CommandParsingException("TERMINATE TRANSACTIONS cannot be followed by another command");
    return new CypherAdminStatement(CypherAdminStatement.Kind.TERMINATE_TRANSACTIONS, transactionIdLiterals(ctx.stringsOrExpression()),
        transactionIdExpression(ctx.stringsOrExpression()), null);
  }

  private List<String> transactionIdLiterals(final Cypher25Parser.StringsOrExpressionContext ctx) {
    if (ctx.stringList() == null)
      return null;
    final List<String> ids = new ArrayList<>();
    for (final Cypher25Parser.StringLiteralContext literal : ctx.stringList().stringLiteral()) {
      final String raw = literal.getText();
      ids.add(CypherASTBuilder.decodeStringLiteral(raw.substring(1, raw.length() - 1)));
    }
    return ids;
  }

  private Expression transactionIdExpression(final Cypher25Parser.StringsOrExpressionContext ctx) {
    return ctx.expression() != null ? expressionBuilder.parseExpression(ctx.expression()) : null;
  }

  private CypherAdminStatement handleAlterCommand(final Cypher25Parser.AlterCommandContext ctx) {
    if (ctx.alterUser() != null)
      return handleAlterUser(ctx.alterUser());
    throw new CommandParsingException("Only ALTER USER is currently supported");
  }

  private CypherAdminStatement handleCreateUser(final Cypher25Parser.CreateUserContext ctx) {
    final String userName = CypherASTBuilder.stripBackticks(ctx.commandNameExpression().getText());
    final boolean ifNotExists = ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null;

    String password = null;
    if (ctx.password() != null && !ctx.password().isEmpty()) {
      final Cypher25Parser.PasswordContext pwdCtx = ctx.password(0);
      password = extractPasswordValue(pwdCtx.passwordExpression());
    }
    if (password == null)
      throw new CommandParsingException("CREATE USER requires SET PASSWORD");

    return new CypherAdminStatement(CypherAdminStatement.Kind.CREATE_USER, userName, password, ifNotExists, false);
  }

  private CypherAdminStatement handleDropUser(final Cypher25Parser.DropUserContext ctx) {
    final String userName = CypherASTBuilder.stripBackticks(ctx.commandNameExpression().getText());
    final boolean ifExists = ctx.IF() != null && ctx.EXISTS() != null;
    return new CypherAdminStatement(CypherAdminStatement.Kind.DROP_USER, userName, null, false, ifExists);
  }

  private CypherAdminStatement handleAlterUser(final Cypher25Parser.AlterUserContext ctx) {
    final String userName = CypherASTBuilder.stripBackticks(ctx.commandNameExpression().getText());

    String password = null;
    if (ctx.password() != null && !ctx.password().isEmpty()) {
      final Cypher25Parser.PasswordContext pwdCtx = ctx.password(0);
      password = extractPasswordValue(pwdCtx.passwordExpression());
    }
    if (password == null)
      throw new CommandParsingException("ALTER USER requires SET PASSWORD");

    return new CypherAdminStatement(CypherAdminStatement.Kind.ALTER_USER, userName, password, false, false);
  }

  private String extractPasswordValue(final Cypher25Parser.PasswordExpressionContext ctx) {
    if (ctx.stringLiteral() != null) {
      final String raw = ctx.stringLiteral().getText();
      return CypherASTBuilder.decodeStringLiteral(raw.substring(1, raw.length() - 1));
    }
    throw new CommandParsingException("Password must be a string literal");
  }

  private CypherDDLStatement handleCreateConstraint(final Cypher25Parser.CreateConstraintContext ctx) {
    // Extract optional constraint name
    final String constraintName = ctx.symbolicNameOrStringParameter() != null
        ? CypherASTBuilder.stripBackticks(ctx.symbolicNameOrStringParameter().getText()) : null;

    // IF NOT EXISTS
    final boolean ifNotExists = ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null;

    // Extract label name and determine if it's a node or relationship constraint
    final boolean forRelationship;
    final String labelName;
    if (ctx.commandNodePattern() != null) {
      forRelationship = false;
      labelName = CypherASTBuilder.stripBackticks(ctx.commandNodePattern().labelType().symbolicNameString().getText());
    } else if (ctx.commandRelPattern() != null) {
      forRelationship = true;
      labelName = CypherASTBuilder.stripBackticks(ctx.commandRelPattern().relType().symbolicNameString().getText());
    } else {
      throw new CommandParsingException("CREATE CONSTRAINT requires a node or relationship pattern");
    }

    // Extract property names from constraintType
    final Cypher25Parser.ConstraintTypeContext constraintType = ctx.constraintType();
    final List<String> propertyNames = extractPropertyNames(constraintType);

    // Determine constraint kind
    final CypherDDLStatement.ConstraintKind constraintKind;
    CypherTypeName typedName = null;
    if (constraintType instanceof Cypher25Parser.ConstraintIsUniqueContext)
      constraintKind = CypherDDLStatement.ConstraintKind.UNIQUE;
    else if (constraintType instanceof Cypher25Parser.ConstraintIsNotNullContext)
      constraintKind = CypherDDLStatement.ConstraintKind.NOT_NULL;
    else if (constraintType instanceof Cypher25Parser.ConstraintKeyContext)
      constraintKind = CypherDDLStatement.ConstraintKind.KEY;
    else if (constraintType instanceof Cypher25Parser.ConstraintTypedContext) {
      constraintKind = CypherDDLStatement.ConstraintKind.TYPED;
      typedName = TypePredicateBuilder.parseTypeName(((Cypher25Parser.ConstraintTypedContext) constraintType).type(), "a property type constraint");
    } else
      throw new CommandParsingException("Unsupported constraint type: " + constraintType.getText());

    return new CypherDDLStatement(CypherDDLStatement.Kind.CREATE_CONSTRAINT, constraintKind,
        constraintName, labelName, propertyNames, ifNotExists, false, forRelationship, typedName);
  }

  private CypherDDLStatement handleDropConstraint(final Cypher25Parser.DropConstraintContext ctx) {
    final String constraintName = CypherASTBuilder.stripBackticks(ctx.symbolicNameOrStringParameter().getText());
    final boolean ifExists = ctx.IF() != null && ctx.EXISTS() != null;
    return new CypherDDLStatement(CypherDDLStatement.Kind.DROP_CONSTRAINT, null,
        constraintName, null, null, false, ifExists, false);
  }

  private CypherDDLStatement handleCreateIndex(final Cypher25Parser.CreateIndexContext ctx) {
    // The grammar supports: INDEX, RANGE INDEX, TEXT INDEX, POINT INDEX, VECTOR INDEX, LOOKUP INDEX, FULLTEXT INDEX.
    // ArcadeDB maps the standard, RANGE, TEXT and POINT indexes to LSM_TREE, VECTOR to LSM_VECTOR and FULLTEXT to FULL_TEXT
    if (ctx.createVectorIndex() != null)
      return handleCreateVectorIndex(ctx.createVectorIndex());
    if (ctx.createFulltextIndex() != null)
      return handleCreateFulltextIndex(ctx.createFulltextIndex());

    final Cypher25Parser.CreateIndex_Context indexCtx;
    if (ctx.createIndex_() != null)
      indexCtx = ctx.createIndex_();
    else
      throw new CommandParsingException("Only standard, RANGE, TEXT, VECTOR and FULLTEXT index types are supported");

    // Extract optional index name
    final String indexName = indexCtx.symbolicNameOrStringParameter() != null
        ? CypherASTBuilder.stripBackticks(indexCtx.symbolicNameOrStringParameter().getText()) : null;

    // IF NOT EXISTS
    final boolean ifNotExists = indexCtx.IF() != null && indexCtx.NOT() != null && indexCtx.EXISTS() != null;

    // Extract label name and determine if it's for a node or relationship
    final boolean forRelationship;
    final String labelName;
    if (indexCtx.commandNodePattern() != null) {
      forRelationship = false;
      labelName = CypherASTBuilder.stripBackticks(indexCtx.commandNodePattern().labelType().symbolicNameString().getText());
    } else if (indexCtx.commandRelPattern() != null) {
      forRelationship = true;
      labelName = CypherASTBuilder.stripBackticks(indexCtx.commandRelPattern().relType().symbolicNameString().getText());
    } else {
      throw new CommandParsingException("CREATE INDEX requires a node or relationship pattern");
    }

    // Extract property names from propertyList
    final List<String> propertyNames = extractIndexPropertyNames(indexCtx.propertyList());

    return new CypherDDLStatement(CypherDDLStatement.Kind.CREATE_INDEX, null,
        indexName, labelName, propertyNames, ifNotExists, false, forRelationship);
  }

  /**
   * {@code CREATE VECTOR INDEX [name] [IF NOT EXISTS] FOR (n:Label) ON n.prop [OPTIONS {...}]}, Neo4j 5 syntax, issue #10678.
   * The {@code indexConfig} in the options is read when the statement runs, so it may be a parameter.
   * <p>
   * {@code WITH [n.a, n.b]}, the additional filtering properties of Neo4j 2025's vector index, is accepted and builds the same
   * index as without it (issue #10863). Neo4j stores those properties in the index so that the {@code WHERE} of a Cypher 25
   * {@code SEARCH} clause can filter on them, and refuses a {@code WHERE} on any other property. ArcadeDB's {@code SEARCH}
   * filters on the record itself while the index is searched (issue #10782), so its {@code WHERE} may read every property
   * with or without the declaration: the statements a Neo4j script runs against the index answer the same.
   */
  private CypherDDLStatement handleCreateVectorIndex(final Cypher25Parser.CreateVectorIndexContext ctx) {
    final List<String> propertyNames = extractIndexPropertyNames(ctx.propertyList());
    if (propertyNames.size() != 1)
      throw new CommandParsingException("A vector index is on exactly one property, found " + propertyNames.size());

    return createIndexStatement(CypherDDLStatement.IndexKind.VECTOR, "CREATE VECTOR INDEX", ctx.symbolicNameOrStringParameter(),
        ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null, ctx.multiLabelNodePattern(), ctx.multiRelTypeRelPattern(),
        propertyNames, ctx.commandOptions());
  }

  /**
   * {@code CREATE FULLTEXT INDEX [name] [IF NOT EXISTS] FOR (n:Label) ON EACH [n.a, n.b] [OPTIONS {...}]}, Neo4j 5 syntax, issue
   * #10678.
   */
  private CypherDDLStatement handleCreateFulltextIndex(final Cypher25Parser.CreateFulltextIndexContext ctx) {
    final List<String> propertyNames = new ArrayList<>();
    for (final Cypher25Parser.PropertyContext prop : ctx.enclosedPropertyList().property())
      propertyNames.add(CypherASTBuilder.stripBackticks(prop.propertyKeyName().getText()));

    return createIndexStatement(CypherDDLStatement.IndexKind.FULLTEXT, "CREATE FULLTEXT INDEX", ctx.symbolicNameOrStringParameter(),
        ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null, ctx.multiLabelNodePattern(), ctx.multiRelTypeRelPattern(),
        propertyNames, ctx.commandOptions());
  }

  /**
   * The statement of a {@code CREATE VECTOR INDEX} or {@code CREATE FULLTEXT INDEX}. Neo4j lets either span several labels
   * ({@code (n:A|B)}); an ArcadeDB index belongs to one type, so the statement builds one index per label and records the name
   * that ties them together (issue #10914).
   */
  private CypherDDLStatement createIndexStatement(final CypherDDLStatement.IndexKind indexKind, final String statement,
      final Cypher25Parser.SymbolicNameOrStringParameterContext nameCtx, final boolean ifNotExists,
      final Cypher25Parser.MultiLabelNodePatternContext nodePattern, final Cypher25Parser.MultiRelTypeRelPatternContext relPattern,
      final List<String> propertyNames, final Cypher25Parser.CommandOptionsContext optionsCtx) {
    final String indexName = nameCtx != null ? CypherASTBuilder.stripBackticks(nameCtx.getText()) : null;

    final boolean forRelationship = relPattern != null;
    final List<Cypher25Parser.SymbolicNameStringContext> labels = forRelationship ?
        relPattern.symbolicNameString() :
        nodePattern.symbolicNameString();
    final List<String> labelNames = new ArrayList<>(labels.size());
    for (final Cypher25Parser.SymbolicNameStringContext label : labels) {
      final String labelName = CypherASTBuilder.stripBackticks(label.getText());
      if (!labelNames.contains(labelName))
        labelNames.add(labelName);
    }

    Expression options = null;
    if (optionsCtx != null) {
      final Cypher25Parser.MapOrParameterContext mapOrParam = optionsCtx.mapOrParameter();
      if (mapOrParam.map() != null)
        options = expressionBuilder.parseMapLiteralExpression(mapOrParam.map());
      else
        options = new ParameterExpression(CypherASTBuilder.stripBackticks(mapOrParam.parameter().parameterName().getText()),
            mapOrParam.parameter().getText());
    }

    return new CypherDDLStatement(CypherDDLStatement.Kind.CREATE_INDEX, null, indexName, labelNames, propertyNames, ifNotExists,
        false, forRelationship, null, indexKind, options);
  }

  private CypherDDLStatement handleDropIndex(final Cypher25Parser.DropIndexContext ctx) {
    final String indexName = CypherASTBuilder.stripBackticks(ctx.symbolicNameOrStringParameter().getText());
    final boolean ifExists = ctx.IF() != null && ctx.EXISTS() != null;
    return new CypherDDLStatement(CypherDDLStatement.Kind.DROP_INDEX, null,
        indexName, null, null, false, ifExists, false);
  }

  /**
   * Extracts property names from a propertyList context (used by CREATE INDEX).
   * Handles both single property (n.id) and property list ((n.first, n.last)).
   */
  private List<String> extractIndexPropertyNames(final Cypher25Parser.PropertyListContext propList) {
    final List<String> names = new ArrayList<>();
    if (propList.enclosedPropertyList() != null) {
      final Cypher25Parser.EnclosedPropertyListContext enclosed = propList.enclosedPropertyList();
      for (final Cypher25Parser.PropertyContext prop : enclosed.property())
        names.add(CypherASTBuilder.stripBackticks(prop.propertyKeyName().getText()));
    } else {
      names.add(CypherASTBuilder.stripBackticks(propList.property().propertyKeyName().getText()));
    }
    return names;
  }

  /**
   * Extracts property names from a constraintType context.
   * Handles both single property (p.id) and property list ((p.first, p.last)).
   */
  private List<String> extractPropertyNames(final Cypher25Parser.ConstraintTypeContext ctx) {
    // propertyList() is defined on each specific subclass, not on the base ConstraintTypeContext
    final Cypher25Parser.PropertyListContext propList;
    if (ctx instanceof Cypher25Parser.ConstraintIsUniqueContext)
      propList = ((Cypher25Parser.ConstraintIsUniqueContext) ctx).propertyList();
    else if (ctx instanceof Cypher25Parser.ConstraintIsNotNullContext)
      propList = ((Cypher25Parser.ConstraintIsNotNullContext) ctx).propertyList();
    else if (ctx instanceof Cypher25Parser.ConstraintKeyContext)
      propList = ((Cypher25Parser.ConstraintKeyContext) ctx).propertyList();
    else if (ctx instanceof Cypher25Parser.ConstraintTypedContext)
      propList = ((Cypher25Parser.ConstraintTypedContext) ctx).propertyList();
    else
      throw new CommandParsingException("Unsupported constraint type for property extraction");

    final List<String> names = new ArrayList<>();
    if (propList.enclosedPropertyList() != null) {
      // Parenthesized list: (p.first, p.last)
      final Cypher25Parser.EnclosedPropertyListContext enclosed = propList.enclosedPropertyList();
      for (final Cypher25Parser.PropertyContext prop : enclosed.property())
        names.add(CypherASTBuilder.stripBackticks(prop.propertyKeyName().getText()));
    } else {
      // Single property: p.id
      names.add(CypherASTBuilder.stripBackticks(propList.property().propertyKeyName().getText()));
    }
    return names;
  }
}
