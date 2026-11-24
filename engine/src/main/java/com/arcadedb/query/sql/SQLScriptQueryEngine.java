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
package com.arcadedb.query.sql;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.ResultRowCapScope;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.*;
import com.arcadedb.query.sql.parser.*;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SQLScriptQueryEngine extends SQLQueryEngine {
  public static final String ENGINE_NAME = "sqlscript";

  public static class SQLScriptQueryEngineFactory implements QueryEngineFactory {
    @Override
    public String getLanguage() {
      return ENGINE_NAME;
    }

    @Override
    public QueryEngine getInstance(final DatabaseInternal database) {
      return new SQLScriptQueryEngine(database);
    }
  }

  protected SQLScriptQueryEngine(final DatabaseInternal database) {
    super(database);
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    // The first thing: whatever the script runs after this finds no cap, only its last statement is the caller's answer (issue #12016)
    final int rowCap = ResultRowCapScope.take();
    final List<Statement> statements = parseScript(query, database);
    for (final Statement statement : statements)
      if (!statement.isIdempotent())
        throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");
    Statement.checkParametersBound(statements, parameters);

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database.getWrappedDatabaseInstance());
    context.setInputParameters(parameters);
    return executeInternal(statements, context, rowCap);
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Object... parameters) {
    // The first thing: whatever the script runs after this finds no cap, only its last statement is the caller's answer (issue #12016)
    final int rowCap = ResultRowCapScope.take();
    final List<Statement> statements = parseScript(query, database);
    for (final Statement statement : statements)
      if (!statement.isIdempotent())
        throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");
    Statement.checkPositionalParameterCount(statements, parameters == null ? 0 : parameters.length);

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database.getWrappedDatabaseInstance());
    context.setInputParameters(parameters);
    return executeInternal(statements, context, rowCap);
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    // The first thing: whatever the script runs after this finds no cap, only its last statement is the caller's answer (issue #12016)
    final int rowCap = ResultRowCapScope.take();
    final List<Statement> statements = parseScript(query, database.getWrappedDatabaseInstance());
    Statement.checkParametersBound(statements, parameters);

    final BasicCommandContext context = new BasicCommandContext();
    context.setConfiguration(configuration);
    context.setDatabase(database.getWrappedDatabaseInstance());
    context.setInputParameters(parameters);
    return executeInternal(statements, context, rowCap);
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Object... parameters) {
    // The first thing: whatever the script runs after this finds no cap, only its last statement is the caller's answer (issue #12016)
    final int rowCap = ResultRowCapScope.take();
    final List<Statement> statements = parseScript(query, database.getWrappedDatabaseInstance());
    Statement.checkPositionalParameterCount(statements, parameters == null ? 0 : parameters.length);

    final BasicCommandContext context = new BasicCommandContext();
    context.setConfiguration(configuration);
    context.setDatabase(database.getWrappedDatabaseInstance());
    context.setInputParameters(parameters);
    return executeInternal(statements, context, rowCap);
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    final List<Statement> statements = parseScript(query, database);
    return new AnalyzedQuery() {
      @Override
      public boolean isIdempotent() {
        for (Statement s : statements)
          if (!s.isIdempotent())
            return false;
        return true;
      }

      @Override
      public boolean isDDL() {
        for (Statement s : statements)
          if (s.isDDL())
            return true;
        return false;
      }

      /**
       * Union of every statement's own operation types, not the interface default derived from
       * {@link #isIdempotent()}/{@link #isDDL()} alone: that default would report a script such as
       * {@code BACKUP DATABASE} as a plain {@code READ}, discarding the write
       * {@link BackupDatabaseStatement#getOperationTypes()} declares (issue #7576). A script is admitted by a
       * caller only when every statement in it is, which is already how {@code isIdempotent()} and {@code isDDL()}
       * aggregate above - so the union, not the first statement's types, is what keeps this consistent with them.
       */
      @Override
      public Set<OperationType> getOperationTypes() {
        final EnumSet<OperationType> all = EnumSet.noneOf(OperationType.class);
        for (final Statement s : statements)
          all.addAll(s.getOperationTypes());
        // getOperationTypes() is documented to return a non-empty set. A script with zero statements is vacuously
        // idempotent (the loop in isIdempotent() above never finds a counterexample), so it is reported as a
        // (vacuous) READ here too, rather than as the only case that would return an empty union.
        if (all.isEmpty())
          all.add(OperationType.READ);
        return all;
      }
    };
  }

  /**
   * A script routes as a WHOLE, on whether ANY statement in it is DDL: the statements share one transaction and one
   * thread, so a script cannot be half on a worker and half off it, and the DDL half is what dictates where both can
   * run (issue #6324, item 5).
   * <p>
   * Unlike plain {@code sql} this parse is NOT free - there is no script statement cache, so
   * {@link #parseScript(String, DatabaseInternal)} parses here and the execution parses again - which is why the
   * caller asks only once a cheap textual filter has established that a DDL verb appears in the text at all
   * ({@code DatabaseAsyncExecutorImpl.mayContainDDL}). A script that mentions none cannot contain a DDL statement and
   * is answered without parsing anything.
   */
  @Override
  public DDLClassification classifyDDL(final String query) {
    for (final Statement statement : parseScript(query, database))
      if (statement instanceof DDLStatement)
        return DDLClassification.DDL;
    return DDLClassification.NOT_DDL;
  }

  /**
   * A script handles its own failure when it has a {@code COMMIT RETRY n} block and every write it makes is inside a {@code BEGIN ...
   * COMMIT} block, where the retries roll back what a failed attempt wrote and the {@code ELSE} branch answers the failure (issue
   * #10379). A write outside any block, before it or after it, is the caller's transaction's: nothing takes it back when the script
   * fails, so the commit is refused as for any other statement.
   */
  @Override
  public boolean handlesItsOwnFailures(final String query) {
    try {
      boolean retryBlock = false;
      int blockDepth = 0;
      for (final Statement statement : parseScript(query, database)) {
        if (statement instanceof BeginStatement)
          blockDepth++;
        else if (statement instanceof CommitStatement commit) {
          if (blockDepth > 0)
            blockDepth--;
          retryBlock |= commit.getRetry() != null;
        } else if (statement instanceof RollbackStatement && blockDepth > 0)
          // ISSUE #12797: A ROLLBACK closes its BEGIN block as executeInternal plans it, so what follows is outside that block
          blockDepth--;
        else if (blockDepth == 0 && !statement.isIdempotent())
          return false;
      }
      return retryBlock;
    } catch (final CommandSQLParsingException e) {
      // A script that does not parse failed before it wrote anything, and is not one that handles anything
      return false;
    }
  }

  public static List<Statement> parseScript(final String script, final DatabaseInternal database) {
    try {
      return new SQLAntlrParser(database).parseScript(script);
    } catch (final CommandSQLParsingException e) {
      throw e.setCommand(script);
    }
  }

  @Override
  public boolean isExecutedByTheLeader() {
    // REPLICATE THE SCRIPT TO THE LEADER
    return true;
  }

  /**
   * Plans the statements of a closed {@code BEGIN ...} block one after the other and answers the empty list the next block collects
   * into. Issue #8633: the block is planned, so it must not be planned again by the next one.
   */
  private static List<Statement> planBlock(final ScriptExecutionPlan plan, final List<Statement> block,
      final CommandContext scriptContext) {
    for (final Statement statement : block)
      plan.chain(statement.createExecutionPlan(scriptContext));
    return new ArrayList<>();
  }

  /**
   * @param rowCap the rows the caller reads at most (see {@link ResultRowCapScope}), {@code 0} for none. The caller reads what the last
   *               statement of the script answers, so that statement is the only one it bounds: a statement before it feeds the
   *               script and is not an answer to the caller
   */
  private ResultSet executeInternal(final List<Statement> statements, final CommandContext scriptContext, final int rowCap) {
    final ScriptExecutionPlan plan = new ScriptExecutionPlan(scriptContext);

    plan.setStatements(statements);

    List<Statement> lastRetryBlock = new ArrayList<>();
    int nestedTxLevel = 0;

    for (final Statement stm : statements) {
      stm.setOriginalStatement(stm);
      // THE USER'S QUOTA WHERE IT IS TIGHTER THAN THE STATEMENT'S OWN LIMIT, AS FOR THE SAME STATEMENT RUN ON ITS OWN: IT USED TO
      // REPLACE A LIMIT BELOW IT (ISSUE #12888). THE STATEMENTS ARE PARSED FOR THIS EXECUTION, SO NO OTHER USER SEES THE CHANGE
      stm.applyTighterProfiledLimit(database, scriptContext.getInputParameters());

      if (stm instanceof BeginStatement)
        nestedTxLevel++;

      if (nestedTxLevel <= 0) {
        final InternalExecutionPlan sub = stm.createExecutionPlan(scriptContext);
        if (rowCap > 0 && stm == statements.getLast() && (stm instanceof SelectStatement || stm instanceof MatchStatement))
          sub.applyRowCap(rowCap);
        plan.chain(sub);
      } else
        lastRetryBlock.add(stm);

      if (stm instanceof CommitStatement commitStatement) {
        if (nestedTxLevel > 0) {
          nestedTxLevel--;
          if (nestedTxLevel == 0) {

            if (commitStatement.getRetry() != null) {
              int nRetries = commitStatement.getRetry().getValue().intValue();
              // the script asks for a retry count no loop can run: refused as the statement mistake it is (issue #12809)
              if (nRetries <= 0)
                throw new CommandSQLParsingException("Invalid retry number " + nRetries);

              final RetryStep step = new RetryStep(lastRetryBlock, nRetries, commitStatement.getElseStatements(),
                  commitStatement.getElseFail(),
                  scriptContext, false);
              final RetryExecutionPlan retryPlan = new RetryExecutionPlan(scriptContext);
              retryPlan.chain(step);
              plan.chain(retryPlan);
              lastRetryBlock = new ArrayList<>();
            } else {
              lastRetryBlock = planBlock(plan, lastRetryBlock, scriptContext);
            }
          }
        } else
          throw new CommandSQLParsingException("Found COMMIT statement without a BEGIN");
      } else if (stm instanceof RollbackStatement && nestedTxLevel > 0) {
        // ISSUE #12797: A ROLLBACK ends the transaction its BEGIN started as a COMMIT does, so the block is planned as it stands.
        // Left open, the count never returned to 0 and every statement from the BEGIN on was dropped without a word
        nestedTxLevel--;
        if (nestedTxLevel == 0)
          lastRetryBlock = planBlock(plan, lastRetryBlock, scriptContext);
      }

      if (stm instanceof LetStatement letStatement) {
        scriptContext.declareScriptVariable(letStatement.getVariableName().getStringValue());
      }
    }

    if (nestedTxLevel > 0)
      throw new CommandSQLParsingException("Found BEGIN statement without a COMMIT or ROLLBACK");

    return new LocalResultSet(plan);
  }
}
