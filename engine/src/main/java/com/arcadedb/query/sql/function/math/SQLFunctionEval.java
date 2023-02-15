/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.arcadedb.query.sql.function.math;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.parser.ParseException;
import com.arcadedb.query.sql.parser.SqlParser;
import com.arcadedb.query.sql.parser.WhereClause;

import java.io.*;

/**
 * Evaluates a complex expression.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionEval extends SQLFunctionMathAbstract {
  public static final String NAME = "eval";

  private WhereClause predicate;

  public SQLFunctionEval() {
    super(NAME);
  }

  public Object execute(Object iThis, final Identifiable iRecord, final Object iCurrentResult, final Object[] iParams, CommandContext iContext) {
    if (iParams.length < 1)
      throw new CommandExecutionException("invalid expression");

    if (predicate == null) {
      try (final ByteArrayInputStream is = new ByteArrayInputStream(iParams[0].toString().getBytes())) {
        predicate = new SqlParser(iContext.getDatabase(), is).ParseCondition();
      } catch (IOException e) {
        throw new CommandSQLParsingException("Error on parsing expression in eval() function", e);
      } catch (ParseException e) {
        throw new CommandSQLParsingException("Error on parsing expression for the eval()", e);
      }

    }
    return predicate.matchesFilters(iRecord, iContext);
  }

  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "eval(<expression>)";
  }

  @Override
  public Object getResult() {
    return null;
  }
}
