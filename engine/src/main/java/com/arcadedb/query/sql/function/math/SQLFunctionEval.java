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

  public Object execute(Object self, final Identifiable record, final Object currentResult, final Object[] params, CommandContext context) {
    if (params.length < 1)
      throw new CommandExecutionException("invalid expression");

    if (predicate == null) {
      try (final ByteArrayInputStream is = new ByteArrayInputStream(params[0].toString().getBytes())) {
        predicate = new SqlParser(context.getDatabase(), is).ParseCondition();
      } catch (IOException e) {
        throw new CommandSQLParsingException("Error on parsing expression in eval() function", e);
      } catch (ParseException e) {
        throw new CommandSQLParsingException("Error on parsing expression for the eval()", e);
      }

    }
    return predicate.matchesFilters(record, context);
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
