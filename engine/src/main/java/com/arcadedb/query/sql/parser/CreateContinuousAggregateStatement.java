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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Map;

public class CreateContinuousAggregateStatement extends DDLStatement {
  public Identifier name;
  public SelectStatement selectStatement;
  public boolean ifNotExists = false;

  public CreateContinuousAggregateStatement() {
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();
    final String caName = name.getStringValue();

    database.getSchema().buildContinuousAggregate()
        .withName(caName)
        .withQuery(selectStatement.toString())
        .withIgnoreIfExists(ifNotExists)
        .create();

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "create continuous aggregate");
    r.setProperty("name", caName);
    result.add(r);
    return result;
  }

  /**
   * Overrides the two-arg form (not just the no-arg debug one) so this renders as SQL wherever a statement is
   * rendered through {@code toString(Map, StringBuilder)} - an enclosing {@code IF}/script block included - instead
   * of throwing {@code UnsupportedOperationException}, and so a parameterised sub-select renders BOUND rather than
   * with its placeholders raw. Same class of bug as issue #7794 on {@code CREATE TRIGGER} (issue #7912).
   */
  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("CREATE CONTINUOUS AGGREGATE ");
    if (ifNotExists)
      builder.append("IF NOT EXISTS ");
    name.toString(params, builder);
    builder.append(" AS ");
    selectStatement.toString(params, builder);
  }

  @Override
  public CreateContinuousAggregateStatement copy() {
    final CreateContinuousAggregateStatement result = new CreateContinuousAggregateStatement();
    result.name = name == null ? null : name.copy();
    result.selectStatement = selectStatement == null ? null : selectStatement.copy();
    result.ifNotExists = ifNotExists;
    return result;
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { name, selectStatement, ifNotExists };
  }
}
