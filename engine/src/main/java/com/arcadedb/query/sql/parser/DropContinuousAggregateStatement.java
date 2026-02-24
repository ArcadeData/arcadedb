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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

public class DropContinuousAggregateStatement extends DDLStatement {
  public Identifier name;
  public boolean ifExists = false;

  public DropContinuousAggregateStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();
    final String caName = name.getStringValue();

    if (!database.getSchema().existsContinuousAggregate(caName)) {
      if (ifExists) {
        final InternalResultSet result = new InternalResultSet();
        final ResultInternal r = new ResultInternal();
        r.setProperty("operation", "drop continuous aggregate");
        r.setProperty("name", caName);
        r.setProperty("dropped", false);
        result.add(r);
        return result;
      }
      throw new CommandExecutionException("Continuous aggregate '" + caName + "' does not exist");
    }

    database.getSchema().dropContinuousAggregate(caName);

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "drop continuous aggregate");
    r.setProperty("name", caName);
    r.setProperty("dropped", true);
    result.add(r);
    return result;
  }

  @Override
  public String toString() {
    return "DROP CONTINUOUS AGGREGATE " + (ifExists ? "IF EXISTS " : "") + name;
  }
}
