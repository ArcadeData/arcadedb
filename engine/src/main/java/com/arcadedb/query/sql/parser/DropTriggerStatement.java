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

import java.util.Map;
import java.util.Objects;

/**
 * SQL Statement for DROP TRIGGER command.
 * Syntax: DROP TRIGGER [IF EXISTS] name
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DropTriggerStatement extends DDLStatement {

  public Identifier name;
  public boolean ifExists = false;

  public DropTriggerStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final InternalResultSet rs = new InternalResultSet();
    final Database db = context.getDatabase();

    if (!db.getSchema().existsTrigger(name.getValue()) && !ifExists) {
      throw new CommandExecutionException("Trigger not found: " + name.getValue());
    }

    if (db.getSchema().existsTrigger(name.getValue())) {
      db.getSchema().dropTrigger(name.getValue());

      final ResultInternal result = new ResultInternal();
      result.setProperty("operation", "drop trigger");
      result.setProperty("triggerName", name.getValue());
      result.setProperty("dropped", true);
      rs.add(result);
    } else {
      final ResultInternal result = new ResultInternal();
      result.setProperty("operation", "drop trigger");
      result.setProperty("triggerName", name.getValue());
      result.setProperty("dropped", false);
      rs.add(result);
    }

    return rs;
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("DROP TRIGGER ");
    if (ifExists) {
      builder.append("IF EXISTS ");
    }
    name.toString(params, builder);
  }

  @Override
  public DropTriggerStatement copy() {
    final DropTriggerStatement result = new DropTriggerStatement(-1);
    result.name = name == null ? null : name.copy();
    result.ifExists = ifExists;
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final DropTriggerStatement that = (DropTriggerStatement) o;

    if (ifExists != that.ifExists)
      return false;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    int result = (ifExists ? 1 : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
