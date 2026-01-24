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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Trigger;
import com.arcadedb.schema.TriggerImpl;

/**
 * SQL Statement for CREATE TRIGGER command.
 * Syntax: CREATE TRIGGER [IF NOT EXISTS] name (BEFORE|AFTER) (CREATE|READ|UPDATE|DELETE)
 *         ON [TYPE] typeName (EXECUTE SQL 'statement' | EXECUTE JAVASCRIPT 'code' | EXECUTE JAVA 'className')
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CreateTriggerStatement extends DDLStatement {

  public Identifier name;
  public Identifier timing;      // BEFORE or AFTER
  public Identifier event;       // CREATE, READ, UPDATE, DELETE
  public Identifier typeName;
  public Identifier actionType;  // SQL, JAVASCRIPT, or JAVA
  public String actionCode;
  public boolean ifNotExists = false;

  public CreateTriggerStatement(final int id) {
    super(id);
  }

  @Override
  public void validate() throws CommandSQLParsingException {
    if (name == null || name.getStringValue() == null || name.getStringValue().trim().isEmpty()) {
      throw new CommandSQLParsingException("Trigger name is required");
    }

    if (timing == null) {
      throw new CommandSQLParsingException("Trigger timing (BEFORE/AFTER) is required");
    }

    final String timingStr = timing.getStringValue().toUpperCase();
    if (!timingStr.equals("BEFORE") && !timingStr.equals("AFTER")) {
      throw new CommandSQLParsingException("Trigger timing must be BEFORE or AFTER");
    }

    if (event == null) {
      throw new CommandSQLParsingException("Trigger event (CREATE/READ/UPDATE/DELETE) is required");
    }

    final String eventStr = event.getStringValue().toUpperCase();
    if (!eventStr.equals("CREATE") && !eventStr.equals("READ") &&
        !eventStr.equals("UPDATE") && !eventStr.equals("DELETE")) {
      throw new CommandSQLParsingException("Trigger event must be CREATE, READ, UPDATE, or DELETE");
    }

    if (typeName == null || typeName.getStringValue() == null || typeName.getStringValue().trim().isEmpty()) {
      throw new CommandSQLParsingException("Trigger type name is required");
    }

    if (actionType == null) {
      throw new CommandSQLParsingException("Trigger action type (SQL/JAVASCRIPT/JAVA) is required");
    }

    final String actionTypeStr = actionType.getStringValue().toUpperCase();
    if (!actionTypeStr.equals("SQL") && !actionTypeStr.equals("JAVASCRIPT") && !actionTypeStr.equals("JAVA")) {
      throw new CommandSQLParsingException("Trigger action type must be SQL, JAVASCRIPT, or JAVA");
    }

    if (actionCode == null || actionCode.trim().isEmpty()) {
      throw new CommandSQLParsingException("Trigger action code is required");
    }
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();

    // Validate inputs
    validate();

    // Check if trigger already exists
    if (database.getSchema().existsTrigger(name.getStringValue())) {
      if (ifNotExists) {
        final InternalResultSet rs = new InternalResultSet();
        final ResultInternal result = new ResultInternal(context.getDatabase());
        result.setProperty("operation", "create trigger");
        result.setProperty("name", name.getStringValue());
        result.setProperty("created", false);
        rs.add(result);
        return rs;
      } else {
        throw new CommandExecutionException("Trigger '" + name.getStringValue() + "' already exists");
      }
    }

    // Check if type exists
    if (!database.getSchema().existsType(typeName.getStringValue())) {
      throw new CommandExecutionException("Type '" + typeName.getStringValue() + "' does not exist");
    }

    // Parse enums
    final Trigger.TriggerTiming triggerTiming = Trigger.TriggerTiming.valueOf(timing.getStringValue().toUpperCase());
    final Trigger.TriggerEvent triggerEvent = Trigger.TriggerEvent.valueOf(event.getStringValue().toUpperCase());
    final Trigger.ActionType triggerActionType = Trigger.ActionType.valueOf(actionType.getStringValue().toUpperCase());

    // Create trigger
    final Trigger trigger = new TriggerImpl(
        name.getStringValue(),
        triggerTiming,
        triggerEvent,
        typeName.getStringValue(),
        triggerActionType,
        actionCode
    );

    // Register trigger in schema
    database.getSchema().createTrigger(trigger);

    // Return result
    final InternalResultSet rs = new InternalResultSet();
    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("operation", "create trigger");
    result.setProperty("name", name.getStringValue());
    result.setProperty("timing", triggerTiming.name());
    result.setProperty("event", triggerEvent.name());
    result.setProperty("typeName", typeName.getStringValue());
    result.setProperty("actionType", triggerActionType.name());
    result.setProperty("created", true);
    rs.add(result);
    return rs;
  }

  @Override
  public String toString() {
    return "CreateTriggerStatement{" +
        "name=" + name +
        ", timing=" + timing +
        ", event=" + event +
        ", typeName=" + typeName +
        ", actionType=" + actionType +
        ", ifNotExists=" + ifNotExists +
        '}';
  }
}
