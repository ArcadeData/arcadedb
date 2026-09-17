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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Trigger;
import com.arcadedb.schema.TriggerImpl;

import java.util.Map;

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
  /**
   * The action code's STRING_LITERAL exactly as it appeared in the source, quotes included. Rendering this instead
   * of re-quoting {@link #actionCode} guarantees an exact round-trip no matter what the body contains (same
   * approach as {@code DefineFunctionStatement.codeQuoted}).
   */
  public String actionCodeQuoted;
  public boolean ifNotExists = false;

  public CreateTriggerStatement() {
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
    if (!"BEFORE".equals(timingStr) && !"AFTER".equals(timingStr)) {
      throw new CommandSQLParsingException("Trigger timing must be BEFORE or AFTER");
    }

    if (event == null) {
      throw new CommandSQLParsingException("Trigger event (CREATE/READ/UPDATE/DELETE) is required");
    }

    final String eventStr = event.getStringValue().toUpperCase();
    if (!"CREATE".equals(eventStr) && !"READ".equals(eventStr) &&
        !"UPDATE".equals(eventStr) && !"DELETE".equals(eventStr)) {
      throw new CommandSQLParsingException("Trigger event must be CREATE, READ, UPDATE, or DELETE");
    }

    if (typeName == null || typeName.getStringValue() == null || typeName.getStringValue().trim().isEmpty()) {
      throw new CommandSQLParsingException("Trigger type name is required");
    }

    if (actionType == null) {
      throw new CommandSQLParsingException("Trigger action type (SQL/JAVASCRIPT/JAVA) is required");
    }

    final String actionTypeStr = actionType.getStringValue().toUpperCase();
    if (!"SQL".equals(actionTypeStr) && !"JAVASCRIPT".equals(actionTypeStr) && !"JAVA".equals(actionTypeStr)) {
      throw new CommandSQLParsingException("Trigger action type must be SQL, JAVASCRIPT, or JAVA");
    }

    if (actionCode == null || actionCode.trim().isEmpty()) {
      throw new CommandSQLParsingException("Trigger action code is required");
    }
  }

  /**
   * Batchable into a DDL script's bulk schema scope (issue #6990). A trigger is schema metadata; registering one visits no record.
   */
  @Override
  public boolean isBulkSchemaScopeSafe(final DatabaseInternal database) {
    return true;
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
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("CREATE TRIGGER ");
    if (ifNotExists)
      builder.append("IF NOT EXISTS ");
    name.toString(params, builder);
    builder.append(' ');
    timing.toString(params, builder);
    builder.append(' ');
    event.toString(params, builder);
    builder.append(" ON TYPE ");
    typeName.toString(params, builder);
    builder.append(" EXECUTE ");
    actionType.toString(params, builder);
    builder.append(' ').append(actionCodeQuoted);
  }

  @Override
  public CreateTriggerStatement copy() {
    final CreateTriggerStatement result = new CreateTriggerStatement();
    result.name = name == null ? null : name.copy();
    result.timing = timing == null ? null : timing.copy();
    result.event = event == null ? null : event.copy();
    result.typeName = typeName == null ? null : typeName.copy();
    result.actionType = actionType == null ? null : actionType.copy();
    result.actionCode = actionCode;
    result.actionCodeQuoted = actionCodeQuoted;
    result.ifNotExists = ifNotExists;
    return result;
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { name, timing, event, typeName, actionType, actionCode, ifNotExists };
  }
}
