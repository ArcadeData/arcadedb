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
package com.arcadedb.schema;

import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Objects;
import java.util.logging.Level;

/**
 * Implementation of the Trigger interface with JSON serialization support.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TriggerImpl implements Trigger, TypeNameDependant {
  private final String name;
  private final TriggerTiming timing;
  private final TriggerEvent event;
  // volatile, not final: a rename of the type rewrites it in place (#10096), see retargetType()
  private volatile String typeName;
  private final ActionType actionType;
  // volatile, not final: a rename of a type the body of a SQL trigger names rewrites it in place (#12941), see carryTypeRename()
  private volatile String actionCode;
  // the types the SQL body names, read off the body they were parsed from and parsed again only when the body changes
  private volatile SQLTriggerBodyTypes bodyTypes;

  public TriggerImpl(final String name, final TriggerTiming timing, final TriggerEvent event,
                     final String typeName, final ActionType actionType, final String actionCode) {
    if (name == null || name.trim().isEmpty())
      throw new IllegalArgumentException("Trigger name cannot be null or empty");
    if (timing == null)
      throw new IllegalArgumentException("Trigger timing cannot be null");
    if (event == null)
      throw new IllegalArgumentException("Trigger event cannot be null");
    if (typeName == null || typeName.trim().isEmpty())
      throw new IllegalArgumentException("Trigger type name cannot be null or empty");
    if (actionType == null)
      throw new IllegalArgumentException("Trigger action type cannot be null");
    if (actionCode == null || actionCode.trim().isEmpty())
      throw new IllegalArgumentException("Trigger action code cannot be null or empty");

    this.name = name;
    this.timing = timing;
    this.event = event;
    this.typeName = typeName;
    this.actionType = actionType;
    this.actionCode = actionCode;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public TriggerTiming getTiming() {
    return timing;
  }

  @Override
  public TriggerEvent getEvent() {
    return event;
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  /**
   * The type this trigger is bound to was renamed (issue #10096). Done in place because the listener adapter registered on the
   * type holds THIS instance, and the name is what the schema persists (a reload binds the trigger by it) and what unregistering
   * its listener looks the type up by: left on the old name, {@code DROP TRIGGER} could not find the type and the listener kept
   * firing.
   */
  void retargetType(final String newTypeName) {
    typeName = newTypeName;
  }

  /**
   * A trigger is part of its type and is dropped with it (issue #10207), so the drop of the type it is bound to is never
   * refused, whatever its body names. The drop of a type the body of a SQL trigger names is (issue #12941): the trigger would
   * fail every write to its own type from then on. A body that names no type that exists is not refused for, unlike the stored
   * names of a view: a body is not checked when the trigger is created, and it fails when it fires, not here.
   */
  @Override
  public void checkTypeDrop(final LocalSchema schema, final String typeName, final String canonicalTypeName) {
    if (canonicalTypeName.equals(TypeNameDependant.canonicalNameOf(schema, this.typeName)))
      return;
    final SQLTriggerBodyTypes body = bodyTypes(schema);
    if (body != null && body.names(schema, canonicalTypeName))
      throw new SchemaException("Cannot drop type '" + typeName + "' because the SQL body of trigger '" + name + "' names it. Drop "
          + "the trigger first with: " + dropStatement() + ", and create it again without the type if it is still needed");
  }

  /** Refuses the rename of a type the SQL body names when the body cannot be re-pointed at the new name with certainty (#12941). */
  @Override
  public void checkTypeRename(final LocalSchema schema, final String oldName, final String newName) {
    final SQLTriggerBodyTypes body = bodyTypes(schema);
    if (body != null && body.namesExactly(oldName)
        && SQLTriggerBodyTypes.retarget(schema.getDatabase(), body.body(), oldName, newName) == null)
      throw new SchemaException("Cannot rename type '" + oldName + "' to '" + newName + "' because the SQL body of trigger '" + name
          + "' names it and the body cannot be re-pointed at the new name with certainty. Drop the trigger first with: "
          + dropStatement() + ", rename the type, and create the trigger again");
  }

  /** Follows the rename to the type the trigger is bound to (#10096) and in the SQL body (#12941). */
  @Override
  public void carryTypeRename(final LocalSchema schema, final String oldName, final String newName) {
    if (oldName.equals(typeName))
      retargetType(newName);

    final SQLTriggerBodyTypes body = bodyTypes(schema);
    if (body == null || !body.namesExactly(oldName))
      return;
    final String retargeted = SQLTriggerBodyTypes.retarget(schema.getDatabase(), body.body(), oldName, newName);
    if (retargeted != null)
      actionCode = retargeted;
    else
      // ONLY A TRIGGER CREATED AFTER THE CHECK OF THE RENAME CAN GET HERE: THE FILES HAVE MOVED, SO THIS CANNOT REFUSE
      LogManager.instance().log(this, Level.WARNING,
          "The body of the SQL trigger '%s' still names the type '%s', renamed to '%s': it cannot be re-pointed with certainty, so "
              + "the trigger will fail until it is dropped and created again", name, oldName, newName);
  }

  /** The types the body names, for a SQL trigger; {@code null} for a JAVASCRIPT or a JAVA one, whose code is not analysed. */
  private SQLTriggerBodyTypes bodyTypes(final LocalSchema schema) {
    if (actionType != ActionType.SQL)
      return null;
    final String code = actionCode;
    SQLTriggerBodyTypes parsed = bodyTypes;
    if (parsed == null || !parsed.body().equals(code)) {
      parsed = SQLTriggerBodyTypes.of(schema.getDatabase(), code);
      bodyTypes = parsed;
    }
    return parsed;
  }

  private String dropStatement() {
    return "DROP TRIGGER " + name;
  }

  @Override
  public ActionType getActionType() {
    return actionType;
  }

  @Override
  public String getActionCode() {
    return actionCode;
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("name", name);
    json.put("timing", timing.name());
    json.put("event", event.name());
    json.put("typeName", typeName);
    json.put("actionType", actionType.name());
    json.put("actionCode", actionCode);
    return json;
  }

  /**
   * Create a TriggerImpl from JSON representation.
   */
  public static TriggerImpl fromJSON(final JSONObject json) {
    return new TriggerImpl(
        json.getString("name"),
        TriggerTiming.valueOf(json.getString("timing")),
        TriggerEvent.valueOf(json.getString("event")),
        json.getString("typeName"),
        ActionType.valueOf(json.getString("actionType")),
        json.getString("actionCode")
    );
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final TriggerImpl trigger = (TriggerImpl) o;
    return Objects.equals(name, trigger.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return "Trigger{" +
        "name='" + name + '\'' +
        ", timing=" + timing +
        ", event=" + event +
        ", typeName='" + typeName + '\'' +
        ", actionType=" + actionType +
        ", actionCode='" + actionCode + '\'' +
        '}';
  }
}
