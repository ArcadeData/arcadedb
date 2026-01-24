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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Represents a database trigger that executes SQL or script code in response to record events.
 * Triggers can be registered to fire BEFORE or AFTER CREATE, READ, UPDATE, or DELETE operations.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface Trigger {

  /**
   * Timing of trigger execution relative to the event.
   */
  enum TriggerTiming {
    BEFORE,
    AFTER
  }

  /**
   * Database event that triggers execution.
   */
  enum TriggerEvent {
    CREATE,
    READ,
    UPDATE,
    DELETE
  }

  /**
   * Type of action to execute when trigger fires.
   */
  enum ActionType {
    SQL,
    JAVASCRIPT,
    JAVA
  }

  /**
   * Get the unique name of this trigger.
   */
  String getName();

  /**
   * Get the timing (BEFORE or AFTER) of this trigger.
   */
  TriggerTiming getTiming();

  /**
   * Get the event (CREATE, READ, UPDATE, DELETE) that fires this trigger.
   */
  TriggerEvent getEvent();

  /**
   * Get the name of the type this trigger applies to.
   */
  String getTypeName();

  /**
   * Get the type of action (SQL or JAVASCRIPT) this trigger executes.
   */
  ActionType getActionType();

  /**
   * Get the action code (SQL statement or JavaScript script).
   */
  String getActionCode();

  /**
   * Serialize this trigger to JSON for schema persistence.
   */
  JSONObject toJSON();
}
