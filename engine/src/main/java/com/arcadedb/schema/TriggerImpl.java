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

import java.util.Objects;

/**
 * Implementation of the Trigger interface with JSON serialization support.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TriggerImpl implements Trigger {
  private final String name;
  private final TriggerTiming timing;
  private final TriggerEvent event;
  private final String typeName;
  private final ActionType actionType;
  private final String actionCode;

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
