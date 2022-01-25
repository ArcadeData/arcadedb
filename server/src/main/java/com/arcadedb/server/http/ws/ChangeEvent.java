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
package com.arcadedb.server.http.ws;

import com.arcadedb.database.Record;
import org.json.JSONObject;

public class ChangeEvent {
  private final TYPE   type;
  private final Record record;

  public enum TYPE {CREATE, UPDATE, DELETE}

  public ChangeEvent(final TYPE type, final Record record) {
    this.type = type;
    this.record = record;
  }

  public Record getRecord() {
    return record;
  }

  public TYPE getType() {
    return type;
  }

  public String toJSON() {
    final var jsonObject = new JSONObject();
    jsonObject.put("changeType", this.type.toString().toLowerCase());
    jsonObject.put("record", this.record.toJSON());
    jsonObject.put("database", this.record.getDatabase().getName());
    return jsonObject.toString();
  }

  @Override
  public String toString() {
    return toJSON();
  }
}
