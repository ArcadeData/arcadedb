/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.serializer;

import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class JsonSerializer {

  public JSONObject serializeRecord(final Document record) {
    final JSONObject object = new JSONObject();

    for (String p : record.getPropertyNames()) {
      Object value = record.get(p);

      if (value instanceof Document)
        value = serializeRecord((Document) value);
      else if (value instanceof Collection) {
        final List<Object> list = new ArrayList<>();
        for (Object o : (Collection) value) {
          if (o instanceof Document)
            o = serializeRecord((Document) o);
          list.add(o);
        }
        value = list;
      }
      object.put(p, value);
    }

    return object;
  }

  public JSONObject serializeResult(final Result record) {
    final JSONObject object = new JSONObject();

    for (String p : record.getPropertyNames()) {
      Object value = record.getProperty(p);

      if (value instanceof Document)
        value = serializeRecord((Document) value);
      else if (value instanceof Result)
        value = serializeResult((Result) value);
      else if (value instanceof Collection) {
        final List<Object> list = new ArrayList<>();
        for (Object o : (Collection) value) {
          if (o instanceof Document)
            o = serializeRecord((Document) o);
          else if (o instanceof Result)
            o = serializeResult((Result) o);
          list.add(o);
        }
        value = list;
      }
      object.put(p, value);
    }

    return object;
  }
}
