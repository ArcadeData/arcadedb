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
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Map;

/**
 * Converts a document in JSON string.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodAsJSON extends AbstractSQLMethod {

  public static final String NAME = "asjson";

  public SQLMethodAsJSON() {
    super(NAME, 0, 0);
  }

  // TEMPORARY TO SUPPORT DEPRECATION OF LEGACY TOJSON METHOD
  protected SQLMethodAsJSON(String name) {
    super(name, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "asJSON()";
  }

  @Override
  public Object execute(final Object me, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {
    if (me == null)
      return null;

    if (me instanceof Result result)
      return result.toJSON();
    else if (me instanceof Document document)
      return document.toJSON();
    else if (me instanceof Map map)
      return new JSONObject(map);
    else if (me instanceof String string) {
      if (string.isEmpty())
        return new JSONObject();
      try {
        if (string.charAt(0) == '[')
          return new JSONArray(string);
        else
          return new JSONObject(string);
      } catch (final JSONException e) {
        // a text that is no JSON is the caller's receiver: VALIDATION on every protocol, where only HTTP answered it as the
        // caller's mistake and every other wire as a server fault (issue #12020)
        throw new InvalidArgumentException(getName() + "(): " + e.getMessage(), e);
      }
    } else if (MultiValue.isMultiValue(me)) {
      final JSONArray json = new JSONArray();
      for (final Object o : MultiValue.getMultiValueIterable(me, false))
        // An element this method has no conversion for (a number, a boolean, a link, a date...) is written the way the value of
        // a map entry is: answering null for it turned [1,2] into [null,null] and dropped the links of a list (issue #12835)
        json.put(hasConversion(o) ? execute(o, currentRecord, context, params) : o);
      return json;
    }

    return null;
  }

  /**
   * Whether {@link #execute} converts the value itself, so the branches stay the single list of what it handles.
   */
  private static boolean hasConversion(final Object value) {
    return value instanceof Result || value instanceof Document || value instanceof Map || value instanceof String
        || MultiValue.isMultiValue(value);
  }
}
