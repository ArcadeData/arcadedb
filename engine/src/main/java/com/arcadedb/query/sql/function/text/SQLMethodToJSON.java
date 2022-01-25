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
package com.arcadedb.query.sql.function.text;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.method.misc.AbstractSQLMethod;
import org.json.JSONObject;

import java.util.Map;

/**
 * Converts a document in JSON string.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodToJSON extends AbstractSQLMethod {

  public static final String NAME = "tojson";

  public SQLMethodToJSON() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "toJSON()";
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, CommandContext iContext, Object ioResult,
      Object[] iParams) {
    if (iThis == null)
      return null;

    if (iThis instanceof Document) {

      return ((Document) iThis).toJSON();

    } else if (iThis instanceof Map) {

      return new JSONObject(iThis);

    } else if (MultiValue.isMultiValue(iThis)) {

      StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      for (Object o : MultiValue.getMultiValueIterable(iThis, false)) {
        if (!first) {
          builder.append(",");
        }
        builder.append(execute(o, iCurrentRecord, iContext, ioResult, iParams));
        first = false;
      }

      builder.append("]");
      return builder.toString();
    }
    return null;
  }
}
