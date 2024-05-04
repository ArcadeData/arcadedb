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

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.*;

/**
 * Returns the record from an Identifiable (a RID).
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodAsRecord extends AbstractSQLMethod {

  public static final String NAME = "asrecord";

  public SQLMethodAsRecord() {
    super(NAME);
  }

  @Override
  public Object execute(final Object value, final Identifiable iCurrentRecord, final CommandContext context, final Object[] iParams) {
    return getRecord(value, context);
  }

  private Object getRecord(final Object obj, final CommandContext context) {
    if (obj != null) {
      if (obj instanceof Identifiable)
        return ((Identifiable) obj).getRecord();
      else if (obj instanceof List) {
        // CHANGE THE LIST CONTENT
        final List list = (List) obj;
        for (int i = 0; i < list.size(); i++)
          list.set(i, getRecord(list.get(i), context));
        return list;

      } else if (obj instanceof String && RID.is(obj))
        return new RID(context != null ? context.getDatabase() : null, (String) obj).getRecord();
    }
    return null;
  }
}
