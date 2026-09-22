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

import java.util.ArrayList;
import java.util.List;

/**
 * Returns the record from an Identifiable (a RID).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodAsRecord extends AbstractSQLMethod {

  public static final String NAME = "asrecord";

  public SQLMethodAsRecord() {
    super(NAME);
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
    return getRecord(value, context);
  }

  private Object getRecord(final Object obj, final CommandContext context) {
    if (obj != null) {
      if (obj instanceof Identifiable identifiable)
        return identifiable.getRecord();
      else if (obj instanceof String string && RID.is(obj))
        return RID.create(context != null ? context.getDatabase() : null, string).getRecord();

      // A List, AN ARRAY (split(), A JSON ARRAY PARAMETER), OR ANY OTHER COLLECTION-SHAPED RECEIVER: BUILD A NEW
      // LIST RATHER THAN REWRITE THE CALLER'S OWN List IN PLACE, WHICH THREW ON AN IMMUTABLE List AND MUTATED A
      // VALUE THE CALLER MAY STILL HOLD (ISSUE #7877)
      final List<Object> list = listReceiverOrNull(obj);
      if (list != null) {
        final List<Object> result = new ArrayList<>(list.size());
        for (final Object item : list)
          result.add(getRecord(item, context));
        return result;
      }
    }
    return null;
  }
}
