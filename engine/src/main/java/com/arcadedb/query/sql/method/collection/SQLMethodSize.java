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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodSize extends AbstractSQLMethod {

  public static final String NAME = "size";

  public SQLMethodSize() {
    super(NAME);
  }

  @Override
  public Object execute(final Object value, final Identifiable iCurrentRecord, final CommandContext iContext,
      final Object[] iParams) {

    final Number size;
    if (value != null) {
      if (value instanceof Result result)
        size = result.getRecord().isPresent() ? result.getRecord().get().size() : -1;
      else if (value instanceof Identifiable record)
        size = record.getRecord(true).size();
      else if (value instanceof String)
        size = value.toString().length();
      else
        size = MultiValue.getSize(value);

    } else {
      size = 0;
    }

    return size;
  }
}
