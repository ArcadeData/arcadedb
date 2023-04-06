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
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Transforms a string in a RID, if valid, otherwise null.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodAsRID extends AbstractSQLMethod {

  public static final String NAME = "asrid";

  public SQLMethodAsRID() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final CommandContext iContext, Object ioResult, final Object[] iParams) {
    if (ioResult != null) {
      if (ioResult instanceof Identifiable)
        return ((Identifiable) ioResult).getIdentity();
      else if (ioResult instanceof String && RID.is(ioResult))
        return new RID(iContext != null ? iContext.getDatabase() : null, (String) ioResult);
    }
    return null;
  }
}
