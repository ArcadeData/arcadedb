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

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.misc.AbstractSQLMethod;
import com.arcadedb.utility.FileUtils;

/**
 * Appends strings. Acts as a concatenation.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodAppend extends AbstractSQLMethod {

  public static final String NAME = "append";

  public SQLMethodAppend() {
    super(NAME, 1, -1);
  }

  @Override
  public String getSyntax() {
    return "append([<value|expression|field>]*)";
  }

  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, CommandContext iContext,
      Object ioResult, Object[] iParams) {
    if (iThis == null || iParams[0] == null)
      return iThis;

    final StringBuilder buffer = new StringBuilder(iThis.toString());
    for (int i = 0; i < iParams.length; ++i) {
      if (iParams[i] != null) {
        buffer.append(FileUtils.getStringContent(iParams[i]));
      }
    }

    return buffer.toString();
  }

}
