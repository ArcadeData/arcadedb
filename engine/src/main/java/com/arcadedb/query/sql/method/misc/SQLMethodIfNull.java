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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

/**
 * Returns argument if result is null else return result.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodIfNull extends AbstractSQLMethod {

  public static final String NAME = "ifnull";

  public SQLMethodIfNull() {
    super(NAME);
  }

  @Override
  public String getSyntax() {
    return "Syntax error: ifnull(<return_value_if_null>)";
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
    /*
     * iFuncParams [0] field/value to check for null [1] return value if [0] is null [2] optional return value if [0] is not null
     */
    if (value == null)
      return params[0];
    else
      return value;
  }
}
