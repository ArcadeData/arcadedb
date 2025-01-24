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
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.*;

/**
 * Returns argument if result is empty else return result.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodIfEmpty extends AbstractSQLMethod {

  public static final String NAME = "ifempty";

  public SQLMethodIfEmpty() {
    super(NAME);
  }

  @Override
  public String getSyntax() {
    return "Syntax error: ifempty(<return_value_if_empty>)";
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context, final Object[] params) {

    if ( (value instanceof String && value.toString().length() == 0)
         || (value instanceof Collection<?> && MultiValue.getSize(value) == 0) )
      return params[0];
    else
      return value;
  }
}
