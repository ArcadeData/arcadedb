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

import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.*;

/**
 * @author Christian Himpe
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodSort extends AbstractSQLMethod {

  public static final String NAME = "sort";

  public SQLMethodSort() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final CommandContext iContext, final Object ioResult, final Object[] iParams) {

    if (ioResult != null && ioResult instanceof List) {
      List<Object> result = new ArrayList((List) ioResult);
      if (iParams != null && iParams.length > 0 && iParams[0] != null && iParams[0] instanceof Boolean && !((Boolean) iParams[0]))
        result.sort((left, right) -> BinaryComparator.compareTo(right, left));
      else
        result.sort((left,right) -> BinaryComparator.compareTo(left, right));
      return result;
    } else {
      return ioResult;
    }
  }

  @Override
  public String getSyntax() {
    return "sort(<bool>)";
  }
}
