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
package com.arcadedb.query.sql.function.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.SQLFunctionFiltered;
import com.arcadedb.utility.FileUtils;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public abstract class SQLFunctionMoveFiltered extends SQLFunctionMove implements SQLFunctionFiltered {

  protected static int supernodeThreshold = 1000; // move to some configuration

  protected SQLFunctionMoveFiltered(final String name) {
    super(name);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable currentRecord, final Object currentResult,
      final Object[] iParameters, final Iterable<Identifiable> iPossibleResults, final CommandContext context) {
    final String[] labels;
    if (iParameters != null && iParameters.length > 0 && iParameters[0] != null)
      labels = MultiValue.array(iParameters, String.class, FileUtils::getStringContent);
    else
      labels = null;

    return SQLQueryEngine.foreachRecord(iArgument -> move(context.getDatabase(), iArgument, labels, iPossibleResults), iThis,
        context);
  }

  protected abstract Object move(Database graph, Identifiable iArgument, String[] labels, Iterable<Identifiable> iPossibleResults);

}
