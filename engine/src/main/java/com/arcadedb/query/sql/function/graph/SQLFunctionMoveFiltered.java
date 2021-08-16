/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.query.sql.function.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.SQLEngine;
import com.arcadedb.query.sql.executor.SQLFunctionFiltered;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public abstract class SQLFunctionMoveFiltered extends SQLFunctionMove implements SQLFunctionFiltered {

  protected static int supernodeThreshold = 1000; // move to some configuration

  public SQLFunctionMoveFiltered() {
    super(NAME, 1, 2);
  }

  public SQLFunctionMoveFiltered(final String iName, final int iMin, final int iMax) {
    super(iName, iMin, iMax);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParameters, final Iterable<Identifiable> iPossibleResults, final CommandContext iContext) {
    final String[] labels;
    if (iParameters != null && iParameters.length > 0 && iParameters[0] != null)
      labels = MultiValue.array(iParameters, String.class, new Callable<Object, Object>() {

        @Override
        public Object call(final Object iArgument) {
          return FileUtils.getStringContent(iArgument);
        }
      });
    else
      labels = null;

    return SQLEngine.foreachRecord(new Callable<Object, Identifiable>() {
      @Override
      public Object call(final Identifiable iArgument) {
        return move(iContext.getDatabase(), iArgument, labels, iPossibleResults);
      }
    }, iThis, iContext);

  }

  protected abstract Object move(Database graph, Identifiable iArgument, String[] labels,
      Iterable<Identifiable> iPossibleResults);

}
