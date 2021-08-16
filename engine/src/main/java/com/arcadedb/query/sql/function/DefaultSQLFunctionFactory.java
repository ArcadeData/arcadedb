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
package com.arcadedb.query.sql.function;

import com.arcadedb.query.sql.function.coll.*;
import com.arcadedb.query.sql.function.geo.SQLFunctionDistance;
import com.arcadedb.query.sql.function.graph.*;
import com.arcadedb.query.sql.function.math.*;
import com.arcadedb.query.sql.function.misc.*;
import com.arcadedb.query.sql.function.stat.*;
import com.arcadedb.query.sql.function.text.SQLFunctionConcat;
import com.arcadedb.query.sql.function.text.SQLFunctionFormat;

/**
 * Default set of SQL function.
 */
public final class DefaultSQLFunctionFactory extends SQLFunctionFactoryTemplate {
  private final SQLFunctionReflectionFactory reflectionFactory;

  public DefaultSQLFunctionFactory() {
    // MISC FUNCTIONS
    register(SQLFunctionAverage.NAME, SQLFunctionAverage.class);
    register(SQLFunctionCoalesce.NAME, new SQLFunctionCoalesce());
    register(SQLFunctionCount.NAME, SQLFunctionCount.class);
    register(SQLFunctionDate.NAME, SQLFunctionDate.class);
    register(SQLFunctionDecode.NAME, new SQLFunctionDecode());
    register(SQLFunctionDifference.NAME, SQLFunctionDifference.class);
    register(SQLFunctionSymmetricDifference.NAME, SQLFunctionSymmetricDifference.class);
    register(SQLFunctionDistance.NAME, new SQLFunctionDistance());
    register(SQLFunctionDistinct.NAME, SQLFunctionDistinct.class);
    register(SQLFunctionEncode.NAME, new SQLFunctionEncode());
    register(SQLFunctionFirst.NAME, new SQLFunctionFirst());
    register(SQLFunctionFormat.NAME, new SQLFunctionFormat());
    register(SQLFunctionIf.NAME, new SQLFunctionIf());
    register(SQLFunctionIfNull.NAME, new SQLFunctionIfNull());
    register(SQLFunctionIntersect.NAME, SQLFunctionIntersect.class);
    register(SQLFunctionLast.NAME, new SQLFunctionLast());
    register(SQLFunctionList.NAME, SQLFunctionList.class);
    register(SQLFunctionMap.NAME, SQLFunctionMap.class);
    register(SQLFunctionMax.NAME, SQLFunctionMax.class);
    register(SQLFunctionMin.NAME, SQLFunctionMin.class);
    register(SQLFunctionSet.NAME, SQLFunctionSet.class);
    register(SQLFunctionSysdate.NAME, SQLFunctionSysdate.class);
    register(SQLFunctionSum.NAME, SQLFunctionSum.class);
    register(SQLFunctionUnionAll.NAME, SQLFunctionUnionAll.class);
    register(SQLFunctionMode.NAME, SQLFunctionMode.class);
    register(SQLFunctionPercentile.NAME, SQLFunctionPercentile.class);
    register(SQLFunctionMedian.NAME, SQLFunctionMedian.class);
    register(SQLFunctionVariance.NAME, SQLFunctionVariance.class);
    register(SQLFunctionStandardDeviation.NAME, SQLFunctionStandardDeviation.class);
    register(SQLFunctionUUID.NAME, SQLFunctionUUID.class);
    register(SQLFunctionConcat.NAME, SQLFunctionConcat.class);
    register(SQLFunctionAbsoluteValue.NAME, SQLFunctionAbsoluteValue.class);
    //graph
    register(SQLFunctionOut.NAME, SQLFunctionOut.class);
    register(SQLFunctionIn.NAME, SQLFunctionIn.class);
    register(SQLFunctionBoth.NAME, SQLFunctionBoth.class);
    register(SQLFunctionOutE.NAME, SQLFunctionOutE.class);
    register(SQLFunctionOutV.NAME, SQLFunctionOutV.class);
    register(SQLFunctionInE.NAME, SQLFunctionInE.class);
    register(SQLFunctionInV.NAME, SQLFunctionInV.class);
    register(SQLFunctionBothE.NAME, SQLFunctionBothE.class);
    register(SQLFunctionBothV.NAME, SQLFunctionBothV.class);
    register(SQLFunctionShortestPath.NAME, SQLFunctionShortestPath.class);
    register(SQLFunctionDijkstra.NAME, SQLFunctionDijkstra.class);
    register(SQLFunctionAstar.NAME, SQLFunctionAstar.class);

    reflectionFactory = new SQLFunctionReflectionFactory(this);
  }

  public SQLFunctionReflectionFactory getReflectionFactory() {
    return reflectionFactory;
  }
}
