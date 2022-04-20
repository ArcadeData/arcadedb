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
package com.arcadedb.query.sql.function;

import com.arcadedb.query.sql.function.coll.SQLFunctionDifference;
import com.arcadedb.query.sql.function.coll.SQLFunctionFirst;
import com.arcadedb.query.sql.function.coll.SQLFunctionIntersect;
import com.arcadedb.query.sql.function.coll.SQLFunctionLast;
import com.arcadedb.query.sql.function.coll.SQLFunctionList;
import com.arcadedb.query.sql.function.coll.SQLFunctionMap;
import com.arcadedb.query.sql.function.coll.SQLFunctionSet;
import com.arcadedb.query.sql.function.coll.SQLFunctionSymmetricDifference;
import com.arcadedb.query.sql.function.coll.SQLFunctionUnionAll;
import com.arcadedb.query.sql.function.geo.SQLFunctionDistance;
import com.arcadedb.query.sql.function.graph.SQLFunctionAstar;
import com.arcadedb.query.sql.function.graph.SQLFunctionBoth;
import com.arcadedb.query.sql.function.graph.SQLFunctionBothE;
import com.arcadedb.query.sql.function.graph.SQLFunctionBothV;
import com.arcadedb.query.sql.function.graph.SQLFunctionDijkstra;
import com.arcadedb.query.sql.function.graph.SQLFunctionIn;
import com.arcadedb.query.sql.function.graph.SQLFunctionInE;
import com.arcadedb.query.sql.function.graph.SQLFunctionInV;
import com.arcadedb.query.sql.function.graph.SQLFunctionOut;
import com.arcadedb.query.sql.function.graph.SQLFunctionOutE;
import com.arcadedb.query.sql.function.graph.SQLFunctionOutV;
import com.arcadedb.query.sql.function.graph.SQLFunctionShortestPath;
import com.arcadedb.query.sql.function.math.SQLFunctionAbsoluteValue;
import com.arcadedb.query.sql.function.math.SQLFunctionAverage;
import com.arcadedb.query.sql.function.math.SQLFunctionMax;
import com.arcadedb.query.sql.function.math.SQLFunctionMin;
import com.arcadedb.query.sql.function.math.SQLFunctionSum;
import com.arcadedb.query.sql.function.misc.SQLFunctionCoalesce;
import com.arcadedb.query.sql.function.misc.SQLFunctionCount;
import com.arcadedb.query.sql.function.misc.SQLFunctionDate;
import com.arcadedb.query.sql.function.misc.SQLFunctionDecode;
import com.arcadedb.query.sql.function.misc.SQLFunctionEncode;
import com.arcadedb.query.sql.function.misc.SQLFunctionIf;
import com.arcadedb.query.sql.function.misc.SQLFunctionIfNull;
import com.arcadedb.query.sql.function.misc.SQLFunctionStrcmpci;
import com.arcadedb.query.sql.function.misc.SQLFunctionSysdate;
import com.arcadedb.query.sql.function.misc.SQLFunctionUUID;
import com.arcadedb.query.sql.function.stat.SQLFunctionMedian;
import com.arcadedb.query.sql.function.stat.SQLFunctionMode;
import com.arcadedb.query.sql.function.stat.SQLFunctionPercentile;
import com.arcadedb.query.sql.function.stat.SQLFunctionStandardDeviation;
import com.arcadedb.query.sql.function.stat.SQLFunctionVariance;
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
    register(SQLFunctionStrcmpci.NAME, SQLFunctionStrcmpci.class);
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
