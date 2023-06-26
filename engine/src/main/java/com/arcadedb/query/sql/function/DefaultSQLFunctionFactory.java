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

// Collections

import com.arcadedb.query.sql.function.coll.SQLFunctionDifference;
import com.arcadedb.query.sql.function.coll.SQLFunctionFirst;
import com.arcadedb.query.sql.function.coll.SQLFunctionIntersect;
import com.arcadedb.query.sql.function.coll.SQLFunctionLast;
import com.arcadedb.query.sql.function.coll.SQLFunctionList;
import com.arcadedb.query.sql.function.coll.SQLFunctionMap;
import com.arcadedb.query.sql.function.coll.SQLFunctionSet;
import com.arcadedb.query.sql.function.coll.SQLFunctionSymmetricDifference;
import com.arcadedb.query.sql.function.coll.SQLFunctionUnionAll;
import com.arcadedb.query.sql.function.geo.SQLFunctionCircle;
import com.arcadedb.query.sql.function.geo.SQLFunctionDistance;
import com.arcadedb.query.sql.function.geo.SQLFunctionLineString;
import com.arcadedb.query.sql.function.geo.SQLFunctionPoint;
import com.arcadedb.query.sql.function.geo.SQLFunctionPolygon;
import com.arcadedb.query.sql.function.geo.SQLFunctionRectangle;
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
import com.arcadedb.query.sql.function.math.SQLFunctionCount;
import com.arcadedb.query.sql.function.math.SQLFunctionEval;
import com.arcadedb.query.sql.function.math.SQLFunctionMax;
import com.arcadedb.query.sql.function.math.SQLFunctionMedian;
import com.arcadedb.query.sql.function.math.SQLFunctionMin;
import com.arcadedb.query.sql.function.math.SQLFunctionMode;
import com.arcadedb.query.sql.function.math.SQLFunctionPercentile;
import com.arcadedb.query.sql.function.math.SQLFunctionRandomInt;
import com.arcadedb.query.sql.function.math.SQLFunctionSquareRoot;
import com.arcadedb.query.sql.function.math.SQLFunctionStandardDeviation;
import com.arcadedb.query.sql.function.math.SQLFunctionSum;
import com.arcadedb.query.sql.function.math.SQLFunctionVariance;
import com.arcadedb.query.sql.function.misc.SQLFunctionBoolAnd;
import com.arcadedb.query.sql.function.misc.SQLFunctionBoolOr;
import com.arcadedb.query.sql.function.misc.SQLFunctionCoalesce;
import com.arcadedb.query.sql.function.misc.SQLFunctionDecode;
import com.arcadedb.query.sql.function.misc.SQLFunctionEncode;
import com.arcadedb.query.sql.function.misc.SQLFunctionIf;
import com.arcadedb.query.sql.function.misc.SQLFunctionIfNull;
import com.arcadedb.query.sql.function.misc.SQLFunctionUUID;
import com.arcadedb.query.sql.function.text.SQLFunctionConcat;
import com.arcadedb.query.sql.function.text.SQLFunctionFormat;
import com.arcadedb.query.sql.function.text.SQLFunctionStrcmpci;
import com.arcadedb.query.sql.function.time.SQLFunctionDate;
import com.arcadedb.query.sql.function.time.SQLFunctionDuration;
import com.arcadedb.query.sql.function.time.SQLFunctionSysdate;

/**
 * Default set of SQL function.
 */
public final class DefaultSQLFunctionFactory extends SQLFunctionFactoryTemplate {
  private final SQLFunctionReflectionFactory reflectionFactory;

  public DefaultSQLFunctionFactory() {
    // Collection
    register(SQLFunctionDifference.NAME, SQLFunctionDifference.class);
    register(SQLFunctionFirst.NAME, new SQLFunctionFirst());
    register(SQLFunctionIntersect.NAME, SQLFunctionIntersect.class);
    register(SQLFunctionLast.NAME, new SQLFunctionLast());
    register(SQLFunctionList.NAME, SQLFunctionList.class);
    register(SQLFunctionMap.NAME, SQLFunctionMap.class);
    register(SQLFunctionSet.NAME, SQLFunctionSet.class);
    register(SQLFunctionSymmetricDifference.NAME, SQLFunctionSymmetricDifference.class);
    register(SQLFunctionUnionAll.NAME, SQLFunctionUnionAll.class);

    // Geo
    register(SQLFunctionCircle.NAME, new SQLFunctionCircle());
    register(SQLFunctionDistance.NAME, new SQLFunctionDistance());
    register(SQLFunctionLineString.NAME, new SQLFunctionLineString());
    register(SQLFunctionPoint.NAME, new SQLFunctionPoint());
    register(SQLFunctionPolygon.NAME, new SQLFunctionPolygon());
    register(SQLFunctionRectangle.NAME, new SQLFunctionRectangle());

    // Graph
    register(SQLFunctionAstar.NAME, SQLFunctionAstar.class);
    register(SQLFunctionBoth.NAME, SQLFunctionBoth.class);
    register(SQLFunctionBothE.NAME, SQLFunctionBothE.class);
    register(SQLFunctionBothV.NAME, SQLFunctionBothV.class);
    register(SQLFunctionDijkstra.NAME, SQLFunctionDijkstra.class);
    register(SQLFunctionIn.NAME, SQLFunctionIn.class);
    register(SQLFunctionInE.NAME, SQLFunctionInE.class);
    register(SQLFunctionInV.NAME, SQLFunctionInV.class);
    register(SQLFunctionOut.NAME, SQLFunctionOut.class);
    register(SQLFunctionOutE.NAME, SQLFunctionOutE.class);
    register(SQLFunctionOutV.NAME, SQLFunctionOutV.class);
    register(SQLFunctionShortestPath.NAME, SQLFunctionShortestPath.class);

    // Math
    register(SQLFunctionAbsoluteValue.NAME, SQLFunctionAbsoluteValue.class);
    register(SQLFunctionAverage.NAME, SQLFunctionAverage.class);
    register(SQLFunctionCount.NAME, SQLFunctionCount.class);
    register(SQLFunctionEval.NAME, SQLFunctionEval.class);
    register(SQLFunctionMax.NAME, SQLFunctionMax.class);
    register(SQLFunctionMedian.NAME, SQLFunctionMedian.class);
    register(SQLFunctionMin.NAME, SQLFunctionMin.class);
    register(SQLFunctionMode.NAME, SQLFunctionMode.class);
    register(SQLFunctionPercentile.NAME, SQLFunctionPercentile.class);
    register(SQLFunctionRandomInt.NAME, SQLFunctionRandomInt.class);
    register(SQLFunctionSquareRoot.NAME, SQLFunctionSquareRoot.class);
    register(SQLFunctionStandardDeviation.NAME, SQLFunctionStandardDeviation.class);
    register(SQLFunctionSum.NAME, SQLFunctionSum.class);
    register(SQLFunctionVariance.NAME, SQLFunctionVariance.class);

    // Misc
    register(SQLFunctionBoolAnd.NAME, SQLFunctionBoolAnd.class);
    register(SQLFunctionBoolOr.NAME, SQLFunctionBoolOr.class);
    register(SQLFunctionCoalesce.NAME, new SQLFunctionCoalesce());
    register(SQLFunctionDecode.NAME, new SQLFunctionDecode());
    register(SQLFunctionEncode.NAME, new SQLFunctionEncode());
    register(SQLFunctionIf.NAME, new SQLFunctionIf());
    register(SQLFunctionIfNull.NAME, new SQLFunctionIfNull());
    register(SQLFunctionUUID.NAME, SQLFunctionUUID.class);

    // Text
    register(SQLFunctionFormat.NAME, new SQLFunctionFormat());
    register(SQLFunctionConcat.NAME, SQLFunctionConcat.class);
    register(SQLFunctionStrcmpci.NAME, SQLFunctionStrcmpci.class);

    // Time
    register(SQLFunctionDate.NAME, new SQLFunctionDate());
    register(SQLFunctionDuration.NAME, new SQLFunctionDuration());
    register(SQLFunctionSysdate.NAME, SQLFunctionSysdate.class);

    reflectionFactory = new SQLFunctionReflectionFactory(this);
  }

  public SQLFunctionReflectionFactory getReflectionFactory() {
    return reflectionFactory;
  }
}
