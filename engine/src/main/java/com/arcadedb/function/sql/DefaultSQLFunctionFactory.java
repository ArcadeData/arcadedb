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
package com.arcadedb.function.sql;

// Collections

import com.arcadedb.function.sql.coll.SQLFunctionDifference;
import com.arcadedb.function.sql.coll.SQLFunctionFirst;
import com.arcadedb.function.sql.coll.SQLFunctionIntersect;
import com.arcadedb.function.sql.coll.SQLFunctionLast;
import com.arcadedb.function.sql.coll.SQLFunctionList;
import com.arcadedb.function.sql.coll.SQLFunctionMap;
import com.arcadedb.function.sql.coll.SQLFunctionSet;
import com.arcadedb.function.sql.coll.SQLFunctionSymmetricDifference;
import com.arcadedb.function.sql.coll.SQLFunctionUnionAll;
import com.arcadedb.function.sql.geo.SQLFunctionGeoArea;
import com.arcadedb.function.sql.geo.SQLFunctionGeoAsGeoJson;
import com.arcadedb.function.sql.geo.SQLFunctionGeoAsText;
import com.arcadedb.function.sql.geo.SQLFunctionGeoBuffer;
import com.arcadedb.function.sql.geo.SQLFunctionGeoDistance;
import com.arcadedb.function.sql.geo.SQLFunctionGeoEnvelope;
import com.arcadedb.function.sql.geo.SQLFunctionGeoGeomFromText;
import com.arcadedb.function.sql.geo.SQLFunctionGeoLineString;
import com.arcadedb.function.sql.geo.SQLFunctionGeoPoint;
import com.arcadedb.function.sql.geo.SQLFunctionGeoPolygon;
import com.arcadedb.function.sql.geo.SQLFunctionST_Contains;
import com.arcadedb.function.sql.geo.SQLFunctionST_Crosses;
import com.arcadedb.function.sql.geo.SQLFunctionST_Disjoint;
import com.arcadedb.function.sql.geo.SQLFunctionST_DWithin;
import com.arcadedb.function.sql.geo.SQLFunctionST_Equals;
import com.arcadedb.function.sql.geo.SQLFunctionST_Intersects;
import com.arcadedb.function.sql.geo.SQLFunctionST_Overlaps;
import com.arcadedb.function.sql.geo.SQLFunctionST_Touches;
import com.arcadedb.function.sql.geo.SQLFunctionST_Within;
import com.arcadedb.function.sql.geo.SQLFunctionGeoX;
import com.arcadedb.function.sql.geo.SQLFunctionGeoY;
import com.arcadedb.function.sql.graph.SQLFunctionAstar;
import com.arcadedb.function.sql.graph.SQLFunctionBellmanFord;
import com.arcadedb.function.sql.graph.SQLFunctionBoth;
import com.arcadedb.function.sql.graph.SQLFunctionBothE;
import com.arcadedb.function.sql.graph.SQLFunctionBothV;
import com.arcadedb.function.sql.graph.SQLFunctionDijkstra;
import com.arcadedb.function.sql.graph.SQLFunctionDuanSSSP;
import com.arcadedb.function.sql.graph.SQLFunctionIn;
import com.arcadedb.function.sql.graph.SQLFunctionInE;
import com.arcadedb.function.sql.graph.SQLFunctionInV;
import com.arcadedb.function.sql.graph.SQLFunctionOut;
import com.arcadedb.function.sql.graph.SQLFunctionOutE;
import com.arcadedb.function.sql.graph.SQLFunctionOutV;
import com.arcadedb.function.sql.graph.SQLFunctionShortestPath;
import com.arcadedb.function.sql.math.SQLFunctionAbsoluteValue;
import com.arcadedb.function.sql.math.SQLFunctionAverage;
import com.arcadedb.function.sql.math.SQLFunctionCount;
import com.arcadedb.function.sql.math.SQLFunctionEval;
import com.arcadedb.function.sql.math.SQLFunctionMax;
import com.arcadedb.function.sql.math.SQLFunctionMedian;
import com.arcadedb.function.sql.math.SQLFunctionMin;
import com.arcadedb.function.sql.math.SQLFunctionMode;
import com.arcadedb.function.sql.math.SQLFunctionPercentile;
import com.arcadedb.function.sql.math.SQLFunctionPow;
import com.arcadedb.function.sql.math.SQLFunctionRandomInt;
import com.arcadedb.function.sql.math.SQLFunctionSquareRoot;
import com.arcadedb.function.sql.math.SQLFunctionStandardDeviation;
import com.arcadedb.function.sql.math.SQLFunctionStandardDeviationP;
import com.arcadedb.function.sql.math.SQLFunctionSum;
import com.arcadedb.function.sql.math.SQLFunctionVariance;
import com.arcadedb.function.sql.math.SQLFunctionVarianceP;
import com.arcadedb.function.sql.misc.SQLFunctionBoolAnd;
import com.arcadedb.function.sql.misc.SQLFunctionBoolOr;
import com.arcadedb.function.sql.misc.SQLFunctionCoalesce;
import com.arcadedb.function.sql.misc.SQLFunctionDecode;
import com.arcadedb.function.sql.misc.SQLFunctionEncode;
import com.arcadedb.function.sql.misc.SQLFunctionIf;
import com.arcadedb.function.sql.misc.SQLFunctionIfEmpty;
import com.arcadedb.function.sql.misc.SQLFunctionIfNull;
import com.arcadedb.function.sql.misc.SQLFunctionUUID;
import com.arcadedb.function.sql.misc.SQLFunctionVersion;
import com.arcadedb.function.sql.text.SQLFunctionConcat;
import com.arcadedb.function.sql.text.SQLFunctionFormat;
import com.arcadedb.function.sql.text.SQLFunctionSearchFields;
import com.arcadedb.function.sql.text.SQLFunctionSearchFieldsMore;
import com.arcadedb.function.sql.text.SQLFunctionSearchIndex;
import com.arcadedb.function.sql.text.SQLFunctionSearchIndexMore;
import com.arcadedb.function.sql.text.SQLFunctionStrcmpci;
import com.arcadedb.function.sql.time.SQLFunctionDate;
import com.arcadedb.function.sql.time.SQLFunctionDuration;
import com.arcadedb.function.sql.time.SQLFunctionSysdate;
import com.arcadedb.function.sql.time.SQLFunctionTimeBucket;
import com.arcadedb.function.sql.time.SQLFunctionCorrelate;
import com.arcadedb.function.sql.time.SQLFunctionDelta;
import com.arcadedb.function.sql.time.SQLFunctionInterpolate;
import com.arcadedb.function.sql.time.SQLFunctionMovingAvg;
import com.arcadedb.function.sql.time.SQLFunctionRate;
import com.arcadedb.function.sql.time.SQLFunctionTsPercentile;
import com.arcadedb.function.sql.time.SQLFunctionLag;
import com.arcadedb.function.sql.time.SQLFunctionLead;
import com.arcadedb.function.sql.time.SQLFunctionRank;
import com.arcadedb.function.sql.time.SQLFunctionRowNumber;
import com.arcadedb.function.sql.time.SQLFunctionTsFirst;
import com.arcadedb.function.sql.time.SQLFunctionTsLast;
import com.arcadedb.function.sql.time.SQLFunctionPromQL;
import com.arcadedb.function.sql.vector.SQLFunctionDenseVectorToSparse;
import com.arcadedb.function.sql.vector.SQLFunctionMultiVectorScore;
import com.arcadedb.function.sql.vector.SQLFunctionSparseVectorCreate;
import com.arcadedb.function.sql.vector.SQLFunctionSparseVectorDot;
import com.arcadedb.function.sql.vector.SQLFunctionSparseVectorToDense;
import com.arcadedb.function.sql.vector.SQLFunctionVectorAdd;
import com.arcadedb.function.sql.vector.SQLFunctionVectorApproxDistance;
import com.arcadedb.function.sql.vector.SQLFunctionVectorAvg;
import com.arcadedb.function.sql.vector.SQLFunctionVectorClip;
import com.arcadedb.function.sql.vector.SQLFunctionVectorCosineSimilarity;
import com.arcadedb.function.sql.vector.SQLFunctionVectorDequantizeInt8;
import com.arcadedb.function.sql.vector.SQLFunctionVectorDimension;
import com.arcadedb.function.sql.vector.SQLFunctionVectorDotProduct;
import com.arcadedb.function.sql.vector.SQLFunctionVectorHasInf;
import com.arcadedb.function.sql.vector.SQLFunctionVectorHasNaN;
import com.arcadedb.function.sql.vector.SQLFunctionVectorHybridScore;
import com.arcadedb.function.sql.vector.SQLFunctionVectorIsNormalized;
import com.arcadedb.function.sql.vector.SQLFunctionVectorL1Norm;
import com.arcadedb.function.sql.vector.SQLFunctionVectorL2Distance;
import com.arcadedb.function.sql.vector.SQLFunctionVectorLInfNorm;
import com.arcadedb.function.sql.vector.SQLFunctionVectorMagnitude;
import com.arcadedb.function.sql.vector.SQLFunctionVectorMax;
import com.arcadedb.function.sql.vector.SQLFunctionVectorMin;
import com.arcadedb.function.sql.vector.SQLFunctionVectorMultiply;
import com.arcadedb.function.sql.vector.SQLFunctionVectorNeighbors;
import com.arcadedb.function.sql.vector.SQLFunctionVectorNormalize;
import com.arcadedb.function.sql.vector.SQLFunctionVectorNormalizeScores;
import com.arcadedb.function.sql.vector.SQLFunctionVectorQuantizeBinary;
import com.arcadedb.function.sql.vector.SQLFunctionVectorQuantizeInt8;
import com.arcadedb.function.sql.vector.SQLFunctionVectorRRFScore;
import com.arcadedb.function.sql.vector.SQLFunctionVectorScale;
import com.arcadedb.function.sql.vector.SQLFunctionVectorScoreTransform;
import com.arcadedb.function.sql.vector.SQLFunctionVectorSparsity;
import com.arcadedb.function.sql.vector.SQLFunctionVectorStdDev;
import com.arcadedb.function.sql.vector.SQLFunctionVectorSubtract;
import com.arcadedb.function.sql.vector.SQLFunctionVectorSum;
import com.arcadedb.function.sql.vector.SQLFunctionVectorToString;
import com.arcadedb.function.sql.vector.SQLFunctionVectorVariance;

/**
 * Default set of SQL functions.
 * <p>
 * This is a singleton to ensure functions are registered only once in the {@link com.arcadedb.function.FunctionRegistry}.
 * </p>
 */
public final class DefaultSQLFunctionFactory extends SQLFunctionFactoryTemplate {
  private static final DefaultSQLFunctionFactory INSTANCE = new DefaultSQLFunctionFactory();

  private final SQLFunctionReflectionFactory reflectionFactory;

  public static DefaultSQLFunctionFactory getInstance() {
    return INSTANCE;
  }

  private DefaultSQLFunctionFactory() {
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

    // Geo — ST_* standard functions
    register(SQLFunctionGeoGeomFromText.NAME, new SQLFunctionGeoGeomFromText());
    register(SQLFunctionGeoPoint.NAME, new SQLFunctionGeoPoint());
    register(SQLFunctionGeoLineString.NAME, new SQLFunctionGeoLineString());
    register(SQLFunctionGeoPolygon.NAME, new SQLFunctionGeoPolygon());
    register(SQLFunctionGeoBuffer.NAME, new SQLFunctionGeoBuffer());
    register(SQLFunctionGeoEnvelope.NAME, new SQLFunctionGeoEnvelope());
    register(SQLFunctionGeoDistance.NAME, new SQLFunctionGeoDistance());
    register(SQLFunctionGeoArea.NAME, new SQLFunctionGeoArea());
    register(SQLFunctionGeoAsText.NAME, new SQLFunctionGeoAsText());
    register(SQLFunctionGeoAsGeoJson.NAME, new SQLFunctionGeoAsGeoJson());
    register(SQLFunctionGeoX.NAME, new SQLFunctionGeoX());
    register(SQLFunctionGeoY.NAME, new SQLFunctionGeoY());

    // Geo — ST_* spatial predicate functions (IndexableSQLFunction)
    register(SQLFunctionST_Within.NAME, new SQLFunctionST_Within());
    register(SQLFunctionST_Intersects.NAME, new SQLFunctionST_Intersects());
    register(SQLFunctionST_Contains.NAME, new SQLFunctionST_Contains());
    register(SQLFunctionST_DWithin.NAME, new SQLFunctionST_DWithin());
    register(SQLFunctionST_Disjoint.NAME, new SQLFunctionST_Disjoint());
    register(SQLFunctionST_Equals.NAME, new SQLFunctionST_Equals());
    register(SQLFunctionST_Crosses.NAME, new SQLFunctionST_Crosses());
    register(SQLFunctionST_Overlaps.NAME, new SQLFunctionST_Overlaps());
    register(SQLFunctionST_Touches.NAME, new SQLFunctionST_Touches());

    // Graph
    register(SQLFunctionAstar.NAME, SQLFunctionAstar.class);
    register(SQLFunctionBellmanFord.NAME, SQLFunctionBellmanFord.class);
    register(SQLFunctionBoth.NAME, SQLFunctionBoth.class);
    register(SQLFunctionBothE.NAME, SQLFunctionBothE.class);
    register(SQLFunctionBothV.NAME, SQLFunctionBothV.class);
    register(SQLFunctionDijkstra.NAME, SQLFunctionDijkstra.class);
    register(SQLFunctionDuanSSSP.NAME, SQLFunctionDuanSSSP.class);
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
    register(SQLFunctionPow.NAME, SQLFunctionPow.class);
    register(SQLFunctionRandomInt.NAME, SQLFunctionRandomInt.class);
    register(SQLFunctionSquareRoot.NAME, SQLFunctionSquareRoot.class);
    register(SQLFunctionStandardDeviation.NAME, SQLFunctionStandardDeviation.class);
    register(SQLFunctionStandardDeviationP.NAME, SQLFunctionStandardDeviationP.class);
    register(SQLFunctionSum.NAME, SQLFunctionSum.class);
    register(SQLFunctionVariance.NAME, SQLFunctionVariance.class);
    register(SQLFunctionVarianceP.NAME, SQLFunctionVarianceP.class);

    // Misc
    register(SQLFunctionBoolAnd.NAME, SQLFunctionBoolAnd.class);
    register(SQLFunctionBoolOr.NAME, SQLFunctionBoolOr.class);
    register(SQLFunctionCoalesce.NAME, new SQLFunctionCoalesce());
    register(SQLFunctionDecode.NAME, new SQLFunctionDecode());
    register(SQLFunctionEncode.NAME, new SQLFunctionEncode());
    register(SQLFunctionIf.NAME, new SQLFunctionIf());
    register(SQLFunctionIfEmpty.NAME, new SQLFunctionIfEmpty());
    register(SQLFunctionIfNull.NAME, new SQLFunctionIfNull());
    register(SQLFunctionUUID.NAME, SQLFunctionUUID.class);
    register(SQLFunctionVersion.NAME, SQLFunctionVersion.class);

    // Text
    register(SQLFunctionFormat.NAME, new SQLFunctionFormat());
    register(SQLFunctionConcat.NAME, SQLFunctionConcat.class);
    register(SQLFunctionSearchFields.NAME, SQLFunctionSearchFields.class);
    register(SQLFunctionSearchFieldsMore.NAME, SQLFunctionSearchFieldsMore.class);
    register(SQLFunctionSearchIndex.NAME, SQLFunctionSearchIndex.class);
    register(SQLFunctionSearchIndexMore.NAME, SQLFunctionSearchIndexMore.class);
    register(SQLFunctionStrcmpci.NAME, SQLFunctionStrcmpci.class);

    // Time
    register(SQLFunctionDate.NAME, new SQLFunctionDate());
    register(SQLFunctionDuration.NAME, new SQLFunctionDuration());
    register(SQLFunctionSysdate.NAME, SQLFunctionSysdate.class);
    // TimeSeries (ts.* namespace)
    register(SQLFunctionTimeBucket.NAME, new SQLFunctionTimeBucket());
    register(SQLFunctionCorrelate.NAME, SQLFunctionCorrelate.class);
    register(SQLFunctionDelta.NAME, SQLFunctionDelta.class);
    register(SQLFunctionTsFirst.NAME, SQLFunctionTsFirst.class);
    register(SQLFunctionTsLast.NAME, SQLFunctionTsLast.class);
    register(SQLFunctionInterpolate.NAME, SQLFunctionInterpolate.class);
    register(SQLFunctionMovingAvg.NAME, SQLFunctionMovingAvg.class);
    register(SQLFunctionRate.NAME, SQLFunctionRate.class);
    register(SQLFunctionTsPercentile.NAME, SQLFunctionTsPercentile.class);
    register(SQLFunctionPromQL.NAME, new SQLFunctionPromQL());
    // Window functions
    register(SQLFunctionLag.NAME, SQLFunctionLag.class);
    register(SQLFunctionLead.NAME, SQLFunctionLead.class);
    register(SQLFunctionRowNumber.NAME, SQLFunctionRowNumber.class);
    register(SQLFunctionRank.NAME, SQLFunctionRank.class);

    // Vectors
    // Basic Operations
    register(SQLFunctionVectorNormalize.NAME, new SQLFunctionVectorNormalize());
    register(SQLFunctionVectorMagnitude.NAME, new SQLFunctionVectorMagnitude());
    register(SQLFunctionVectorDimension.NAME, new SQLFunctionVectorDimension());
    register(SQLFunctionVectorDotProduct.NAME, new SQLFunctionVectorDotProduct());
    // Similarity Scoring
    register(SQLFunctionVectorCosineSimilarity.NAME, new SQLFunctionVectorCosineSimilarity());
    register(SQLFunctionVectorL2Distance.NAME, new SQLFunctionVectorL2Distance());
    // Vector Arithmetic
    register(SQLFunctionVectorAdd.NAME, new SQLFunctionVectorAdd());
    register(SQLFunctionVectorSubtract.NAME, new SQLFunctionVectorSubtract());
    register(SQLFunctionVectorMultiply.NAME, new SQLFunctionVectorMultiply());
    register(SQLFunctionVectorScale.NAME, new SQLFunctionVectorScale());
    // Vector Aggregations
    register(SQLFunctionVectorSum.NAME, new SQLFunctionVectorSum());
    register(SQLFunctionVectorAvg.NAME, new SQLFunctionVectorAvg());
    register(SQLFunctionVectorMin.NAME, new SQLFunctionVectorMin());
    register(SQLFunctionVectorMax.NAME, new SQLFunctionVectorMax());
    // Reranking Functions
    register(SQLFunctionVectorRRFScore.NAME, new SQLFunctionVectorRRFScore());
    register(SQLFunctionVectorNormalizeScores.NAME, new SQLFunctionVectorNormalizeScores());
    // Hybrid Search Scoring
    register(SQLFunctionVectorHybridScore.NAME, new SQLFunctionVectorHybridScore());
    register(SQLFunctionVectorScoreTransform.NAME, new SQLFunctionVectorScoreTransform());
    // Sparse Vectors
    register(SQLFunctionSparseVectorCreate.NAME, new SQLFunctionSparseVectorCreate());
    register(SQLFunctionSparseVectorDot.NAME, new SQLFunctionSparseVectorDot());
    register(SQLFunctionSparseVectorToDense.NAME, new SQLFunctionSparseVectorToDense());
    register(SQLFunctionDenseVectorToSparse.NAME, new SQLFunctionDenseVectorToSparse());
    // Multi-Vector Operations
    register(SQLFunctionMultiVectorScore.NAME, new SQLFunctionMultiVectorScore());
    // Quantization & Optimization
    register(SQLFunctionVectorQuantizeInt8.NAME, new SQLFunctionVectorQuantizeInt8());
    register(SQLFunctionVectorQuantizeBinary.NAME, new SQLFunctionVectorQuantizeBinary());
    register(SQLFunctionVectorDequantizeInt8.NAME, new SQLFunctionVectorDequantizeInt8());
    register(SQLFunctionVectorApproxDistance.NAME, new SQLFunctionVectorApproxDistance());
    // Vector Analysis
    register(SQLFunctionVectorL1Norm.NAME, new SQLFunctionVectorL1Norm());
    register(SQLFunctionVectorLInfNorm.NAME, new SQLFunctionVectorLInfNorm());
    register(SQLFunctionVectorVariance.NAME, new SQLFunctionVectorVariance());
    register(SQLFunctionVectorStdDev.NAME, new SQLFunctionVectorStdDev());
    register(SQLFunctionVectorSparsity.NAME, new SQLFunctionVectorSparsity());
    // Vector Validation
    register(SQLFunctionVectorIsNormalized.NAME, new SQLFunctionVectorIsNormalized());
    register(SQLFunctionVectorHasNaN.NAME, new SQLFunctionVectorHasNaN());
    register(SQLFunctionVectorHasInf.NAME, new SQLFunctionVectorHasInf());
    register(SQLFunctionVectorClip.NAME, new SQLFunctionVectorClip());
    register(SQLFunctionVectorToString.NAME, new SQLFunctionVectorToString());
    // Existing
    register(SQLFunctionVectorNeighbors.NAME, new SQLFunctionVectorNeighbors());

    reflectionFactory = new SQLFunctionReflectionFactory(this);
  }

  public SQLFunctionReflectionFactory getReflectionFactory() {
    return reflectionFactory;
  }
}
