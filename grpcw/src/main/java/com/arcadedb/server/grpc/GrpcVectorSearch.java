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
package com.arcadedb.server.grpc;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.vector.FullTextQuery;
import com.arcadedb.server.vector.HybridSearch;
import com.arcadedb.server.vector.VectorSearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Bridges the vector, hybrid and full-text gRPC RPCs onto the shared implementation in
 * {@code com.arcadedb.server.vector} (issue #7306).
 * <p>
 * The bridge is deliberately a pure translation, request to arguments and result to message, with no validation
 * and no bounds of its own. That is what makes the gRPC surface agree with the HTTP routes and the MCP tools by
 * construction rather than by review: there is one implementation of "what is a legal vector search", and adding
 * a protocol cannot fork it.
 */
final class GrpcVectorSearch {
  private GrpcVectorSearch() {
  }

  static VectorSearchResponse search(final Database database, final VectorSearchRequest request) {
    final JSONObject args = new JSONObject()
        .put("indexName", request.getIndexName())
        .put("queryVector", floats(request.getQueryVectorList()))
        .put("sparse", request.getSparse());
    if (request.getK() > 0)
      args.put("k", request.getK());
    if (request.hasEfSearch())
      args.put("efSearch", request.getEfSearch());
    if (!request.getFilter().isEmpty())
      args.put("filter", request.getFilter());
    if (request.getQueryIndicesCount() > 0)
      args.put("queryIndices", ints(request.getQueryIndicesList()));

    final JSONObject result = VectorSearch.search(database, args);

    return VectorSearchResponse.newBuilder()
        .setIndexName(result.getString("indexName", ""))
        .setSparse(result.getBoolean("sparse", false))
        .setScoring(result.getString("scoring", ""))
        .setCandidateLimit(result.getInt("candidateLimit", 0))
        .setTruncated(result.getBoolean("truncated", false))
        .setCount(result.getInt("count", 0))
        .addAllResults(hits(result))
        .build();
  }

  static HybridSearchResponse hybridSearch(final Database database, final HybridSearchRequest request) {
    final JSONObject args = new JSONObject()
        .put("vectorIndexName", request.getVectorIndexName())
        .put("queryVector", floats(request.getQueryVectorList()))
        .put("sparse", request.getSparse());
    if (request.getK() > 0)
      args.put("k", request.getK());
    if (request.hasEfSearch())
      args.put("efSearch", request.getEfSearch());
    if (request.getQueryIndicesCount() > 0)
      args.put("queryIndices", ints(request.getQueryIndicesList()));
    if (!request.getFilter().isEmpty())
      args.put("filter", request.getFilter());
    if (!request.getFulltextQuery().isEmpty())
      args.put("fulltextQuery", request.getFulltextQuery());
    if (!request.getFulltextIndexName().isEmpty())
      args.put("fulltextIndexName", request.getFulltextIndexName());
    if (!request.getFusionStrategy().isEmpty())
      args.put("fusionStrategy", request.getFusionStrategy());
    if (request.getWeightsCount() > 0) {
      final JSONObject weights = new JSONObject();
      for (final Map.Entry<String, Float> entry : request.getWeightsMap().entrySet())
        weights.put(entry.getKey(), entry.getValue().doubleValue());
      args.put("weights", weights);
    }
    if (request.hasExpand()) {
      final HybridExpand expand = request.getExpand();
      final JSONObject expandArgs = new JSONObject();
      if (expand.getEdgeTypesCount() > 0)
        expandArgs.put("edgeTypes", new JSONArray(expand.getEdgeTypesList()));
      if (!expand.getDirection().isEmpty())
        expandArgs.put("direction", expand.getDirection());
      if (expand.getMaxDepth() > 0)
        expandArgs.put("maxDepth", expand.getMaxDepth());
      args.put("expand", expandArgs);
    }

    final JSONObject result = HybridSearch.search(database, args);

    final HybridSearchResponse.Builder builder = HybridSearchResponse.newBuilder()
        .setVectorIndexName(result.getString("vectorIndexName", ""))
        .setFulltextIndexName(result.getString("fulltextIndexName", ""))
        .setSparse(result.getBoolean("sparse", false))
        .setScoring(result.getString("scoring", ""))
        .setFused(result.getBoolean("fused", false))
        .setFusionStrategy(result.getString("fusionStrategy", ""))
        .setTruncated(result.getBoolean("truncated", false))
        .setCount(result.getInt("count", 0))
        .addAllResults(hits(result));

    final JSONObject legs = result.getJSONObject("legs", null);
    if (legs != null)
      for (final String leg : legs.keySet())
        builder.putLegs(leg, GrpcTypeConverter.toGrpcValue(legs.get(leg)));

    return builder.build();
  }

  static FullTextSearchResponse fullTextSearch(final Database database, final FullTextSearchRequest request) {
    final JSONObject args = new JSONObject().put("queryText", request.getQueryText());
    if (!request.getIndexName().isEmpty())
      args.put("indexName", request.getIndexName());
    if (!request.getTypeName().isEmpty())
      args.put("typeName", request.getTypeName());
    if (request.getPropertiesCount() > 0)
      args.put("properties", new JSONArray(request.getPropertiesList()));
    if (request.getLimit() > 0)
      args.put("limit", request.getLimit());

    final JSONObject result = FullTextQuery.search(database, args);

    return FullTextSearchResponse.newBuilder()
        .setIndexName(result.getString("indexName", ""))
        .setSimilarity(result.getString("similarity", ""))
        .setCount(result.getInt("count", 0))
        .addAllResults(hits(result))
        .build();
  }

  /**
   * Converts the {@code results} array every search service returns. The shape is uniform across the three, so
   * the conversion is too: {@code distance} and {@code score} are optional in the message because exactly one of
   * them is present per hit and a client has to be able to tell "absent" from "zero" - a cosine distance of 0.0
   * is the best possible hit, not a missing one.
   */
  private static List<SearchHit> hits(final JSONObject result) {
    final JSONArray results = result.getJSONArray("results", new JSONArray());
    final List<SearchHit> converted = new ArrayList<>(results.length());
    for (int i = 0; i < results.length(); i++) {
      final JSONObject hit = results.getJSONObject(i);
      final SearchHit.Builder builder = SearchHit.newBuilder().setRid(hit.getString("rid", ""));

      if (hit.has("distance") && !hit.isNull("distance"))
        builder.setDistance(hit.getDouble("distance"));
      if (hit.has("score") && !hit.isNull("score"))
        builder.setScore(hit.getDouble("score"));
      // A fused hybrid hit carries its rank under "fusedScore", not "score" - that is the JSON key the MCP
      // hybrid_search tool has always used and this move kept. SearchHit.score is documented as "sparse,
      // full-text or fused score", so it is the field that carries it on the wire. Without this the whole
      // point of the RPC - a fused, multi-leg result - arrives with nothing to rank it by.
      else if (hit.has("fusedScore") && !hit.isNull("fusedScore"))
        builder.setScore(hit.getDouble("fusedScore"));
      if (hit.has("depth") && !hit.isNull("depth"))
        builder.setDepth(hit.getInt("depth"));

      final JSONArray sources = hit.getJSONArray("sources", null);
      if (sources != null)
        for (int s = 0; s < sources.length(); s++)
          builder.addSources(sources.getString(s));

      final JSONArray path = hit.getJSONArray("path", null);
      if (path != null)
        for (int s = 0; s < path.length(); s++)
          builder.addPath(path.getString(s));

      final JSONObject properties = hit.getJSONObject("properties", null);
      if (properties != null) {
        final GrpcRecord.Builder record = GrpcRecord.newBuilder().setRid(hit.getString("rid", ""));
        final Object typeName = properties.opt("@type");
        if (typeName instanceof final String type)
          record.setType(type);
        for (final String property : properties.keySet())
          record.putProperties(property, GrpcTypeConverter.toGrpcValue(properties.get(property)));
        builder.setRecord(record);
      }

      converted.add(builder.build());
    }
    return converted;
  }

  private static JSONArray floats(final List<Float> values) {
    final JSONArray array = new JSONArray();
    for (final Float value : values)
      array.put(value.doubleValue());
    return array;
  }

  private static JSONArray ints(final List<Integer> values) {
    final JSONArray array = new JSONArray();
    for (final Integer value : values)
      array.put(value.intValue());
    return array;
  }
}
