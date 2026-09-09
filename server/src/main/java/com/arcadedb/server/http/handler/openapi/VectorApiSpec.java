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
package com.arcadedb.server.http.handler.openapi;

import com.arcadedb.server.vector.FullTextQuery;
import com.arcadedb.server.vector.HybridSearch;
import com.arcadedb.server.vector.VectorLeg;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.math.BigDecimal;
import java.util.List;

/**
 * Documents the vector, hybrid and full-text retrieval routes added by issue #7306.
 * <p>
 * Every bound advertised here is read from the implementation constants rather than written out, so the
 * document and the enforcement cannot drift: {@link VectorLeg#MAX_K}, {@link VectorLeg#MAX_EF_SEARCH},
 * {@link FullTextQuery#MAX_LIMIT} and the {@link HybridSearch} expansion caps are the same values the MCP tool
 * schemas advertise and the gRPC RPCs enforce.
 */
public class VectorApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/vector/{database}/search", createSearchPath());
    openAPI.getPaths().addPathItem("/api/v1/vector/{database}/hybrid", createHybridPath());
    openAPI.getPaths().addPathItem("/api/v1/vector/{database}/fulltext", createFullTextPath());

    openAPI.getComponents().addSchemas("VectorSearchRequest", createSearchRequestSchema());
    openAPI.getComponents().addSchemas("VectorSearchResponse", createSearchResponseSchema());
    openAPI.getComponents().addSchemas("HybridSearchRequest", createHybridRequestSchema());
    openAPI.getComponents().addSchemas("HybridSearchResponse", createHybridResponseSchema());
    openAPI.getComponents().addSchemas("FullTextSearchRequest", createFullTextRequestSchema());
    openAPI.getComponents().addSchemas("FullTextSearchResponse", createFullTextResponseSchema());
  }

  private PathItem createSearchPath() {
    final Operation post = SpecBuilders.operation("vectorSearch", "Vector",
        "kNN search over a vector index",
        """
            Returns the nearest neighbors of a pre-computed query vector in a dense LSM_VECTOR or sparse \
            LSM_SPARSE_VECTOR index. ArcadeDB does not generate embeddings: the caller supplies the vector.

            Dense results expose a 'distance' (lower is better); sparse results expose a 'score' (higher is \
            better), and 'scoring' names which of the two the response carries. A filtered search inspects a \
            bounded candidate window whose size is reported as 'candidateLimit', so 'truncated' means the \
            window was filled and more matches may exist - raise 'k' to see them.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.jsonBody("Vector search request", "VectorSearchRequest", true));
    post.setResponses(vectorResponses(SpecBuilders.jsonResponse(
        "Ranked neighbors, nearest first", "VectorSearchResponse")));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createHybridPath() {
    final Operation post = SpecBuilders.operation("hybridSearch", "Vector",
        "Fused vector, full-text and graph-expansion search",
        """
            Fuses a vector retrieval leg, an optional full-text retrieval leg and an optional depth-limited \
            graph expansion leg into one ranked list, using the engine's own vector.fuse rather than a \
            re-implementation.

            Fusion needs at least two sources. A request naming only the vector leg reports 'fused': false and \
            returns that leg's native distance or score rather than a fabricated fused one. The expansion leg \
            is ranked by traversal order and carries no score, so it can only be fused with the RRF \
            strategy.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.jsonBody("Hybrid search request", "HybridSearchRequest", true));
    post.setResponses(vectorResponses(SpecBuilders.jsonResponse(
        "Fused results, best first, each naming the legs it came from", "HybridSearchResponse")));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createFullTextPath() {
    final Operation post = SpecBuilders.operation("fullTextSearch", "Vector",
        "Full-text search over a FULL_TEXT index",
        """
            Runs a Lucene-syntax query against an ArcadeDB FULL_TEXT index and returns the matching documents \
            ranked by score, highest first.

            Address the index either by 'indexName', or by 'typeName' with optional 'properties'; 'indexName' \
            wins when both are supplied. Because the limit is pushed down per bucket, a hit deleted between \
            the index scan and the record load is skipped rather than back-filled, so a search can \
            legitimately return fewer than 'limit' results.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.jsonBody("Full-text search request", "FullTextSearchRequest", true));
    post.setResponses(vectorResponses(SpecBuilders.jsonResponse(
        "Matching documents, highest score first", "FullTextSearchResponse")));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  /**
   * The status codes every search route can answer with. 400 is the interesting one: every bound below is
   * enforced server-side and reported as a client error naming the argument, so a caller never has to guess
   * which limit it crossed.
   */
  private ApiResponses vectorResponses(final ApiResponse success) {
    return SpecBuilders.standardResponses("200", success, "400", "401", "403", "404", "500");
  }

  private Schema<?> createSearchRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("kNN search over a dense or sparse vector index");
    schema.addProperty("indexName", SpecBuilders.string("Name of an LSM_VECTOR or LSM_SPARSE_VECTOR index"));
    schema.addProperty("queryVector", SpecBuilders.arrayOf(SpecBuilders.number(null),
        "Dense query vector, or the sparse weights matching 'queryIndices' when sparse is true"));
    schema.addProperty("queryIndices", SpecBuilders.arrayOf(SpecBuilders.integer(null),
        "Sparse dimension ids matching the 'queryVector' weights; omit to use the vector's own positions. Requires sparse=true"));
    schema.addProperty("k", boundedInteger("Maximum number of results to return", 1, VectorLeg.MAX_K, VectorLeg.DEFAULT_K));
    schema.addProperty("efSearch", boundedInteger(
        "Dense-index search beam width: higher values improve recall at higher cost. Rejected for a sparse index",
        1, VectorLeg.MAX_EF_SEARCH, null));
    schema.addProperty("filter", SpecBuilders.string("""
        Optional read-only SQL WHERE predicate applied to a bounded candidate set. Evaluated against each \
        expanded neighbor row, where record properties are flattened and @rid, @type, record, plus distance \
        (dense) or score (sparse) are available. At most """ + VectorLeg.MAX_FILTER_EXPRESSION + " characters."));
    schema.addProperty("sparse", SpecBuilders.bool("Search an LSM_SPARSE_VECTOR index instead of a dense one"));
    schema.setRequired(List.of("indexName", "queryVector", "k"));
    return schema;
  }

  private Schema<?> createSearchResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Ranked neighbors of the query vector");
    schema.addProperty("indexName", SpecBuilders.string("Index that was searched"));
    schema.addProperty("sparse", SpecBuilders.bool("Whether the sparse path was taken"));
    schema.addProperty("scoring", SpecBuilders.string("""
        Which direction is better and how it was computed, e.g. 'distance_lower_is_better:COSINE' or \
        'score_higher_is_better:dot_product'. Read it rather than assuming, because the two paths rank in \
        opposite directions."""));
    schema.addProperty("candidateLimit", SpecBuilders.integer(
        "Size of the candidate window the search inspected, which a filter over-fetches into"));
    schema.addProperty("truncated", SpecBuilders.bool("""
        True when the result window was filled, so further matches may exist. False for a short result: the \
        search already returned every match it could find within 'candidateLimit'."""));
    schema.addProperty("count", SpecBuilders.integer("Number of results returned"));
    schema.addProperty("results", SpecBuilders.arrayOf(hitSchema(), "Hits, nearest or highest-scoring first"));
    return schema;
  }

  private Schema<?> createHybridRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Fused vector, full-text and graph-expansion search");
    schema.addProperty("vectorIndexName", SpecBuilders.string("Name of an LSM_VECTOR or LSM_SPARSE_VECTOR index"));
    schema.addProperty("queryVector", SpecBuilders.arrayOf(SpecBuilders.number(null), "Query vector for the vector leg"));
    schema.addProperty("queryIndices", SpecBuilders.arrayOf(SpecBuilders.integer(null),
        "Sparse dimension ids matching the 'queryVector' weights. Requires sparse=true"));
    schema.addProperty("k", boundedInteger("Maximum number of fused results to return", 1, VectorLeg.MAX_K,
        VectorLeg.DEFAULT_K));
    schema.addProperty("efSearch", boundedInteger("Dense-index search beam width", 1, VectorLeg.MAX_EF_SEARCH, null));
    schema.addProperty("sparse", SpecBuilders.bool("Use the sparse vector path"));
    schema.addProperty("filter", SpecBuilders.string(
        "Optional read-only SQL WHERE predicate applied to the vector leg's candidate window"));
    schema.addProperty("fulltextQuery", SpecBuilders.string("""
        Lucene-syntax query for the full-text leg. Goes together with 'fulltextIndexName': half a leg is \
        refused rather than silently dropped. Omit both to search without a full-text leg."""));
    schema.addProperty("fulltextIndexName", SpecBuilders.string(
        "Full-text index the full-text leg searches. Required whenever 'fulltextQuery' is given, and refused without it"));
    schema.addProperty("fusionStrategy", SpecBuilders.string(
        "How the legs are combined. Only RRF can consume the graph expansion leg, which is ranked by traversal order"));
    schema.addProperty("weights", SpecBuilders.object("""
        Per-leg weight applied to every rank contribution. The only accepted keys are 'vector', 'fulltext' and \
        'expand', and a weight for a leg the request does not ask for is refused rather than ignored."""));

    final Schema<Object> expand = SpecBuilders.object("""
        Optional graph expansion leg, seeded from the union of the retrieval legs and ranked by breadth-first \
        discovery order.""");
    expand.addProperty("edgeTypes", SpecBuilders.arrayOf(SpecBuilders.string(null), "Edge types to walk"));
    expand.addProperty("direction", SpecBuilders.string("out, in, or both"));
    expand.addProperty("maxDepth", boundedInteger("Hops to walk from a seed", 1, HybridSearch.MAX_DEPTH, 1));
    schema.addProperty("expand", expand);

    schema.setRequired(List.of("vectorIndexName", "queryVector", "k"));
    return schema;
  }

  private Schema<?> createHybridResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Fused results and per-leg accounting");
    schema.addProperty("vectorIndexName", SpecBuilders.string("Vector index that was searched"));
    schema.addProperty("fulltextIndexName", SpecBuilders.string(
        "Full-text index that was searched, present whenever the full-text leg ran - including when it matched nothing"));
    schema.addProperty("sparse", SpecBuilders.bool("Whether the vector leg took the sparse path"));
    schema.addProperty("scoring", SpecBuilders.string("Scoring direction of the vector leg"));
    schema.addProperty("legs", SpecBuilders.object(
        "Per-leg accounting: how many rows each leg contributed, and whether the expansion hit its seed or fan-out cap"));
    schema.addProperty("fused", SpecBuilders.bool("""
        False when only one leg produced rows: fusion needs at least two sources, so the response carries that \
        leg's native distance or score instead of a fused one."""));
    schema.addProperty("fusionStrategy", SpecBuilders.string("Strategy actually applied; absent when 'fused' is false"));
    schema.addProperty("truncated", SpecBuilders.bool("True when the result window was filled"));
    schema.addProperty("count", SpecBuilders.integer("Number of results returned"));
    schema.addProperty("results", SpecBuilders.arrayOf(fusedHitSchema(), "Fused hits, best first"));
    return schema;
  }

  private Schema<?> createFullTextRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Full-text search over a FULL_TEXT index");
    schema.addProperty("queryText", SpecBuilders.string("Lucene-syntax query, e.g. 'java' or '+java -python'. Must not be blank"));
    schema.addProperty("indexName", SpecBuilders.string("Full-text index to search; wins over 'typeName' when both are given"));
    schema.addProperty("typeName", SpecBuilders.string(
        "Type whose full-text index to search. Usable alone only when the type carries exactly one"));
    schema.addProperty("properties", SpecBuilders.arrayOf(SpecBuilders.string(null),
        "Indexed properties, to pick between several full-text indexes on the same type"));
    schema.addProperty("limit", boundedInteger("Maximum number of results to return", 1, FullTextQuery.MAX_LIMIT,
        FullTextQuery.DEFAULT_LIMIT));
    schema.setRequired(List.of("queryText"));
    return schema;
  }

  private Schema<?> createFullTextResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Documents matching the full-text query");
    schema.addProperty("indexName", SpecBuilders.string("Index that was searched"));
    schema.addProperty("similarity", SpecBuilders.string("Similarity function the index scores with, e.g. BM25"));
    schema.addProperty("count", SpecBuilders.integer("Number of results returned"));
    schema.addProperty("results", SpecBuilders.arrayOf(hitSchema(), "Hits, highest score first"));
    return schema;
  }

  private Schema<?> hitSchema() {
    final Schema<Object> hit = SpecBuilders.object("One hit");
    hit.addProperty("rid", SpecBuilders.string("Record id of the hit"));
    hit.addProperty("distance", SpecBuilders.number("Dense vector distance, lower is better. Absent on a scored hit"));
    hit.addProperty("score", SpecBuilders.number("Sparse or full-text score, higher is better. Absent on a distance hit"));
    hit.addProperty("properties", SpecBuilders.object("The record's properties"));
    return hit;
  }

  private Schema<?> fusedHitSchema() {
    final Schema<Object> hit = SpecBuilders.object("One fused hit");
    hit.addProperty("rid", SpecBuilders.string("Record id of the hit"));
    hit.addProperty("score", SpecBuilders.number("Fused score, higher is better. Present when 'fused' is true"));
    hit.addProperty("distance", SpecBuilders.number(
        "Vector distance, present instead of 'score' on an unfused dense response"));
    hit.addProperty("sources", SpecBuilders.arrayOf(SpecBuilders.string(null),
        "Which legs contributed this hit: vector, fulltext, expand"));
    hit.addProperty("depth", SpecBuilders.integer("Hops from the seed, for a hit the expansion leg contributed"));
    hit.addProperty("path", SpecBuilders.arrayOf(SpecBuilders.string(null),
        "Record ids from the seed to this hit, seed included"));
    hit.addProperty("properties", SpecBuilders.object("The record's properties"));
    return hit;
  }

  /**
   * An integer property carrying the bound the server actually enforces, so a generated client rejects locally
   * exactly what the server would reject remotely.
   */
  private Schema<?> boundedInteger(final String description, final int minimum, final int maximum,
      final Integer defaultValue) {
    final Schema<Number> schema = SpecBuilders.integer(description);
    schema.setMinimum(BigDecimal.valueOf(minimum));
    schema.setMaximum(BigDecimal.valueOf(maximum));
    if (defaultValue != null)
      schema.setDefault(defaultValue);
    return schema;
  }
}
