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

import com.arcadedb.query.search.FullTextSearchOperation;
import com.arcadedb.query.search.HybridSearchOperation;
import com.arcadedb.query.search.VectorSearchLeg;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.math.BigDecimal;
import java.util.List;

/**
 * Documents the search endpoints under {@code /api/v1/vector/{database}} (issue #7306): kNN over a vector index,
 * the fused hybrid search, and full-text search.
 * <p>
 * Every numeric bound in this document is read from the operation that enforces it
 * ({@link VectorSearchLeg}, {@link HybridSearchOperation}, {@link FullTextSearchOperation}) rather than restated,
 * so the published contract cannot drift from what the server will actually accept - which is the same reason the
 * MCP tool schemas read them from the same constants.
 */
public class VectorApiSpec implements OpenApiContributor {

  private static final String TAG = "Vector";

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
    final Operation post = SpecBuilders.operation("vectorSearch", TAG,
        "k-nearest-neighbor search over a vector index",
        """
            Searches a dense LSM_VECTOR or sparse LSM_SPARSE_VECTOR index with a pre-computed query vector. \
            ArcadeDB does not generate embeddings: the caller supplies the vector.

            Dense results carry a distance, where lower is better; sparse results carry a score, where higher \
            is better. The 'scoring' field of the response says which, and names the similarity function, so a \
            client never has to infer the ranking direction from the index type.

            A filtered search inspects a bounded candidate window whose size is reported as 'candidateLimit'. \
            The 'truncated' flag means that window was filled and further matches may exist, so raising 'k' can \
            return more; when fewer than 'k' results come back the flag is false and the search already returned \
            everything it could find inside the window.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.jsonBody("Query vector, target index and result window",
        "VectorSearchRequest", true));
    post.setResponses(searchResponses("Ranked neighbors", "VectorSearchResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createHybridPath() {
    final Operation post = SpecBuilders.operation("hybridSearch", TAG,
        "Fused vector, full-text and graph-expansion search",
        """
            Retrieves records by fusing a vector search, an optional full-text search and an optional graph \
            expansion into one ranked list, so a neighbor of a strong match can itself rank.

            Add the full-text leg with 'fulltextIndexName' and 'fulltextQuery' together, and the graph leg with \
            'expand'. Fusion needs at least two sources: a request that names only the vector leg answers \
            'fused: false' and returns that leg's own distance or score rather than a fabricated fused one.

            Graph expansion is ranked by traversal order and carries no score, so it can only be fused with the \
            RRF strategy; DBSF and LINEAR need a score on every row and are rejected when 'expand' is present.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.jsonBody("Retrieval legs, fusion strategy and weights",
        "HybridSearchRequest", true));
    post.setResponses(searchResponses("Fused ranking", "HybridSearchResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createFullTextPath() {
    final Operation post = SpecBuilders.operation("fullTextSearch", TAG,
        "Full-text search over a FULL_TEXT index",
        """
            Searches a full-text index and returns the matching records ranked by relevance.

            Address the index either by 'indexName' or by 'typeName' plus optional 'properties'; 'indexName' \
            wins when both are given. An index declared on a supertype is named for the supertype, so a subtype \
            name resolves nothing.

            Query syntax: '+a +b' requires both terms, 'a -b' excludes b, 'a b' matches either, '"exact phrase"' \
            requires all terms in the same record without enforcing their order, 'pre*' matches a prefix, \
            'term~' is a fuzzy match, 'field:term' restricts to one property of a multi-property index, and \
            'term^2' boosts a term. The 'similarity' field of the response says whether the scores are BM25 or \
            legacy CLASSIC coordination counts.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.jsonBody("Query text, target index and result window",
        "FullTextSearchRequest", true));
    post.setResponses(searchResponses("Ranked matches", "FullTextSearchResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  /**
   * The response set shared by the three searches. 400 is the one worth naming: every argument fault - an index
   * that is not a vector index, a query vector of the wrong dimensionality, an out-of-range window - arrives as
   * an {@code IllegalArgumentException} from the shared operation and is mapped to 400 with the operation's own
   * message, which names the field and, where it can, the legal values.
   */
  private ApiResponses searchResponses(final String successDescription, final String componentName) {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse(successDescription, componentName));
    responses.addApiResponse("400", SpecBuilders.errorResponse(
        "Malformed request: unknown or wrong-type index, wrong vector dimensionality, or an out-of-range "
            + "'k', 'efSearch' or 'limit'"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    return responses;
  }

  private static Schema<Number> boundedInteger(final String description, final int min, final int max) {
    final Schema<Number> schema = SpecBuilders.integer(description);
    schema.setMinimum(BigDecimal.valueOf(min));
    schema.setMaximum(BigDecimal.valueOf(max));
    return schema;
  }

  private static Schema<?> numberArray(final String description) {
    return SpecBuilders.arrayOf(new Schema<>().type("number"), description);
  }

  /**
   * The vector-leg properties shared by {@code /search} and {@code /hybrid}. The two endpoints name the index
   * differently - {@code indexName} against {@code vectorIndexName} - which is the only difference between them,
   * so the field name is a parameter here rather than a reason to write the block twice.
   */
  private static void addVectorLegProperties(final Schema<Object> schema, final String indexNameField,
      final String indexDescription) {
    schema.addProperty(indexNameField, SpecBuilders.string(indexDescription));
    schema.addProperty("queryVector", numberArray(
        "Dense query vector, or the sparse weights matching 'queryIndices' when sparse is true"));
    schema.addProperty("queryIndices", SpecBuilders.arrayOf(new Schema<>().type("integer"),
        "Sparse dimension ids matching the 'queryVector' weights; omit to use the vector's own positions. "
            + "Requires sparse=true"));
    schema.addProperty("k", boundedInteger(
        "Maximum number of results to return (default " + VectorSearchLeg.DEFAULT_K + ")",
        1, VectorSearchLeg.MAX_K));
    schema.addProperty("efSearch", boundedInteger(
        "Dense-index search beam width: higher values improve recall at higher cost. Applies only to a dense "
            + "LSM_VECTOR index", 1, VectorSearchLeg.MAX_EF_SEARCH));
    schema.addProperty("filter", SpecBuilders.string(
        "Read-only SQL WHERE predicate applied to the bounded candidate set. It is evaluated against each "
            + "expanded neighbor row, where the record properties are flattened and @rid, @type, record, plus "
            + "distance (dense) or score (sparse) are available"));
    schema.addProperty("sparse", SpecBuilders.bool(
        "Search an LSM_SPARSE_VECTOR index with vector.sparseNeighbors instead of a dense one"));
  }

  private Schema<?> createSearchRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("kNN search request");
    addVectorLegProperties(schema, "indexName", "Name of an LSM_VECTOR or LSM_SPARSE_VECTOR index");
    schema.setRequired(List.of("indexName", "queryVector", "k"));
    return schema;
  }

  private Schema<?> createSearchResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Ranked neighbors of the query vector");
    schema.addProperty("indexName", SpecBuilders.string("The index that was searched"));
    schema.addProperty("sparse", SpecBuilders.bool("Whether the sparse path was used"));
    schema.addProperty("scoring", SpecBuilders.string(
        "Ranking direction and similarity function, e.g. 'distance_lower_is_better:COSINE'"));
    schema.addProperty("candidateLimit", SpecBuilders.integer(
        "Size of the candidate window the search inspected, which a filter over-fetches into"));
    schema.addProperty("truncated", SpecBuilders.bool(
        "The result window was filled, so further matches may exist: raise 'k' to see them"));
    schema.addProperty("count", SpecBuilders.integer("Number of entries in 'results'"));
    schema.addProperty("results", SpecBuilders.arrayOf(hitSchema(true), "Neighbors in ranked order"));
    return schema;
  }

  private static Schema<?> hitSchema(final boolean vectorHit) {
    final Schema<Object> hit = SpecBuilders.object("One matching record");
    hit.addProperty("rid", SpecBuilders.string("Record id, in #bucket:position form"));
    if (vectorHit) {
      hit.addProperty("distance", new Schema<>().type("number")
          .description("Dense-index distance, lower is better. Present only on a dense search"));
      hit.addProperty("score", new Schema<>().type("number")
          .description("Sparse-index score, higher is better. Present only on a sparse search"));
    } else {
      hit.addProperty("score", new Schema<>().type("number").description("Relevance score, higher is better"));
    }
    hit.addProperty("properties", SpecBuilders.object("The record's own properties"));
    return hit;
  }

  private Schema<?> createHybridRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Fused search request");
    addVectorLegProperties(schema, "vectorIndexName", "Name of an LSM_VECTOR or LSM_SPARSE_VECTOR index");
    schema.addProperty("fulltextIndexName", SpecBuilders.string(
        "Full-text index for the second leg, e.g. 'Article[content]'. Must be given with 'fulltextQuery'"));
    schema.addProperty("fulltextQuery", SpecBuilders.string(
        "Full-text query for the second leg. Must be given with 'fulltextIndexName'"));

    final Schema<Object> expand = SpecBuilders.object(
        "Graph expansion leg, seeded from the records the retrieval legs found. Requires the RRF strategy");
    expand.addProperty("edgeTypes", SpecBuilders.arrayOf(new Schema<>().type("string"),
        "Edge types to traverse; omit to traverse every edge type. An unknown name is rejected rather than "
            + "silently matching nothing"));
    expand.addProperty("direction", SpecBuilders.string("Traversal direction")._enum(List.of("out", "in", "both")));
    expand.addProperty("maxDepth", boundedInteger("Hops to walk", 1, HybridSearchOperation.MAX_DEPTH));
    schema.addProperty("expand", expand);

    schema.addProperty("fusionStrategy", SpecBuilders.string(
            "Fusion strategy. DBSF and LINEAR need a score on every row, so neither can be combined with the "
                + "rank-only expansion leg")
        ._enum(List.of("RRF", "DBSF", "LINEAR")));

    final Schema<Object> weights = SpecBuilders.object(
        "Per-leg fusion weights. A weight naming a leg the request does not ask for is rejected rather than "
            + "silently ignored");
    weights.addProperty("vector", new Schema<>().type("number").description("Default 1.0"));
    weights.addProperty("fulltext", new Schema<>().type("number").description("Default 1.0"));
    weights.addProperty("expand", new Schema<>().type("number").description(
        "Default 0.5, because an arbitrary neighbor should not outrank a direct match"));
    schema.addProperty("weights", weights);

    schema.setRequired(List.of("vectorIndexName", "queryVector", "k"));
    return schema;
  }

  private Schema<?> createHybridResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Fused ranking across the retrieval legs");
    schema.addProperty("vectorIndexName", SpecBuilders.string("The vector index that was searched"));
    schema.addProperty("fulltextIndexName", SpecBuilders.string(
        "The full-text index that was searched, present whenever that leg ran - including when it matched "
            + "nothing and so could not become a fusion source"));
    schema.addProperty("sparse", SpecBuilders.bool("Whether the vector leg used the sparse path"));
    schema.addProperty("scoring", SpecBuilders.string("Ranking direction and similarity of the vector leg"));
    schema.addProperty("legs", SpecBuilders.object(
        "Per-leg report: row counts, and for the expansion leg its direction, edge types, depth, seed count "
            + "and whether either budget was filled"));
    schema.addProperty("fused", SpecBuilders.bool(
        "False when only one leg produced rows, in which case each result carries that leg's native distance "
            + "or score instead of a fused one"));
    schema.addProperty("fusionStrategy", SpecBuilders.string("The strategy used, present only when fused"));
    schema.addProperty("truncated", SpecBuilders.bool("The result window was filled: raise 'k' to see more"));
    schema.addProperty("count", SpecBuilders.integer("Number of entries in 'results'"));

    final Schema<Object> hit = SpecBuilders.object("One fused result");
    hit.addProperty("rid", SpecBuilders.string("Record id, in #bucket:position form"));
    hit.addProperty("fusedScore", new Schema<>().type("number").description("Fused score, present when fused"));
    hit.addProperty("distance", new Schema<>().type("number")
        .description("Vector-leg distance, present on an unfused dense response"));
    hit.addProperty("score", new Schema<>().type("number")
        .description("Vector-leg score, present on an unfused sparse response"));
    hit.addProperty("sources", SpecBuilders.arrayOf(new Schema<>().type("string"),
        "The legs this record came from"));
    hit.addProperty("depth", SpecBuilders.integer("Hops from its seed, present only for an expansion result"));
    hit.addProperty("path", SpecBuilders.arrayOf(new Schema<>().type("string"),
        "RID path back to the seed, including the seed. Present only for an expansion result"));
    hit.addProperty("properties", SpecBuilders.object("The record's own properties"));
    schema.addProperty("results", SpecBuilders.arrayOf(hit, "Results in fused rank order"));
    return schema;
  }

  private Schema<?> createFullTextRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Full-text search request");
    schema.addProperty("indexName", SpecBuilders.string(
        "Name of the full-text index, e.g. 'Article[content]' or 'Article[title,body]'"));
    schema.addProperty("typeName", SpecBuilders.string(
        "Type carrying the index, used instead of 'indexName'. Usable alone only when the type carries exactly "
            + "one full-text index"));
    schema.addProperty("properties", SpecBuilders.arrayOf(new Schema<>().type("string"),
        "Indexed properties, used with 'typeName'. Must be given in the order the index declares them"));
    schema.addProperty("queryText", SpecBuilders.string("The full-text query"));
    schema.addProperty("limit", boundedInteger(
        "Maximum number of results to return (default " + FullTextSearchOperation.DEFAULT_LIMIT + ")",
        1, FullTextSearchOperation.MAX_LIMIT));
    schema.setRequired(List.of("queryText"));
    return schema;
  }

  private Schema<?> createFullTextResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Records matching the full-text query");
    schema.addProperty("indexName", SpecBuilders.string("The index that was searched"));
    schema.addProperty("similarity", SpecBuilders.string(
        "Whether the scores are BM25 or legacy CLASSIC coordination counts"));
    schema.addProperty("count", SpecBuilders.integer("Number of entries in 'results'"));
    schema.addProperty("results", SpecBuilders.arrayOf(hitSchema(false), "Matches in descending score order"));
    return schema;
  }
}
