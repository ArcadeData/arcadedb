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

import com.arcadedb.query.facet.FacetResultsSamples;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The OpenAPI contract of the {@code facets} member: the component schemas {@code Facets}, {@code FacetsMeta},
 * {@code FacetResult} and {@code FacetBucket}, their use in the buffered and the ndjson query response, and a check that the JSON
 * the engine really writes validates against them.
 */
class FacetsApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);
  }

  private Schema<?> schema(final String name) {
    return openAPI.getComponents().getSchemas().get(name);
  }

  private List<Object> enumOf(final String member) {
    return new ArrayList<Object>(((Schema<?>) schema("FacetResult").getProperties().get(member)).getEnum());
  }

  @Test
  void queryResponseReferencesFacets() {
    final Schema<?> facets = (Schema<?>) schema("QueryResponse").getProperties().get("facets");
    assertThat(facets.get$ref()).endsWith("/Facets");
    assertThat(schema("QueryResponse").getRequired()).doesNotContain("facets");
  }

  @Test
  void facetResultSourceEnumMatchesServer() {
    assertThat(enumOf("source"))
        .containsExactly("index", "scan", "unavailable");
  }

  @Test
  void facetResultTypeEnumListsEveryKind() {
    assertThat(enumOf("type"))
        .containsExactly("terms", "range", "histogram", "hierarchy", "distinct", "stats");
  }

  @Test
  void requiredMembersMatchWhatIsAlwaysWritten() {
    assertThat(schema("Facets").getRequired()).containsExactly("meta");
    assertThat(schema("FacetsMeta").getRequired()).containsExactlyInAnyOrder("exact", "patched", "lagMs");
    assertThat(schema("FacetResult").getRequired()).containsExactlyInAnyOrder("type", "source");
    assertThat(schema("FacetBucket").getRequired()).containsExactly("count");
  }

  @Test
  void facetsMapsEveryOtherMemberToAFacetResult() {
    final Schema<?> facets = schema("Facets");
    assertThat(((Schema<?>) facets.getProperties().get("meta")).get$ref()).endsWith("/FacetsMeta");
    assertThat(((Schema<?>) facets.getAdditionalProperties()).get$ref()).endsWith("/FacetResult");
  }

  @Test
  void descriptionsStateTheSemantics() {
    assertThat(schema("Facets").getDescription()).contains("full").contains("LIMIT");
    assertThat(((Schema<?>) schema("FacetsMeta").getProperties().get("exact")).getDescription()).contains("patch budget");
    assertThat(((Schema<?>) schema("FacetResult").getProperties().get("reason")).getDescription()).contains("unavailable");
  }

  @Test
  void ndjsonEventHasFacetsLine() {
    final Schema<?> event = schema("NdJsonQueryEvent");
    assertThat(((Schema<?>) event.getProperties().get("facets")).get$ref()).endsWith("/Facets");
    assertThat(event.getDescription()).contains("facets");
  }

  @Test
  void serverResponseValidatesAgainstSchema() {
    final List<String> problems = new ArrayList<>();
    validateFacets("allKinds", FacetResultsSamples.allKinds().toJSON(), problems);
    validateFacets("plan5Members", FacetResultsSamples.plan5Members().toJSON(), problems);
    assertThat(problems).isEmpty();
  }

  @Test
  void validatorCatchesAnUndeclaredMember() {
    final List<String> problems = new ArrayList<>();
    final JSONObject facets = FacetResultsSamples.allKinds().toJSON();
    facets.getJSONObject("brand").put("surprise", 1);
    facets.getJSONObject("brand").getJSONArray("buckets").getJSONObject(0).put("other", 1);
    facets.getJSONObject("size").remove("source");
    validateFacets("mutated", facets, problems);
    assertThat(problems).hasSize(3);
  }

  private void validateFacets(final String label, final JSONObject facets, final List<String> problems) {
    final Schema<?> facetsSchema = schema("Facets");
    for (final String name : facets.keySet()) {
      if ("meta".equals(name))
        validateObject(label + ".meta", facets.getJSONObject(name), schema("FacetsMeta"), problems);
      else {
        final JSONObject result = facets.getJSONObject(name);
        validateObject(label + "." + name, result, schema("FacetResult"), problems);
        if (result.has("type") && !enumOf("type").contains(result.get("type")))
          problems.add(label + "." + name + ": type " + result.get("type") + " is not in the enum");
        if (result.has("source") && !enumOf("source").contains(result.get("source")))
          problems.add(label + "." + name + ": source " + result.get("source") + " is not in the enum");
        final JSONArray buckets = result.getJSONArray("buckets", new JSONArray());
        for (int i = 0; i < buckets.length(); i++)
          validateObject(label + "." + name + ".buckets[" + i + "]", buckets.getJSONObject(i), schema("FacetBucket"), problems);
      }
    }
    for (final String required : facetsSchema.getRequired())
      if (!facets.has(required))
        problems.add(label + ": missing " + required);
  }

  private static void validateObject(final String path, final JSONObject json, final Schema<?> schema, final List<String> problems) {
    for (final String member : json.keySet())
      if (!schema.getProperties().containsKey(member))
        problems.add(path + ": member '" + member + "' is not declared");
    if (schema.getRequired() != null)
      for (final String required : schema.getRequired())
        if (!json.has(required))
          problems.add(path + ": required member '" + required + "' is absent");
  }
}
