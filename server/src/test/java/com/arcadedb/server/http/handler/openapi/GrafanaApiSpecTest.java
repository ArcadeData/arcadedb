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

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GrafanaApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new GrafanaApiSpec().contribute(openAPI);
  }

  @Test
  void healthReportsStatusAndDatabase() {
    assertThat(openAPI.getPaths().get("/api/v1/ts/{database}/grafana/health").getGet()
        .getOperationId()).isEqualTo("checkGrafanaHealth");
    assertThat(openAPI.getComponents().getSchemas().get("GrafanaHealth").getProperties().keySet())
        .containsExactlyInAnyOrder("status", "database");
  }

  @Test
  void metadataDescribesTypesFieldsAndTags() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("GrafanaMetadata");
    assertThat(schema.getProperties().keySet())
        .containsExactlyInAnyOrder("types", "aggregationTypes");
    final Schema<?> type = schema.getProperties().get("types").getItems();
    assertThat(type.getProperties().keySet())
        .containsExactlyInAnyOrder("name", "fields", "tags");
    assertThat(type.getProperties().get("fields").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("name", "dataType");
  }

  // GetGrafanaMetadataHandler puts the same {name, dataType} object into both the fields and the
  // tags array, distinguished only by the column's role. 'tags' is therefore not a plain string
  // array; it carries the same shape as 'fields'.
  @Test
  void metadataTagsShareTheFieldShapeNotPlainStrings() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("GrafanaMetadata");
    final Schema<?> type = schema.getProperties().get("types").getItems();
    assertThat(type.getProperties().get("tags").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("name", "dataType");
  }

  @Test
  void queryRequestRequiresTargets() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("GrafanaQueryRequest");
    assertThat(schema.getRequired()).containsExactly("targets");
    // PostGrafanaQueryHandler.executeRawQuery reads target.getJSONArray("fields") to project
    // columns on a non-aggregated query, in addition to refId, type, tags, and aggregation.
    assertThat(schema.getProperties().get("targets").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("refId", "type", "tags", "aggregation", "fields");
  }

  // PostGrafanaQueryHandler reads 'from' and 'to' as top-level long timestamps
  // (payload.getLong("from", ...) / payload.getLong("to", ...)), never from a nested 'range'
  // object, and GrafanaTimeSeriesHandlerIT confirms this by setting them at the top level too.
  @Test
  void fromAndToAreTopLevelIntegersNotNestedUnderRange() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("GrafanaQueryRequest");
    assertThat(schema.getProperties().keySet())
        .containsExactlyInAnyOrder("targets", "from", "to", "maxDataPoints");
    assertThat(schema.getProperties().get("from").getType()).isEqualTo("integer");
    assertThat(schema.getProperties().get("to").getType()).isEqualTo("integer");
  }

  @Test
  void queryResponseIsTheGrafanaDataFrameEnvelopeNotSimpleJson() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("GrafanaQueryResponse");
    assertThat(schema.getProperties().keySet()).containsExactly("results");

    final Schema<?> perRefId = (Schema<?>) schema.getProperties().get("results").getAdditionalProperties();
    assertThat(perRefId)
        .as("results is keyed by refId, so it must be modelled with additionalProperties")
        .isNotNull();
    // PostGrafanaQueryHandler.buildErrorFrame puts {error, frames: []} in place of a normal frame
    // set when a target names a missing type, a non-TimeSeries type, or an unresolvable
    // aggregation field, so 'error' is a real optional sibling of 'frames', not just 'frames'.
    assertThat(perRefId.getProperties().keySet()).containsExactlyInAnyOrder("frames", "error");

    final Schema<?> frame = perRefId.getProperties().get("frames").getItems();
    assertThat(frame.getProperties().keySet()).containsExactlyInAnyOrder("schema", "data");
    final Schema<?> frameSchema = frame.getProperties().get("schema");
    assertThat(frameSchema.getProperties().get("fields").getItems()
        .getProperties().keySet()).containsExactlyInAnyOrder("name", "type");
    assertThat(frame.getProperties().get("data").getProperties().keySet()).containsExactly("values");
  }

  @Test
  void everyGrafanaOperationIsTaggedGrafanaAndTakesTheDatabasePath() {
    for (final String suffix : List.of("health", "metadata", "query")) {
      final PathItem item = openAPI.getPaths().get("/api/v1/ts/{database}/grafana/" + suffix);
      final Operation op = item.getGet() != null ? item.getGet() : item.getPost();
      assertThat(op.getTags()).as("%s", suffix).containsExactly("Grafana");
      assertThat(op.getParameters().stream().map(Parameter::getName).toList())
          .as("%s", suffix).contains("database");
    }
  }
}
