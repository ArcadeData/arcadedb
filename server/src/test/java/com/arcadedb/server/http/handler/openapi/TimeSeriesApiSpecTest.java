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
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class TimeSeriesApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new TimeSeriesApiSpec().contribute(openAPI);
  }

  @Test
  void writeTakesLineProtocolTextNotJson() {
    final Operation post = openAPI.getPaths().get("/api/v1/ts/{database}/write").getPost();
    assertThat(post.getOperationId()).isEqualTo("writeTimeSeries");
    assertThat(post.getRequestBody().getContent().keySet())
        .as("the body is InfluxDB Line Protocol text, not JSON")
        .containsExactly("text/plain");
  }

  @Test
  void writeDeclaresPrecisionEnum() {
    final Operation post = openAPI.getPaths().get("/api/v1/ts/{database}/write").getPost();
    final Parameter precision = post.getParameters().stream()
        .filter(p -> "precision".equals(p.getName())).findFirst().orElseThrow();
    assertThat(precision.getSchema().getEnum()).containsExactly("ns", "us", "ms", "s");
    assertThat(precision.getRequired()).isFalse();
  }

  @Test
  void precisionDescriptionPinsTheHardCodedNanosecondDefault() {
    // PostTimeSeriesWriteHandler.execute falls back to Precision.NANOSECONDS in code when the
    // 'precision' query parameter is absent; the default is hard-coded in the handler, not read
    // from a server setting. Pinned so the wording cannot be reverted to claim otherwise.
    final Operation post = openAPI.getPaths().get("/api/v1/ts/{database}/write").getPost();
    final Parameter precision = post.getParameters().stream()
        .filter(p -> "precision".equals(p.getName())).findFirst().orElseThrow();
    assertThat(precision.getDescription())
        .isEqualTo("Unit of the timestamps in the body. Defaults to nanoseconds when omitted.");
  }

  @Test
  void writeSucceedsWith204AndNeverWith200() {
    final Operation post = openAPI.getPaths().get("/api/v1/ts/{database}/write").getPost();
    assertThat(post.getResponses()).containsKey("204");
    assertThat(post.getResponses().get("204").getContent()).isNull();
    assertThat(post.getResponses().get("200")).isNull();
  }

  @Test
  void writeErrorBodyCarriesTheIngestionCounts() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("TimeSeriesWriteError");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "error", "requestId", "written", "dropped", "unknownTypes", "nonTimeSeriesTypes", "unavailableTypes");
    assertThat(openAPI.getPaths().get("/api/v1/ts/{database}/write").getPost()
        .getResponses().get("400").getContent().get("application/json").getSchema().get$ref())
        .isEqualTo("#/components/schemas/TimeSeriesWriteError");
  }

  @Test
  void queryRequestModelsTheNestedAggregation() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("TimeSeriesQueryRequest");
    assertThat(schema.getRequired()).containsExactly("type");
    assertThat(schema.getProperties().keySet())
        .containsExactlyInAnyOrder("type", "from", "to", "tags", "fields", "aggregation", "limit");

    final Schema<?> aggregation = schema.getProperties().get("aggregation");
    assertThat(aggregation.getProperties().keySet())
        .containsExactlyInAnyOrder("bucketInterval", "requests");
    assertThat(aggregation.getProperties().get("requests").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("field", "type", "alias");
  }

  @Test
  void queryResponseIsAOneOfOverTheRawAndAggregatedShapes() {
    final Operation post = openAPI.getPaths().get("/api/v1/ts/{database}/query").getPost();
    final Schema<?> success = post.getResponses().get("200")
        .getContent().get("application/json").getSchema();
    assertThat(success.getOneOf())
        .as("the raw and aggregated shapes are structurally different")
        .hasSize(2);
    assertThat(success.getOneOf().stream().map(Schema::get$ref).toList())
        .containsExactlyInAnyOrder(
            "#/components/schemas/TimeSeriesRawResponse",
            "#/components/schemas/TimeSeriesAggregatedResponse");
  }

  @Test
  void rawAndAggregatedResponsesHaveTheirDistinctFields() {
    assertThat(openAPI.getComponents().getSchemas().get("TimeSeriesRawResponse")
        .getProperties().keySet()).containsExactlyInAnyOrder("type", "columns", "rows", "count");
    final Schema<?> aggregated = openAPI.getComponents().getSchemas()
        .get("TimeSeriesAggregatedResponse");
    assertThat(aggregated.getProperties().keySet())
        .containsExactlyInAnyOrder("type", "aggregations", "buckets", "count");
    assertThat(aggregated.getProperties().get("buckets").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("timestamp", "values");
  }

  @Test
  void latestDeclaresTypeRequiredAndTagOptional() {
    final Operation get = openAPI.getPaths().get("/api/v1/ts/{database}/latest").getGet();
    assertThat(get.getOperationId()).isEqualTo("getTimeSeriesLatest");
    final Map<String, Boolean> required = get.getParameters().stream()
        .collect(Collectors.toMap(Parameter::getName, Parameter::getRequired));
    assertThat(required).containsEntry("type", true).containsEntry("tag", false);
  }

  @Test
  void tagDescriptionPinsTheColonSeparatorAndAllOccurrencesConjoinedSemantics() {
    // GetTimeSeriesLatestHandler.buildTagFilter splits each 'tag' query parameter on the first ':'
    // (not '='), and since issue #7321 reads every occurrence out of the Deque and ANDs them, the way
    // POST /ts/{database}/query ANDs the pairs of its 'tags' object. Pinned so the wording cannot be
    // reverted to claim '=' or first-occurrence-only semantics.
    //
    // The refusal sentence is issue #7334: an unresolvable or malformed occurrence used to be DROPPED, which
    // widened the query silently, and a contract change on a documented endpoint has to be documented.
    final Operation get = openAPI.getPaths().get("/api/v1/ts/{database}/latest").getGet();
    final Parameter tag = get.getParameters().stream()
        .filter(p -> "tag".equals(p.getName())).findFirst().orElseThrow();
    assertThat(tag.getDescription()).isEqualTo(
        "Tag filter in name:value form. Repeat the parameter to narrow to one series across several tags: "
            + "every occurrence must match. An occurrence that carries no ':' separator, or whose name is no "
            + "TAG column of the type, is refused with 400 rather than ignored.");
  }

  /**
   * Issue #7334: the 400 of both read endpoints says which refusal it is. A generic "Bad request" against an
   * endpoint whose most likely refusal is a mistyped tag tells the caller nothing, and until this change it
   * was not a refusal at all - the term was dropped and the query widened.
   */
  @Test
  void bothReadEndpointsDocumentTheUnresolvableTagRefusal() {
    assertThat(openAPI.getPaths().get("/api/v1/ts/{database}/query").getPost()
        .getResponses().get("400").getDescription())
        .contains("TAG column")
        .contains("declared TAG columns");
    assertThat(openAPI.getPaths().get("/api/v1/ts/{database}/latest").getGet()
        .getResponses().get("400").getDescription())
        .contains("name:value")
        .contains("TAG column");
  }

  @Test
  void tagIsDeclaredAsARepeatableParameterSoGeneratedClientsCanSendMoreThanOne() {
    // Issue #7321. A plain string parameter makes every generated client take a single tag, which is the
    // very limitation the handler no longer has. Declared as an array with form style and explode, the
    // shape that serializes as tag=a:1&tag=b:2 - the same declaration Prometheus 'match[]' already uses.
    final Operation get = openAPI.getPaths().get("/api/v1/ts/{database}/latest").getGet();
    final Parameter tag = get.getParameters().stream()
        .filter(p -> "tag".equals(p.getName())).findFirst().orElseThrow();
    assertThat(tag.getSchema().getType()).isEqualTo("array");
    assertThat(tag.getSchema().getItems().getType()).isEqualTo("string");
    assertThat(tag.getStyle()).isEqualTo(Parameter.StyleEnum.FORM);
    assertThat(tag.getExplode()).isTrue();
  }

  @Test
  void latestResponseAllowsANullLatestSample() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("TimeSeriesLatestResponse");
    assertThat(schema.getProperties().keySet())
        .containsExactlyInAnyOrder("type", "columns", "latest");
    assertThat(schema.getProperties().get("latest").getNullable())
        .as("latest is JSON null when the series is empty")
        .isTrue();
  }
}
