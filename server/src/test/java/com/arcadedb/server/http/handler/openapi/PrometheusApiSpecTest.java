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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusApiSpecTest {
  private static final String BASE = "/api/v1/ts/{database}/prom";

  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new PrometheusApiSpec().contribute(openAPI);
  }

  @Test
  void remoteWriteAndReadDeclareProtobufBinaryBodies() {
    for (final String suffix : List.of("write", "read")) {
      final Operation post = openAPI.getPaths().get(BASE + "/" + suffix).getPost();
      assertThat(post.getRequestBody().getContent())
          .as("%s", suffix).containsKey("application/x-protobuf");
      assertThat(post.getRequestBody().getContent().get("application/x-protobuf")
          .getSchema().getFormat()).as("%s", suffix).isEqualTo("binary");
      assertThat(post.getTags()).as("%s", suffix).containsExactly("Prometheus");
    }
  }

  @Test
  void remoteWriteSucceedsWith204() {
    final Operation post = openAPI.getPaths().get(BASE + "/write").getPost();
    assertThat(post.getOperationId()).isEqualTo("prometheusRemoteWrite");
    assertThat(post.getResponses()).containsKey("204");
    assertThat(post.getResponses().get("204").getContent()).isNull();
  }

  @Test
  void remoteReadReturnsProtobuf() {
    final Operation post = openAPI.getPaths().get(BASE + "/read").getPost();
    assertThat(post.getOperationId()).isEqualTo("prometheusRemoteRead");
    assertThat(post.getResponses().get("200").getContent())
        .as("the read response is a protobuf ReadResponse, not JSON")
        .containsKey("application/x-protobuf");
  }

  @Test
  void instantQueryDeclaresItsParameters() {
    final Operation get = openAPI.getPaths().get(BASE + "/api/v1/query").getGet();
    assertThat(get.getOperationId()).isEqualTo("promQLQuery");
    assertThat(get.getTags()).containsExactly("PromQL");
    final Map<String, Boolean> required = get.getParameters().stream()
        .collect(Collectors.toMap(Parameter::getName, Parameter::getRequired));
    assertThat(required)
        .containsEntry("query", true)
        .containsEntry("time", false)
        .containsEntry("lookback_delta", false);
  }

  @Test
  void rangeQueryRequiresStartEndAndStep() {
    final Operation get = openAPI.getPaths().get(BASE + "/api/v1/query_range").getGet();
    assertThat(get.getOperationId()).isEqualTo("promQLQueryRange");
    final Map<String, Boolean> required = get.getParameters().stream()
        .collect(Collectors.toMap(Parameter::getName, Parameter::getRequired));
    assertThat(required)
        .containsEntry("query", true)
        .containsEntry("start", true)
        .containsEntry("end", true)
        .containsEntry("step", true)
        .containsEntry("lookback_delta", false);
  }

  @Test
  void labelsAndLabelValuesShareTheStringDataEnvelope() {
    assertThat(openAPI.getPaths().get(BASE + "/api/v1/labels").getGet().getOperationId())
        .isEqualTo("promQLLabels");
    final Operation values = openAPI.getPaths()
        .get(BASE + "/api/v1/label/{name}/values").getGet();
    assertThat(values.getOperationId()).isEqualTo("promQLLabelValues");
    assertThat(values.getParameters().stream().map(Parameter::getName).toList())
        .contains("database", "name");

    for (final String path : List.of(BASE + "/api/v1/labels", BASE + "/api/v1/label/{name}/values")) {
      assertThat(openAPI.getPaths().get(path).getGet().getResponses().get("200")
          .getContent().get("application/json").getSchema().get$ref())
          .as("%s", path).isEqualTo("#/components/schemas/PromQLLabelsResponse");
    }

    final Schema<?> schema = openAPI.getComponents().getSchemas().get("PromQLLabelsResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder("status", "data");
    assertThat(schema.getProperties().get("data").getItems().getType()).isEqualTo("string");
  }

  @Test
  void seriesRequiresMatchAndReturnsLabelMaps() {
    final Operation get = openAPI.getPaths().get(BASE + "/api/v1/series").getGet();
    assertThat(get.getOperationId()).isEqualTo("promQLSeries");
    final Parameter match = get.getParameters().stream()
        .filter(p -> "match[]".equals(p.getName())).findFirst().orElseThrow();
    assertThat(match.getRequired()).isTrue();
    assertThat(match.getSchema().getType())
        .as("match[] must be an array, not a scalar, so a generated client can send more than one")
        .isEqualTo("array");
    assertThat(match.getSchema().getItems().getType()).isEqualTo("string");
    assertThat(match.getExplode()).isTrue();

    final Schema<?> schema = openAPI.getComponents().getSchemas().get("PromQLSeriesResponse");
    assertThat(schema.getProperties().get("data").getItems().getType()).isEqualTo("object");
  }

  @Test
  void dataResponseCarriesResultTypeAndResult() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("PromQLDataResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder("status", "data");
    final Schema<?> data = schema.getProperties().get("data");
    assertThat(data.getProperties().keySet()).containsExactlyInAnyOrder("resultType", "result");
    final Schema resultType = data.getProperties().get("resultType");
    assertThat(resultType.getEnum()).containsExactlyInAnyOrder("vector", "matrix", "scalar");
  }

  @Test
  void everyPromQlOperationUsesTheErrorEnvelopeNotTheGenericOne() {
    final Schema<?> error = openAPI.getComponents().getSchemas().get("PromQLErrorResponse");
    assertThat(error.getProperties().keySet())
        .containsExactlyInAnyOrder("status", "errorType", "error");

    for (final String path : List.of(BASE + "/api/v1/query", BASE + "/api/v1/query_range",
        BASE + "/api/v1/labels", BASE + "/api/v1/label/{name}/values", BASE + "/api/v1/series")) {
      assertThat(openAPI.getPaths().get(path).getGet().getResponses().get("400")
          .getContent().get("application/json").getSchema().get$ref())
          .as("%s must report errors in the Prometheus envelope", path)
          .isEqualTo("#/components/schemas/PromQLErrorResponse");
    }
  }
}
