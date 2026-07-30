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

import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SpecBuildersTest {
  @Test
  void pathParameterIsRequiredAndTyped() {
    final Parameter param = SpecBuilders.pathParam("database", "Database name");
    assertThat(param.getIn()).isEqualTo("path");
    assertThat(param.getRequired()).isTrue();
    assertThat(param.getSchema().getType()).isEqualTo("string");
  }

  @Test
  void pathParameterCarriesEnumValues() {
    final Parameter param = SpecBuilders.pathParam("language", "Query language", List.of("sql", "cypher"));
    assertThat(param.getSchema().getEnum()).containsExactly("sql", "cypher");
  }

  @Test
  void optionalQueryParameterIsNotRequired() {
    final Parameter param = SpecBuilders.queryParam("precision", "Timestamp precision", false);
    assertThat(param.getIn()).isEqualTo("query");
    assertThat(param.getRequired()).isFalse();
  }

  @Test
  void repeatableQueryParameterIsAnArrayWithFormExplode() {
    final Parameter param = SpecBuilders.repeatableQueryParam("match[]", "Series selector", true);
    assertThat(param.getIn()).isEqualTo("query");
    assertThat(param.getRequired()).isTrue();
    assertThat(param.getStyle()).isEqualTo(Parameter.StyleEnum.FORM);
    assertThat(param.getExplode()).isTrue();
    assertThat(param.getSchema().getType()).isEqualTo("array");
    assertThat(param.getSchema().getItems().getType()).isEqualTo("string");
  }

  @Test
  void rawBodyDeclaresMediaTypeAndFormat() {
    final RequestBody body = SpecBuilders.rawBody("Snappy protobuf", "application/x-protobuf", "binary");
    assertThat(body.getContent()).containsKey("application/x-protobuf");
    assertThat(body.getContent().get("application/x-protobuf").getSchema().getFormat()).isEqualTo("binary");
  }

  @Test
  void jsonResponseReferencesComponentSchema() {
    final ApiResponse response = SpecBuilders.jsonResponse("OK", "BatchResponse");
    assertThat(response.getContent().get("application/json").getSchema().get$ref())
        .isEqualTo("#/components/schemas/BatchResponse");
  }

  @Test
  void emptyResponseCarriesNoContent() {
    assertThat(SpecBuilders.emptyResponse("No content").getContent()).isNull();
  }

  @Test
  void operationCarriesIdSummaryAndSingleTag() {
    final Operation op = SpecBuilders.operation("listDatabases", "Database", "List databases", "Lists them");
    assertThat(op.getOperationId()).isEqualTo("listDatabases");
    assertThat(op.getTags()).containsExactly("Database");
  }

  @Test
  void publicOperationClearsSecurityToEmptyList() {
    final Operation op = SpecBuilders.operation("checkHealth", "Health", "Liveness", "Liveness probe");
    SpecBuilders.publicOperation(op);
    assertThat(op.getSecurity()).isNotNull().isEmpty();
  }

  @Test
  void basicAuthOnlyExcludesBearer() {
    final Operation op = SpecBuilders.operation("downloadSnapshot", "Cluster", "Snapshot", "Streams a snapshot");
    SpecBuilders.basicAuthOnly(op);
    assertThat(op.getSecurity()).hasSize(1);
    assertThat(op.getSecurity().getFirst()).containsOnlyKeys("basicAuth");
  }
}
