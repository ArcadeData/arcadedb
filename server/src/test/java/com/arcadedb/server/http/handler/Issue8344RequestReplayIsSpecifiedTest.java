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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.http.IdempotencyCache;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8344: the replay protection {@code X-Request-Id} gives every POST route (issue #5023), and the {@code 409} +
 * {@code Retry-After} a retry receives while the first request is still executing (issue #8324), were not in the
 * OpenAPI document, so a client generated from it knew of neither.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8344RequestReplayIsSpecifiedTest {
  private static final String REQUEST_ID_PARAM_REF = "#/components/parameters/RequestIdParam";

  private final OpenAPI openAPI = new OpenApiSpecGenerator(null).generateSpec();

  @Test
  void everyReplayedPostRouteDeclaresTheRequestIdAndTheInFlightConflict() {
    openAPI.getPaths().forEach((path, item) -> {
      final Operation post = item.getPost();
      if (post == null || path.equals("/api/v1/batch/{database}"))
        return;
      assertThat(refs(post.getParameters())).as("POST %s", path).contains(REQUEST_ID_PARAM_REF);
      final ApiResponse conflict = post.getResponses().get("409");
      assertThat(conflict).as("POST %s", path).isNotNull();
      assertThat(conflict.getDescription()).as("POST %s", path).contains("still executing");
      assertThat(conflict.getHeaders()).as("POST %s", path).containsKeys("Retry-After", IdempotencyCache.HEADER_REQUEST_ID);
    });
  }

  @Test
  void theCommandRouteIsCovered() {
    // Guards the loop above against passing vacuously on a document with no matching route
    final Operation command = openAPI.getPaths().get("/api/v1/command/{database}").getPost();
    assertThat(refs(command.getParameters())).contains(REQUEST_ID_PARAM_REF);
    assertThat(command.getResponses()).containsKey("409");
  }

  @Test
  void theBulkLoadRouteIsNotPromisedAReplayItDoesNotGive() {
    // Its body is never buffered, so it cannot be part of the replay key (issue #7381)
    final Operation batch = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(refs(batch.getParameters())).doesNotContain(REQUEST_ID_PARAM_REF);
  }

  @Test
  void aReadIsNotPromisedAReplay() {
    openAPI.getPaths().forEach((path, item) -> {
      if (item.getGet() != null)
        assertThat(refs(item.getGet().getParameters())).as("GET %s", path).doesNotContain(REQUEST_ID_PARAM_REF);
    });
  }

  @Test
  void theParameterDescribesTheReplaySemantics() {
    final Parameter param = openAPI.getComponents().getParameters().get("RequestIdParam");
    assertThat(param.getName()).isEqualTo(IdempotencyCache.HEADER_REQUEST_ID);
    assertThat(param.getIn()).isEqualTo("header");
    assertThat(param.getRequired()).isFalse();
    assertThat(param.getDescription())
        .contains(GlobalConfiguration.HA_IDEMPOTENCY_CACHE_TTL_MS.getKey())
        .contains(GlobalConfiguration.HA_IDEMPOTENCY_CACHE_MAX_BODY_BYTES.getKey())
        .contains("2xx")
        .contains("409");
  }

  private static List<String> refs(final List<Parameter> parameters) {
    return parameters == null ? List.of() : parameters.stream().map(Parameter::get$ref).filter(r -> r != null).toList();
  }
}
