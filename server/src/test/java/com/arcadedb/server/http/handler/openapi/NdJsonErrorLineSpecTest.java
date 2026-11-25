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
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The in-band {@code error} line of the NDJSON query and batch streams, as the document declares it.
 */
class NdJsonErrorLineSpecTest {
  /*
   * Issue #8235: the in-band {@code error} line of a streamed query now carries the status, exception class and
   * {@code exceptionArgs} the buffered encoding would have answered with. A generated client only reads what the
   * document declares, so the schema has to say so.
   */
  // Regression: #8235 the query stream error line declares status, exception and exceptionArgs
  @Test
  void theQueryStreamErrorLineDeclaresStatusExceptionAndArguments() {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);

    final Schema<?> event = openAPI.getComponents().getSchemas().get("NdJsonQueryEvent");
    assertThat(event).isNotNull();
    final Schema<?> error = (Schema<?>) event.getProperties().get("error");
    assertThat(error.getProperties()).containsKeys("message", "status", "exception", "exceptionArgs");
    // Every error line carries both; exception and exceptionArgs may be absent
    assertThat(error.getRequired()).containsExactlyInAnyOrder("message", "status");
  }

  /*
   * Issue #8899: the in-band {@code error} line of both NDJSON streams carries {@code retryAfter} for a retryable
   * refusal, and the query line now carries the rest of the buffered error body. A generated client only reads what
   * the document declares, so both event schemas have to say so.
   */
  // Regression: #8899 both stream error lines declare retryAfter and the buffered body members
  @Test
  void theQueryStreamErrorLineDeclaresRetryAfterAndTheBufferedBodyMembers() {
    final Schema<?> error = errorOf("NdJsonQueryEvent");
    assertThat(error.getProperties()).containsKeys("retryAfter", "error", "detail", "requestId");
    assertThat(error.getProperties().get("retryAfter").getType()).isEqualTo("integer");
    // Optional: only a retryable refusal has a back-off, and production mode drops 'detail'
    assertThat(error.getRequired()).containsExactlyInAnyOrder("message", "status");
  }

  // Regression: #8899 both stream error lines declare retryAfter and the buffered body members
  @Test
  void theBatchStreamErrorLineDeclaresRetryAfter() {
    final Schema<?> error = errorOf("NdJsonBatchEvent");
    assertThat(error.getProperties()).containsKey("retryAfter");
    assertThat(error.getProperties().get("retryAfter").getType()).isEqualTo("integer");
  }

  private static Schema<?> errorOf(final String component) {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);
    final Schema<?> event = openAPI.getComponents().getSchemas().get(component);
    assertThat(event).isNotNull();
    return (Schema<?>) event.getProperties().get("error");
  }
}
