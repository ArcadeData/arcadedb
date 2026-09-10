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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7311: the streaming batch encoding is only reachable by a generated client if the document says it
 * exists - the content type on the 200, the {@code Accept} header that selects it, and the shape of a line.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class BatchStreamingApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);
  }

  @Test
  void theBatch200OffersBothEncodings() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getResponses().get("200").getContent().keySet())
        .as("the buffered body stays the documented default and the stream is offered alongside it")
        .contains("application/json", "application/x-ndjson");
  }

  @Test
  void anAcceptHeaderParameterSelectsTheStream() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    final Parameter accept = post.getParameters().stream()
        .filter(p -> "Accept".equals(p.getName()) && "header".equals(p.getIn()))
        .findFirst().orElse(null);

    assertThat(accept).as("a generated client has no other way to ask for the stream").isNotNull();
    assertThat(accept.getRequired()).isFalse();
    assertThat(accept.getSchema().getEnum()).contains("application/json", "application/x-ndjson");
  }

  @Test
  void theStreamedLineCarriesTheThreeEventKinds() {
    final Schema<?> event = openAPI.getComponents().getSchemas().get("NdJsonBatchEvent");
    assertThat(event).isNotNull();
    assertThat(event.getProperties().keySet()).containsExactlyInAnyOrder("progress", "summary", "error");

    final Schema<?> progress = (Schema<?>) event.getProperties().get("progress");
    assertThat(progress.getProperties().keySet())
        .as("a chunk acknowledgement carries the phase, the counters and the line accounting")
        .contains("phase", "verticesCreated", "edgesCreated", "linesRead", "linesSkipped", "bytesRead");

    assertThat(((Schema<?>) event.getProperties().get("error")).getProperties())
        .as("the status the buffered encoding would have used travels in band, since 200 is already sent, and "
            + "the bookmark is on the FAILED line too - a batch is not atomic, so a failed load still committed "
            + "the chunks a READ_YOUR_WRITES client has to read back")
        .containsKeys("status", "commitIndex", "statusMapped");
    assertThat(((Schema<?>) event.getProperties().get("summary")).getProperties())
        .as("so does the read-your-writes bookmark, which can no longer be a response header")
        .containsKey("commitIndex");
  }

  @Test
  void theDescriptionTellsACallerHowToAskForIt() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getDescription())
        .contains("application/x-ndjson")
        .contains("progress")
        .as("a progress counter is an upper bound, exactly like the partial-commit counters")
        .contains("records attempted");
  }

  /**
   * The 400 and 408 this operation declares are not made unreachable by negotiating the stream: a load that
   * fails before it has acknowledged anything still carries them, because the status line has not been sent
   * yet. A document that implied otherwise would send a generated client looking for those failures only in
   * the body.
   */
  @Test
  void theDescriptionSaysAPreStreamFailureKeepsItsStatusCode() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getDescription()).contains("before it has acknowledged anything");
    assertThat(post.getResponses().keySet()).contains("400", "408");
  }
}
