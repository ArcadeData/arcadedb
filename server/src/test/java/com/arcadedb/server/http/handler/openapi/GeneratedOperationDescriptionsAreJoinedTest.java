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

import com.arcadedb.server.http.handler.OpenApiSpecGenerator;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.headers.Header;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The same invariant {@code GeneratedDescriptionTextBlocksAreJoinedTest} pins, over the half of the document that
 * one does not reach. It walks the component SCHEMAS; a description also hangs off every operation, parameter,
 * request body, response and response HEADER, and none of those were being read.
 * <p>
 * Which is how issue #8062's {@code arcadedb-session-partial-commit} header description shipped with runs of eleven
 * literal spaces mid-sentence and a green suite: a Java text block whose continuation lines are not joined with a
 * trailing {@code \\} keeps its own indentation, that indentation goes verbatim into the generated document, and
 * the client renders the gaps. The sibling test's javadoc already says the defect is a property of how a text block
 * is written rather than of any one field - so the surface it is asserted over is what decides whether the next one
 * is caught, and the header surface was not in it.
 * <p>
 * A separate class rather than another method on the sibling, because the existing test is not modified: the two
 * together cover the whole document.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GeneratedOperationDescriptionsAreJoinedTest {

  @Test
  void noOperationParameterOrResponseHeaderDescriptionCarriesACollapsedTextBlockIndent() {
    final OpenAPI document = new OpenApiSpecGenerator(null).generateSpec();

    assertThat(document.getPaths()).as("the document has to have paths at all, or this test asserts nothing")
        .isNotEmpty();

    int descriptionsRead = 0;
    for (final Map.Entry<String, PathItem> path : document.getPaths().entrySet())
      for (final Operation operation : path.getValue().readOperations())
        descriptionsRead += assertOperation(path.getKey(), operation);

    // The walk has to actually reach text, or an accidentally empty traversal would pass forever.
    assertThat(descriptionsRead).as("descriptions actually read and asserted").isGreaterThan(100);
  }

  private static int assertOperation(final String path, final Operation operation) {
    int read = assertSingleSpaced(path, "description", operation.getDescription())
        + assertSingleSpaced(path, "summary", operation.getSummary());

    if (operation.getParameters() != null)
      for (final Parameter parameter : operation.getParameters())
        read += assertSingleSpaced(path, "parameter '" + parameter.getName() + "'", parameter.getDescription());

    final RequestBody body = operation.getRequestBody();
    if (body != null)
      read += assertSingleSpaced(path, "request body", body.getDescription());

    if (operation.getResponses() != null)
      for (final Map.Entry<String, ApiResponse> response : operation.getResponses().entrySet()) {
        read += assertSingleSpaced(path, "response " + response.getKey(), response.getValue().getDescription());

        final Map<String, Header> headers = response.getValue().getHeaders();
        if (headers != null)
          for (final Map.Entry<String, Header> header : headers.entrySet())
            read += assertSingleSpaced(path, "response " + response.getKey() + " header '" + header.getKey() + "'",
                header.getValue().getDescription());
      }

    return read;
  }

  private static int assertSingleSpaced(final String path, final String what, final String description) {
    if (description == null)
      return 0;

    assertThat(description)
        .as("%s %s: a run of spaces mid-sentence is a text block whose continuation lines were not joined with a "
            + "trailing backslash, and it renders that way for the client", path, what)
        .doesNotContain("  ");
    return 1;
  }
}
