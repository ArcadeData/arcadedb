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
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The OpenAPI document is generated from Java text blocks, and a text block whose continuation lines are not joined
 * with a trailing {@code \\} keeps its own indentation as literal spaces. That ships VERBATIM into the document a
 * client renders, so the description arrives with visible multi-space gaps mid-sentence (PR #7755 review).
 * <p>
 * Asserted over every description in the document rather than only the one that had it: the defect is a property of
 * how a text block is written, so the next one written the same way is the same bug, and a test that named one field
 * would not see it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7741DivergenceCausesSchemaTest {

  @Test
  void noGeneratedDescriptionCarriesACollapsedTextBlockIndent() {
    final OpenAPI document = new OpenApiSpecGenerator(null).generateSpec();

    final Map<String, Schema> schemas = document.getComponents().getSchemas();
    assertThat(schemas).as("the document has to have schemas at all, or this test asserts nothing").isNotEmpty();

    schemas.forEach((name, schema) -> assertDescriptionsAreSingleSpaced(name, schema));
  }

  private static void assertDescriptionsAreSingleSpaced(final String path, final Schema<?> schema) {
    if (schema == null)
      return;

    if (schema.getDescription() != null)
      assertThat(schema.getDescription())
          .as("%s: a run of spaces mid-sentence is a text block whose continuation lines were not joined with a "
              + "trailing backslash, and it renders that way for the client", path)
          .doesNotContain("  ");

    if (schema.getProperties() != null)
      schema.getProperties().forEach((property, nested) -> assertDescriptionsAreSingleSpaced(path + "." + property,
          (Schema<?>) nested));

    if (schema.getItems() != null)
      assertDescriptionsAreSingleSpaced(path + "[]", schema.getItems());
    if (schema.getAdditionalProperties() instanceof final Schema<?> values)
      assertDescriptionsAreSingleSpaced(path + "{}", values);
  }
}
