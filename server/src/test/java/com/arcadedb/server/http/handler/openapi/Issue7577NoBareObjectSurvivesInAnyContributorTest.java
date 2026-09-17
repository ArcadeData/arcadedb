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

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7577: a {@code {"type": "object"}} with neither {@code properties} nor {@code additionalProperties}
 * carries no information, so a strict generator emits an empty model and the real content becomes unreachable
 * through typed access.
 * <p>
 * Issue #7568 fixed the four in {@code VectorApiSpec} and pinned them with a sweep over that one contributor.
 * This widens the same sweep to <b>every</b> contributor, and to the schemas declared inline under a path as well
 * as the registered components - which is where most of the remaining ones were, since
 * {@code SpecBuilders.jsonBody(..., null, ...)} and {@code jsonResponse(..., null)} used to produce exactly this
 * shape for every body that named no component. {@code SecurityAdminApiSpec} was made of nothing else.
 * <p>
 * Widening the sweep is what closes the issue for good: a bare object added tomorrow fails here rather than
 * waiting for someone to regenerate a client.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7577">issue #7577</a>
 */
class Issue7577NoBareObjectSurvivesInAnyContributorTest {

  /**
   * The sweep. One assertion over every contributor rather than one test each, so the failure message is the
   * whole list of offenders and a reviewer fixes them in one pass instead of discovering them one red run at a
   * time.
   */
  @Test
  void noSchemaAnywhereInTheDocumentIsABareObject() {
    final List<String> bare = new ArrayList<>();

    for (final OpenApiContributor contributor : OpenApiContributors.all()) {
      final String name = contributor.getClass().getSimpleName();
      final OpenAPI openAPI = OpenApiContributors.contribute(contributor);

      for (final Map.Entry<String, Schema<?>> entry : OpenApiContributors.everySchema(openAPI).entrySet())
        OpenApiContributors.walk(name + "." + entry.getKey(), entry.getValue(), (path, schema) -> {
          if (isBareObject(schema))
            bare.add(path);
        });
    }

    assertThat(bare)
        .as("a 'type: object' with neither 'properties' nor 'additionalProperties' generates an empty model, so "
            + "whatever it really holds is unreachable through typed access. Declare the properties, or say it "
            + "is an open map with SpecBuilders.freeFormObject / mapOf, or - for something that is not an object "
            + "at all - SpecBuilders.anyValue")
        .isEmpty();
  }

  /**
   * The open-map builders are what the fix leans on, so they are pinned here rather than left implied: a
   * {@code freeFormObject} that stopped setting {@code additionalProperties} would make the sweep above start
   * failing everywhere at once, and this says why in one line.
   */
  @Test
  void theOpenMapBuildersAreWhatTakesASchemaOutOfTheBareCase() {
    assertThat(isBareObject(SpecBuilders.object("plain"))).as("the plain builder is the starting point, not the "
        + "end state: it is bare until properties are added").isTrue();
    assertThat(isBareObject(SpecBuilders.freeFormObject("open"))).isFalse();
    assertThat(isBareObject(SpecBuilders.mapOf(SpecBuilders.string("a value"), "typed map"))).isFalse();
    assertThat(isBareObject(SpecBuilders.anyValue("anything")))
        .as("anyValue is not an object at all - it has no type - which is what OpenAPI means by unconstrained")
        .isFalse();
    assertThat(SpecBuilders.anyValue("anything").getType()).isNull();
  }

  /**
   * An un-named body is an open map, not an empty model. This is the rule that fixed eleven of the offenders at
   * once, and it is worth its own assertion because it is a change to a shared builder rather than to a schema:
   * a reader of {@code SecurityAdminApiSpec} alone would not see it.
   */
  @Test
  void aBodyOrResponseThatNamesNoComponentIsDeclaredAnOpenMap() {
    assertThat(SpecBuilders.jsonBody("body", null, true).getContent().get(SpecBuilders.JSON).getSchema()
        .getAdditionalProperties()).isEqualTo(Boolean.TRUE);
    assertThat(SpecBuilders.jsonResponse("response", null).getContent().get(SpecBuilders.JSON).getSchema()
        .getAdditionalProperties()).isEqualTo(Boolean.TRUE);
  }

  private static boolean isBareObject(final Schema<?> schema) {
    return "object".equals(schema.getType())
        && (schema.getProperties() == null || schema.getProperties().isEmpty())
        && schema.getAdditionalProperties() == null;
  }
}
