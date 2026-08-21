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

import com.arcadedb.Constants;
import com.arcadedb.server.http.handler.openapi.OpenApiContributor;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * All nine contributors write into one shared {@code components.schemas} map, so a schema name
 * registered by two contributors would have the second silently overwrite the first: every {@code
 * $ref} still resolves, the operation inventory is unchanged, and each contributor's own test
 * passes because it builds a spec containing only itself. This guards the shared namespace
 * directly, across the real, registered contributor list.
 */
class OpenApiSpecGeneratorTest {

  @Test
  void noTwoContributorsRegisterTheSameSchemaName() {
    final Map<String, String> schemaOwner = new HashMap<>();

    for (final OpenApiContributor contributor : OpenApiSpecGenerator.contributors()) {
      final OpenAPI openAPI = new OpenAPI();
      openAPI.setPaths(new Paths());
      openAPI.setComponents(new Components());
      contributor.contribute(openAPI);

      final String contributorName = contributor.getClass().getSimpleName();
      final Map<String, Schema> schemas = openAPI.getComponents().getSchemas();
      if (schemas == null)
        continue;

      for (final String schemaName : schemas.keySet()) {
        final String existingOwner = schemaOwner.get(schemaName);
        assertThat(existingOwner)
            .as("schema '%s' is registered by both %s and %s", schemaName, existingOwner, contributorName)
            .isNull();
        schemaOwner.put(schemaName, contributorName);
      }
    }
  }

  /**
   * publish-contract.yml compares the served document's {@code info.version} to the bare release
   * tag. {@link Constants#getVersion()} appends a build suffix ("... (build ...)"); using it here
   * would break that comparison on every publish. This guards {@code createApiInfo()} in the only
   * unit lane that runs on this machine: the IT that also asserts this cannot bind port 2480 here.
   */
  @Test
  void infoVersionIsTheBareReleaseVersionNotTheBuildStampedOne() {
    final OpenAPI openAPI = new OpenApiSpecGenerator(null).generateSpec();

    final String version = openAPI.getInfo().getVersion();

    assertThat(version)
        .as("publish-contract.yml compares info.version to the bare release tag, so it must equal "
            + "Constants.getRawVersion(), not the build-stamped Constants.getVersion()")
        .isEqualTo(Constants.getRawVersion());
    assertThat(version)
        .as("a build-stamped version (\"... (build ...)\") would never equal the release tag and "
            + "would break the publish gate forever")
        .doesNotContain(" (build ");
  }
}
