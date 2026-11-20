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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sweeps over every {@link OpenApiContributor}: no schema anywhere in the document is a bare object, and every
 * component schema says which fields it always sends.
 */
class OpenApiSchemaSweepTest {
  /*
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
  // Regression: #7577 no schema in any contributor is a bare object
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

  // Regression: #7577 no schema in any contributor is a bare object
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

  // Regression: #7577 no schema in any contributor is a bare object
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

  /*
   * Issue #7578: a schema with no {@code required} list types every one of its fields as optional in a generated
   * client, even the fields the server always sends. Every caller in a statically typed language then null-checks a
   * case the server cannot produce, or casts the check away - and each client works around it differently.
   * <p>
   * Thirty-four of the fifty-five component schemas declared none; {@code McpApiSpec} and
   * {@code SecurityAdminApiSpec} had none whatsoever, the latter because it registered no schemas at all. The work
   * was never writing {@code setRequired(...)}: it was establishing, per schema, which fields the handler
   * <em>really</em> always sends, since a field wrongly marked required makes the contract lie in the other
   * direction. Reading the handlers is also what turned up three properties {@code ServerInfo} declared that no
   * handler has ever written, and several the handlers write that no schema declared.
   * <p>
   * This is the sweep that keeps the answer from decaying one schema at a time. The exemptions below are the
   * schemas that genuinely have no always-present member, each with the reason; anything else must say what it
   * always sends.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7578">issue #7578</a>
   */
  /**
   * The schemas with no {@code required} list, and why each is right to have none. An exemption is a claim about
   * behaviour, so it carries its reason here rather than being a bare name on a list.
   */
  private static final Map<String, String> EXEMPT = Map.ofEntries(
      // A partial update: every field is optional BECAUSE an omitted one keeps its current value. This schema
      // exists precisely so McpConfig can carry the response's required list without the document refusing the
      // partial update its own description asks for.
      Map.entry("McpConfigUpdate", "a partial update: an omitted field keeps its current value"),
      Map.entry("McpDatabaseOverride", "every field is optional by design - an omitted one inherits the server-wide value"),
      // The two streaming line schemas name their kind by WHICH key is present, so requiring any of them would
      // refuse the other kinds of line.
      Map.entry("NdJsonQueryEvent", "exactly one of 'record', 'stats' or 'error' is present, so none of them can be required"),
      Map.entry("NdJsonBatchEvent", "exactly one of 'progress', 'summary' or 'error' is present"),
      // A oneOf wrapper: the constraint lives in the branches it names.
      Map.entry("BatchLine", "a oneOf over BatchVertexLine and BatchEdgeLine, which carry the required lists"),
      Map.entry("JsonRpcMessage", "a oneOf over one envelope and a batch of them; the envelope carries the required list"),
      Map.entry("VerifyDatabaseResponse", "a oneOf over the local and cluster shapes, which carry the required lists"),
      Map.entry("TransferLeaderRequest", "an empty object is a valid request - it lets Raft choose the target"),
      // Issue #10295: PostBeginHandler falls back to the database's default isolation when 'isolationLevel' is absent
      Map.entry("BeginRequest", "an empty object is a valid request - it begins at the database's default isolation"),
      Map.entry("UpdateUserRequest", "both members are optional; a body carrying neither is accepted and changes nothing"),
      // Every member is optional and the schema's own descriptions say so: `reason` is "Optional.", `catchUp`
      // reads "false (or absent)", and `fingerprints` says "Omit them to have every document seeded", which is
      // exactly what an admission does. An empty body is therefore a valid seed request.
      Map.entry("SecuritySeedRequest",
          "every member is optional - an empty body asks for a full seed, which is what an admission sends"));

  // Regression: #7578 every component schema declares a required list
  /**
   * The sweep. Collected into one list rather than asserted per schema so the failure names every offender at
   * once.
   */
  @Test
  void everyComponentSchemaSaysWhatItAlwaysSends() {
    final List<String> silent = new ArrayList<>();

    for (final OpenApiContributor contributor : OpenApiContributors.all()) {
      final OpenAPI openAPI = OpenApiContributors.contribute(contributor);
      for (final Map.Entry<String, Schema> entry : OpenApiContributors.components(openAPI).entrySet()) {
        final Schema<?> schema = entry.getValue();
        if (EXEMPT.containsKey(entry.getKey()))
          continue;
        if (schema.getRequired() == null || schema.getRequired().isEmpty())
          silent.add(contributor.getClass().getSimpleName() + "." + entry.getKey());
      }
    }

    assertThat(silent)
        .as("a schema with no 'required' list types every field optional in a generated client, even the ones "
            + "the server always sends. Read the handler and declare what it writes unconditionally - or, if "
            + "the schema genuinely has no such field, add it to EXEMPT with the reason")
        .isEmpty();
  }

  // Regression: #7578 every component schema declares a required list
  /**
   * The other half of honesty: a name in {@code required} that the schema does not even declare makes the
   * document invalid, and a generator either drops it or fails. Cheap to get wrong when a property is renamed.
   */
  @Test
  void nothingIsRequiredThatTheSchemaDoesNotDeclare() {
    final List<String> dangling = new ArrayList<>();

    for (final OpenApiContributor contributor : OpenApiContributors.all()) {
      final String owner = contributor.getClass().getSimpleName();
      final OpenAPI openAPI = OpenApiContributors.contribute(contributor);

      for (final Map.Entry<String, Schema<?>> entry : OpenApiContributors.everySchema(openAPI).entrySet())
        OpenApiContributors.walk(owner + "." + entry.getKey(), entry.getValue(), (path, schema) -> {
          if (schema.getRequired() == null)
            return;
          final Set<String> declared = schema.getProperties() == null ? Set.of() : schema.getProperties().keySet();
          for (final String required : schema.getRequired())
            if (!declared.contains(required))
              dangling.add(path + " requires '" + required + "', which it does not declare");
        });
    }

    assertThat(dangling).as("a required name that is not a property of the same schema makes the document "
        + "invalid").isEmpty();
  }

  // Regression: #7578 every component schema declares a required list
  /**
   * Every exemption has to name a schema that exists. Without this, a schema renamed or removed leaves its
   * exemption behind and silently excuses whatever takes that name next.
   */
  @Test
  void everyExemptionNamesASchemaThatStillExists() {
    final List<String> names = new ArrayList<>();
    for (final OpenApiContributor contributor : OpenApiContributors.all())
      names.addAll(OpenApiContributors.components(OpenApiContributors.contribute(contributor)).keySet());

    assertThat(names).as("an exemption for a schema that no longer exists excuses whatever takes its name next")
        .containsAll(EXEMPT.keySet());
  }

  // Regression: #7578 every component schema declares a required list
  /**
   * The split that made {@code McpConfig} possible, asserted as a pair: the response says what it always sends
   * and the request says nothing is mandatory, and they describe the same field set. One schema could not do
   * both, which is why this is the one place in the sweep that added a component rather than a list.
   */
  @Test
  void theMcpConfigResponseAndItsPartialUpdateDescribeTheSameFieldsWithDifferentObligations() {
    final OpenAPI openAPI = OpenApiContributors.contribute(new McpApiSpec());
    final Schema<?> response = openAPI.getComponents().getSchemas().get("McpConfig");
    final Schema<?> update = openAPI.getComponents().getSchemas().get("McpConfigUpdate");

    assertThat(response.getRequired())
        .as("MCPConfiguration.toJSON writes these ten on every answer")
        .containsExactlyInAnyOrder("enabled", "allowReads", "allowInsert", "allowUpdate", "allowDelete",
            "allowSchemaChange", "allowAdmin", "profile", "allowedUsers", "allowedOrigins");
    assertThat(response.getRequired())
        .as("the two maps are written only when they hold something")
        .doesNotContain("principalProfiles", "databases");

    assertThat(update.getRequired()).as("a partial update mandates nothing").isNullOrEmpty();
    assertThat(update.getProperties().keySet())
        .as("the update is derived from the response rather than written out again, so the two cannot drift "
            + "into describing different fields")
        .isEqualTo(response.getProperties().keySet());
  }

  // Regression: #7578 every component schema declares a required list
  /**
   * {@code ServerInfo} is the schema the sweep found to be describing a different endpoint than the one that
   * exists. Pinned by name because it is the one place where establishing the required list changed the
   * PROPERTIES: {@code status}, {@code mode} and {@code uptime} were never members {@code GetServerHandler}
   * writes, so a generated client carried three fields that are always null.
   */
  @Test
  void serverInfoDeclaresTheMembersTheHandlerActuallyWrites() {
    final Schema<?> serverInfo = OpenApiContributors.contribute(new CoreApiSpec())
        .getComponents().getSchemas().get("ServerInfo");

    assertThat(serverInfo.getProperties().keySet())
        .as("three of the four members this used to declare were never written by any handler")
        .doesNotContain("status", "mode", "uptime");
    assertThat(serverInfo.getRequired())
        .as("GetServerHandler opens with these four on every answer, whatever 'mode' asks for")
        .containsExactlyInAnyOrder("user", "version", "serverName", "languages");
    assertThat(serverInfo.getProperties().keySet())
        .as("and the optional sections are the ones 'mode' selects")
        .contains("metrics", "settings", "ha", "ports");
  }
}
