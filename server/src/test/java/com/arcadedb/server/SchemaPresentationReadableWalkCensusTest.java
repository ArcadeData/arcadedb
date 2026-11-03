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
package com.arcadedb.server;

import com.arcadedb.security.SecurityHelper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A route that presents the schema to a caller - a catalog, an introspection answer, a schema document, a census - must
 * enumerate the types through {@link SecurityHelper#readableTypes}, so a type the bound principal may not read is not
 * described to it. Three such routes (the PostgreSQL catalog, GraphQL introspection, the MCP/AI schema document) and the
 * gRPC database census walked {@code Schema.getTypes()} instead and described every type to every user.
 * <p>
 * This census turns the next such route into a red build: every walk of {@code getTypes()} in the main sources of the
 * modules that answer clients (every module but the engine, plus the engine's Cypher procedures, which are presentation)
 * is either gone onto the readable walk or listed here with the reason it is not a presentation of the schema.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SchemaPresentationReadableWalkCensusTest {

  private static final Path REACTOR_ROOT = Path.of("..");

  /** {@code getSchema().getTypes()} or {@code schema.getTypes()}: a walk of every type of a schema. */
  private static final Pattern TYPE_WALK = Pattern.compile("(?:getSchema\\(\\)|\\bschema)\\s*\\.\\s*getTypes\\(\\)");

  /** The engine package that presents the schema to a Cypher caller, the one part of the engine this census covers. */
  private static final String ENGINE_PROCEDURES = "engine/src/main/java/com/arcadedb/query/opencypher/procedures/";

  /** Files allowed to walk every type, each with why it is not a presentation of the schema to a caller. */
  private static final Map<String, String> ALLOWED = Map.ofEntries(//
      Map.entry("console/src/main/java/com/arcadedb/console/Console.java",
          "a client: the listing it walks is the one the server answered it, or an embedded database it owns"),
      Map.entry("gremlin/src/main/java/com/arcadedb/gremlin/ArcadeCountGlobalStep.java",
          "a traversal: the count reads every bucket through the per-bucket gate, which refuses a denied type"),
      Map.entry("gremlin/src/main/java/com/arcadedb/gremlin/ArcadeGraph.java",
          "a traversal: the records are read through the per-bucket gate"),
      Map.entry("gremlin/src/main/java/com/arcadedb/gremlin/LightweightEdges.java",
          "a traversal: the records are read through the per-bucket gate"),
      Map.entry("ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterAlerts.java", "server-internal, answers no caller"),
      Map.entry("integration/src/main/java/com/arcadedb/integration/exporter/format/JsonlExporterFormat.java",
          "an export: every record is read through the per-bucket gate"),
      Map.entry("integration/src/main/java/com/arcadedb/integration/importer/format/AbstractImporterFormat.java",
          "an import: writes, through the per-bucket gate"),
      Map.entry("integration/src/main/java/com/arcadedb/integration/importer/format/CSVImporterFormat.java",
          "an import: writes, through the per-bucket gate"),
      Map.entry("server/src/main/java/com/arcadedb/server/security/ServerSecurityDatabaseUser.java",
          "derives the per-type permissions themselves"),
      Map.entry("server/src/main/java/com/arcadedb/server/http/handler/GetGrafanaMetadataHandler.java",
          "filters each type with SecurityHelper.canAccessType"),
      Map.entry("server/src/main/java/com/arcadedb/server/http/handler/GetPromQLLabelsHandler.java",
          "filters each type with SecurityHelper.canAccessType"),
      Map.entry("server/src/main/java/com/arcadedb/server/http/handler/GetPromQLLabelValuesHandler.java",
          "filters each type with SecurityHelper.canAccessType"),
      Map.entry("server/src/main/java/com/arcadedb/server/http/handler/PostPrometheusReadHandler.java",
          "filters each type with SecurityHelper.canAccessType"),
      Map.entry(ENGINE_PROCEDURES + "db/DbLabels.java", "filters each type with SecurityHelper.canAccessType"),
      Map.entry(ENGINE_PROCEDURES + "db/DbPropertyKeys.java", "filters each type with SecurityHelper.canAccessType"),
      Map.entry(ENGINE_PROCEDURES + "db/DbRelationshipTypes.java", "filters each type with SecurityHelper.canAccessType"),
      Map.entry(ENGINE_PROCEDURES + "db/DbSchemaVisualization.java", "filters each type with SecurityHelper.canAccessType"),
      Map.entry(ENGINE_PROCEDURES + "db/DbMigrateLegacyTemporals.java", "a maintenance write, through the per-bucket gate"),
      Map.entry(ENGINE_PROCEDURES + "ProcedureSchemaTypes.java", "validates the type names an algorithm call wrote"));

  @Test
  void everyWalkOfTheTypesThatAnswersACallerIsTheReadableOne() throws IOException {
    assertThat(REACTOR_ROOT.resolve("server").resolve("pom.xml"))
        .as("the scan must start at the reactor root, or it checks nothing").isRegularFile();

    final Map<String, String> sources = readMainSources();
    assertThat(sources).as("the scan must find the main sources of the modules").hasSizeGreaterThan(500);
    assertThat(sources).as("the scan must reach the files it is known to cover")
        .containsKeys("server/src/main/java/com/arcadedb/server/info/SchemaInfo.java",
            ENGINE_PROCEDURES + "meta/MetaSchema.java");

    final List<String> offenders = new ArrayList<>();
    for (final Map.Entry<String, String> source : sources.entrySet())
      if (TYPE_WALK.matcher(source.getValue()).find() && !ALLOWED.containsKey(source.getKey()))
        offenders.add(source.getKey());

    assertThat(offenders).as("walk the types with SecurityHelper.readableTypes(database), or list the file in ALLOWED with the reason "
        + "it does not present the schema to a caller").isEmpty();

    final List<String> stale = new ArrayList<>();
    for (final String allowed : ALLOWED.keySet())
      if (!sources.containsKey(allowed) || !TYPE_WALK.matcher(sources.get(allowed)).find())
        stale.add(allowed);
    assertThat(stale).as("allow-list entries that no longer walk the types: remove them").isEmpty();
  }

  @Test
  void thePatternFlagsTheShapesItIsAbout() {
    assertThat(TYPE_WALK.matcher("for (DocumentType t : database.getSchema().getTypes())").find()).isTrue();
    assertThat(TYPE_WALK.matcher("final var types = schema.getTypes();").find()).isTrue();
    assertThat(TYPE_WALK.matcher("SecurityHelper.readableTypes((DatabaseInternal) database)").find()).isFalse();
    assertThat(TYPE_WALK.matcher("registry.getTypes()").find()).isFalse();
  }

  private static Map<String, String> readMainSources() throws IOException {
    final Map<String, String> sources = new TreeMap<>();
    try (final Stream<Path> modules = Files.list(REACTOR_ROOT)) {
      for (final Path module : modules.filter(Files::isDirectory).toList()) {
        final Path mainSources = module.resolve("src").resolve("main").resolve("java");
        if (!Files.isDirectory(mainSources))
          continue;
        try (final Stream<Path> files = Files.walk(mainSources)) {
          for (final Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
            final String relative = REACTOR_ROOT.relativize(file).toString().replace('\\', '/');
            if (relative.startsWith("engine/") && !relative.startsWith(ENGINE_PROCEDURES))
              continue;
            sources.put(relative, Files.readString(file, StandardCharsets.UTF_8));
          }
        }
      }
    }
    return sources;
  }
}
