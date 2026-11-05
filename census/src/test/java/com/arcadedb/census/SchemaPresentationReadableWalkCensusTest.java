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
package com.arcadedb.census;

import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityHelper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A route that presents the schema to a caller - a catalog, an introspection answer, a schema document, a census - must
 * enumerate the types through {@link SecurityHelper#readableTypes}, so a type the bound principal may not read is not
 * described to it. Four such routes (the PostgreSQL catalog, GraphQL introspection, the MCP/AI schema document, the gRPC
 * database census) walked {@link Schema#getTypes()} instead and described every type to every user (#11185).
 * <p>
 * The census ranges over the invariant, not over a directory or a spelling (issue #11197): it reads the COMPILED classes
 * of every module, the engine included, and records every call site of {@link Schema#getTypes()} - on the interface or
 * on any class that implements it, called or passed as a method reference - keyed by the class and the method it is
 * written in. A call site cannot be hidden by a local variable name, a static import, a method reference or a line
 * break, and nothing stays out of range because of where its file sits. The source-text census it replaces could be
 * passed both ways: it skipped the engine but for one package, and a schema hoisted into a local under any other name
 * split its pattern across two lines.
 * <p>
 * It lives in a module of its own because no other place can see every module: a module's tests run before the modules
 * that depend on it are compiled. What it reads is {@link CompiledClasses}: every class of every module, whatever its
 * package (#11273), the modules listed from the source tree, so a module with main sources the build forgot to put on
 * this module's classpath fails the census rather than escaping it.
 * <p>
 * Every call site is an offender unless {@link #ALLOWED} lists its key with the reason it does not present the schema to
 * a caller, and every entry of {@link #ALLOWED} must still be a call site, so the reviewed set cannot go stale.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SchemaPresentationReadableWalkCensusTest {
  /** The authority: every walk of every type of a schema is a call of this method. */
  private static final String AUTHORITY = "com/arcadedb/schema/Schema";
  private static final String WALK      = "getTypes";
  private static final String WALK_DESC = "()Ljava/util/Collection;";

  // REASONS SHARED BY SEVERAL ENTRIES
  private static final String READ_THROUGH_GATE = "reads the records of each type through the per-bucket gate, which refuses a denied type";
  private static final String SCHEMA_INTERNAL   = "schema bookkeeping: resolves the types holding a schema member, answers no caller";
  private static final String GRAPH_INTERNAL    = "graph engine: resolves the edge or vertex types its own walk covers, answers no caller";
  private static final String GAV_INTERNAL      = "graph analytical view: resolves the types the view covers to build and maintain it, answers no caller";
  private static final String PLANNER           = "query planning or execution: resolves the types a pattern can match and reads their records through the per-bucket gate";
  private static final String MAINTENANCE       = "a maintenance walk over every type (check, compare, repair, validation): an administrative operation, not a listing";
  private static final String CLIENT            = "a client: the listing it walks is the one the server answered it, or an embedded database it owns";
  private static final String IMPORT_EXPORT     = "an import or an export: writes or reads the records through the per-bucket gate";

  /**
   * The reviewed call sites, keyed as {@link CompiledClasses} keys them, each with why it does not present the schema to
   * a caller.
   */
  private static final Map<String, String> ALLOWED = new TreeMap<>(Map.ofEntries(//
      // THE READABLE WALK ITSELF
      Map.entry("security/SecurityHelper#readableTypes", "the readable walk itself: every type, filtered for the bound user"),
      Map.entry("server/security/ServerSecurityDatabaseUser#withFileAccess", "derives the per-type permissions themselves"),
      // SCHEMA
      Map.entry("schema/Schema#hasUnidirectionalEdgeTypes", "answers a yes/no for the engine's own edge handling"),
      Map.entry("schema/MultiTypeIndexes#entries", SCHEMA_INTERNAL),
      Map.entry("schema/MultiTypeIndexes#find", SCHEMA_INTERNAL),
      Map.entry("schema/MultiTypeIndexes#remove", SCHEMA_INTERNAL),
      Map.entry("engine/Dictionary#updateName", "renames a dictionary entry in every type that uses it: a schema write"),
      Map.entry("engine/UnreferencedFiles#collectClaimedFileIds", "collects the files a type claims, to tell the unreferenced ones apart"),
      Map.entry("engine/timeseries/TimeSeriesShardOrder#typesOf", "orders the time series types for the engine's own locking"),
      Map.entry("query/opencypher/query/CypherConstraintRegistry#findOwner", "resolves the type holding a named constraint for DDL"),
      Map.entry("query/opencypher/query/CypherConstraintRegistry#findIndexOwner", "resolves the constraint owning an index for DDL"),
      Map.entry("query/opencypher/query/CypherSchemaCommands#dropUnrecordedConstraint", "resolves the type a dropped constraint belongs to: DDL"),
      Map.entry("query/sql/parser/DropTypeStatement#hasLightweightEdges", "DDL: checks what a DROP TYPE would leave behind"),
      // MAINTENANCE
      Map.entry("database/DatabaseComparator#compareTypes", MAINTENANCE),
      Map.entry("database/DatabaseComparatorErrorsReporter#compareTypes", MAINTENANCE),
      Map.entry("database/DocumentValidator#findUnsatisfiableDateRegexps", MAINTENANCE),
      Map.entry("database/DocumentValidator#requireDateConstraintsSatisfiableUnder", MAINTENANCE),
      Map.entry("engine/DatabaseChecker#check", MAINTENANCE),
      Map.entry("engine/DatabaseChecker#checkExternalProperties", MAINTENANCE),
      Map.entry("engine/DatabaseChecker#checkIndexes", MAINTENANCE),
      Map.entry("graph/GraphDatabaseChecker#collectEdgeSegmentBucketIds", MAINTENANCE),
      Map.entry("graph/GraphDatabaseChecker#collectEdgesToReconnect", MAINTENANCE),
      Map.entry("graph/GraphDatabaseChecker#reclaimOrphanedEdgeSegments", MAINTENANCE),
      Map.entry("query/opencypher/procedures/db/DbMigrateLegacyTemporals#execute", "a maintenance write, through the per-bucket gate"),
      // GRAPH ENGINE
      Map.entry("graph/GraphEngine#forEachLightEdgeFarCopy", GRAPH_INTERNAL),
      Map.entry("graph/GraphEngine#getAllVertices", READ_THROUGH_GATE),
      Map.entry("graph/GraphEngine#lightEdgeTypesWithoutFarCopy", GRAPH_INTERNAL),
      Map.entry("graph/GraphEngine#reconnectEdgesFromSurvivors", GRAPH_INTERNAL),
      Map.entry("graph/IncomingEdgeLookup$Snapshot#addLightweightEdges", GRAPH_INTERNAL),
      Map.entry("graph/IncomingEdgeLookup#closure", GRAPH_INTERNAL),
      Map.entry("graph/IncomingEdgeLookup$Snapshot#scanEdgesInto", GRAPH_INTERNAL),
      Map.entry("function/sql/graph/SQLFunctionBellmanFord#getAllVertices", READ_THROUGH_GATE),
      Map.entry("graph/GAVVertex#providerCovers", GAV_INTERNAL),
      Map.entry("graph/olap/CSRBuilder#concreteEdgeTypes", GAV_INTERNAL),
      Map.entry("graph/olap/CSRBuilder#concreteVertexTypes", GAV_INTERNAL),
      Map.entry("graph/olap/GraphAnalyticalView#coversEdgeType", GAV_INTERNAL),
      Map.entry("graph/olap/GraphAnalyticalView#coversEveryEdge", GAV_INTERNAL),
      Map.entry("graph/olap/GraphAnalyticalView#holdsJoinedEdgeTypes", GAV_INTERNAL),
      Map.entry("graph/olap/GraphAnalyticalView#joinedVertexTypes", GAV_INTERNAL),
      Map.entry("graph/olap/GraphAnalyticalView#registerChangeListeners", GAV_INTERNAL),
      Map.entry("graph/olap/GraphAnalyticalView#unlistedVertexBuckets", GAV_INTERNAL),
      // QUERY PLANNING AND EXECUTION
      Map.entry("query/opencypher/Labels#matchingVertexTypes", PLANNER),
      Map.entry("query/opencypher/executor/CypherExecutionPlan#labelsAreTypeDisjoint", PLANNER),
      Map.entry("query/opencypher/executor/CypherExecutionPlan$PairJoinStatistics#vertices", PLANNER),
      Map.entry("query/opencypher/executor/operators/GAVEdgeRef#trackedEdgeTypes", PLANNER),
      Map.entry("query/opencypher/executor/steps/PairHashJoinOp#executeOLTP", PLANNER),
      Map.entry("query/opencypher/executor/steps/PartitionedTriangleOp#executeOLTP", PLANNER),
      Map.entry("query/opencypher/executor/steps/TypeCountStep#countAllVertices", "counts each type through countType, which refuses a denied type"),
      Map.entry("query/opencypher/optimizer/statistics/StatisticsProvider#calculateAverageDegree", PLANNER),
      Map.entry("query/sql/executor/FetchFromLightweightEdgeTypeStep#init", PLANNER),
      Map.entry("query/sql/parser/FindReferencesStatement#resolveTypesToScan", READ_THROUGH_GATE),
      // GREMLIN
      Map.entry("gremlin/ArcadeCountGlobalStep#processNextStart", "a traversal: the count reads every bucket through the per-bucket gate, which refuses a denied type"),
      Map.entry("gremlin/ArcadeGraph#edges", READ_THROUGH_GATE),
      Map.entry("gremlin/ArcadeGraph#vertices", READ_THROUGH_GATE),
      Map.entry("gremlin/LightweightEdges#exist", GRAPH_INTERNAL),
      Map.entry("gremlin/LightweightEdges#lightweightTypeNames", GRAPH_INTERNAL),
      // IMPORT / EXPORT / CLIENTS
      Map.entry("integration/exporter/format/JsonlExporterFormat#exportDatabase", IMPORT_EXPORT),
      Map.entry("integration/importer/format/AbstractImporterFormat#countRecordsOfKind", IMPORT_EXPORT),
      Map.entry("integration/importer/format/CSVImporterFormat#findVertexByKey", IMPORT_EXPORT),
      Map.entry("console/Console#executeInfo", CLIENT)));

  @Test
  void everyWalkOfTheTypesThatAnswersACallerIsTheReadableOne() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    assertThat(classes).as("the census must read the classes of every module").hasSizeGreaterThan(3000);

    final Map<String, Set<String>> callSites = callSitesOf(classes);
    assertThat(callSites).as("the census must reach the call sites it is known to cover")
        .containsKeys("security/SecurityHelper#readableTypes");

    assertThat(CompiledClasses.unreviewed(callSites, ALLOWED)).as(
        "walk the types with SecurityHelper.readableTypes(database), or list the call site in ALLOWED with the reason it does "
            + "not present the schema to a caller").isEmpty();
    assertThat(CompiledClasses.stale(callSites, ALLOWED)).as("ALLOWED entries that no longer walk the types: remove them").isEmpty();
  }

  /**
   * The control: the shapes the source-text census could not see - a hoisted schema under another name, a method
   * reference, a call on an implementation, a class outside the {@code com.arcadedb} packages (#11273) - are call sites
   * here, written in classes of this module's own tests and read the way the census reads a module.
   */
  @Test
  void theCensusSeesEveryShapeOfTheWalk() throws Exception {
    final Map<String, byte[]> probes = CompiledClasses.ofTheProbes();
    assertThat(probes).as("the probes must be read as a module is").containsKey("org/acme/routes/OutOfPackageSchemaProbe.class");

    // the probes implement no Schema: the owners come from the modules, as in the census
    final Map<String, byte[]> withModules = new HashMap<>(CompiledClasses.ofEveryModule());
    withModules.putAll(probes);
    final Map<String, Set<String>> sites = callSitesOf(withModules);

    assertThat(sites.keySet()).contains("census/CensusProbe#hoisted", "census/CensusProbe#methodReference",
        "census/CensusProbe#onTheImplementation", "census/CensusProbe#inALambda", "census/CensusProbe$Nested#hoisted",
        "org/acme/routes/OutOfPackageSchemaProbe#listEveryTypeToTheCaller");
    assertThat(sites.keySet()).doesNotContain("census/CensusProbe#readable", "census/CensusProbe#notASchema");
  }

  /**
   * Every call site of the walk, keyed by {@code class#method}, with the owners it was called on. The owners are the
   * authority and every class that implements it, directly or through a super type.
   */
  private static Map<String, Set<String>> callSitesOf(final Map<String, byte[]> classes) {
    final Set<String> owners = CompiledClasses.implementationsOf(AUTHORITY, classes);
    return CompiledClasses.callSitesOf(classes, (owner, name, desc) -> owners.contains(owner) && WALK.equals(name) && WALK_DESC.equals(desc));
  }
}
