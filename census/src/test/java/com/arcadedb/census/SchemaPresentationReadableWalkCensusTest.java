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
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

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
 * that depend on it are compiled. The modules it reads are listed from the source tree, so a module with main sources
 * the build forgot to put on this module's classpath fails the census rather than escaping it.
 * <p>
 * Every call site is an offender unless {@link #ALLOWED} lists its key with the reason it does not present the schema to
 * a caller, and every entry of {@link #ALLOWED} must still be a call site, so the reviewed set cannot go stale.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SchemaPresentationReadableWalkCensusTest {
  private static final Path   REACTOR_ROOT = Path.of("..");
  /** The authority: every walk of every type of a schema is a call of this method. */
  private static final String AUTHORITY    = "com/arcadedb/schema/Schema";
  private static final String WALK         = "getTypes";
  private static final String WALK_DESC    = "()Ljava/util/Collection;";

  /** Modules with main sources the default build does not compile, each with why the census does not read them. */
  private static final Map<String, String> NOT_BUILT = Map.of(//
      "native", "built only under the 'native' profile: GraalVM image-build features, no route");

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
   * The reviewed call sites, as {@code class#method} relative to {@code com/arcadedb} (nested classes and lambdas counted
   * for their outer class and method), each with why it does not present the schema to a caller.
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
      Map.entry("graph/GraphDatabaseChecker#collectEdgeSegmentBucketIds", MAINTENANCE),
      Map.entry("graph/GraphDatabaseChecker#collectEdgesToReconnect", MAINTENANCE),
      Map.entry("graph/GraphDatabaseChecker#reclaimOrphanedEdgeSegments", MAINTENANCE),
      Map.entry("query/opencypher/procedures/db/DbMigrateLegacyTemporals#execute", "a maintenance write, through the per-bucket gate"),
      Map.entry("server/ha/raft/ClusterAlerts#findSingleBucketTypes", "server-internal, answers no caller"),
      // GRAPH ENGINE
      Map.entry("graph/GraphEngine#forEachLightEdgeFarCopy", GRAPH_INTERNAL),
      Map.entry("graph/GraphEngine#getAllVertices", READ_THROUGH_GATE),
      Map.entry("graph/GraphEngine#lightEdgeTypesWithoutFarCopy", GRAPH_INTERNAL),
      Map.entry("graph/GraphEngine#reconnectEdgesFromSurvivors", GRAPH_INTERNAL),
      Map.entry("graph/IncomingEdgeLookup#addLightweightEdges", GRAPH_INTERNAL),
      Map.entry("graph/IncomingEdgeLookup#closure", GRAPH_INTERNAL),
      Map.entry("graph/IncomingEdgeLookup#scanEdgesInto", GRAPH_INTERNAL),
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
      Map.entry("query/opencypher/executor/CypherExecutionPlan#vertices", PLANNER),
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
    final Map<String, Path> modules = classRootsOfEveryModule();
    final Map<String, byte[]> classes = readClasses(modules);
    assertThat(classes).as("the census must read the classes of every module").hasSizeGreaterThan(3000);

    final Map<String, Set<String>> callSites = callSitesOf(classes);
    assertThat(callSites).as("the census must reach the call sites it is known to cover")
        .containsKeys("security/SecurityHelper#readableTypes");

    final List<String> offenders = new ArrayList<>();
    for (final Map.Entry<String, Set<String>> site : callSites.entrySet())
      if (!ALLOWED.containsKey(site.getKey()))
        offenders.add(site.getKey() + " (" + String.join(", ", site.getValue()) + ")");
    assertThat(offenders).as("walk the types with SecurityHelper.readableTypes(database), or list the call site in ALLOWED with "
        + "the reason it does not present the schema to a caller").isEmpty();

    final List<String> stale = new ArrayList<>();
    for (final String allowed : ALLOWED.keySet())
      if (!callSites.containsKey(allowed))
        stale.add(allowed);
    assertThat(stale).as("ALLOWED entries that no longer walk the types: remove them").isEmpty();
  }

  /**
   * The control: the shapes the source-text census could not see - a hoisted schema under another name, a method
   * reference, a call on an implementation - are call sites here, written in a class of this module's own tests.
   */
  @Test
  void theCensusSeesEveryShapeOfTheWalk() throws Exception {
    final Map<String, byte[]> probe = new HashMap<>();
    final Path root = Path.of(CensusProbe.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    try (final Stream<Path> files = Files.walk(root)) {
      for (final Path file : files.filter(p -> p.toString().endsWith(".class")).toList()) {
        final String name = root.relativize(file).toString().replace('\\', '/');
        if (outerClassOf(name).endsWith("/CensusProbe"))
          probe.put(name, Files.readAllBytes(file));
      }
    }

    // the probe implements no Schema: the owners come from the modules, as in the census
    final Map<String, byte[]> withModules = new HashMap<>(readClasses(classRootsOfEveryModule()));
    withModules.putAll(probe);
    final Map<String, Set<String>> sites = callSitesOf(withModules);

    assertThat(sites.keySet()).contains("census/CensusProbe#hoisted", "census/CensusProbe#methodReference",
        "census/CensusProbe#onTheImplementation", "census/CensusProbe#inALambda");
    assertThat(sites.keySet()).doesNotContain("census/CensusProbe#readable", "census/CensusProbe#notASchema");
  }

  /**
   * The class root (a directory or a jar) of every module with main sources, found through one of its own source files,
   * so no naming convention decides which module a root belongs to.
   */
  private static Map<String, Path> classRootsOfEveryModule() throws IOException, URISyntaxException {
    assertThat(REACTOR_ROOT.resolve("engine").resolve("pom.xml")).as("the census must start at the reactor root, or it checks "
        + "nothing").isRegularFile();

    final Map<String, Path> roots = new TreeMap<>();
    final List<String> missing = new ArrayList<>();
    try (final Stream<Path> modules = Files.list(REACTOR_ROOT)) {
      for (final Path module : modules.filter(Files::isDirectory).sorted().toList()) {
        final Path sources = module.resolve("src").resolve("main").resolve("java");
        final String name = module.getFileName().toString();
        if (!Files.isDirectory(sources) || NOT_BUILT.containsKey(name))
          continue;

        final String anchor = anchorClassOf(sources);
        final URL resource = SchemaPresentationReadableWalkCensusTest.class.getClassLoader().getResource(anchor);
        if (resource == null) {
          missing.add(name + " (" + anchor + ")");
          continue;
        }
        roots.put(name, rootOf(resource, anchor));
      }
    }
    assertThat(missing).as("modules with main sources this module's classpath does not carry: add each one to census/pom.xml")
        .isEmpty();
    assertThat(roots).as("the census must find the engine").containsKey("engine");
    return roots;
  }

  /** The class file of the first top-level class of a module's sources, as a resource name. */
  private static String anchorClassOf(final Path sources) throws IOException {
    try (final Stream<Path> files = Files.walk(sources)) {
      final Path first = files.filter(p -> p.toString().endsWith(".java"))
          .filter(p -> !p.getFileName().toString().equals("package-info.java") && !p.getFileName().toString().equals("module-info.java"))
          .sorted().findFirst().orElseThrow(() -> new AssertionError("no source file under " + sources));
      final String relative = sources.relativize(first).toString().replace('\\', '/');
      return relative.substring(0, relative.length() - ".java".length()) + ".class";
    }
  }

  private static Path rootOf(final URL resource, final String anchor) throws URISyntaxException {
    if ("jar".equals(resource.getProtocol())) {
      final String spec = resource.getPath();
      return Path.of(URI.create(spec.substring(0, spec.indexOf("!/"))));
    }
    Path root = Path.of(resource.toURI());
    for (int i = anchor.split("/").length; i > 0; i--)
      root = root.getParent();
    return root;
  }

  /** Every class file under the {@code com/arcadedb} packages of each root, keyed by its path in the root. */
  private static Map<String, byte[]> readClasses(final Map<String, Path> roots) throws IOException {
    final Map<String, byte[]> classes = new HashMap<>();
    for (final Map.Entry<String, Path> root : roots.entrySet()) {
      final int before = classes.size();
      if (Files.isDirectory(root.getValue()))
        readClassTree(root.getValue(), classes);
      else
        try (final FileSystem jar = FileSystems.newFileSystem(root.getValue())) {
          readClassTree(jar.getPath("/"), classes);
        }
      assertThat(classes.size()).as("no class read from module " + root.getKey() + " at " + root.getValue()).isGreaterThan(before);
    }
    return classes;
  }

  private static void readClassTree(final Path root, final Map<String, byte[]> classes) throws IOException {
    final Path packages = root.resolve("com").resolve("arcadedb");
    if (!Files.isDirectory(packages))
      return;
    try (final Stream<Path> files = Files.walk(packages)) {
      files.filter(p -> p.toString().endsWith(".class")).forEach(file -> {
        try {
          classes.put(root.relativize(file).toString().replace('\\', '/'), Files.readAllBytes(file));
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    }
  }

  /**
   * Every call site of the walk, keyed by {@code class#method} relative to {@code com/arcadedb}, with the owners it was
   * called on. The owners are the authority and every class that implements it, directly or through a super type.
   */
  private static Map<String, Set<String>> callSitesOf(final Map<String, byte[]> classes) throws IOException {
    final Map<String, List<String>> supertypes = new HashMap<>();
    for (final byte[] bytes : classes.values()) {
      final ClassReader reader = new ClassReader(bytes);
      final List<String> parents = new ArrayList<>(List.of(reader.getInterfaces()));
      if (reader.getSuperName() != null)
        parents.add(reader.getSuperName());
      supertypes.put(reader.getClassName(), parents);
    }
    final Set<String> owners = implementationsOf(AUTHORITY, supertypes);

    final Map<String, Set<String>> sites = new TreeMap<>();
    for (final Map.Entry<String, byte[]> entry : classes.entrySet()) {
      final String outerClass = outerClassOf(entry.getKey()).substring("com/arcadedb/".length());
      new ClassReader(entry.getValue()).accept(new ClassVisitor(Opcodes.ASM9) {
        @Override
        public MethodVisitor visitMethod(final int access, final String methodName, final String descriptor, final String signature,
            final String[] exceptions) {
          final String key = outerClass + "#" + methodOf(methodName);
          return new MethodVisitor(Opcodes.ASM9) {
            @Override
            public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc,
                final boolean isInterface) {
              if (owners.contains(owner) && WALK.equals(name) && WALK_DESC.equals(desc))
                sites.computeIfAbsent(key, k -> new TreeSet<>()).add(owner.substring(owner.lastIndexOf('/') + 1) + "." + name);
            }

            @Override
            public void visitInvokeDynamicInsn(final String name, final String desc, final Handle bootstrap,
                final Object... bootstrapArguments) {
              // A METHOD REFERENCE (schema::getTypes) IS A HANDLE PASSED TO THE LAMBDA BOOTSTRAP, NOT A CALL
              for (final Object argument : bootstrapArguments)
                if (argument instanceof Handle handle && owners.contains(handle.getOwner()) && WALK.equals(handle.getName())
                    && WALK_DESC.equals(handle.getDesc()))
                  sites.computeIfAbsent(key, k -> new TreeSet<>())
                      .add(handle.getOwner().substring(handle.getOwner().lastIndexOf('/') + 1) + "::" + handle.getName());
            }
          };
        }
      }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }
    return sites;
  }

  /** {@code type} and every class or interface whose super types reach it. */
  private static Set<String> implementationsOf(final String type, final Map<String, List<String>> supertypes) {
    final Set<String> result = new HashSet<>();
    result.add(type);
    final Map<String, Boolean> memo = new LinkedHashMap<>();
    for (final String candidate : supertypes.keySet())
      if (reaches(candidate, type, supertypes, memo))
        result.add(candidate);
    return result;
  }

  private static boolean reaches(final String from, final String type, final Map<String, List<String>> supertypes,
      final Map<String, Boolean> memo) {
    final Deque<String> pending = new ArrayDeque<>(List.of(from));
    final Set<String> seen = new HashSet<>();
    while (!pending.isEmpty()) {
      final String current = pending.pop();
      if (current.equals(type) || Boolean.TRUE.equals(memo.get(current))) {
        memo.put(from, true);
        return true;
      }
      if (seen.add(current))
        pending.addAll(supertypes.getOrDefault(current, List.of()));
    }
    memo.put(from, false);
    return false;
  }

  /** {@code a/b/C$1.class} -> {@code a/b/C}: lambdas and nested classes count for their file. */
  private static String outerClassOf(final String relative) {
    final String withoutExtension = relative.endsWith(".class") ? relative.substring(0, relative.length() - ".class".length()) : relative;
    final int nested = withoutExtension.indexOf('$');
    return nested < 0 ? withoutExtension : withoutExtension.substring(0, nested);
  }

  /** {@code lambda$execute$3} -> {@code execute}: a lambda counts for the method it is written in, whatever its number. */
  private static String methodOf(final String methodName) {
    if (methodName.startsWith("lambda$")) {
      final int end = methodName.lastIndexOf('$');
      return end > "lambda$".length() ? methodName.substring("lambda$".length(), end) : methodName;
    }
    return methodName;
  }
}
