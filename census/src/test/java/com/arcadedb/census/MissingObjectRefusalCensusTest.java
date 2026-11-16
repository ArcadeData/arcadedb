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

import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11384: a statement refusing a named object that does not exist (an index, a trigger, a view, a bucket, a type, a method, a
 * procedure) raised a plain {@code CommandExecutionException} or {@code DatabaseOperationException}, which no {@code ErrorCategory}
 * rule classifies, so every protocol answered the caller's typo as a server fault (Postgres {@code XX000}, Bolt
 * {@code UnknownError}), while the sibling statements that reached the same name through the schema answered {@code 42P01}. It was
 * the third round of the same defect after #10153, #11115 and #11314, each fixed at the sites reported. A missing schema object is
 * a {@code SchemaException} now, a missing record a {@code RecordNotFoundException}, a method or procedure nothing answers an
 * {@code UnknownFunctionException} / {@code UnknownProcedureException}.
 * <p>
 * The census ranges over the constructions of every throwable outside the families that are answered as the missing object they
 * report (issue #11633: it used to range over a list of five ArcadeDB types, so a refusal raised as an
 * {@code IllegalArgumentException} was never seen), in the compiled classes of every module ({@link CompiledClasses},
 * {@link CallOperands}), and reads the message each one is given, followed through locals: a string constant, or the recipe of a
 * string concatenation. A construction whose message says that something is not found, does not exist, cannot be found or is not
 * defined is an offender unless it is listed below with why.
 * <p>
 * The message is the one thing this census has to read as text, because the defect IS what the message says: the exception type is
 * the same whether the refusal names a missing object or a broken server. Wording a new refusal another way escapes it, which is why
 * {@code ExceptionCategoryTest.MissingObjectCategory} (engine) also pins the statements of the report through {@code ErrorCategory.of}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MissingObjectRefusalCensusTest {
  /**
   * The exceptions a protocol answers as the missing object they report, with their subclasses: a missing schema object, a
   * missing record, a missing database, a function or procedure nothing answers. Every other throwable is an offender when its
   * message reports a missing object: the census ranges over the constructions of every throwable, so a refusal raised as a type
   * nobody thought of is seen too (issue #11633: an IllegalArgumentException, answered as a generic invalid argument, was not).
   */
  private static final Set<String> CLASSIFIED = Set.of(//
      "com/arcadedb/exception/SchemaException",//
      "com/arcadedb/exception/RecordNotFoundException",//
      "com/arcadedb/exception/DatabaseNotFoundException",//
      "com/arcadedb/exception/UnknownFunctionException",//
      "com/arcadedb/exception/UnknownProcedureException",//
      // answered 404 by HTTP and NOT_FOUND by gRPC
      "com/arcadedb/server/ServerControlPlane$NotFoundException",//
      // the resource-not-found error of the MCP protocol
      "com/arcadedb/mcp/MCPResourceNotFoundException");

  private static final Pattern MISSING = Pattern.compile(
      "(?i)not found|does not exist|doesn't exist|cannot find|can't find|not defined|unknown (procedure|method|function|index|type|bucket|property|trigger)");

  private static final String INTERNAL   = "an engine invariant on an internal id (page, file, dictionary entry, record kind, column): "
      + "a server fault or a corruption, not a name the caller looked up";
  private static final String RETRY      = "a ConcurrentModificationException: the retry signal of a concurrent change, not a refusal";
  private static final String FILE       = "a file or path on disk the caller or the configuration names: an I/O or configuration "
      + "failure, not an object of the database";
  private static final String DEPLOYMENT = "a module, class or algorithm missing from the server or the JVM: a deployment fault";
  private static final String SESSION    = "a transaction, cursor, prepared statement or insert session of the connection, answered by "
      + "its protocol's own session code";
  private static final String SERVER     = "a server user, cluster peer or port: server administration, answered by the control plane "
      + "or the security layer, not an object of a database";
  private static final String DATABASE   = "a database the server does not hold or the user may not see (one answer for both, so names "
      + "cannot be probed), refused by the endpoint with the databases the user can pick";
  private static final String VARIABLE   = "a variable of the statement: its own semantic error (UndefinedVariable), not a stored object";
  private static final String TOOL       = "an argument of an MCP or AI tool call, answered to the model as the tool's own error";
  private static final String PROTOCOL   = "a name of the protocol's own language (a Redis database index, a PromQL function, a query "
      + "language, a database setting, a JSON element), refused in that protocol's terms";
  private static final String DATA       = "a row of the imported data referencing a vertex the import did not create: a data error";

  /** The reviewed constructions, as {@code class#method} relative to {@code com/arcadedb}, each with why it stays. */
  private static final Map<String, String> REVIEWED = new TreeMap<>(Map.ofEntries(//
      Map.entry("query/opencypher/query/OpenCypherQueryEngine#missingUser",
          "carries an IllegalArgumentException as its cause, which every protocol reads (VALIDATION), and keeps the message the "
              + "Cypher command() rewrap would otherwise replace"),
      Map.entry("database/DatabaseContext#getContext", "no transaction context on the current thread: an engine invariant, a server fault"),
      Map.entry("engine/LocalBucket#checkRestoreTargetIsFree",
          "the page a restore targets is past the end of its bucket: the precondition of the restore protocol, not a name looked up"),
      Map.entry("query/sql/parser/RebuildIndexStatement#buildIndex",
          "the bucket of an index that exists is gone: an orphaned index, a database inconsistency the message says how to repair"),
      Map.entry("query/opencypher/parser/FunctionValidator#unknownFunction",
          "a CommandParsingException carrying the UnknownFunctionException as its cause, which every protocol reads (issue #11115)"),
      Map.entry("function/sql/graph/SQLFunctionHeuristicPathFinderAbstract#bindCustomHeuristicFunction",
          "wraps the UnknownFunctionException of the function lookup as its cause, which every protocol reads (42883 on Postgres)"),
      Map.entry("query/sql/method/conversion/SQLMethodConvert#execute",
          "a data type keyword the argument of convert() spells wrong, refused as the statement's own mistake, not a schema object"),
      Map.entry("graphql/schema/GraphQLFragments#getDefinition", "a fragment the GraphQL document uses and does not define: invalid document"),
      Map.entry("graphql/schema/GraphQLSchema#coerceArguments", "an argument the GraphQL query does not declare: invalid document"),
      Map.entry("graphql/schema/GraphQLSchema#executeQuery", "a GraphQL query whose definition names no target type: invalid document"),
      Map.entry("redis/query/RedisQueryEngine#executeSingleCommandInternal",
          "a command Redis does not have, which Redis itself refuses as a command it cannot read (ERR unknown command)"),
      Map.entry("query/sql/parser/BackupDatabaseStatement#executeSimple", DEPLOYMENT),
      Map.entry("query/sql/parser/ExportDatabaseStatement#executeSimple", DEPLOYMENT),
      Map.entry("query/sql/parser/ImportDatabaseStatement#executeSimple", DEPLOYMENT),
      Map.entry("server/ArcadeDBServer#restoreDatabaseFromStartupCommand", DEPLOYMENT),
      Map.entry("server/ServerControlPlane#performRestore", DEPLOYMENT),
      Map.entry("server/ServerControlPlane#runImport", DEPLOYMENT),
      // issue #11633: the census ranges over every throwable since, and these are the sites it found that report no missing
      // object of a database
      Map.entry("database/RecordFactory#newImmutableRecord", INTERNAL),
      Map.entry("database/RecordFactory#newMutableRecord", INTERNAL),
      Map.entry("engine/Dictionary#getNameById", INTERNAL),
      Map.entry("engine/Dictionary#updateName", INTERNAL),
      Map.entry("engine/FileManager#getFile", INTERNAL),
      Map.entry("engine/PageManager#getCachedPage", INTERNAL),
      Map.entry("engine/timeseries/TimeSeriesBucket#columnOffset", INTERNAL),
      Map.entry("index/lsm/LSMTreeIndexExternalSorter$RunReader#advance", INTERNAL),
      Map.entry("index/vector/ContiguousPageReader#ensurePageHolds", INTERNAL),
      Map.entry("serializer/BinarySerializer#deserializeValue", INTERNAL),
      Map.entry("schema/LocalSchema#load", INTERNAL),
      Map.entry("server/ha/raft/RaftLogEntryCodec#readTxPreparedAtIndex", INTERNAL),
      Map.entry("query/sql/executor/ColumnarGroupByStep#accelerate",
          "a Decline of the columnar accelerator: control flow that falls back to the row path, never reaches a caller"),
      Map.entry("database/TransactionContext#edgeSegmentPageKey", RETRY),
      Map.entry("engine/PageManager#checkPageVersion", RETRY),
      Map.entry("server/ha/raft/ArcadeStateMachine#localVersionsOf", RETRY),
      Map.entry("console/Console#executeLoad", FILE),
      Map.entry("integration/importer/Neo4jImporter#openInputStream", FILE),
      Map.entry("integration/importer/OrientDBImporter#run", FILE),
      Map.entry("integration/importer/SourceDiscovery#seekZipEntry", FILE),
      Map.entry("server/ServerControlPlane#resolveBackupFile", FILE),
      Map.entry("server/monitor/ServerQueryProfiler#loadSavedRun", FILE),
      Map.entry("server/grpc/GrpcServerPlugin#resolveTlsCertKey", FILE),
      Map.entry("utility/FileUtils#resolveAsTheFileSystemDoes", FILE),
      Map.entry("serializer/BinarySerializer#toClass", DEPLOYMENT),
      Map.entry("schema/trigger/JavaClassTriggerExecutor#<init>", DEPLOYMENT),
      Map.entry("query/java/JavaQueryEngine#command", DEPLOYMENT),
      Map.entry("server/ServerControlPlane#executeImmediateBackup", DEPLOYMENT),
      Map.entry("server/backup/BackupTask#initializeReflection", DEPLOYMENT),
      Map.entry("server/backup/BackupTask#performBackup", DEPLOYMENT),
      Map.entry("server/http/handler/AbstractServerHttpHandler#static", DEPLOYMENT),
      Map.entry("function/polyglot/JavascriptFunctionDefinition#execute",
          "the body of a stored JavaScript function does not define the function it is named after: a broken definition"),
      Map.entry("server/http/handler/DatabaseAbstractHandler#setTransactionInThreadLocal", SESSION),
      Map.entry("server/http/handler/PostBatchHandler#resolveSession", SESSION),
      Map.entry("server/http/handler/PostCommitHandler#execute", SESSION),
      Map.entry("server/http/ws/insert/WebSocketInsertSessionManager#resolve", SESSION),
      Map.entry("server/http/ws/insert/WebSocketInsertSessionManager#resolveExternalTransaction", SESSION),
      Map.entry("postgres/PostgresNetworkExecutor#applySessionCommand", SESSION),
      Map.entry("postgres/PostgresNetworkExecutor#parseKeywordSetCommand", SESSION),
      Map.entry("server/ServerControlPlane#dropUser", SERVER),
      Map.entry("server/security/ServerSecurity#addDatabaseToUser", SERVER),
      Map.entry("server/security/ServerSecurity#setUserPassword", SERVER),
      Map.entry("server/security/ServerSecurity#updateUser", SERVER),
      Map.entry("server/security/ServerSecurity#updateUserClusterWide", SERVER),
      Map.entry("server/ha/raft/RaftClusterManager#removePeer", SERVER),
      Map.entry("server/ha/raft/RaftHAPlugin#resolveShutdownTarget", SERVER),
      Map.entry("server/gremlin/GremlinServerPlugin#findFreePort", SERVER),
      Map.entry("server/ServerControlPlane#dropDatabase", DATABASE),
      Map.entry("server/info/SchemaInfo#forUser", DATABASE),
      Map.entry("server/ai/AiSchemaDigest#forUser", DATABASE),
      Map.entry("redis/RedisNetworkExecutor#getAuthorizedDatabase", PROTOCOL),
      Map.entry("redis/RedisNetworkExecutor#select", PROTOCOL),
      Map.entry("engine/timeseries/promql/PromQLEvaluator#evaluateFunction", PROTOCOL),
      Map.entry("query/QueryEngineManager#getEngine", PROTOCOL),
      Map.entry("query/sql/parser/AlterDatabaseStatement#executeSimpleAlter", PROTOCOL),
      Map.entry("serializer/json/JSONArray#checkIndex", PROTOCOL),
      Map.entry("serializer/json/JSONObject#getElement", PROTOCOL),
      Map.entry("server/http/handler/PostTimeSeriesQueryHandler#executeAggregation",
          "a field of the aggregation request, rendered by the handler itself as its named 400 and never classified"),
      Map.entry("server/ai/AiSchemaDigest#typeDetail", TOOL),
      Map.entry("mcp/tools/MCPToolUtils#unknownDatabase", TOOL),
      Map.entry("mcp/tools/SampleRecordsTool#resolveTypeNames", TOOL),
      Map.entry("query/opencypher/parser/VariableScopeValidator#checkExpressionScope", VARIABLE),
      Map.entry("query/opencypher/parser/VariableScopeValidator#checkPatternPredicateVariables", VARIABLE),
      Map.entry("query/opencypher/parser/VariableScopeValidator#checkVarNotShadowed", VARIABLE),
      Map.entry("query/opencypher/parser/VariableScopeValidator#validateDeleteClauseScope", VARIABLE),
      Map.entry("query/opencypher/parser/VariableScopeValidator#validateRemoveClauseScope", VARIABLE),
      Map.entry("query/opencypher/parser/VariableScopeValidator#validateSetClauseScope", VARIABLE),
      Map.entry("query/opencypher/parser/VariableScopeValidator#validateSubqueryScope", VARIABLE),
      Map.entry("query/sql/parser/CreateIndexStatement#executeDDL", VARIABLE),
      Map.entry("query/sql/parser/CreatePropertyStatement#resolveTypeName", VARIABLE),
      Map.entry("query/sql/parser/CreateTypeAbstractStatement#executeDDL", VARIABLE),
      Map.entry("integration/importer/format/JsonlImporterFormat#loadEdge", DATA)));

  @Test
  void noRefusalOfAMissingObjectIsAnUnclassifiedException() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    assertThat(classes).as("the census must read the classes of every module").hasSizeGreaterThan(3000);

    final Map<String, Set<String>> all = constructions(classes, null);
    // the scan still reaches the authority, the constructions of every throwable: an ArcadeDB exception in the SQL executor, and a
    // JDK one, which is what a refusal outside the ArcadeDB families is raised as (issue #11633)
    assertThat(all.keySet()).as("the census no longer reaches the constructions of com/arcadedb/exception/CommandExecutionException")
        .anyMatch(site -> site.startsWith("query/sql/executor/SelectExecutionPlanner#"));
    assertThat(all.values().stream().flatMap(Set::stream))
        .as("the census no longer reaches the constructions of java/lang/IllegalArgumentException")
        .anyMatch(construction -> construction.startsWith("java/lang/IllegalArgumentException: "));

    final Map<String, Set<String>> offenders = constructions(classes, MISSING);
    assertThat(CompiledClasses.unreviewed(offenders, REVIEWED)).as("a refusal of a missing object is the caller's mistake: raise a "
        + "SchemaException (a missing type, index, bucket, property, trigger or view), a RecordNotFoundException (a missing record), "
        + "an UnknownFunctionException or UnknownProcedureException (a method, function or procedure nothing answers), or list the "
        + "site in REVIEWED with the reason it is a server fault (issue #11384)").isEmpty();
    assertThat(CompiledClasses.stale(offenders, REVIEWED)).as("REVIEWED entries that no longer refuse a missing object: remove them")
        .isEmpty();
  }

  /**
   * The control: a message concatenated across lines and a constant held in a local are seen, and so is a refusal raised as a type
   * of the JDK (issue #11633); the schema family is not.
   */
  @Test
  void theCensusSeesTheMessageWhereverItIsBuilt() throws Exception {
    assertThat(constructions(CompiledClasses.ofTheProbes(), MISSING).keySet()).containsExactlyInAnyOrder(
        "census/ShapeProbes$MissingObject#concatenated", "census/ShapeProbes$MissingObject#throughALocal",
        "census/ShapeProbes$MissingObject#asAnArgumentFailure");
  }

  /**
   * The constructions of a throwable outside the {@link #CLASSIFIED} families given a message whose readable text
   * {@code matching} finds, or every one of them when it is null, keyed by {@code class#method}, each with the constructed type
   * and the message. Outside the exception package and the constructors of a throwable, where a subclass passes its own message
   * up.
   */
  private static Map<String, Set<String>> constructions(final Map<String, byte[]> classes, final Pattern matching) {
    final Throwables throwables = new Throwables(classes);
    final Map<String, Set<String>> sites = new TreeMap<>();
    for (final CallOperands.Site site : CallOperands.of(classes,
        (owner, name, descriptor) -> "<init>".equals(name) && descriptor.startsWith("(Ljava/lang/String;") && throwables.isThrowable(owner)
            && !throwables.isClassified(owner))) {
      if (site.key().startsWith("exception/") || site.key().endsWith("#<init>") && throwables.isThrowable(site.in().owner().name))
        continue;
      for (final AbstractInsnNode origin : site.argument(0)) {
        final String text = textOf(origin);
        if (matching == null || (text != null && matching.matcher(text).find()))
          sites.computeIfAbsent(site.key(), k -> new TreeSet<>()).add(site.call().owner + ": " + text);
      }
    }
    return sites;
  }

  /**
   * Which classes are throwables, and which of them are {@link #CLASSIFIED}: the super classes are read from the compiled classes
   * of the modules, and a JDK class is asked of the platform without initializing it. A library class the census does not read
   * counts as a throwable when its name says so, so a refusal raised as one is seen rather than skipped.
   */
  private static final class Throwables {
    private final Map<String, String>  superNames = new HashMap<>();
    private final Map<String, Boolean> throwable  = new HashMap<>();
    private final Map<String, Boolean> classified = new HashMap<>();

    Throwables(final Map<String, byte[]> classes) {
      for (final byte[] bytes : classes.values()) {
        final ClassReader reader = new ClassReader(bytes);
        superNames.put(reader.getClassName(), reader.getSuperName());
      }
    }

    boolean isThrowable(final String type) {
      final Boolean known = throwable.get(type);
      if (known != null)
        return known;
      final boolean result;
      if (superNames.containsKey(type)) {
        final String superName = superNames.get(type);
        result = superName != null && isThrowable(superName);
      } else if (type.startsWith("java/") || type.startsWith("javax/")) {
        result = isJdkThrowable(type);
      } else
        result = type.endsWith("Exception") || type.endsWith("Error");
      throwable.put(type, result);
      return result;
    }

    boolean isClassified(final String type) {
      final Boolean known = classified.get(type);
      if (known != null)
        return known;
      final String superName = superNames.get(type);
      final boolean result = CLASSIFIED.contains(type) || superName != null && isClassified(superName);
      classified.put(type, result);
      return result;
    }

    private static boolean isJdkThrowable(final String type) {
      try {
        return Throwable.class.isAssignableFrom(Class.forName(type.replace('/', '.'), false, ClassLoader.getPlatformClassLoader()));
      } catch (final ClassNotFoundException e) {
        return false;
      }
    }
  }

  /** The text of a message: a string constant, or the constant parts of a concatenation's recipe. Null for anything else. */
  private static String textOf(final AbstractInsnNode origin) {
    if (origin instanceof LdcInsnNode ldc && ldc.cst instanceof String constant)
      return constant;
    if (origin instanceof InvokeDynamicInsnNode concat && "makeConcatWithConstants".equals(concat.name) && concat.bsmArgs.length > 0
        && concat.bsmArgs[0] instanceof String recipe)
      return recipe.replace('\u0001', ' ').replace('\u0002', ' ');
    return null;
  }
}
