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
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;

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
 * The census ranges over the constructions of the exceptions no rule classifies as the caller's (or, for the parsing ones, that
 * classify a missing name as a syntax error), in the compiled classes of every module ({@link CompiledClasses},
 * {@link CallOperands}), and reads the message each one is given, followed through locals: a string constant, or the recipe of a
 * string concatenation. A construction whose message says that something is not found, does not exist, cannot be found or is not
 * defined is an offender unless it is listed below with why.
 * <p>
 * The message is the one thing this census has to read as text, because the defect IS what the message says: the exception type is
 * the same whether the refusal names a missing object or a broken server. Wording a new refusal another way escapes it, which is why
 * {@code Issue11384MissingObjectCategoryTest} (engine) also pins the statements of the report through {@code ErrorCategory.of}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MissingObjectRefusalCensusTest {
  /** The exceptions whose message decides nothing about the category they are answered with. */
  private static final Set<String> UNCLASSIFIED = Set.of(//
      "com/arcadedb/exception/CommandExecutionException",//
      "com/arcadedb/exception/DatabaseOperationException",//
      "com/arcadedb/exception/ArcadeDBException",//
      // a parsing failure: a missing name is answered as a syntax error ("42601") rather than as the missing object it is
      "com/arcadedb/exception/CommandSQLParsingException",//
      "com/arcadedb/exception/CommandParsingException");

  private static final Pattern MISSING = Pattern.compile(
      "(?i)not found|does not exist|doesn't exist|cannot find|can't find|not defined|unknown (procedure|method|function|index|type|bucket|property|trigger)");

  /** The reviewed constructions, as {@code class#method} relative to {@code com/arcadedb}, each with why it stays. */
  private static final Map<String, String> REVIEWED = new TreeMap<>(Map.ofEntries(//
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
      Map.entry("graphql/schema/GraphQLSchema#buildWhereClause", "an argument the GraphQL query does not declare: invalid document"),
      Map.entry("graphql/schema/GraphQLSchema#executeQuery", "a GraphQL query whose definition names no target type: invalid document"),
      Map.entry("redis/query/RedisQueryEngine#executeSingleCommandInternal",
          "a command Redis does not have, which Redis itself refuses as a command it cannot read (ERR unknown command)"),
      Map.entry("query/sql/parser/BackupDatabaseStatement#executeSimple", "the backup module is missing from the server: a deployment fault"),
      Map.entry("query/sql/parser/ExportDatabaseStatement#executeSimple", "the exporter is missing from the server: a deployment fault"),
      Map.entry("query/sql/parser/ImportDatabaseStatement#executeSimple", "the importer is missing from the server: a deployment fault"),
      Map.entry("server/ArcadeDBServer#restoreDatabaseFromStartupCommand", "the restore module is missing from the server: a deployment fault"),
      Map.entry("server/ServerControlPlane#performRestore", "the restore module is missing from the server: a deployment fault"),
      Map.entry("server/ServerControlPlane#runImport", "the importer is missing from the server: a deployment fault")));

  @Test
  void noRefusalOfAMissingObjectIsAnUnclassifiedException() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    assertThat(classes).as("the census must read the classes of every module").hasSizeGreaterThan(3000);

    final Map<String, Set<String>> all = constructions(classes, null);
    // the scan still reaches the authority: the constructions of CommandExecutionException in the SQL executor
    assertThat(all.keySet()).as("the census no longer reaches the constructions of com/arcadedb/exception/CommandExecutionException")
        .anyMatch(site -> site.startsWith("query/sql/executor/SelectExecutionPlanner#"));

    final Map<String, Set<String>> offenders = constructions(classes, MISSING);
    assertThat(CompiledClasses.unreviewed(offenders, REVIEWED)).as("a refusal of a missing object is the caller's mistake: raise a "
        + "SchemaException (a missing type, index, bucket, property, trigger or view), a RecordNotFoundException (a missing record), "
        + "an UnknownFunctionException or UnknownProcedureException (a method, function or procedure nothing answers), or list the "
        + "site in REVIEWED with the reason it is a server fault (issue #11384)").isEmpty();
    assertThat(CompiledClasses.stale(offenders, REVIEWED)).as("REVIEWED entries that no longer refuse a missing object: remove them")
        .isEmpty();
  }

  /** The control: a message concatenated across lines and a constant held in a local are seen, the schema family is not. */
  @Test
  void theCensusSeesTheMessageWhereverItIsBuilt() throws Exception {
    assertThat(constructions(CompiledClasses.ofTheProbes(), MISSING).keySet()).containsExactlyInAnyOrder(
        "census/ShapeProbes$MissingObject#concatenated", "census/ShapeProbes$MissingObject#throughALocal");
  }

  /**
   * The constructions of an {@link #UNCLASSIFIED} exception given a message (outside the exception package, where a subclass
   * passes its own message up) whose readable text {@code matching} finds, or every one of them when it is null, keyed by
   * {@code class#method}, each with the message.
   */
  private static Map<String, Set<String>> constructions(final Map<String, byte[]> classes, final Pattern matching) {
    final Map<String, Set<String>> sites = new TreeMap<>();
    for (final CallOperands.Site site : CallOperands.of(classes,
        (owner, name, descriptor) -> UNCLASSIFIED.contains(owner) && "<init>".equals(name) && descriptor.startsWith("(Ljava/lang/String;"))) {
      if (site.key().startsWith("exception/"))
        continue;
      for (final AbstractInsnNode origin : site.argument(0)) {
        final String text = textOf(origin);
        if (matching == null || (text != null && matching.matcher(text).find()))
          sites.computeIfAbsent(site.key(), k -> new TreeSet<>()).add(String.valueOf(text));
      }
    }
    return sites;
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
