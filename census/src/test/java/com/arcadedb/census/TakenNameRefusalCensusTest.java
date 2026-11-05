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
import org.objectweb.asm.Type;
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
 * Issue #10153: creating a schema object under a taken name was a server fault on every wire - HTTP 500, Postgres {@code XX000},
 * Bolt's generic DatabaseError - or, for the {@code SchemaException} ones, the opposite of what happened: the {@code SCHEMA}
 * category ("the schema does not define this"). The refusals raised two bare types with no conflict arm anywhere. They raise
 * {@code AlreadyExistsException} now, which every wire maps to its own conflict code; the per-statement behaviour is pinned by
 * {@code Issue10153AlreadyExistsTest}, and three earlier fixes (#10073, #10074, #10075) each landed one name and left the others.
 * <p>
 * The census ranges over the authority (#11288): every construction of a {@code Throwable} whose message words a taken name - a
 * constant, or the constant parts of a string concatenation, saying "already exists", "already registered" or "already defined" -
 * in the compiled classes of the {@code engine} module, which owns the schema ({@link CompiledClasses}, {@link CallOperands}). One
 * that is not an {@code AlreadyExistsException} is an offender unless listed with the reason it is not a schema object's name.
 * The source-text census it replaces matched {@code throw new X(...);} with the phrase in its arguments, so an exception built
 * first and thrown later, or a message held in a local, passed it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TakenNameRefusalCensusTest {
  private static final String  MODULE         = "engine";
  private static final String  ALREADY_EXISTS = "com/arcadedb/exception/AlreadyExistsException";
  private static final Pattern TAKEN          = Pattern.compile("already (?:exists|registered|defined)");

  private static final String INTERNAL           = "an internal invariant on the file registry, never a name a caller chose";
  private static final String ANOTHER_DEFINITION = "an index the request would accept by name, refused for its definition";
  private static final String VARIABLE           = "a variable of the statement, not a schema object";
  private static final String SEALED_FILE        = "a sealed file in the directory, which no schema object holds";

  /** The refusals that word a taken name but are not a schema object's, each with why. */
  private static final Map<String, String> NOT_A_SCHEMA_OBJECT_NAME = new TreeMap<>(Map.ofEntries(//
      Map.entry("database/LocalDatabase#create", "the embedded DatabaseFactory.create() on a path that holds a database: the server "
          + "turns it into its own DatabaseAlreadyExistsException (#10074) before any wire sees it"),//
      // INTERNAL INVARIANTS ON THE FILE REGISTRY, NEVER A NAME A CALLER CHOSE
      Map.entry("schema/LocalSchema#registerFile", INTERNAL),//
      Map.entry("engine/FileManager#checkFileNameMatches", INTERNAL),//
      Map.entry("schema/LocalSchema#createBucketIndex", "a bucket index named after the bucket and System.nanoTime(): an internal "
          + "invariant, not a caller's name"),//
      // IF NOT EXISTS (OR ignoreIfExists) ANSWERED BY AN INDEX OF ANOTHER DEFINITION: A 400 BY DESIGN (#6921, #7228, #5765)
      Map.entry("query/sql/parser/CreateIndexStatement#executeDDL", ANOTHER_DEFINITION),//
      Map.entry("schema/IndexBuilder#conflictWithExistingIndex", ANOTHER_DEFINITION),//
      Map.entry("schema/ManualIndexBuilder#conflictWithExistingManualIndex", ANOTHER_DEFINITION),//
      // A CYPHER VARIABLE BOUND TWICE, OR WITH TWO TYPES, IN ONE STATEMENT: A PARSE ERROR, NOT A SCHEMA OBJECT NAME
      Map.entry("query/opencypher/parser/CypherSemanticValidator#checkCreateBinding", VARIABLE),//
      Map.entry("query/opencypher/parser/CypherSemanticValidator#checkMergeBinding", VARIABLE),//
      Map.entry("query/opencypher/parser/CypherSemanticValidator#declareVar", VARIABLE),//
      Map.entry("query/opencypher/parser/CypherSemanticValidator$FunctionArgumentChecks#checkUsedAs", VARIABLE),//
      Map.entry("query/opencypher/procedures/db/VectorSearchClauseProcedure#execute", "SEARCH ... SCORE AS naming a variable an "
          + "earlier clause bound: a semantic error of the statement, not a schema object name"),//
      // A TIMESERIES TYPE RENAMED OR CREATED ONTO A SEALED FILE LEFT IN THE DIRECTORY: A FILE NO SCHEMA OBJECT HOLDS (#10796, #10925)
      Map.entry("schema/LocalTimeSeriesType#checkEngineFilesCanMove", SEALED_FILE),//
      Map.entry("schema/LocalTimeSeriesType#checkNoSealedFileInTheWay", SEALED_FILE),//
      Map.entry("engine/timeseries/TimeSeriesSealedStore#moveFile", "the same file met by the move itself, the guard behind the "
          + "check that ATOMIC_MOVE does not replace it (#10796)")));

  @Test
  void noTakenSchemaObjectNameIsRefusedWithAnotherType() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.byModule().get(MODULE);
    assertThat(classes).as("the census must read the " + MODULE + " module").isNotNull();
    final Set<String> conflicts = CompiledClasses.implementationsOf(ALREADY_EXISTS, classes);

    final Map<String, Set<String>> refusals = new TreeMap<>();
    int taken = 0;
    for (final CallOperands.Site site : CallOperands.of(classes,
        (owner, name, desc) -> "<init>".equals(name) && isThrowable(owner, classes)))
      if (wordsATakenName(site)) {
        ++taken;
        if (!conflicts.contains(site.call().owner))
          refusals.computeIfAbsent(site.key(), k -> new TreeSet<>()).add(site.call().owner.substring(site.call().owner.lastIndexOf('/') + 1));
      }
    assertThat(taken).as("the census must reach the taken-name refusals this issue moved to AlreadyExistsException").isGreaterThan(25);

    assertThat(CompiledClasses.unreviewed(refusals, NOT_A_SCHEMA_OBJECT_NAME)).as("a taken schema object name must raise "
        + "AlreadyExistsException, which every wire answers as a conflict (issue #10153); a refusal that is not about a schema "
        + "object's name goes in NOT_A_SCHEMA_OBJECT_NAME with why").isEmpty();
    assertThat(CompiledClasses.stale(refusals, NOT_A_SCHEMA_OBJECT_NAME)).as("entries that no longer refuse a taken name: remove "
        + "them").isEmpty();
  }

  /** Whether a String argument of the constructor is, or concatenates, a constant wording a taken name. */
  private static boolean wordsATakenName(final CallOperands.Site site) {
    final Type[] parameters = Type.getArgumentTypes(site.call().desc);
    for (int i = 0; i < parameters.length; i++)
      if ("Ljava/lang/String;".equals(parameters[i].getDescriptor()))
        for (final AbstractInsnNode origin : site.argument(i))
          if (origin instanceof LdcInsnNode ldc && ldc.cst instanceof String text && TAKEN.matcher(text).find())
            return true;
          else if (origin instanceof InvokeDynamicInsnNode concatenation && concatenation.bsmArgs.length > 0
              && concatenation.bsmArgs[0] instanceof String recipe && TAKEN.matcher(recipe).find())
            return true;
    return false;
  }

  /** Whether {@code owner} is a {@code Throwable}, up its super classes in {@code classes} and then in the JDK. */
  private static boolean isThrowable(String owner, final Map<String, byte[]> classes) {
    while (owner != null) {
      final byte[] bytes = classes.get(owner + ".class");
      if (bytes == null) {
        try {
          return Throwable.class.isAssignableFrom(Class.forName(owner.replace('/', '.'), false,
              TakenNameRefusalCensusTest.class.getClassLoader()));
        } catch (final ClassNotFoundException | LinkageError e) {
          return false;
        }
      }
      owner = new ClassReader(bytes).getSuperName();
    }
    return false;
  }
}
