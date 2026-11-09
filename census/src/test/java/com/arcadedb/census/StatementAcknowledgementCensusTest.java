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

import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11385: whether a statement answers rows the caller asked for, or rows that only acknowledge what it did, was a list of
 * classes kept by the Postgres wire, written for the writes and the {@code DDLStatement} subclasses. DEFINE FUNCTION, DELETE
 * FUNCTION and SLEEP are neither, so pgjdbc's {@code executeUpdate()} refused them ("A result was returned when none was
 * expected") and a statement described with Describe('S') ran and was then refused with {@code 0A000}. The answer is
 * {@link Statement#returnsAcknowledgementOnly()} now, decided by each statement.
 * <p>
 * The census ranges over the authority, the {@link Statement} type: every concrete subclass in the compiled classes of every module
 * ({@link CompiledClasses}) must have a verdict in {@link #VERDICTS}, which is the answer a freshly constructed instance (no RETURN
 * clause) gives. A new statement class fails here until somebody decides which of the two it is, instead of being answered like a
 * query by default because nobody thought of the protocols.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StatementAcknowledgementCensusTest {
  private static final String AUTHORITY = "com/arcadedb/query/sql/parser/Statement";

  /** Whether each concrete statement class answers only an acknowledgement, keyed by its simple name. */
  private static final Map<String, Boolean> VERDICTS = new TreeMap<>();

  static {
    // every schema change (DDLStatement): its row describes the change (issue #11312)
    for (final String ddl : List.of("AlterBucketStatement", "AlterDatabaseStatement", "AlterGraphAnalyticalViewStatement",
        "AlterMaterializedViewStatement", "AlterPropertyStatement", "AlterTimeSeriesTypeStatement", "AlterTypeStatement",
        "CompactIndexStatement", "CompactTimeSeriesTypeStatement", "CreateBucketStatement", "CreateContinuousAggregateStatement",
        "CreateDocumentTypeStatement", "CreateEdgeTypeStatement", "CreateGraphAnalyticalViewStatement", "CreateIndexStatement",
        "CreateMaterializedViewStatement", "CreatePropertyStatement", "CreateTimeSeriesTypeStatement", "CreateTriggerStatement",
        "CreateVertexTypeStatement", "DropBucketStatement", "DropContinuousAggregateStatement", "DropGraphAnalyticalViewStatement",
        "DropIndexStatement", "DropMaterializedViewStatement", "DropPropertyStatement", "DropTriggerStatement", "DropTypeStatement",
        "RebuildGraphAnalyticalViewStatement", "RebuildIndexStatement", "RebuildTypeStatement", "RefreshContinuousAggregateStatement",
        "RefreshMaterializedViewStatement", "TruncateBucketStatement", "TruncateTypeStatement"))
      VERDICTS.put(ddl, true);

    // the writes with no RETURN clause, whose rows echo what they wrote (issue #11017)
    for (final String write : List.of("InsertStatement", "UpdateStatement", "DeleteStatement", "CreateEdgeStatement",
        "CreateVertexStatement", "CreateVertexStatementEmpty", "CreateVertexStatementEmptyNoTarget", "CreateVertexStatementNoTarget",
        "TruncateRecordStatement", "RestoreDocumentStatement", "RestoreVertexStatement", "RestoreEdgeStatement"))
      VERDICTS.put(write, true);

    // the function library, the session and the transaction (issue #11385)
    for (final String acknowledged : List.of("DefineFunctionStatement", "DeleteFunctionStatement", "SleepStatement", "LockStatement",
        "ConsoleStatement", "SetGlobalStatement", "LetStatement", "BeginStatement", "CommitStatement", "RollbackStatement"))
      VERDICTS.put(acknowledged, true);

    // the queries, the reports the caller reads, and the script control whose rows are the script's own result
    for (final String rows : List.of("SelectStatement", "SelectWithoutTargetStatement", "MatchStatement", "TraverseStatement",
        "ExplainStatement", "ProfileStatement", "FindReferencesStatement", "CheckDatabaseStatement", "AlignDatabaseStatement",
        "BackupDatabaseStatement", "ExportDatabaseStatement", "ImportDatabaseStatement", "MoveVertexStatement", "ExpressionStatement",
        "ReturnStatement", "BreakStatement", "IfStatement", "ForEachBlock", "WhileBlock", "Statement"))
      VERDICTS.put(rows, false);
  }

  @Test
  void everyStatementClassHasAVerdictAndAnswersIt() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    final Set<String> statements = concreteStatements(classes);
    // the scan still reaches the authority, and the statements the defect was found on
    assertThat(statements).as("the census no longer reaches the concrete subclasses of " + AUTHORITY)
        .contains(AUTHORITY, "com/arcadedb/query/sql/parser/DefineFunctionStatement", "com/arcadedb/query/sql/parser/SelectStatement");

    final List<String> undecided = new ArrayList<>();
    final List<String> wrong = new ArrayList<>();
    final Set<String> seen = new TreeSet<>();
    for (final String internalName : statements) {
      final String simpleName = internalName.substring(internalName.lastIndexOf('/') + 1);
      seen.add(simpleName);
      final Boolean verdict = VERDICTS.get(simpleName);
      if (verdict == null) {
        undecided.add(internalName);
        continue;
      }
      final Statement statement = (Statement) Class.forName(internalName.replace('/', '.')).getDeclaredConstructor().newInstance();
      if (statement.returnsAcknowledgementOnly() != verdict)
        wrong.add(simpleName + " answers " + statement.returnsAcknowledgementOnly() + ", the verdict is " + verdict);
    }

    assertThat(undecided).as("decide whether these statements answer only an acknowledgement (override "
        + "Statement.returnsAcknowledgementOnly()) or rows the caller reads, and record it in VERDICTS (issue #11385)").isEmpty();
    assertThat(wrong).as("statements whose returnsAcknowledgementOnly() disagrees with their verdict").isEmpty();
    assertThat(new TreeSet<>(VERDICTS.keySet())).as("VERDICTS entries naming no statement class any more: remove them")
        .isSubsetOf(seen);
  }

  /** {@link #AUTHORITY} and every concrete, named class of {@code classes} that extends it. */
  private static Set<String> concreteStatements(final Map<String, byte[]> classes) {
    final Set<String> result = new TreeSet<>();
    for (final String type : CompiledClasses.implementationsOf(AUTHORITY, classes)) {
      final byte[] bytes = classes.get(type + ".class");
      if (bytes == null)
        continue;
      final ClassReader reader = new ClassReader(bytes);
      // an anonymous subclass is a one-off of the statement it is written in, which decides for it
      if ((reader.getAccess() & (Opcodes.ACC_ABSTRACT | Opcodes.ACC_INTERFACE)) == 0 && !type.matches(".*\\$\\d+.*"))
        result.add(type);
    }
    return result;
  }
}
