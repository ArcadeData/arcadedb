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
package com.arcadedb.bolt;

import com.arcadedb.database.RID;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.InvalidPropertyTypeException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.ValidationException;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7624, item 1. Issue #7123 was reported as "Bolt collapses every failure to
 * {@code Neo.DatabaseError.General.UnknownError}" and was closed by making {@code GrpcErrorMapper} exhaustive over
 * all ten {@link ErrorCategory} values - while Bolt kept its own ladder, which covered five of them. So
 * {@link ErrorCategory#NOT_FOUND}, {@link ErrorCategory#SCHEMA} and {@link ErrorCategory#VALIDATION} still reached
 * a Bolt client as the generic database error: a Neo4j driver treats that as a server fault, logs it as internal,
 * makes it non-retryable, and leaves the application no way to tell "you asked for something that does not exist"
 * from "the database is broken".
 * <p>
 * {@code classifyExecutionError} now switches over the enum, so a new category cannot be added without this
 * transport answering for it - which is the point of routing every protocol through one classification.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7624BoltErrorCategoryCoverageTest {

  /**
   * The three categories that used to fall through to {@code DATABASE_ERROR}. Each is asserted to be a
   * {@code ClientError}, which is the property that actually matters to a driver: it is what stops the failure
   * being logged as an internal server fault and retried.
   */
  @Test
  void recordNotFoundNoLongerReadsAsAServerFault() {
    final Throwable e = new RecordNotFoundException("Record #1:42 not found", new RID(1, 42));
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.ENTITY_NOT_FOUND_ERROR);
    assertThat(BoltErrorCodes.ENTITY_NOT_FOUND_ERROR).isEqualTo("Neo.ClientError.Statement.EntityNotFound");
  }

  @Test
  void schemaErrorNoLongerReadsAsAServerFault() {
    final Throwable e = new SchemaException("Type 'Nope' not found");
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
    assertThat(BoltErrorCodes.SEMANTIC_ERROR).startsWith("Neo.ClientError.");
  }

  @Test
  void validationErrorNoLongerReadsAsAServerFault() {
    assertThat(BoltNetworkExecutor.classifyExecutionError(new ValidationException("property 'name' is mandatory"),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.ARGUMENT_ERROR);
    assertThat(BoltNetworkExecutor.classifyExecutionError(new QueryNotIdempotentException("write on a query path"),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.ARGUMENT_ERROR);
    assertThat(BoltNetworkExecutor.classifyExecutionError(new IllegalArgumentException("bad argument"),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.ARGUMENT_ERROR);
    assertThat(BoltErrorCodes.ARGUMENT_ERROR).isEqualTo("Neo.ClientError.Statement.ArgumentError");
  }

  /**
   * The whole cause chain is searched, not just the outermost throwable: a failure is wrapped differently
   * depending on how the request arrived (directly, inside the auto-commit transaction wrapper, or doubly wrapped
   * on the {@code CALL} path), which is why the classification lives in {@link ErrorCategory} at all.
   */
  @Test
  void theNewCategoriesAreFoundThroughAWrapper() {
    assertThat(BoltNetworkExecutor.classifyExecutionError(
        new RuntimeException("command failed", new RecordNotFoundException("gone", new RID(1, 42))),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.ENTITY_NOT_FOUND_ERROR);
    assertThat(BoltNetworkExecutor.classifyExecutionError(
        new RuntimeException("command failed", new SchemaException("no such type")),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
    assertThat(BoltNetworkExecutor.classifyExecutionError(
        new RuntimeException("command failed", new ValidationException("nope")),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.ARGUMENT_ERROR);
  }

  /**
   * A {@link ErrorCategory#PARSING} failure that arrives wrapped inside an execution error keeps the three-way
   * distinction the statement path already makes when the same exception arrives unwrapped - it is delegated to
   * {@code classifyParsingError} rather than collapsed onto one title.
   */
  @Test
  void aWrappedParsingErrorKeepsItsOwnTitle() {
    assertThat(BoltNetworkExecutor.classifyExecutionError(
        new RuntimeException("command failed", new CommandParsingException("unexpected token")),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
    assertThat(BoltNetworkExecutor.classifyExecutionError(
        new RuntimeException("command failed", new CommandSemanticException("undefined variable")),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
    assertThat(BoltNetworkExecutor.classifyExecutionError(
        new RuntimeException("command failed", new CommandParameterMissingException("threshold")),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.PARAMETER_MISSING_ERROR);
  }

  /**
   * {@link InvalidPropertyTypeException} lives in {@link ErrorCategory#VALIDATION} with every other bad argument,
   * and Neo4j has a finer title for it (#7729). Switching to the enum must not lose that, and must not lose it at
   * the cost of the {@link ErrorCategory#RETRY}-first precedence either: a chain carrying both a conflict and a
   * type error still has to keep the verdict a managed-transaction driver acts on.
   */
  @Test
  void theTypeErrorRefinementSurvivesTheSwitchWithoutOutrankingAConflict() {
    assertThat(BoltNetworkExecutor.classifyExecutionError(
        new InvalidPropertyTypeException("TypeError: InvalidPropertyType - ..."), BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TYPE_ERROR);

    assertThat(BoltNetworkExecutor.classifyExecutionError(
        new ConcurrentModificationException("retry", new InvalidPropertyTypeException("TypeError: ...")),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
  }

  /**
   * Only {@link ErrorCategory#SERVER} may answer with the caller's default - that is what lets the transaction
   * path report an unclassified failure as TransactionNotFound. Every other category has to name itself, or the
   * collapse #7624 is about comes straight back. Written as a sweep over the enum so a category added later fails
   * here rather than silently reaching a driver as an unexplained server fault.
   */
  @Test
  void everyCategoryButServerAnswersWithSomethingOtherThanTheDefault() {
    final Set<ErrorCategory> covered = EnumSet.noneOf(ErrorCategory.class);
    for (final Throwable sample : new Throwable[] {
        new ConcurrentModificationException("retry"),
        new ArithmeticErrorException("overflow"),
        new DuplicatedKeyException("idx", "[k]", new RID(1, 0)),
        new RecordNotFoundException("gone", new RID(1, 42)),
        new SchemaException("no such type"),
        new SecurityException("not allowed"),
        new ValidationException("nope"),
        new CommandParsingException("unexpected token"),
        new TimeoutException("deadline"),
        new IllegalStateException("something else") }) {
      final ErrorCategory category = ErrorCategory.of(sample);
      covered.add(category);

      final String code = BoltNetworkExecutor.classifyExecutionError(sample, BoltErrorCodes.DATABASE_ERROR);
      if (category == ErrorCategory.SERVER)
        assertThat(code).as("%s must keep the caller's default", category).isEqualTo(BoltErrorCodes.DATABASE_ERROR);
      else {
        assertThat(code).as("%s must not collapse into the generic database error", category)
            .isNotEqualTo(BoltErrorCodes.DATABASE_ERROR);
        assertThat(code).as("%s must name a Neo4j status", category).startsWith("Neo.");
      }
    }

    assertThat(covered).as("the sweep must exercise every category, or it proves nothing about the ones it misses")
        .containsExactlyInAnyOrderElementsOf(EnumSet.allOf(ErrorCategory.class));
  }
}
