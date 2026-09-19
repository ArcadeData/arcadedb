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

import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.server.security.ServerSecurityException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7874: Bolt's database-SELECTION failures must name themselves.
 * <p>
 * Issues #7123 and #7624 swept the query and transaction paths so a failure stopped collapsing into
 * {@code Neo.DatabaseError.General.UnknownError}. {@code ensureDatabase()} - the method RUN and BEGIN call
 * before anything else, and so the first failure a Neo4j driver meets - was never swept, because it does not go
 * through {@link com.arcadedb.exception.ErrorCategory} at all, which is exactly why a category-driven sweep
 * missed it. A typo in the {@code database} connection parameter, the most common Bolt misconfiguration there
 * is, reached the client as an unexplained server fault indistinguishable from a broken database.
 * <p>
 * The three answers are deliberately different verdicts, not three spellings of one:
 * <ul>
 *   <li>{@code DatabaseNotFound} is PERMANENT and the caller's - no retry helps;</li>
 *   <li>{@code DatabaseUnavailable} is TRANSIENT - the identical request succeeds once the database is open;</li>
 *   <li>{@code Forbidden} is the caller's identity, not the database's state.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7874BoltDatabaseSelectionClassificationTest {

  @Test
  void aDatabaseThatDoesNotExistIsAPermanentClientError() {
    assertThat(BoltNetworkExecutor.classifyDatabaseSelectionError(
        new DatabaseNotFoundException("Database '/data/nosuchdb' does not exist")))
        .isEqualTo(BoltErrorCodes.DATABASE_NOT_FOUND_ERROR);
    assertThat(BoltErrorCodes.DATABASE_NOT_FOUND_ERROR).isEqualTo("Neo.ClientError.Database.DatabaseNotFound");
  }

  /**
   * The precedence that matters: {@link DatabaseNotFoundException} EXTENDS {@link DatabaseNotAvailableException},
   * so an arm order driven by the hierarchy rather than by the verdict would report a name that will never exist
   * as a transient condition and have the driver retry it until its deadline.
   */
  @Test
  void theNotFoundVerdictOutranksTheUnavailableOneItExtends() {
    assertThat(new DatabaseNotFoundException("x")).isInstanceOf(DatabaseNotAvailableException.class);
    assertThat(BoltNetworkExecutor.classifyDatabaseSelectionError(new DatabaseNotFoundException("x")))
        .isNotEqualTo(BoltErrorCodes.DATABASE_UNAVAILABLE_ERROR);
  }

  @Test
  void aDatabaseThatIsThereButNotServeableIsTransient() {
    assertThat(BoltNetworkExecutor.classifyDatabaseSelectionError(
        new DatabaseNotAvailableException("Database 'db' is not available")))
        .isEqualTo(BoltErrorCodes.DATABASE_UNAVAILABLE_ERROR);
    assertThat(BoltNetworkExecutor.classifyDatabaseSelectionError(new DatabaseIsClosedException("db")))
        .isEqualTo(BoltErrorCodes.DATABASE_UNAVAILABLE_ERROR);
    assertThat(BoltErrorCodes.DATABASE_UNAVAILABLE_ERROR).startsWith("Neo.TransientError.");
  }

  /**
   * {@code ServerSecurityException} extends {@code ServerException}, not {@code java.lang.SecurityException}, so
   * the engine-side {@code ErrorCategory} cannot see it and answers SERVER. Reporting a refusal as a database
   * fault is worse than merely unhelpful: a driver reads a {@code DatabaseError} as a reason to try another node.
   */
  @Test
  void aRefusalToReachTheDatabaseIsForbiddenAndNotADatabaseFault() {
    assertThat(BoltNetworkExecutor.classifyDatabaseSelectionError(
        new ServerSecurityException("User 'reader' is not authorized on database 'sales'")))
        .isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR);
  }

  /**
   * The half that must not move: a genuine open failure - corruption, an I/O error - IS the server's fault and
   * stays the generic database error. Narrowing everything would be the same loss of information in the other
   * direction.
   */
  @Test
  void aGenuineOpenFailureStaysADatabaseError() {
    assertThat(BoltNetworkExecutor.classifyDatabaseSelectionError(
        new DatabaseOperationException("Error on loading page 12 from file 'sales_0.bucket'")))
        .isEqualTo(BoltErrorCodes.DATABASE_ERROR);
  }

  /**
   * The whole cause chain is searched, for the reason every other classifier in this file searches it: the
   * exception reaches {@code ensureDatabase()}'s catch wrapped by whatever raised it.
   */
  @Test
  void theVerdictIsFoundThroughAWrapper() {
    assertThat(BoltNetworkExecutor.classifyDatabaseSelectionError(
        new RuntimeException("cannot open", new DatabaseNotFoundException("does not exist"))))
        .isEqualTo(BoltErrorCodes.DATABASE_NOT_FOUND_ERROR);
    assertThat(BoltNetworkExecutor.classifyDatabaseSelectionError(
        new RuntimeException("cannot open", new ServerSecurityException("nope"))))
        .isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR);
  }

  /**
   * Every answer this method can give has to be a Neo4j status a driver can act on, and none of the three new
   * verdicts may be the generic one the issue is about.
   */
  @Test
  void noSelectionVerdictButTheGenuineServerFaultIsTheGenericDatabaseError() {
    for (final Throwable sample : new Throwable[] {
        new DatabaseNotFoundException("gone"),
        new DatabaseNotAvailableException("closed"),
        new DatabaseIsClosedException("closed"),
        new ServerSecurityException("forbidden") }) {
      final String code = BoltNetworkExecutor.classifyDatabaseSelectionError(sample);
      assertThat(code).as("%s must name itself", sample.getClass().getSimpleName())
          .isNotEqualTo(BoltErrorCodes.DATABASE_ERROR);
      assertThat(code).as("%s must name a Neo4j status", sample.getClass().getSimpleName()).startsWith("Neo.");
    }
  }
}
