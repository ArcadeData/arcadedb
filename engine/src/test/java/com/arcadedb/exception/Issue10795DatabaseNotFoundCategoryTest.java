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
package com.arcadedb.exception;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10795: a name that is no database ({@link DatabaseNotFoundException}, raised by the open of a directory that is not there)
 * classified as {@link ErrorCategory#SERVER}, so every protocol that asks {@link ErrorCategory} answered a mistyped database name
 * as a server fault. It is {@link ErrorCategory#DATABASE_NOT_FOUND}: the caller's to correct, and neither the transient
 * {@link ErrorCategory#UNAVAILABLE} of the type it extends nor the missing RECORD of {@link ErrorCategory#NOT_FOUND}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10795DatabaseNotFoundCategoryTest {

  @Test
  void theOpenOfANameThatIsNoDatabaseIsDatabaseNotFound() {
    final File missing = new File("target/databases/issue10795-never-created");
    FileUtils.deleteRecursively(missing);

    try (final DatabaseFactory factory = new DatabaseFactory(missing.getPath())) {
      assertThatThrownBy(() -> factory.open(ComponentFile.MODE.READ_WRITE)).isInstanceOfSatisfying(DatabaseNotFoundException.class, e -> {
        assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.DATABASE_NOT_FOUND);
        assertThat(ErrorCategory.classifiedCause(e)).isSameAs(e);
      });
    } finally {
      FileUtils.deleteRecursively(missing);
    }
    assertThat(missing).doesNotExist();
  }

  @Test
  void noWrapperRelabelsANameThatIsNoDatabase() {
    final DatabaseNotFoundException missing = new DatabaseNotFoundException("Database 'nope' does not exist");

    // A parser or a validator that wraps the failure (GraphQL, the query engines) must not turn it into the caller's syntax error
    assertThat(ErrorCategory.of(new CommandParsingException("failed", missing))).isEqualTo(ErrorCategory.DATABASE_NOT_FOUND);
    assertThat(ErrorCategory.of(new ValidationException("failed", missing))).isEqualTo(ErrorCategory.DATABASE_NOT_FOUND);
    // nor the transient verdict of the type it extends, when both ride the same chain
    assertThat(ErrorCategory.of(new DatabaseNotAvailableException("held").initCause(missing)))
        .isEqualTo(ErrorCategory.DATABASE_NOT_FOUND);
    assertThat(ErrorCategory.classifiedCause(new TransactionException("commit", missing))).isSameAs(missing);
  }

  @Test
  void aHeldDatabaseKeepsItsTransientVerdict() {
    assertThat(ErrorCategory.of(new DatabaseNotAvailableException("held"))).isEqualTo(ErrorCategory.UNAVAILABLE);
    assertThat(ErrorCategory.of(new TransactionException("commit", new DatabaseNotAvailableException("held"))))
        .isEqualTo(ErrorCategory.UNAVAILABLE);
  }

  @Test
  void aConflictOrAStopOnRequestStillWins() {
    final DatabaseNotFoundException missing = new DatabaseNotFoundException("Database 'nope' does not exist");

    assertThat(ErrorCategory.of(new ConcurrentModificationException("conflict", missing))).isEqualTo(ErrorCategory.RETRY);
    assertThat(ErrorCategory.of(new QueryTerminatedException("stopped").initCause(missing))).isEqualTo(ErrorCategory.TERMINATED);
  }
}
