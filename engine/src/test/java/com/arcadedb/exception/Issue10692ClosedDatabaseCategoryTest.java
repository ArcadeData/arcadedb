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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #10692: a database closed, dropped or restored under a handle a client had already resolved is something the operator did and
 * undoes, not a server fault. {@link DatabaseIsClosedException} fell through every rule of {@link ErrorCategory} to
 * {@link ErrorCategory#SERVER}, so Postgres answered {@code XX000} internal_error for an operator's {@code close database}, while Bolt
 * answered {@code DatabaseUnavailable} for the same handle.
 * <p>
 * The census: every type a closed handle or the refused resolution of a held name raises maps to a category that is not
 * {@link ErrorCategory#SERVER}, however it is wrapped on the way out.
 */
class Issue10692ClosedDatabaseCategoryTest {
  private static final String PATH = "target/databases/Issue10692ClosedDatabaseCategoryTest";

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(PATH));
  }

  @Test
  void whatAClosedHandleRaisesIsUnavailable() {
    final Database database;
    try (final DatabaseFactory factory = new DatabaseFactory(PATH)) {
      database = factory.create();
      database.getSchema().createDocumentType("Doc");
      database.close();
    }

    // the exception the engine actually raises, not one built by hand
    final Throwable onQuery = catchThrowable(() -> database.query("sql", "select from Doc"));
    final Throwable onBegin = catchThrowable(database::begin);

    for (final Throwable raised : new Throwable[] { onQuery, onBegin }) {
      assertThat(raised).isInstanceOf(DatabaseIsClosedException.class);
      assertThat(ErrorCategory.of(raised)).isEqualTo(ErrorCategory.UNAVAILABLE);
      assertThat(ErrorCategory.classifiedCause(raised)).isSameAs(raised);
    }
  }

  @Test
  void noTypeOfTheClosedOrHeldFamilyIsAServerFault() {
    final Throwable[] family = {
        new DatabaseIsClosedException("db"),
        new DatabaseIsReopeningException("db"),
        new DatabaseNotAvailableException("Database 'db' is not available: it was closed by a 'close database' command") };

    for (final Throwable error : family) {
      assertThat(ErrorCategory.of(error)).as(error.getClass().getSimpleName()).isNotEqualTo(ErrorCategory.SERVER);
      // as the auto-commit wrapper and the query engines deliver it
      assertThat(ErrorCategory.of(new TransactionException("Error on commit", error))).as(error.getClass().getSimpleName())
          .isNotEqualTo(ErrorCategory.SERVER);
      assertThat(ErrorCategory.of(new CommandExecutionException("Error on executing the query", error)))
          .as(error.getClass().getSimpleName()).isNotEqualTo(ErrorCategory.SERVER);
    }
  }

  @Test
  void aClosedHandleIsUnavailableEvenUnderAWrapperThatBlamesTheCaller() {
    // GraphQL and the query engines wrap execution failures in a CommandParsingException
    assertThat(ErrorCategory.of(new CommandParsingException("failed", new DatabaseIsClosedException("db"))))
        .isEqualTo(ErrorCategory.UNAVAILABLE);
  }

  @Test
  void theCloseForReopenStaysRetryable() {
    assertThat(ErrorCategory.of(new DatabaseIsReopeningException("db"))).isEqualTo(ErrorCategory.RETRY);
  }
}
