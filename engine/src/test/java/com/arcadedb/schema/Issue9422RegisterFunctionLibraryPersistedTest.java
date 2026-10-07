/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.function.sql.SQLFunctionDefinition;
import com.arcadedb.function.sql.SQLFunctionLibraryDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9422: {@code Schema.registerFunctionLibrary} of a persistable (js/sql/cypher) library only put it in a map, so
 * it reached {@code schema.json} by chance and was never replicated. It now runs in a schema recording session, which is
 * observable without a cluster through {@code SCHEMA_AFTER_FILE_CHANGES}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9422RegisterFunctionLibraryPersistedTest extends TestHelper {

  private final AtomicInteger  sessions = new AtomicInteger();
  private final Callable<Void> counter  = () -> {
    sessions.incrementAndGet();
    return null;
  };

  @BeforeEach
  void registerCounter() {
    ((DatabaseInternal) database).registerCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, counter);
  }

  @AfterEach
  void unregisterCounter() {
    ((DatabaseInternal) database).unregisterCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, counter);
  }

  @Test
  void registeredSqlLibrarySurvivesReopenWithoutOtherDdl() {
    final SQLFunctionLibraryDefinition library = new SQLFunctionLibraryDefinition(database, "lib9422");
    library.registerFunction(new SQLFunctionDefinition(database, "f", "SELECT 1 AS result"));

    database.getSchema().registerFunctionLibrary(library);
    reopenDatabase();

    assertThat(database.getSchema().hasFunctionLibrary("lib9422")).isTrue();
    assertThat(database.getSchema().getFunction("lib9422", "f")).isNotNull();
  }

  @Test
  void registrationRunsInARecordingSession() {
    sessions.set(0);

    database.getSchema().registerFunctionLibrary(new SQLFunctionLibraryDefinition(database, "lib9422"));

    assertThat(sessions.get()).isEqualTo(1);
  }

  @Test
  void duplicateRegistrationIsRefused() {
    database.getSchema().registerFunctionLibrary(new SQLFunctionLibraryDefinition(database, "lib9422"));

    assertThatThrownBy(() -> database.getSchema().registerFunctionLibrary(new SQLFunctionLibraryDefinition(database, "lib9422")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void nativeLibraryOpensNoRecordingSession() throws Exception {
    sessions.set(0);

    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("java9422", Math.class));

    assertThat(sessions.get()).isZero();
    assertThat(database.getSchema().hasFunctionLibrary("java9422")).isTrue();
  }
}
