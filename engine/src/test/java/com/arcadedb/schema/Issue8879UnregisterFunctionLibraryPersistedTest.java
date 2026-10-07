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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8879: {@code Schema.unregisterFunctionLibrary} removed the library from memory only. It neither saved the
 * schema nor ran in a schema recording session, so the library came back on reopen and, under HA, was never removed
 * from the followers. A recording session is observable without a cluster through {@code SCHEMA_AFTER_FILE_CHANGES},
 * which fires once at the end of every outermost session; the cluster-level proof is in
 * {@code Issue8404FunctionDdlReplicationIT}.
 */
class Issue8879UnregisterFunctionLibraryPersistedTest extends TestHelper {

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
  void removalSurvivesReopen() {
    database.command("sql", "DEFINE FUNCTION lib8879.f 'SELECT 1 AS result' LANGUAGE sql");
    database.command("sql", "DEFINE FUNCTION keep8879.f 'SELECT 2 AS result' LANGUAGE sql");

    database.getSchema().unregisterFunctionLibrary("lib8879");
    assertThat(database.getSchema().hasFunctionLibrary("lib8879")).isFalse();

    reopenDatabase();

    assertThat(database.getSchema().hasFunctionLibrary("lib8879")).isFalse();
    assertThat(database.getSchema().hasFunctionLibrary("keep8879")).isTrue();
  }

  @Test
  void removalOfAPolyglotLibrarySurvivesReopen() {
    database.command("sql", "DEFINE FUNCTION js8879.f \"return 1;\" LANGUAGE js");

    database.getSchema().unregisterFunctionLibrary("js8879");
    reopenDatabase();

    assertThat(database.getSchema().hasFunctionLibrary("js8879")).isFalse();
  }

  @Test
  void removalRunsInARecordingSession() {
    database.command("sql", "DEFINE FUNCTION lib8879.f 'SELECT 1 AS result' LANGUAGE sql");
    sessions.set(0);

    database.getSchema().unregisterFunctionLibrary("lib8879");

    assertThat(sessions.get()).isEqualTo(1);
    assertThat(database.getSchema().hasFunctionLibrary("lib8879")).isFalse();
  }

  /**
   * A library backed by native Java code is never written to the schema file, so removing it changes nothing on disk
   * nor on any other node: it stays an in-memory removal, without a schema save or a replicated entry.
   */
  @Test
  void removalOfANativeLibraryOpensNoRecordingSession() throws Exception {
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("java8879", Math.class));
    sessions.set(0);

    database.getSchema().unregisterFunctionLibrary("java8879");

    assertThat(sessions.get()).isZero();
    assertThat(database.getSchema().hasFunctionLibrary("java8879")).isFalse();
  }

  @Test
  void removalOfAnUnknownLibraryIsANoOp() {
    sessions.set(0);

    database.getSchema().unregisterFunctionLibrary("missing8879");

    assertThat(sessions.get()).isZero();
  }
}
