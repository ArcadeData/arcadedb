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
package com.arcadedb.database;

import com.arcadedb.Profiler;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8316: a {@code close()} that marked the instance closed and then threw skipped the step that removes it from
 * {@link DatabaseFactory}'s active-instance registry. The closed instance stayed registered: the next open of the path
 * was refused as "already in use", and the server's lookup reused the dead instance instead of opening the files.
 * The unregistration now runs in a finally, keyed on the instance being closed, and a query engine that fails to close
 * no longer aborts the rest of the teardown.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8316FailedCloseUnregistersTest {
  private static final String DB_PATH = "./target/databases/test-issue-8316-failed-close";

  @AfterEach
  void cleanup() {
    LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = null;
    final Database leftover = DatabaseFactory.getActiveDatabaseInstance(DB_PATH);
    if (leftover != null && leftover.isOpen())
      leftover.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void aCloseThatThrowsAfterMarkingTheInstanceClosedStillUnregistersIt() {
    final LocalDatabase database = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(DB_PATH)).isSameAs(database);

    LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = () -> {
      throw new IllegalStateException("simulated failure after the instance was marked closed");
    };

    assertThatThrownBy(database::close).hasStackTraceContaining("simulated failure after the instance was marked closed");
    LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = null;

    assertThat(database.isOpen()).isFalse();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(DB_PATH))
        .as("a closed instance left in the registry is what the server's lookup reused in place of the files on disk")
        .isNull();
    assertThatCode(() -> Profiler.INSTANCE.toJSON())
        .as("nor may it stay in the JVM-wide profiler, whose every read would then fail on it")
        .doesNotThrowAnyException();
  }

  @Test
  void aQueryEngineThatFailsToCloseDoesNotAbortTheRestOfTheTeardown() {
    final LocalDatabase database = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
    database.getSchema().createDocumentType("Doc");
    database.transaction(() -> database.newDocument("Doc").set("n", 1).save());

    final AtomicBoolean closeAttempted = new AtomicBoolean();
    database.registerReusableQueryEngine((QueryEngine) Proxy.newProxyInstance(QueryEngine.class.getClassLoader(),
        new Class<?>[] { QueryEngine.class }, (proxy, method, args) -> switch (method.getName()) {
          case "getLanguage" -> "issue8316-failing";
          case "close" -> {
            closeAttempted.set(true);
            throw new IllegalStateException("simulated query engine close failure");
          }
          case "hashCode" -> System.identityHashCode(proxy);
          case "equals" -> proxy == args[0];
          case "toString" -> "issue8316-failing-engine";
          default -> null;
        }));

    database.close();

    assertThat(closeAttempted.get()).isTrue();
    assertThat(database.isOpen()).isFalse();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(DB_PATH)).isNull();

    // The files and the lock were released too: the same directory opens again, with its data.
    final Database reopened = new DatabaseFactory(DB_PATH).open();
    try {
      assertThat(reopened.countType("Doc", true)).isEqualTo(1L);
    } finally {
      reopened.drop();
    }
  }
}
