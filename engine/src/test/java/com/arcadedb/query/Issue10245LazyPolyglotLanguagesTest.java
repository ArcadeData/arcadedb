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
package com.arcadedb.query;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.polyglot.PolyglotQueryEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10245: the first statement of any language in a JVM built the GraalVM polyglot Engine, because the
 * {@link QueryEngineManager} constructor enumerated its languages to register them. The registry now asks for them
 * only when a caller wants a language no built-in engine answers, or the complete list.
 * <p>
 * The manager is a JVM-wide singleton that any earlier test may already have resolved, so these tests build their own
 * instance with a counting language source instead of the real one: the source stands for the shared Engine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10245LazyPolyglotLanguagesTest extends TestHelper {
  private final AtomicInteger      enumerations = new AtomicInteger();
  private       QueryEngineManager manager;

  @BeforeEach
  void createManager() {
    GlobalConfiguration.POLYGLOT_ENGINE_ENABLED.reset();
    manager = new QueryEngineManager(() -> {
      enumerations.incrementAndGet();
      return List.of("js", "sql");
    });
  }

  @AfterEach
  void closeManager() {
    manager.close();
    GlobalConfiguration.POLYGLOT_ENGINE_ENABLED.reset();
  }

  @Test
  void builtInLanguagesDoNotEnumerateThePolyglotLanguages() {
    assertThat(manager.getEngine("sql", (DatabaseInternal) database)).isNotNull();
    assertThat(manager.getEngine("SQLScript", (DatabaseInternal) database)).isNotNull();
    assertThat(manager.getEngine("opencypher", (DatabaseInternal) database)).isNotNull();
    assertThat(manager.isLanguageRegistered("sql")).isTrue();

    assertThat(enumerations.get()).isZero();
  }

  @Test
  void aPolyglotLanguageIsResolvedOnFirstDemandAndOnlyOnce() {
    assertThat(manager.isLanguageRegistered("js")).isTrue();
    assertThat(enumerations.get()).isEqualTo(1);

    assertThat(manager.getAvailableLanguages()).contains("js", "sql", "sqlscript", "java");
    assertThat(manager.isLanguageRegistered("JS")).isTrue();
    assertThat(manager.isLanguageRegistered("lang12345")).isFalse();
    assertThatThrownBy(() -> manager.getEngine("lang12345", (DatabaseInternal) database))//
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("lang12345");

    assertThat(enumerations.get()).isEqualTo(1);
  }

  @Test
  void theCompleteListResolvesThePolyglotLanguages() {
    assertThat(manager.getAvailableLanguages()).contains("js");
    assertThat(enumerations.get()).isEqualTo(1);
  }

  @Test
  void aPolyglotLanguageNeverDisplacesABuiltInEngine() {
    // The source also answers "sql": the built-in SQL engine must keep the name, as it did when the constructor
    // registered the polyglot languages first and the built-in engines over them.
    manager.getAvailableLanguages();
    assertThat(manager.getEngine("sql", (DatabaseInternal) database)).isNotInstanceOf(PolyglotQueryEngine.class);
  }

  @Test
  void concurrentLookupsAndRegistrationsResolveThePolyglotLanguagesOnce() throws Exception {
    final int threads = 8;
    final CountDownLatch start = new CountDownLatch(1);
    final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
    final List<Thread> workers = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final int thread = t;
      final Thread worker = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < 50; i++) {
            if (thread % 2 == 0)
              assertThat(manager.isLanguageRegistered("js")).isTrue();
            else
              assertThat(manager.getEngine("JS", (DatabaseInternal) database)).isNotNull();
            if (i == 10)
              manager.register(new PolyglotQueryEngine.PolyglotQueryEngineFactory("custom" + thread));
          }
        } catch (final Throwable e) {
          failures.add(e);
        }
      });
      workers.add(worker);
      worker.start();
    }
    start.countDown();
    for (final Thread worker : workers)
      worker.join();

    assertThat(failures).isEmpty();
    assertThat(enumerations.get()).isEqualTo(1);
    assertThat(manager.getAvailableLanguages()).contains("js", "sql", "custom0", "custom7");
  }

  @Test
  void aFailingEnumerationIsNotRetriedOnEveryLookup() {
    final QueryEngineManager failing = new QueryEngineManager(() -> {
      enumerations.incrementAndGet();
      throw new IllegalStateException("no GraalVM");
    });
    try {
      assertThat(failing.isLanguageRegistered("js")).isFalse();
      assertThat(failing.isLanguageRegistered("lang12345")).isFalse();
      assertThat(failing.getAvailableLanguages()).contains("sql");
      assertThat(enumerations.get()).isEqualTo(1);
    } finally {
      failing.close();
    }
  }

  @Test
  void disabledPolyglotNeverEnumeratesAndCanBeEnabledLater() {
    GlobalConfiguration.POLYGLOT_ENGINE_ENABLED.setValue(false);

    assertThat(manager.getAvailableLanguages()).doesNotContain("js").contains("sql");
    assertThat(manager.isLanguageRegistered("js")).isFalse();
    assertThatThrownBy(() -> manager.getEngine("js", (DatabaseInternal) database)).isInstanceOf(IllegalArgumentException.class);
    assertThat(enumerations.get()).isZero();

    GlobalConfiguration.POLYGLOT_ENGINE_ENABLED.setValue(true);
    assertThat(manager.isLanguageRegistered("js")).isTrue();
    assertThat(enumerations.get()).isEqualTo(1);
  }
}
