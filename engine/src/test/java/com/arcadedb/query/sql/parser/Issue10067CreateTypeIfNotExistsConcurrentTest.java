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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.TypeAlreadyExistsException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10067: {@code CREATE ... TYPE ... IF NOT EXISTS} checked that the type was missing and then created it, two steps, so two
 * statements racing on the same name had the loser fail with {@code Cannot create type ... because already exists} - which is what
 * two replicas asking the leader for the same new label at the same time produced. IF NOT EXISTS is the form that is safe to repeat
 * and to run concurrently: every statement answers, exactly one says it created the type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10067CreateTypeIfNotExistsConcurrentTest extends TestHelper {

  private static final int THREADS = 8;
  private static final int ROUNDS  = 25;

  @Test
  void statementsRacingOnTheSameNameAllAnswerAndExactlyOneCreatesIt() throws Exception {
    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      for (final String kind : new String[] { "VERTEX", "EDGE", "DOCUMENT" })
        for (int round = 0; round < ROUNDS; round++) {
          final String name = "Racing" + kind + round;
          final CyclicBarrier together = new CyclicBarrier(THREADS);
          final AtomicInteger created = new AtomicInteger();
          final List<Future<?>> futures = new ArrayList<>();
          for (int t = 0; t < THREADS; t++)
            futures.add(pool.submit(() -> {
              together.await(30, TimeUnit.SECONDS);
              try (final ResultSet rs = database.command("sql", "CREATE " + kind + " TYPE " + name + " IF NOT EXISTS")) {
                final Result row = rs.next();
                assertThat(row.<String>getProperty("typeName")).isEqualTo(name);
                if (Boolean.TRUE.equals(row.getProperty("created")))
                  created.incrementAndGet();
              }
              return null;
            }));
          for (final Future<?> future : futures)
            future.get(60, TimeUnit.SECONDS);

          assertThat(created.get()).as("%s: exactly one statement creates the type", name).isEqualTo(1);
          assertThat(database.getSchema().existsType(name)).isTrue();
        }
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * The same race for a statement that links super types (the composite type of a node with two labels), which since issue #10147
   * takes the async quiescence and then the write lock for the create and the links together: the losers still all answer, exactly
   * one creates the type, and it ends up under both of its super types.
   */
  @Test
  void statementsRacingOnTheSameTypeWithSuperTypesAllAnswerAndExactlyOneCreatesIt() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE RacingBaseA").close();
    database.command("sql", "CREATE VERTEX TYPE RacingBaseB").close();
    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      for (int round = 0; round < 10; round++) {
        final String name = "RacingChild" + round;
        final CyclicBarrier together = new CyclicBarrier(THREADS);
        final AtomicInteger created = new AtomicInteger();
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < THREADS; t++)
          futures.add(pool.submit(() -> {
            together.await(30, TimeUnit.SECONDS);
            try (final ResultSet rs = database.command("sql", "CREATE VERTEX TYPE " + name + " IF NOT EXISTS EXTENDS RacingBaseA, RacingBaseB")) {
              if (Boolean.TRUE.equals(rs.next().getProperty("created")))
                created.incrementAndGet();
            }
            return null;
          }));
        for (final Future<?> future : futures)
          future.get(60, TimeUnit.SECONDS);

        assertThat(created.get()).as("%s: exactly one statement creates the type", name).isEqualTo(1);
        assertThat(database.getSchema().getType(name).getSuperTypes()).extracting(type -> type.getName())
            .containsExactlyInAnyOrder("RacingBaseA", "RacingBaseB");
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /** Without IF NOT EXISTS the loser of the race is still told the type is there: the guarantee is not widened. */
  @Test
  void withoutIfNotExistsTheTypeBeingThereIsStillAnError() {
    database.command("sql", "CREATE VERTEX TYPE Plain");

    assertThatThrownBy(() -> database.command("sql", "CREATE VERTEX TYPE Plain")).isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");
    assertThatThrownBy(() -> database.getSchema().createVertexType("Plain")).isInstanceOf(TypeAlreadyExistsException.class)
        .hasMessageContaining("Cannot create type 'Plain' because already exists");
  }
}
