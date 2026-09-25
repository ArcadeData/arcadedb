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
package com.arcadedb.gremlin;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.security.SecurityDatabaseUser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8295: the permission to use the {@code io()} step is enforced by {@link ArcadeIoRegistrationStrategy}, and a
 * traversal strategy is part of the request - {@code withoutStrategies(...)} is a source instruction the caller sends.
 * Naming the strategy there used to remove the only check, so any user allowed to run Gremlin could dump the database
 * to a host path of their choice. The strategy is now pinned in the strategy set every ArcadeDB traversal source
 * starts from, so the removal is ignored whatever front end carries it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8295IoStrategyCannotBeRemovedTest {
  private static final String REFUSAL = "is not allowed to use the io() step";

  private ArcadeGraph graph;
  private File        target;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-8295-io");
    graph.getDatabase().getSchema().createVertexType("P");
    graph.getDatabase().transaction(() -> graph.getDatabase().newVertex("P").set("name", "x").save());
    target = new File("./target/test-gremlin-8295-io-dump.xml");
    target.delete();
  }

  @AfterEach
  void teardown() {
    unbindUser();
    target.delete();
    if (graph != null)
      graph.drop();
  }

  @Test
  void theIoStrategySurvivesWithoutStrategies() {
    final GraphTraversalSource g = graph.traversal().withoutStrategies(ArcadeIoRegistrationStrategy.class);
    assertThat(g.getStrategies().getStrategy(ArcadeIoRegistrationStrategy.class)).isPresent();
  }

  @Test
  void otherStrategiesCanStillBeRemoved() {
    // ONLY THE PERMISSION STRATEGY IS PINNED: THE OPTIMIZER STAYS REMOVABLE (THE DIFFERENTIAL TESTS DEPEND ON IT)
    final GraphTraversalSource g = graph.traversal().withoutStrategies(ArcadeTraversalStrategy.class, ArcadeIoRegistrationStrategy.class);
    assertThat(g.getStrategies().getStrategy(ArcadeTraversalStrategy.class)).isEmpty();
    assertThat(g.getStrategies().getStrategy(ArcadeIoRegistrationStrategy.class)).isPresent();
  }

  @Test
  void aFluentWithoutStrategiesStillRefusesIo() {
    bindUser(false);
    assertThatThrownBy(() -> graph.traversal().withoutStrategies(ArcadeIoRegistrationStrategy.class).io(target.getAbsolutePath()).write().iterate())
        .isInstanceOf(SecurityException.class).hasMessageContaining(REFUSAL);
    assertThat(target).doesNotExist();
  }

  @Test
  void aStringWithoutStrategiesStillRefusesIo() {
    // THE PATH POST /api/v1/command TAKES WITH THE DEFAULT gremlin-lang ENGINE
    bindUser(false);
    final String path = target.getAbsolutePath().replace('\\', '/');
    assertThatThrownBy(() -> graph.gremlin("g.withoutStrategies(ArcadeIoRegistrationStrategy).io('" + path + "').write()").execute()
        .stream().forEach(r -> {
        })).hasStackTraceContaining(REFUSAL);
    assertThat(target).doesNotExist();

    assertThatThrownBy(() -> graph.gremlin("g.withoutStrategies(ArcadeIoRegistrationStrategy).io('" + path + "').read()").execute()
        .stream().forEach(r -> {
        })).hasStackTraceContaining(REFUSAL);
  }

  @Test
  void theServerAdministratorCanStillUseIo() {
    bindUser(true);
    graph.traversal().withoutStrategies(ArcadeIoRegistrationStrategy.class).io(target.getAbsolutePath()).write().iterate();
    assertThat(target).exists();
  }

  private void bindUser(final boolean administrator) {
    DatabaseContext.INSTANCE.init((DatabaseInternal) graph.getDatabase()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return true;
      }

      @Override
      public boolean isServerAdministrator() {
        return administrator;
      }

      @Override
      public String getName() {
        return administrator ? "root" : "alice";
      }

      @Override
      public long getResultSetLimit() {
        return -1;
      }

      @Override
      public long getReadTimeout() {
        return -1;
      }
    });
  }

  private void unbindUser() {
    if (graph == null)
      return;
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(graph.getDatabase().getDatabasePath());
    if (context != null)
      context.setCurrentUser(null);
  }
}
