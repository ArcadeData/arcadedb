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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10325: an edge of a unidirectional type whose destination vertex was deleted by an earlier, committed transaction was
 * refused only at commit, as a retryable {@link ConcurrentModificationException} ("deleted by a concurrent transaction. Please
 * retry"), which no retry can ever satisfy. A destination already gone when the edge is created is a not-found, as SQL
 * {@code CREATE EDGE} and every bidirectional type answer it; the retryable refusal stays for a target deleted concurrently.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10325UnidirectionalEdgeToMissingVertexTest extends TestHelper {
  private RID source;
  private RID gone;

  @BeforeEach
  void seed() {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL").close();
    database.getSchema().buildEdgeType().withName("UL").withLightweight(true).withBidirectional(false).create();
    database.transaction(() -> {
      source = database.newVertex("V").save().getIdentity();
      gone = database.newVertex("V").save().getIdentity();
    });
    database.transaction(() -> gone.asVertex().delete());
  }

  private long edges(final String type) {
    // count(), NOT count(*): THE CACHED COUNTER IS NOT GROUND TRUTH
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + type)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private void assertRefusedAsNotFound(final Runnable creation) {
    database.begin();
    try {
      assertThatThrownBy(creation::run).isInstanceOf(RecordNotFoundException.class).isNotInstanceOf(NeedRetryException.class)
          .hasMessageContaining(gone.toString());
    } finally {
      database.rollback();
    }
    assertThat(edges("U")).isZero();
  }

  @Test
  void vertexNewEdgeByRidIsRefusedAsNotFound() {
    assertRefusedAsNotFound(() -> source.asVertex().newEdge("U", gone));
  }

  @Test
  void newEdgesUnidirectionalIsRefusedAsNotFound() {
    assertRefusedAsNotFound(() -> ((DatabaseInternal) database).getGraphEngine()
        .newEdges((VertexInternal) source.asVertex(), List.of(new GraphEngine.CreateEdgeOperation("U", gone, new Object[0])), false));
  }

  @Test
  void newEdgesBidirectionalFlagOnAUnidirectionalTypeIsRefusedAsNotFound() {
    assertRefusedAsNotFound(() -> ((DatabaseInternal) database).getGraphEngine()
        .newEdges((VertexInternal) source.asVertex(), List.of(new GraphEngine.CreateEdgeOperation("U", gone, new Object[0])), true));
  }

  @Test
  void aBatchWithAMissingDestinationLeavesNoEdgeOfItBehind() {
    // THE SOURCE ITSELF AS THE DESTINATION THAT EXISTS: A VERTEX CREATED HERE WOULD TAKE THE FREED SLOT OF THE DELETED ONE
    final RID[] alive = { source };

    // THE REFUSAL IS CAUGHT AND THE TRANSACTION COMMITTED: THE EDGE BEFORE THE MISSING DESTINATION MUST NOT HAVE BEEN SAVED, OR IT
    // WOULD BE COMMITTED WITHOUT ITS ENTRY IN THE SOURCE'S OUT LIST
    database.begin();
    assertThatThrownBy(() -> ((DatabaseInternal) database).getGraphEngine().newEdges((VertexInternal) source.asVertex(),
        List.of(new GraphEngine.CreateEdgeOperation("U", alive[0], new Object[0]),
            new GraphEngine.CreateEdgeOperation("U", gone, new Object[0])), false)).isInstanceOf(RecordNotFoundException.class);
    database.commit();

    assertThat(edges("U")).isZero();
    assertThat(source.asVertex().countEdges(Vertex.DIRECTION.OUT, "U")).isZero();
  }

  @Test
  void lightweightUnidirectionalEdgeIsRefusedAsNotFound() {
    database.begin();
    try {
      assertThatThrownBy(() -> source.asVertex().newEdge("UL", gone)).isInstanceOf(RecordNotFoundException.class)
          .isNotInstanceOf(NeedRetryException.class);
    } finally {
      database.rollback();
    }
    assertThat(source.asVertex().countEdges(Vertex.DIRECTION.OUT, "UL")).isZero();
  }

  @Test
  void sqlCreateEdgeStillRefusesAsNotFound() {
    assertRefusedAsNotFound(() -> database.command("sql", "CREATE EDGE U FROM " + source + " TO " + gone).close());
  }

  @Test
  void aDestinationInABucketThatDoesNotExistIsRefusedAsNotFound() {
    final RID dangling = new RID(999_999, 0);
    database.begin();
    try {
      assertThatThrownBy(() -> source.asVertex().newEdge("U", dangling)).isInstanceOf(RecordNotFoundException.class)
          .isNotInstanceOf(NeedRetryException.class);
    } finally {
      database.rollback();
    }
    assertThat(edges("U")).isZero();
  }

  @Test
  void aDestinationCreatedInTheSameTransactionIsAccepted() {
    database.transaction(() -> {
      final Vertex fresh = database.newVertex("V").save();
      source.asVertex().newEdge("U", fresh.getIdentity());
    });
    assertThat(edges("U")).isEqualTo(1);
  }

  @Test
  void aTargetDeletedConcurrentlyAfterTheEdgeWasCreatedIsStillARetryableConflict() throws Exception {
    final RID[] target = new RID[1];
    database.transaction(() -> target[0] = database.newVertex("V").save().getIdentity());

    database.begin();
    source.asVertex().newEdge("U", target[0]);

    // ANOTHER THREAD DELETES THE TARGET AND COMMITS WHILE THIS TRANSACTION IS STILL OPEN
    final CountDownLatch deleted = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread deleter = new Thread(() -> {
      try {
        database.transaction(() -> target[0].asVertex().delete());
      } catch (final Throwable t) {
        failure.set(t);
      } finally {
        deleted.countDown();
      }
    });
    deleter.start();
    deleted.await();
    deleter.join();
    assertThat(failure.get()).isNull();

    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    assertThat(edges("U")).isZero();
  }
}
