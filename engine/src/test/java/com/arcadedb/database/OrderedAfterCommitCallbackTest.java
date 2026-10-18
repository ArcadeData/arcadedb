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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10249: an {@link OrderedAfterCommitCallback} takes its place in the commit order while its transaction holds its locks
 * and cannot be refused any more, and is then either run or abandoned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OrderedAfterCommitCallbackTest extends TestHelper {
  private final List<String> events = new ArrayList<>();

  private class Recorder implements OrderedAfterCommitCallback {
    private final String name;

    private Recorder(final String name) {
      this.name = name;
    }

    @Override
    public void onCommitPoint() {
      events.add(name + ":commitPoint");
    }

    @Override
    public void onAbandoned() {
      events.add(name + ":abandoned");
    }

    @Override
    public void run() {
      events.add(name + ":run");
    }
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }

  @Test
  void theCommitPointComesBeforeTheRunOfACommittedTransaction() {
    database.transaction(() -> {
      database.newDocument("Doc").set("n", 1).save();
      transaction().addAfterCommitCallback(new Recorder("a"));
      transaction().addAfterCommitCallback(new Recorder("b"));
    });

    // Every callback is at the commit point before the first one runs: the places are taken while the locks are held
    assertThat(events).containsExactly("a:commitPoint", "b:commitPoint", "a:run", "b:run");
  }

  @Test
  void aRolledBackTransactionNeverReachesTheCommitPoint() {
    database.begin();
    database.newDocument("Doc").set("n", 1).save();
    transaction().addAfterCommitCallback(new Recorder("a"));
    database.rollback();

    assertThat(events).isEmpty();
  }

  @Test
  void aTransactionThatConflictsAtCommitNeverReachesTheCommitPoint() throws Exception {
    final RID[] created = new RID[1];
    database.transaction(() -> created[0] = database.newDocument("Doc").set("n", 0).save().getIdentity());
    final RID rid = created[0];

    database.begin();
    final MutableDocument mine = database.lookupByRID(rid, true).asDocument().modify();
    mine.set("n", 1).save();
    transaction().addAfterCommitCallback(new Recorder("a"));

    // Another transaction commits a change of the same record first
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(() -> database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("n", 2).save())).get(1,
          TimeUnit.MINUTES);
    } finally {
      executor.shutdownNow();
    }

    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    assertThat(events).isEmpty();
  }

  @Test
  void anErrorInAnEarlierCallbackAbandonsTheOnesThatWillNotRun() {
    database.begin();
    database.newDocument("Doc").set("n", 1).save();
    transaction().addAfterCommitCallback(() -> {
      throw new StackOverflowError("an error, not an exception");
    });
    transaction().addAfterCommitCallback(new Recorder("a"));
    transaction().addAfterCommitCallback(new Recorder("b"));

    assertThatThrownBy(() -> database.commit()).hasRootCauseInstanceOf(StackOverflowError.class);

    assertThat(events).containsExactly("a:commitPoint", "b:commitPoint", "a:abandoned", "b:abandoned");
  }

  @Test
  void aCallbackThatFailsTakingItsPlaceIsStillAbandonedWhenItWillNotRun() {
    database.begin();
    database.newDocument("Doc").set("n", 1).save();
    transaction().addAfterCommitCallback(() -> {
      throw new StackOverflowError("an error, not an exception");
    });
    transaction().addAfterCommitCallback(new Recorder("a") {
      @Override
      public void onCommitPoint() {
        throw new IllegalStateException("no place");
      }
    });

    assertThatThrownBy(() -> database.commit()).hasRootCauseInstanceOf(StackOverflowError.class);

    // Told it is abandoned though it never took a place: it has nothing to release and says nothing more
    assertThat(events).containsExactly("a:abandoned");
  }

  @Test
  void aCallbackThatFailsTakingItsPlaceIsStillRun() {
    database.transaction(() -> {
      database.newDocument("Doc").set("n", 1).save();
      transaction().addAfterCommitCallback(new Recorder("a") {
        @Override
        public void onCommitPoint() {
          throw new IllegalStateException("no place");
        }
      });
    });

    assertThat(events).containsExactly("a:run");
  }

  private TransactionContext transaction() {
    return ((DatabaseInternal) database).getTransaction();
  }
}
