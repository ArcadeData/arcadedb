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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The contexts of a thread whose transaction ran to its end are released with the thread, instead of staying in the static
 * context registry until the dead-thread sweep; a thread that still has an active transaction keeps them, for the sweep to
 * roll the transaction back.
 */
class DatabaseContextReleaseIdleThreadTest extends TestHelper {

  @Test
  void aThreadWhoseTransactionEndedReleasesItsContexts() throws Exception {
    database.getSchema().createDocumentType("T");
    final String path = ((DatabaseInternal) database).getDatabasePath();
    final AtomicBoolean released = new AtomicBoolean();
    final AtomicReference<Object> contextAfter = new AtomicReference<>("unset");
    final Thread thread = new Thread(() -> {
      database.begin();
      database.newDocument("T").save();
      database.commit();
      released.set(DatabaseContext.INSTANCE.releaseCurrentThreadContextsIfIdle());
      contextAfter.set(DatabaseContext.INSTANCE.getContextIfExists(path));
    });
    thread.start();
    thread.join();

    assertThat(released).isTrue();
    assertThat(contextAfter.get()).isNull();
  }

  @Test
  void aThreadWithAnActiveTransactionKeepsItsContexts() throws Exception {
    database.getSchema().createDocumentType("T");
    final String path = ((DatabaseInternal) database).getDatabasePath();
    final AtomicBoolean released = new AtomicBoolean(true);
    final AtomicReference<Object> contextAfter = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      database.begin();
      database.newDocument("T").save();
      released.set(DatabaseContext.INSTANCE.releaseCurrentThreadContextsIfIdle());
      contextAfter.set(DatabaseContext.INSTANCE.getContextIfExists(path));
      database.rollback();
    });
    thread.start();
    thread.join();

    assertThat(released).isFalse();
    assertThat(contextAfter.get()).isNotNull();
  }
}
