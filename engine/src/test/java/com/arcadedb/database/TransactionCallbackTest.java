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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionCallbackTest extends TestHelper {

  @Test
  void callbackFiresAfterCommit() {
    final AtomicBoolean fired = new AtomicBoolean(false);

    database.begin();
    database.getSchema().createDocumentType("CallbackTest1");
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> fired.set(true));
    database.commit();

    assertThat(fired.get()).isTrue();
  }

  @Test
  void callbackNotFiredOnRollback() {
    final AtomicBoolean fired = new AtomicBoolean(false);

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> fired.set(true));
    database.rollback();

    assertThat(fired.get()).isFalse();
  }

  @Test
  void multipleCallbacksFireInOrder() {
    final List<Integer> order = new ArrayList<>();

    database.begin();
    database.getSchema().createDocumentType("CallbackTest2");
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> order.add(1));
    tx.addAfterCommitCallback(() -> order.add(2));
    tx.addAfterCommitCallback(() -> order.add(3));
    database.commit();

    assertThat(order).containsExactly(1, 2, 3);
  }

  @Test
  void callbackErrorDoesNotAffectCommit() {
    final AtomicInteger counter = new AtomicInteger(0);

    database.begin();
    database.getSchema().createDocumentType("CallbackTest3");
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> counter.incrementAndGet());
    tx.addAfterCommitCallback(() -> { throw new RuntimeException("test error"); });
    tx.addAfterCommitCallback(() -> counter.incrementAndGet());
    database.commit();

    // All callbacks should have run despite the error in the second one
    assertThat(counter.get()).isEqualTo(2);
    // And the type should exist (commit succeeded)
    assertThat(database.getSchema().existsType("CallbackTest3")).isTrue();
  }
}
