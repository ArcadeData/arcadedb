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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionContextRemoveFileTest extends TestHelper {

  @Test
  void removeFileRemovesValueNotIndex() throws Exception {
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

    final Field lockedFilesField = TransactionContext.class.getDeclaredField("lockedFiles");
    lockedFilesField.setAccessible(true);

    // Inject a list that simulates what commit1stPhase sets: file IDs that are
    // larger than their positions in the list, so index-based removal would pick
    // the wrong element or throw IndexOutOfBoundsException.
    final List<Integer> injected = new ArrayList<>(List.of(10, 20, 30));
    lockedFilesField.set(tx, injected);

    // removeFile(20) must remove the value 20 (object-based), not the element at index 20.
    // With the bug: list.remove(20) -> IndexOutOfBoundsException (size is 3).
    // With the fix: list.remove(Integer.valueOf(20)) -> [10, 30].
    tx.removeFile(20);

    assertThat(injected).containsExactly(10, 30);

    database.rollback();
  }

  @Test
  void removeFileForAbsentIdIsNoOp() throws Exception {
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

    final Field lockedFilesField = TransactionContext.class.getDeclaredField("lockedFiles");
    lockedFilesField.setAccessible(true);

    final List<Integer> injected = new ArrayList<>(List.of(10, 20, 30));
    lockedFilesField.set(tx, injected);

    tx.removeFile(99);

    assertThat(injected).containsExactly(10, 20, 30);

    database.rollback();
  }

  @Test
  void removeFileWhenLockedFilesNullIsNoOp() {
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

    // lockedFiles is null until commit1stPhase; removeFile must not throw.
    tx.removeFile(5);

    database.rollback();
  }
}
