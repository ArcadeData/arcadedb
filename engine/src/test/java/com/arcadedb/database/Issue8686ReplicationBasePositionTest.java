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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8686: the replication position a transaction records at begin is per transaction. A context is reused across
 * transactions on a thread, so {@code begin()} must clear it, or a transaction begun by any other route would read as
 * prepared at the previous one's position.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8686ReplicationBasePositionTest extends TestHelper {

  @Test
  void beginClearsThePositionOfAPreviousTransaction() {
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    assertThat(tx.getReplicationBasePosition()).as("unknown until a replicated database stamps it").isEqualTo(-1L);

    tx.setReplicationBasePosition(42L);
    assertThat(tx.getReplicationBasePosition()).isEqualTo(42L);
    database.commit();

    database.begin();
    assertThat(((DatabaseInternal) database).getTransaction().getReplicationBasePosition()).isEqualTo(-1L);
    database.rollback();
  }
}
