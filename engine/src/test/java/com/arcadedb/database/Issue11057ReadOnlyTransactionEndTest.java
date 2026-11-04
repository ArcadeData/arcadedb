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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.TransactionException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11057, the embedded half (PR #11124 review): {@code BasicDatabase.rollback()} promises a no-op when no transaction is
 * active, but on a database opened {@code READ_ONLY} both {@code rollback()} and {@code commit()} asserted a WRITE intent before
 * looking for a transaction. An idle rollback threw {@code DatabaseIsReadOnlyException}, and the read transaction
 * {@code begin()} does open there could be neither committed nor rolled back, so it stayed open on the thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11057ReadOnlyTransactionEndTest extends TestHelper {
  private static final String TYPE = "Issue11057Type";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE);
    database.transaction(() -> database.newDocument(TYPE).set("id", 1).save());
  }

  @Test
  void aReadOnlyDatabaseEndsItsReadTransactionsAndAnIdleRollbackIsANoOp() {
    database.close();

    final Database readOnly = factory.open(ComponentFile.MODE.READ_ONLY);
    try {
      readOnly.rollback();
      assertThat(readOnly.isTransactionActive()).isFalse();

      readOnly.begin();
      assertThat(readOnly.countType(TYPE, true)).isEqualTo(1);
      readOnly.rollback();
      assertThat(readOnly.isTransactionActive()).as("the read transaction is rolled back").isFalse();
      readOnly.rollback();

      readOnly.begin();
      assertThat(readOnly.countType(TYPE, true)).isEqualTo(1);
      readOnly.commit();
      assertThat(readOnly.isTransactionActive()).as("the read transaction is committed").isFalse();

      // THE COUNTERWEIGHTS: A WRITE IS STILL REFUSED WHERE IT IS MADE, AND A COMMIT WITHOUT A TRANSACTION STILL THROWS
      readOnly.begin();
      assertThatThrownBy(() -> readOnly.newDocument(TYPE).set("id", 2).save()).isInstanceOf(DatabaseIsReadOnlyException.class);
      readOnly.rollback();
      assertThat(readOnly.isTransactionActive()).isFalse();
      assertThatThrownBy(readOnly::commit).isInstanceOf(TransactionException.class);

      assertThat(readOnly.countType(TYPE, true)).isEqualTo(1);
    } finally {
      if (readOnly.isOpen())
        readOnly.close();
    }
  }
}
