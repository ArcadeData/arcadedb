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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12889: {@link TransactionContext#hasChangedNothing()}, the one definition of "this transaction has no write" a layer above
 * the engine asks before it decides that committing it needs nothing of the database's files (the HA commit gates exempt the empty
 * transaction an HTTP request holds around a write it forwarded to the leader). Every kind of write has to make it false, or a
 * transaction that wrote would be let through a gate that exists to refuse it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class TransactionHasChangedNothingTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.id LONG").close();
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("T").set("id", i).save();
    });
  }

  private TransactionContext transaction() {
    return ((DatabaseInternal) database).getTransaction();
  }

  @Test
  void aTransactionThatOnlyReadsHasChangedNothing() {
    database.begin();
    try {
      assertThat(transaction().hasChangedNothing()).as("just begun").isTrue();
      database.query("sql", "SELECT FROM T").close();
      assertThat(transaction().hasChangedNothing()).as("after a read").isTrue();
    } finally {
      database.rollback();
    }
  }

  @Test
  void aCreatedRecordIsAWrite() {
    database.begin();
    try {
      database.newDocument("T").set("id", 10).save();
      assertThat(transaction().hasChangedNothing()).isFalse();
    } finally {
      database.rollback();
    }
  }

  /** An update is deferred to the commit: no page has changed yet, the queued record is what says the transaction writes. */
  @Test
  void aQueuedUpdateIsAWrite() {
    database.begin();
    try {
      final MutableDocument document;
      try (final ResultSet result = database.query("sql", "SELECT FROM T WHERE id = 1")) {
        document = result.next().getRecord().get().asDocument().modify();
      }
      document.set("id", 100).save();
      assertThat(transaction().hasChangedNothing()).isFalse();
    } finally {
      database.rollback();
    }
  }

  @Test
  void aDeleteIsAWrite() {
    database.begin();
    try {
      database.command("sql", "DELETE FROM T WHERE id = 2").close();
      assertThat(transaction().hasChangedNothing()).isFalse();
    } finally {
      database.rollback();
    }
  }
}
