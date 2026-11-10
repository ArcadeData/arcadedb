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
import com.arcadedb.database.TransactionContext.ChangeStamp;
import com.arcadedb.engine.Bucket;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11394: the change stamp of a transaction, the key of a structure derived from what the transaction holds (a hierarchy's private
 * view). It must differ after anything that changes what a read through the transaction returns, and stay equal across a read.
 */
class TransactionChangeStampTest extends TestHelper {
  private final AtomicInteger deletes     = new AtomicInteger();
  private       int           refuseAtNth = -1;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.id LONG").close();
    database.getSchema().getType("T").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (refuseAtNth > 0 && deletes.incrementAndGet() == refuseAtNth)
        throw new IllegalStateException("refused");
      return true;
    });
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("T").set("id", i).save();
    });
  }

  private ChangeStamp stamp() {
    final IntHashSet files = new IntHashSet();
    for (final Bucket bucket : database.getSchema().getType("T").getBuckets(false))
      files.add(bucket.getFileId());
    return ((DatabaseInternal) database).getTransaction().changeStamp(files);
  }

  @Test
  void aReadDoesNotMoveTheStampAndAWriteDoes() {
    database.begin();
    try {
      database.command("sql", "UPDATE T SET id = 100 WHERE id = 1").close();
      final ChangeStamp held = stamp();
      database.query("sql", "SELECT FROM T").close();
      assertThat(stamp()).isEqualTo(held);
      // a write into a page the transaction already holds: no page is added, only its bytes change
      database.command("sql", "UPDATE T SET id = 101 WHERE id = 2").close();
      assertThat(stamp()).isNotEqualTo(held);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aStatementTakenBackMovesTheStamp() {
    database.begin();
    try {
      database.command("sql", "UPDATE T SET id = 100 WHERE id = 1").close();
      final ChangeStamp before = stamp();
      deletes.set(0);
      refuseAtNth = 2;
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM T WHERE id > 3").close()).hasStackTraceContaining("refused");
      refuseAtNth = -1;
      // the bytes are back to what they were, but a view derived while the statement ran described the in-between state
      assertThat(stamp()).isNotEqualTo(before);
    } finally {
      database.rollback();
    }
  }

  @Test
  void theStampIsNotAffectedByFilesTheTransactionDidNotWrite() {
    database.begin();
    try {
      final IntHashSet none = new IntHashSet();
      final ChangeStamp before = ((DatabaseInternal) database).getTransaction().changeStamp(none);
      database.command("sql", "UPDATE T SET id = 100 WHERE id = 1").close();
      final ChangeStamp after = ((DatabaseInternal) database).getTransaction().changeStamp(none);
      // no page of the (empty) file set is counted, and no write of another file moves the structural part either (issue #11472)
      assertThat(after.pages()).isZero();
      assertThat(after.pageWrites()).isZero();
      assertThat(after).isEqualTo(before);
    } finally {
      database.rollback();
    }
  }

  /** Issue #11472: a record written to another bucket, whatever the write, leaves the stamp of the watched files as it was. */
  @Test
  void aWriteToAnotherFileLeavesTheStampAndAWriteToTheWatchedOneMovesIt() {
    database.command("sql", "CREATE DOCUMENT TYPE U").close();
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newDocument("U").set("id", i).save();
    });
    database.begin();
    try {
      database.command("sql", "UPDATE T SET id = 100 WHERE id = 1").close();
      final ChangeStamp held = stamp();

      database.newDocument("U").set("id", 50).save();
      assertThat(stamp()).as("a record created in another file").isEqualTo(held);
      database.command("sql", "UPDATE U SET id = 51 WHERE id = 1").close();
      assertThat(stamp()).as("a record updated in another file").isEqualTo(held);
      database.command("sql", "DELETE FROM U WHERE id = 2").close();
      assertThat(stamp()).as("a record deleted in another file").isEqualTo(held);

      database.newDocument("T").set("id", 60).save();
      final ChangeStamp created = stamp();
      assertThat(created).as("a record created in a watched file").isNotEqualTo(held);
      database.command("sql", "DELETE FROM T WHERE id = 3").close();
      assertThat(stamp()).as("a record deleted in a watched file").isNotEqualTo(created);
    } finally {
      database.rollback();
    }
  }
}
