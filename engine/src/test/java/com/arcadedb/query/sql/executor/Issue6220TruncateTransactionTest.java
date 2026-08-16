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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6220: {@code TRUNCATE TYPE} and {@code TRUNCATE BUCKET} committed the caller's
 * transaction from the inside - {@code schema.dropIndex()} per index before a single record was touched, then
 * {@code commit(); begin()} once per {@code arcadedb.truncateBatchSize} records - so an explicit {@code ROLLBACK}
 * recovered at most whatever the last, uncommitted batch held: nothing at all on an indexed type.
 * <p>
 * The other half of the same fix: with no transaction active the statement now opens its own, where before every
 * delete failed with "Transaction not begun" and the batched drop/rebuild fast path could therefore only ever run on
 * a caller's transaction - the one place it must not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6220TruncateTransactionTest extends TestHelper {

  @Test
  void rollbackPutsBackAnUnindexedTypeSpanningManyBatches() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 100);

    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> {
      for (int i = 0; i < 1100; i++)
        database.command("sql", "INSERT INTO T SET n = ?", i);
    });

    assertThat(database.countType("T", false)).isEqualTo(1100L);

    database.begin();
    database.command("sql", "TRUNCATE TYPE `T` UNSAFE").close();
    database.rollback();

    assertThat(database.countType("T", false)).isEqualTo(1100L);
  }

  @Test
  void rollbackPutsBackAnIndexedType() {
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.n INTEGER");
    database.command("sql", "CREATE INDEX ON U(n) NOTUNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO U SET n = ?", i);
    });

    assertThat(database.countType("U", false)).isEqualTo(5L);

    database.begin();
    try (final ResultSet rs = database.command("sql", "TRUNCATE TYPE `U` UNSAFE")) {
      assertThat(rs.next().<Boolean>getProperty("transactional"))
          .as("the statement must report which path it took").isTrue();
    }
    database.rollback();

    assertThat(database.countType("U", false)).isEqualTo(5L);
    assertThat(database.getSchema().getType("U").getAllIndexes(true)).isNotEmpty();
    assertThat(database.query("sql", "SELECT FROM U WHERE n = 3").stream().count()).isEqualTo(1L);
  }

  @Test
  void commitStillRemovesEverything() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 100);

    database.command("sql", "CREATE DOCUMENT TYPE V2");
    database.command("sql", "CREATE PROPERTY V2.n INTEGER");
    database.command("sql", "CREATE INDEX ON V2(n) NOTUNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < 350; i++)
        database.command("sql", "INSERT INTO V2 SET n = ?", i);
    });

    database.begin();
    database.command("sql", "TRUNCATE TYPE `V2` UNSAFE").close();
    database.commit();

    assertThat(database.countType("V2", false)).isEqualTo(0L);
    assertThat(database.query("sql", "SELECT FROM V2 WHERE n = 3").stream().count()).isEqualTo(0L);

    database.transaction(() -> database.command("sql", "INSERT INTO V2 SET n = 3"));
    assertThat(database.query("sql", "SELECT FROM V2 WHERE n = 3").stream().count()).isEqualTo(1L);
  }

  /**
   * The shape the issue is really about: reload a staging type in one unit of work. When the insert half fails, the
   * old rows must still be there.
   */
  @Test
  void aFailedReloadLeavesTheOldRowsInPlace() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 10);

    database.command("sql", "CREATE DOCUMENT TYPE Staging");
    database.command("sql", "CREATE PROPERTY Staging.n INTEGER");
    database.command("sql", "CREATE INDEX ON Staging(n) UNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.command("sql", "INSERT INTO Staging SET n = ?", i);
    });

    assertThatThrownBy(() -> database.transaction(() -> {
      database.command("sql", "TRUNCATE TYPE `Staging` UNSAFE").close();
      database.command("sql", "INSERT INTO Staging SET n = 1000").close();
      throw new IllegalStateException("the reload failed halfway");
    })).isInstanceOf(IllegalStateException.class);

    assertThat(database.countType("Staging", false)).isEqualTo(40L);
    assertThat(database.query("sql", "SELECT FROM Staging WHERE n = 1000").stream().count()).isZero();
    for (int i = 0; i < 40; i++)
      assertThat(database.query("sql", "SELECT FROM Staging WHERE n = ?", i).stream().count()).isEqualTo(1L);
  }

  /**
   * With no transaction active the statement owns one: it must open it (the deletes used to fail with "Transaction
   * not begun", which made this whole path unreachable), take the drop/rebuild fast path, and hand the caller's
   * thread back without a transaction on it.
   */
  @Test
  void truncateWithNoActiveTransactionOwnsOneAndRebuildsTheIndexes() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 10);

    database.command("sql", "CREATE DOCUMENT TYPE W");
    database.command("sql", "CREATE PROPERTY W.n INTEGER");
    database.command("sql", "CREATE INDEX ON W(n) UNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < 35; i++)
        database.command("sql", "INSERT INTO W SET n = ?", i);
    });

    try (final ResultSet rs = database.command("sql", "TRUNCATE TYPE `W` UNSAFE")) {
      assertThat(rs.next().<Boolean>getProperty("transactional"))
          .as("a truncate that owns its transaction commits it: a rollback cannot put the records back").isFalse();
    }

    assertThat(database.isTransactionActive()).as("TRUNCATE must not leave its own transaction open").isFalse();
    assertThat(database.countType("W", false)).isZero();
    assertThat(database.getSchema().getType("W").getAllIndexes(true)).isNotEmpty();
    assertThat(database.getSchema().getIndexByName("W[n]").countEntries()).isZero();

    // the recreated unique index accepts every key back
    database.transaction(() -> {
      for (int i = 0; i < 35; i++)
        database.command("sql", "INSERT INTO W SET n = ?", i);
    });
    assertThat(database.countType("W", false)).isEqualTo(35L);
  }

  /**
   * The deletes now belong to the caller's transaction, so they must also see what that transaction has written but
   * not yet committed - a truncate that only cleared the on-disk rows would leave the ones inserted a statement ago.
   */
  @Test
  void truncateAlsoRemovesRecordsTheSameTransactionJustInserted() {
    database.command("sql", "CREATE DOCUMENT TYPE Fresh");
    database.command("sql", "CREATE PROPERTY Fresh.n INTEGER");
    database.command("sql", "CREATE INDEX ON Fresh(n) UNIQUE");

    database.transaction(() -> database.command("sql", "INSERT INTO Fresh SET n = 1"));

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Fresh SET n = 2").close();
      database.command("sql", "TRUNCATE TYPE `Fresh` UNSAFE").close();
      database.command("sql", "INSERT INTO Fresh SET n = 1").close();
    });

    assertThat(database.countType("Fresh", false)).isEqualTo(1L);
    assertThat(database.query("sql", "SELECT FROM Fresh WHERE n = 1").stream().count()).isEqualTo(1L);
    assertThat(database.query("sql", "SELECT FROM Fresh WHERE n = 2").stream().count()).isZero();
  }

  @Test
  void rollbackPutsBackAPolymorphicTruncate() {
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE DOCUMENT TYPE Derived EXTENDS Base");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Base SET n = 1");
      database.command("sql", "INSERT INTO Derived SET n = 2");
    });

    database.begin();
    database.command("sql", "TRUNCATE TYPE `Base` POLYMORPHIC UNSAFE").close();
    assertThat(database.countType("Base", true)).as("the deletes are visible inside the transaction").isZero();
    database.rollback();

    assertThat(database.countType("Base", true)).isEqualTo(2L);
    assertThat(database.countType("Derived", false)).isEqualTo(1L);
  }

  @Test
  void truncateBucketRollbackPutsBackTheRecords() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 10);

    database.command("sql", "CREATE DOCUMENT TYPE B BUCKETS 1");
    database.transaction(() -> {
      for (int i = 0; i < 35; i++)
        database.command("sql", "INSERT INTO B SET n = ?", i);
    });

    final String bucketName = database.getSchema().getType("B").getBuckets(false).getFirst().getName();

    database.begin();
    database.command("sql", "TRUNCATE BUCKET `" + bucketName + "` UNSAFE").close();
    database.rollback();

    assertThat(database.countBucket(bucketName)).isEqualTo(35L);

    // ...and with no transaction of its own to break, TRUNCATE BUCKET still clears it
    try (final ResultSet rs = database.command("sql", "TRUNCATE BUCKET `" + bucketName + "` UNSAFE")) {
      assertThat(rs.next().<Boolean>getProperty("transactional")).isFalse();
    }
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.countBucket(bucketName)).isZero();
  }
}
