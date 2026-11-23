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
package com.arcadedb.partitioning;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12961: on a type with partitioned bucket selection every update that changed ANY indexed property was refused
 * ("Cannot modify primary key"). Only a change of the partition key itself must be refused, and as a caller error.
 */
class Issue12961PartitionedIndexedUpdateTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Account BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Account.id INTEGER");
      database.command("sql", "CREATE PROPERTY Account.email STRING");
      database.command("sql", "CREATE PROPERTY Account.region STRING");
      database.command("sql", "CREATE INDEX ON Account (id) UNIQUE");
      database.command("sql", "CREATE INDEX ON Account (email) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Account (region, id) NOTUNIQUE");
      database.command("sql", "ALTER TYPE Account BUCKETSELECTIONSTRATEGY `partitioned('id')`");
      database.command("sql", "INSERT INTO Account SET id = 1, email = 'a@x', region = 'eu'");
    });
  }

  @Test
  void secondaryIndexedPropertiesCanBeUpdated() {
    database.transaction(() -> database.command("sql", "UPDATE Account SET email = 'b@x', region = 'us' WHERE id = 1"));
    assertEmail("b@x");
    assertThat(count("SELECT FROM Account WHERE region = 'us' AND id = 1")).isEqualTo(1);

    database.transaction(() -> database.command("sql", "UPDATE Account CONTENT {\"id\": 1, \"email\": \"c@x\"} WHERE id = 1"));
    assertEmail("c@x");

    database.transaction(() -> database.command("sql", "UPDATE Account MERGE {\"email\": \"d@x\"} WHERE id = 1"));
    assertEmail("d@x");

    database.transaction(() -> database.command("sql", "UPDATE Account REMOVE email WHERE id = 1"));
    assertThat(count("SELECT FROM Account WHERE email = 'd@x'")).isZero();

    database.transaction(() -> database.command("opencypher", "MATCH (a:Account {id: 1}) SET a.email = 'e@x'"));
    assertEmail("e@x");

    database.transaction(() -> database.command("opencypher", "MATCH (a:Account {id: 1}) SET a += {email: 'f@x', id: 1}"));
    assertEmail("f@x");
  }

  @Test
  void sameValueWriteOfThePartitionKeyIsAccepted() {
    database.transaction(() -> database.command("sql", "UPDATE Account SET id = 1, email = 'z@x' WHERE id = 1"));
    assertEmail("z@x");
  }

  @Test
  void partitionKeyChangeIsRefused() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE Account SET id = 2 WHERE id = 1")))
        .isInstanceOf(ValidationException.class).hasMessageContaining("partition key [id]");
    // openCypher wraps the refusal in a CommandExecutionException; the server classifies it along the cause chain
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (a:Account {id: 1}) SET a.id = 3, a.email = 'q@x'")))
        .hasRootCauseInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE Account REMOVE id WHERE id = 1")))
        .isInstanceOf(ValidationException.class);

    assertThat(count("SELECT FROM Account WHERE id = 1 AND email = 'a@x'")).isEqualTo(1);
    assertThat(count("SELECT FROM Account WHERE id <> 1")).isZero();
  }

  /**
   * A second update of the record in the same transaction diffs against the indexed snapshot the first one left, not against
   * the committed buffer: the partition key must be read from a baseline that still carries it.
   */
  @Test
  void aRecordUpdatedTwiceInOneTransactionIsAccepted() {
    database.transaction(() -> {
      database.command("sql", "UPDATE Account SET email = 'b@x' WHERE id = 1");
      database.command("sql", "UPDATE Account SET email = 'c@x', region = 'us' WHERE id = 1");
    });
    assertEmail("c@x");
    assertThat(count("SELECT FROM Account WHERE email = 'b@x'")).as("the intermediate value left no index entry").isZero();
    assertThat(count("SELECT FROM Account WHERE region = 'us' AND id = 1")).isEqualTo(1);
  }

  @Test
  void aPartitionKeyChangeAfterAnIndexedUpdateOfTheSameTransactionIsRefused() {
    assertThatThrownBy(() -> database.transaction(() -> {
      database.command("sql", "UPDATE Account SET email = 'b@x' WHERE id = 1");
      database.command("sql", "UPDATE Account SET id = 2 WHERE email = 'b@x'");
    })).isInstanceOf(ValidationException.class).hasMessageContaining("partition key [id]");

    assertThat(count("SELECT FROM Account WHERE id = 1 AND email = 'a@x'")).isEqualTo(1);
    assertThat(count("SELECT FROM Account WHERE email = 'b@x'")).isZero();
  }

  /**
   * Creating an edge rewrites the vertex (its edge-list head), after an update of the vertex in the same transaction left a
   * snapshot behind: neither is a change of the partition key.
   */
  @Test
  void edgesCanBeCreatedOnARecordUpdatedInTheSameTransaction() {
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE TYPE Knows");
      database.command("sql", "INSERT INTO Account SET id = 2, email = 'two@x', region = 'eu'");
    });
    database.transaction(() -> {
      database.command("sql", "UPDATE Account SET email = 'b@x' WHERE id = 1");
      database.command("sql", "CREATE EDGE Knows FROM (SELECT FROM Account WHERE id = 1) TO (SELECT FROM Account WHERE id = 2)");
      database.command("sql", "UPDATE Account SET email = 'c@x' WHERE id = 1");
    });
    assertEmail("c@x");
    assertThat(count("SELECT FROM Knows")).isEqualTo(1);
  }

  /**
   * A partitioned type can outlive its partition index (nothing re-checks it after the assignment). With another index on the
   * type the partition key is then in no snapshot an index diff builds, so a second update of the record in the transaction
   * read it as null and refused a key that had not moved.
   */
  @Test
  void aPartitionedTypeWhoseIndexWasDroppedStillAcceptsIndexedUpdates() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Orphan BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Orphan.k INTEGER");
      database.command("sql", "CREATE PROPERTY Orphan.email STRING");
      database.command("sql", "CREATE INDEX ON Orphan (k) UNIQUE");
      database.command("sql", "CREATE INDEX ON Orphan (email) NOTUNIQUE");
      database.command("sql", "ALTER TYPE Orphan BUCKETSELECTIONSTRATEGY `partitioned('k')`");
      database.command("sql", "DROP INDEX `Orphan[k]`");
      database.command("sql", "INSERT INTO Orphan SET k = 7, email = 'a@x'");
    });
    assertThat(database.getSchema().getType("Orphan").getBucketSelectionStrategy().getName()).isEqualTo("partitioned");

    database.transaction(() -> {
      database.command("sql", "UPDATE Orphan SET email = 'b@x' WHERE k = 7");
      database.command("sql", "UPDATE Orphan SET email = 'c@x' WHERE k = 7");
    });
    assertThat(count("SELECT FROM Orphan WHERE email = 'c@x' AND k = 7")).isEqualTo(1);
    assertThat(count("SELECT FROM Orphan WHERE email = 'b@x'")).isZero();

    // and the key that no index covers any more is still protected, from the committed buffer and from the snapshot
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE Orphan SET k = 8, email = 'd@x' WHERE k = 7")))
        .isInstanceOf(ValidationException.class).hasMessageContaining("partition key [k]");
    assertThatThrownBy(() -> database.transaction(() -> {
      database.command("sql", "UPDATE Orphan SET email = 'e@x' WHERE k = 7");
      database.command("sql", "UPDATE Orphan SET k = 8 WHERE k = 7");
    })).isInstanceOf(ValidationException.class).hasMessageContaining("partition key [k]");
    assertThat(count("SELECT FROM Orphan WHERE email = 'c@x' AND k = 7")).isEqualTo(1);
  }

  /**
   * An index created inside the transaction that had already written the record is built over its pending state, and leaves the
   * snapshot the next update diffs against. That snapshot must carry the partition key too, which here no index covers.
   */
  @Test
  void aRecordUpdatedAfterAnIndexBuildInTheSameTransactionKeepsItsPartitionKey() {
    createOrphanedType("Late");
    database.begin();
    database.command("sql", "UPDATE Late SET note = 'n1' WHERE k = 7");
    database.command("sql", "CREATE INDEX ON Late (email) NOTUNIQUE");
    database.command("sql", "UPDATE Late SET email = 'b@x' WHERE k = 7");
    database.command("sql", "UPDATE Late SET email = 'c@x' WHERE k = 7");
    database.commit();
    assertThat(count("SELECT FROM Late WHERE email = 'c@x' AND k = 7 AND note = 'n1'")).isEqualTo(1);
    assertThat(count("SELECT FROM Late WHERE email = 'b@x'")).as("the intermediate value left no index entry").isZero();

    createOrphanedType("Late2");
    database.begin();
    try {
      database.command("sql", "UPDATE Late2 SET note = 'n1' WHERE k = 7");
      database.command("sql", "CREATE INDEX ON Late2 (email) NOTUNIQUE");
      assertThatThrownBy(() -> database.command("sql", "UPDATE Late2 SET k = 8 WHERE k = 7")).isInstanceOf(ValidationException.class)
          .hasMessageContaining("partition key [k]");
    } finally {
      database.rollback();
    }
    assertThat(count("SELECT FROM Late2 WHERE k = 7 AND email = 'a@x'")).isEqualTo(1);
  }

  /** Each property of a composite partition key is the key: a change of one of them alone is refused, any other is not. */
  @Test
  void aChangeOfOneOfTheCompositePartitionKeyPropertiesIsRefused() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Shard BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Shard.tenant STRING");
      database.command("sql", "CREATE PROPERTY Shard.id INTEGER");
      database.command("sql", "CREATE PROPERTY Shard.email STRING");
      database.command("sql", "CREATE INDEX ON Shard (tenant, id) UNIQUE");
      database.command("sql", "CREATE INDEX ON Shard (email) NOTUNIQUE");
      database.command("sql", "ALTER TYPE Shard BUCKETSELECTIONSTRATEGY `partitioned('tenant','id')`");
      database.command("sql", "INSERT INTO Shard SET tenant = 'acme', id = 1, email = 'a@x'");
    });

    database.transaction(() -> database.command("sql", "UPDATE Shard SET email = 'b@x', tenant = 'acme', id = 1"));
    assertThat(count("SELECT FROM Shard WHERE tenant = 'acme' AND id = 1 AND email = 'b@x'")).isEqualTo(1);

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE Shard SET id = 2")))
        .isInstanceOf(ValidationException.class).hasMessageContaining("partition key [tenant, id]");
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE Shard SET tenant = 'globex'")))
        .isInstanceOf(ValidationException.class).hasMessageContaining("partition key [tenant, id]");
    assertThat(count("SELECT FROM Shard WHERE tenant = 'acme' AND id = 1")).isEqualTo(1);
  }

  /**
   * A DATETIME read back as a {@link Date} (the opt-in {@code arcadedb.dateTimeImplementation}) is mutable: one mutated in place
   * after a save must not move the baseline of the next update of the record with it, or the partition key would move unseen.
   */
  @Test
  void aDatePartitionKeyMutatedInPlaceIsRefused() {
    // per database, so nothing leaks to another test: the serializer is the one this database reads and writes with
    ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(Date.class);
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Tick BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Tick.ts DATETIME");
      database.command("sql", "CREATE PROPERTY Tick.email STRING");
      database.command("sql", "CREATE INDEX ON Tick (ts) UNIQUE");
      database.command("sql", "CREATE INDEX ON Tick (email) NOTUNIQUE");
      database.command("sql", "ALTER TYPE Tick BUCKETSELECTIONSTRATEGY `partitioned('ts')`");
      database.newDocument("Tick").set("ts", new Date(1_000_000L)).set("email", "a@x").save();
    });

    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument tick = database.iterateType("Tick", true).next().asDocument().modify();
      tick.set("email", "b@x").save();
      ((Date) tick.get("ts")).setTime(2_000_000L);
      tick.save();
    })).isInstanceOf(ValidationException.class).hasMessageContaining("partition key [ts]");

    assertThat(count("SELECT FROM Tick WHERE email = 'a@x'")).isEqualTo(1);
  }

  /** A partitioned type whose partition index was dropped and that has no other index: one record, k = 7. */
  private void createOrphanedType(final String name) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE " + name + " BUCKETS 4");
      database.command("sql", "CREATE PROPERTY " + name + ".k INTEGER");
      database.command("sql", "CREATE PROPERTY " + name + ".email STRING");
      database.command("sql", "CREATE PROPERTY " + name + ".note STRING");
      database.command("sql", "CREATE INDEX ON " + name + " (k) UNIQUE");
      database.command("sql", "ALTER TYPE " + name + " BUCKETSELECTIONSTRATEGY `partitioned('k')`");
      database.command("sql", "DROP INDEX `" + name + "[k]`");
      database.command("sql", "INSERT INTO " + name + " SET k = 7, email = 'a@x'");
    });
  }

  private void assertEmail(final String email) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Account WHERE email = ?", email)) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((Integer) r.getProperty("id")).isEqualTo(1);
    }
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.stream().count();
    }
  }
}
