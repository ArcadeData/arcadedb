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

import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #8075 (parent of #8051 and #8066), follow-ups to #7467.
 * <ul>
 * <li>#8051: the engine's retraction of a refused create frees the body through the permission-checked delete, so a
 * role with {@code createRecord} but not {@code deletedRecord} turned one refused row into a rollback-only
 * transaction.</li>
 * <li>#8066: a refused in-transaction UPDATE left its new key queued in the transaction's index changes, so the
 * chunk's commit died naming the row already reported refused.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8075RefusedRowLeavesNoTraceTest {
  private static final String PATH = "target/databases/Issue8075RefusedRowLeavesNoTraceTest";
  private static final String TYPE = "Keyed8075";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("name", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "name");
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  /** #8051: an ingestion role (create + read, no delete) must still get the one-row refusal, not a doomed batch. */
  @Test
  void aRefusedCreateByARoleWithoutDeleteDoesNotDoomTheTransaction() {
    bindIngestionUser();

    database.transaction(() -> {
      database.newDocument(TYPE).set("name", "dup").set("row", 1).save();
      assertThat(catchThrowable(() -> database.newDocument(TYPE).set("name", "dup").set("row", 2).save()))
          .isInstanceOf(DuplicatedKeyException.class);
      database.newDocument(TYPE).set("name", "other").set("row", 3).save();
    });

    assertThat(database.countType(TYPE, false)).isEqualTo(2);
  }

  /** #8051 on the restore path. */
  @Test
  void aRefusedRestoreByARoleWithoutDeleteDoesNotDoomTheTransaction() {
    final RID[] freed = new RID[2];
    database.transaction(() -> {
      freed[0] = database.newDocument(TYPE).set("name", "s0").save().getIdentity();
      freed[1] = database.newDocument(TYPE).set("name", "s1").save().getIdentity();
    });
    database.transaction(() -> {
      database.deleteRecord(freed[0].asDocument());
      database.deleteRecord(freed[1].asDocument());
    });

    bindIngestionUser();

    database.transaction(() -> {
      // The key is queued by THIS transaction, so the duplicate is decided inline (against committed state it is
      // decided at commit). The new record takes one of the two freed slots; the other is the restore target.
      final RID taken = database.newDocument(TYPE).set("name", "x").save().getIdentity();
      final RID target = taken.equals(freed[0]) ? freed[1] : freed[0];
      final MutableDocument shell = database.newDocument(TYPE).set("name", "x");
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(target.getBucketId());
      assertThat(catchThrowable(() -> ((DatabaseInternal) database).getEmbedded().restoreRecord(shell, bucket, target.getPosition())))
          .isInstanceOf(DuplicatedKeyException.class);
    });

    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  /** #8066: a refused update must not leave its key queued: the rest of the chunk commits. */
  @Test
  void aRefusedUpdateLeavesNoKeyQueuedAndTheRestOfTheChunkCommits() {
    database.transaction(() -> {
      database.newDocument(TYPE).set("name", "a").set("row", 1).save();
      final MutableDocument mb = database.newDocument(TYPE).set("name", "b").set("row", 2);
      mb.save();
      mb.set("name", "a");
      assertThat(catchThrowable(mb::save)).isInstanceOf(DuplicatedKeyException.class);

      database.newDocument(TYPE).set("name", "c").set("row", 3).save();
    });

    assertThat(database.countType(TYPE, false)).isEqualTo(3);
    assertThat(rowOf("a")).isEqualTo(1);
    assertThat(rowOf("b")).as("the refused update must not have been written").isEqualTo(2);
    assertThat(rowOf("c")).isEqualTo(3);
  }

  private int rowOf(final String key) {
    return database.query("sql", "SELECT FROM " + TYPE + " WHERE name = '" + key + "'").next().<Integer>getProperty("row");
  }

  private void bindIngestionUser() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "ingest";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return access != ACCESS.DELETE_RECORD;
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return access != ACCESS.DELETE_RECORD;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
