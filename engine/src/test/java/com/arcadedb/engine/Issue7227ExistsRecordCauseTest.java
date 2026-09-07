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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7227, item 1: the third site that turned an {@link IOException} into a
 * {@link DatabaseOperationException} without chaining it.
 * <p>
 * {@code LocalBucket.existsRecord} runs on the ordinary query and traversal path - a bucket scan skipping
 * deleted slots, a graph traversal validating an edge endpoint, {@code FetchFromRidsStep} resolving a RID -
 * so an I/O error there is precisely when the caller needs to tell a permission problem from a full volume
 * from a short file. The message alone tells it none of them. {@code isRecordStoredInSinglePage}, which reads
 * the same size marker through the same helper, has chained since it was written; #7141 chained the two sites
 * it named, and this one was missed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7227ExistsRecordCauseTest extends TestHelper {

  private static final String TYPE           = "Doc7227";
  private static final String INJECTED_CAUSE = "injected: read-only volume";

  private RID rid;

  @BeforeEach
  void createOneRecord() {
    database.getSchema().createDocumentType(TYPE).createProperty("id", Type.INTEGER);
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument(TYPE);
      doc.set("id", 1);
      doc.save();
      rid = doc.getIdentity();
    });
    assertThat(rid).isNotNull();
  }

  @AfterEach
  void clearFaultInjection() {
    PageManager.setPageReadFaultInjector(null);
  }

  /**
   * The direct {@code Bucket} entry point: what {@code GraphEngine}, {@code BucketIterator} and
   * {@code RestoreEdgeStatement} call.
   */
  @Test
  void theCauseOfAFailedExistenceCheckSurvivesOnTheBucketApi() {
    final Bucket bucket = database.getSchema().getBucketById(rid.getBucketId());

    database.begin();
    try {
      failEveryReadOfTheRecordsBucket();

      assertThatThrownBy(() -> bucket.existsRecord(rid))
          .as("an existence check that failed on I/O must say WHY, not only that it failed")
          .isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining(rid.toString())
          .hasCauseInstanceOf(IOException.class)
          .hasRootCauseMessage(INJECTED_CAUSE);
    } finally {
      PageManager.setPageReadFaultInjector(null);
      database.rollback();
    }
  }

  /**
   * The database-level entry point: {@code LocalDatabase.existsRecord} resolves the bucket and delegates, so
   * everything reaching the check through {@code Database} - SQL's {@code FetchFromRidsStep}, the Gremlin
   * deleted-element filters, the server and HA database wrappers - lands on the same catch.
   */
  @Test
  void theCauseSurvivesThroughTheDatabaseLevelEntryPoint() {
    database.begin();
    try {
      failEveryReadOfTheRecordsBucket();

      assertThatThrownBy(() -> database.existsRecord(rid))
          .isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining(rid.toString())
          .hasCauseInstanceOf(IOException.class)
          .hasRootCauseMessage(INJECTED_CAUSE);
    } finally {
      PageManager.setPageReadFaultInjector(null);
      database.rollback();
    }
  }

  /**
   * The fixture has to be able to fail for the right reason: with no fault injected the very same call answers
   * {@code true}, so a green run above is the chained cause and not a record that was never readable.
   */
  @Test
  void theSameCheckSucceedsWhenTheDiskIsHealthy() {
    assertThat(database.getSchema().getBucketById(rid.getBucketId()).existsRecord(rid)).isTrue();
    assertThat(database.existsRecord(rid)).isTrue();
  }

  private void failEveryReadOfTheRecordsBucket() {
    final int bucketId = rid.getBucketId();
    PageManager.setPageReadFaultInjector(pageId -> {
      if (pageId.getFileId() == bucketId)
        throw new IOException(INJECTED_CAUSE);
    });
  }
}
