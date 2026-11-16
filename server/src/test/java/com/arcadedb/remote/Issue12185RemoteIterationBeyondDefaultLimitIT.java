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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12185: the driver's own whole-result reads (iterateType, iterateBucket) and the streaming API stopped at the
 * server's default row limit, while countType() reported every record. The server cap is lowered to 50 so 120 records
 * cross it.
 */
class Issue12185RemoteIterationBeyondDefaultLimitIT extends BaseGraphServerTest {
  private static final int    CAP       = 50;
  private static final int    RECORDS   = 120;
  private static final String TYPE_NAME = "Item12185";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT, CAP);
  }

  @BeforeEach
  void createRecords() {
    final Database database = getServerDatabase(0, getDatabaseName());
    // ONE BUCKET, SO iterateBucket() ON IT COVERS THE WHOLE TYPE
    database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE_NAME).set("i", i).save();
    });
  }

  @Test
  void iterateTypeYieldsEveryRecord() {
    try (final RemoteDatabase remote = newRemoteDatabase()) {
      assertThat(remote.countType(TYPE_NAME, true)).isEqualTo(RECORDS);
      assertThat(count(remote.iterateType(TYPE_NAME, true))).isEqualTo(RECORDS);
      assertThat(count(remote.iterateType(TYPE_NAME, false))).isEqualTo(RECORDS);
    }
  }

  @Test
  void iterateBucketYieldsEveryRecord() {
    try (final RemoteDatabase remote = newRemoteDatabase()) {
      final String bucket = remote.getSchema().getType(TYPE_NAME).getBuckets(false).getFirst().getName();
      assertThat(remote.countBucket(bucket)).isEqualTo(RECORDS);
      assertThat(count(remote.iterateBucket(bucket))).isEqualTo(RECORDS);
    }
  }

  @Test
  void iterationIgnoresTheApplicationRowCap() {
    // setMaxResultRows() caps the commands the application writes; an iteration is "every record of the type"
    try (final RemoteDatabase remote = newRemoteDatabase()) {
      remote.setMaxResultRows(10);
      assertThat(count(remote.iterateType(TYPE_NAME, true))).isEqualTo(RECORDS);
    }
  }

  @Test
  void queryStreamYieldsEveryRow() {
    try (final RemoteDatabase remote = newRemoteDatabase()) {
      try (final ResultSet rs = remote.queryStream("sql", "SELECT i FROM " + TYPE_NAME, Map.of())) {
        assertThat(rs.stream().count()).isEqualTo(RECORDS);
      }
      try (final ResultSet rs = remote.commandStream("sql", "SELECT i FROM " + TYPE_NAME, Map.of())) {
        assertThat(rs.stream().count()).isEqualTo(RECORDS);
      }
    }
  }

  @Test
  void queryStreamStillHonorsTheApplicationRowCap() {
    try (final RemoteDatabase remote = newRemoteDatabase()) {
      remote.setMaxResultRows(30);
      try (final ResultSet rs = remote.queryStream("sql", "SELECT i FROM " + TYPE_NAME, Map.of())) {
        assertThat(rs.stream().count()).isEqualTo(30);
      }
    }
  }

  @Test
  void bufferedQueryKeepsTheServerDefaultCap() {
    // Unchanged on purpose: the buffered query() is capped by the server default and warns (issue #5711)
    try (final RemoteDatabase remote = newRemoteDatabase()) {
      try (final ResultSet rs = remote.query("sql", "SELECT i FROM " + TYPE_NAME)) {
        assertThat(rs.stream().count()).isEqualTo(CAP);
      }
    }
  }

  private static long count(final Iterator<Record> iterator) {
    long n = 0;
    while (iterator.hasNext()) {
      iterator.next();
      n++;
    }
    return n;
  }

  private RemoteDatabase newRemoteDatabase() {
    return new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }
}
