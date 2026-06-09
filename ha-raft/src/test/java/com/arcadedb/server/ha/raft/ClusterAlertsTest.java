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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ClusterAlerts} single-bucket type detection: the core diagnostic behind the
 * cluster write-retry alert. Runs against a plain embedded database (no Raft cluster needed).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ClusterAlertsTest {
  private static final String   DB_PATH = "./target/databases/test-cluster-alerts";
  private              Database db;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    db = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void flagsTypesWithASingleBucket() {
    db.command("sql", "CREATE VERTEX TYPE SingleBucketV BUCKETS 1");
    db.command("sql", "CREATE DOCUMENT TYPE SingleBucketD BUCKETS 1");

    final List<String> flagged = ClusterAlerts.findSingleBucketTypes(db);

    assertThat(flagged).contains("SingleBucketV", "SingleBucketD");
  }

  @Test
  void doesNotFlagTypesWithMultipleBuckets() {
    db.command("sql", "CREATE VERTEX TYPE MultiBucketV BUCKETS 8");
    db.command("sql", "CREATE EDGE TYPE MultiBucketE BUCKETS 8");

    final List<String> flagged = ClusterAlerts.findSingleBucketTypes(db);

    assertThat(flagged).doesNotContain("MultiBucketV", "MultiBucketE");
  }

  @Test
  void growingBucketCountClearsTheFlag() {
    db.command("sql", "CREATE VERTEX TYPE Account BUCKETS 1");
    assertThat(ClusterAlerts.findSingleBucketTypes(db)).contains("Account");

    // Add a second bucket to the live type: the contention diagnostic must clear.
    db.command("sql", "ALTER TYPE Account BUCKET +Account_extra1");

    assertThat(ClusterAlerts.findSingleBucketTypes(db)).doesNotContain("Account");
  }

  @Test
  void resultIsSortedForDeterministicOutput() {
    db.command("sql", "CREATE VERTEX TYPE Zebra BUCKETS 1");
    db.command("sql", "CREATE VERTEX TYPE Apple BUCKETS 1");
    db.command("sql", "CREATE VERTEX TYPE Mango BUCKETS 1");

    final List<String> flagged = ClusterAlerts.findSingleBucketTypes(db);

    assertThat(flagged).isSorted();
  }
}
