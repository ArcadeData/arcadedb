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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for Issue #2814: Filtering with index - https://github.com/ArcadeData/arcadedb/issues/2814
 * Tests that parameterized multi-field UPDATE statements correctly update indexes.
 *
 * The bug manifests when:
 * 1. A non-unique index exists on a property (e.g., status)
 * 2. A parameterized UPDATE statement updates multiple fields including the indexed field
 * 3. After the update, WHERE clauses using the indexed field return incorrect results
 */
public class Issue2814FilteringWithIndexTest extends TestHelper {

  private String parentRid;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      // Create two types with LINK and INDEX like in the issue
      database.command("sql", "CREATE DOCUMENT TYPE Parent");
      database.command("sql", "CREATE DOCUMENT TYPE Child");
      database.command("sql", "CREATE PROPERTY Child.uid STRING");
      database.command("sql", "CREATE PROPERTY Child.status STRING (default 'synced')");
      database.command("sql", "CREATE PROPERTY Child.version INTEGER (default 1)");
      database.command("sql", "CREATE PROPERTY Child.parent LINK OF Parent");

      // Create non-unique index on status field
      database.command("sql", "CREATE INDEX ON Child (status) NOTUNIQUE");

      // Create parent
      ResultSet result = database.command("sql", "INSERT INTO Parent SET name = 'p1' RETURN @this");
      parentRid = result.next().getIdentity().get().toString();

      // Insert 3 children WITHOUT explicit status (use default 'synced')
      database.command("sql", "INSERT INTO Child SET uid = 'c1', parent = " + parentRid);
      database.command("sql", "INSERT INTO Child SET uid = 'c2', parent = " + parentRid);
      database.command("sql", "INSERT INTO Child SET uid = 'c3', parent = " + parentRid);

      // Mark c1 and c2 as pending
      database.command("sql", "UPDATE Child SET status = 'pending' WHERE uid = 'c1'");
      database.command("sql", "UPDATE Child SET status = 'pending' WHERE uid = 'c2'");
    });
  }

  @Test
  void filteringBeforeParameterizedUpdate() {
    // Verify initial state - should find 2 pending children
    database.transaction(() -> {
      ResultSet pending = database.query("sql", "SELECT uid, status FROM Child WHERE status = 'pending'");
      List<Result> pendingList = pending.stream().toList();

      assertThat(pendingList).hasSize(2);

      List<String> uids = pendingList.stream()
          .map(r -> r.<String>getProperty("uid"))
          .sorted()
          .toList();
      assertThat(uids).containsExactly("c1", "c2");
    });
  }

  @Test
  void filteringAfterParameterizedMultiFieldUpdate() {
    // This is the main test for the bug: parameterized multi-field UPDATE breaks index
    database.transaction(() -> {
      // Update c1 with parameterized multi-field UPDATE (including version field)
      Map<String, Object> params = new HashMap<>();
      params.put("uid", "c1");
      params.put("version", 2);
      params.put("status", "synced");

      database.command("sql", "UPDATE Child SET version = :version, status = :status WHERE uid = :uid", params);
    });

    // BUG TEST: After parameterized update, WHERE status='pending' should find c2
    database.transaction(() -> {
      ResultSet pending = database.query("sql", "SELECT uid, status FROM Child WHERE status = 'pending'");
      List<Result> pendingList = pending.stream().toList();

      // Should find exactly 1 pending record (c2)
      assertThat(pendingList).hasSize(1);
      assertThat(pendingList.get(0).<String>getProperty("uid")).isEqualTo("c2");
    });

    // BUG TEST: WHERE status='synced' should find c1 and c3
    database.transaction(() -> {
      ResultSet synced = database.query("sql", "SELECT uid, status FROM Child WHERE status = 'synced'");
      List<Result> syncedList = synced.stream().toList();

      // Should find exactly 2 synced records (c1, c3)
      assertThat(syncedList).hasSize(2);

      List<String> uids = syncedList.stream()
          .map(r -> r.<String>getProperty("uid"))
          .sorted()
          .toList();
      assertThat(uids).containsExactly("c1", "c3");
    });

    // Verify all 3 children still exist with correct statuses
    database.transaction(() -> {
      ResultSet all = database.query("sql", "SELECT uid, status, version FROM Child ORDER BY uid");
      List<Result> allList = all.stream().toList();

      assertThat(allList).hasSize(3);

      // c1 should be synced with version 2
      Result c1 = allList.stream()
          .filter(r -> "c1".equals(r.getProperty("uid")))
          .findFirst()
          .orElseThrow();
      assertThat(c1.<String>getProperty("status")).isEqualTo("synced");
      assertThat(c1.<Integer>getProperty("version")).isEqualTo(2);

      // c2 should still be pending
      Result c2 = allList.stream()
          .filter(r -> "c2".equals(r.getProperty("uid")))
          .findFirst()
          .orElseThrow();
      assertThat(c2.<String>getProperty("status")).isEqualTo("pending");

      // c3 should be synced (was never changed)
      Result c3 = allList.stream()
          .filter(r -> "c3".equals(r.getProperty("uid")))
          .findFirst()
          .orElseThrow();
      assertThat(c3.<String>getProperty("status")).isEqualTo("synced");
    });
  }

  @Test
  void filteringAfterParameterizedSingleFieldUpdate() {
    // Test that single-field parameterized UPDATE works correctly (comparison test)
    database.transaction(() -> {
      // Update c2 with parameterized single-field UPDATE (only status)
      Map<String, Object> params = new HashMap<>();
      params.put("uid", "c2");
      params.put("status", "synced");

      database.command("sql", "UPDATE Child SET status = :status WHERE uid = :uid", params);
    });

    // Verify filtering works correctly after single-field parameterized update
    database.transaction(() -> {
      ResultSet pending = database.query("sql", "SELECT uid, status FROM Child WHERE status = 'pending'");
      List<Result> pendingList = pending.stream().toList();

      // After updating c2, only c1 should still be pending
      assertThat(pendingList).hasSize(1);
      assertThat(pendingList.get(0).<String>getProperty("uid")).isEqualTo("c1");
    });

    database.transaction(() -> {
      ResultSet synced = database.query("sql", "SELECT uid, status FROM Child WHERE status = 'synced'");
      List<Result> syncedList = synced.stream().toList();

      // Should find c2 and c3 as synced
      assertThat(syncedList).hasSize(2);

      List<String> uids = syncedList.stream()
          .map(r -> r.<String>getProperty("uid"))
          .sorted()
          .toList();
      assertThat(uids).containsExactly("c2", "c3");
    });
  }
}
