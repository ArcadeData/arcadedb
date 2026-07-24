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
package com.arcadedb.server.security;

import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A security refresh must never make an already granted permission transiently disappear. The database
 * access map is rebuilt on every schema change (createType() calls updateSecurity()), so a reader racing
 * with the rebuild would otherwise observe the zeroed map and be denied, surfacing as a spurious
 * "User 'root' is not allowed to update schema".
 */
class ServerSecurityDatabaseUserConcurrencyTest {

  @Test
  void permissionsNeverTransientlyDisappearDuringRefresh() throws Exception {
    final JSONObject groups = new JSONObject().put("admin",
        new JSONObject().put("access", new JSONArray(new String[] { "updateSchema", "updateSecurity", "updateDatabaseSettings" })));

    final ServerSecurityDatabaseUser user = new ServerSecurityDatabaseUser("db", "root", new String[] { "admin" });
    user.updateDatabaseConfiguration(groups);
    assertThat(user.requestAccessOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();

    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicLong denials = new AtomicLong();
    final CountDownLatch done = new CountDownLatch(2);

    final Thread refresher = new Thread(() -> {
      try {
        while (!stop.get())
          user.updateDatabaseConfiguration(groups);
      } finally {
        done.countDown();
      }
    });

    final Thread reader = new Thread(() -> {
      try {
        for (int i = 0; i < 2_000_000 && !stop.get(); ++i)
          if (!user.requestAccessOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA))
            denials.incrementAndGet();
      } finally {
        done.countDown();
      }
    });

    refresher.start();
    reader.start();
    reader.join();
    stop.set(true);
    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    refresher.join();

    assertThat(denials.get()).as("permission denied while the security map was being refreshed").isZero();
  }

  /**
   * updateDatabaseConfiguration() picks the most restrictive limit across the groups of a single
   * configuration. It must recompute from scratch on every call, otherwise a refresh that relaxes or drops
   * a limit is ignored and the user stays pinned to the tightest value ever configured.
   */
  @Test
  void refreshingTheConfigurationCanRelaxLimits() {
    final ServerSecurityDatabaseUser user = new ServerSecurityDatabaseUser("db", "root", new String[] { "admin" });

    user.updateDatabaseConfiguration(groupsWithLimits(10, 100));
    assertThat(user.getResultSetLimit()).isEqualTo(10);
    assertThat(user.getReadTimeout()).isEqualTo(100);

    user.updateDatabaseConfiguration(groupsWithLimits(50, 500));
    assertThat(user.getResultSetLimit()).as("a relaxed result set limit is picked up").isEqualTo(50);
    assertThat(user.getReadTimeout()).as("a relaxed read timeout is picked up").isEqualTo(500);

    // Dropping the limits entirely restores "unlimited"
    user.updateDatabaseConfiguration(new JSONObject().put("admin", new JSONObject().put("access", new JSONArray())));
    assertThat(user.getResultSetLimit()).as("a removed result set limit goes back to unlimited").isEqualTo(-1);
    assertThat(user.getReadTimeout()).as("a removed read timeout goes back to unlimited").isEqualTo(-1);
  }

  private static JSONObject groupsWithLimits(final long resultSetLimit, final long readTimeout) {
    return new JSONObject().put("admin", new JSONObject().put("access", new JSONArray())
        .put("resultSetLimit", resultSetLimit)
        .put("readTimeout", readTimeout));
  }
}
