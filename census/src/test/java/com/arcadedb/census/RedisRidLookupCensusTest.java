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
package com.arcadedb.census;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10765: a RID that names no record is "not there" for every command that takes one, on both Redis surfaces. HGET and
 * HEXISTS answered an error where HMGET answered nil, because #9055 had fixed only HMGET: each command called
 * {@code lookupByRID} itself and answered a missing record its own way. A RID is now resolved in ONE place,
 * {@code RedisRecords.lookupByRid}, and the behaviour is pinned by {@code Issue10765RedisMissingRidParityTest}.
 * <p>
 * The census ranges over the authority (#11288): every call of {@code lookupByRID} on a {@code Database} (or anything that
 * implements it), or method reference to it, in the compiled classes of the {@code redisw} module ({@link CompiledClasses}). The
 * source-text census it replaces matched the spelling {@code .lookupByRID(} in the module's {@code redis} package.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RedisRidLookupCensusTest {
  private static final String MODULE   = "redisw";
  private static final String DATABASE = "com/arcadedb/database/Database";
  private static final String METHOD   = "lookupByRID";

  private static final Map<String, String> RESOLVES_THE_RID = Map.of(//
      "redis/RedisRecords#lookupByRid", "the one place a Redis command resolves a RID, answering a missing record as not there");

  @Test
  void everyRedisRidLookupGoesThroughTheSharedHelper() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.byModule().get(MODULE);
    assertThat(classes).as("the census must read the " + MODULE + " module").isNotNull();

    final Set<String> databases = CompiledClasses.implementationsOf(DATABASE, CompiledClasses.ofEveryModule());
    final Map<String, Set<String>> callSites = CompiledClasses.callSitesOf(classes,
        (owner, name, desc) -> databases.contains(owner) && METHOD.equals(name));
    assertThat(callSites).as("the census must reach the shared helper").containsKey("redis/RedisRecords#lookupByRid");

    assertThat(CompiledClasses.unreviewed(callSites, RESOLVES_THE_RID)).as(
        "a Redis command resolving a RID by itself answers a missing record its own way: use RedisRecords.lookupByRid (issue #10765)")
        .isEmpty();
  }
}
