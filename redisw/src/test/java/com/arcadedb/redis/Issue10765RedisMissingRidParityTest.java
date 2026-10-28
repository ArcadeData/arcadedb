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
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10765: a RID that names no record is "not there" for every command that takes one, on both Redis surfaces. HGET and
 * HEXISTS on the RESP wire answered {@code ERR Record ... not found} and HGET on the {@code redis} query language threw, while
 * HMGET answered nil and the query language's HEXISTS answered 0.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10765RedisMissingRidParityTest extends BaseRedisServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @Test
  void respWireAnswersAMissingRidAsNotThere() {
    final String[] rids = createItem();
    final String existing = rids[0];
    final String missing = rids[1];

    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);

      assertThat(jedis.hget(getDatabaseName(), missing)).isNull();
      assertThat(jedis.hexists(getDatabaseName(), missing)).isFalse();
      assertThat(jedis.hmget(getDatabaseName(), missing)).containsExactly((String) null);
      assertThat(jedis.hdel(getDatabaseName(), missing)).isZero();

      // the existing record is still answered, so the "not there" above is not a lookup that never finds anything
      assertThat(jedis.hget(getDatabaseName(), existing)).contains("\"n\":1");
      assertThat(jedis.hexists(getDatabaseName(), existing)).isTrue();

      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  void queryLanguageAnswersAMissingRidAsNotThere() {
    final String[] rids = createItem();
    final String existing = rids[0];
    final String missing = rids[1];
    final Database database = getServerDatabase(0, getDatabaseName());

    assertThat(value(database, "HEXISTS " + missing)).isEqualTo(0);
    assertThat(value(database, "HEXISTS " + existing)).isEqualTo(1);

    try (final ResultSet rs = database.command("redis", "HGET " + missing)) {
      assertThat(rs.hasNext()).as("HGET of a missing RID answers no record, as the RESP wire answers nil").isFalse();
    }
    try (final ResultSet rs = database.command("redis", "HGET " + existing)) {
      assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(1);
    }

    final List<?> multi = (List<?>) value(database, "HMGET " + missing + " " + existing);
    assertThat(multi).hasSize(2);
    assertThat(multi.get(0)).isNull();
    assertThat(multi.get(1)).isNotNull();

    assertThat(value(database, "HDEL " + missing)).isEqualTo(0);
  }

  /**
   * The structural guarantee: a RID is resolved in ONE place, {@link RedisRecords#lookupByRid}, so a command added later cannot
   * call {@code lookupByRID} directly and answer a missing record with an error where its siblings answer nil - which is how HGET
   * and HEXISTS drifted from HMGET after #9055 fixed only HMGET.
   */
  @Test
  void everyRidLookupGoesThroughTheSharedHelper() throws IOException {
    final Path sources = Path.of("src/main/java/com/arcadedb/redis");
    assertThat(sources).isDirectory();

    final List<String> offenders = new ArrayList<>();
    try (final Stream<Path> files = Files.walk(sources)) {
      for (final Path file : files.filter(f -> f.toString().endsWith(".java")).toList()) {
        if (file.getFileName().toString().equals("RedisRecords.java"))
          continue;
        if (Files.readString(file).contains(".lookupByRID("))
          offenders.add(file.toString());
      }
    }
    assertThat(offenders).as("RID lookups outside RedisRecords.lookupByRid").isEmpty();
  }

  private String[] createItem() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateDocumentType("Issue10765Item");
    final String[] existing = new String[1];
    database.transaction(() -> existing[0] = database.newDocument("Issue10765Item").set("n", 1).save().getIdentity().toString());
    final String missing = existing[0].substring(0, existing[0].indexOf(':') + 1) + "99999";
    return new String[] { existing[0], missing };
  }

  private static Object value(final Database database, final String command) {
    try (final ResultSet rs = database.command("redis", command)) {
      final Result result = rs.next();
      return result.getProperty("value");
    }
  }
}
