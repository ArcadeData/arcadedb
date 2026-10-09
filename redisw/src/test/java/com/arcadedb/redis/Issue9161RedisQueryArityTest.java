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

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9161: the RESP wire path refuses a wrong argument count (#9059) but {@code database.command("redis", ...)} kept a
 * lenient syntax ({@code INCR k 5} used 5 as the amount, {@code INCRBY k} defaulted it to 1, a surplus argument of GET/SET was
 * ignored). Both surfaces now share the one arity table, and the query language HDEL answers like the wire one: a RID is parsed
 * up front, only a missing record is skipped, and every other failure reaches the caller.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9161RedisQueryArityTest extends BaseRedisServerTest {

  @Test
  void aWrongArgumentCountIsRefusedLikeOnTheWire() {
    final Database database = getServerDatabase(0, getDatabaseName());

    refused(database, "INCR issue9161a 5", "incr");
    refused(database, "INCR", "incr");
    refused(database, "DECR issue9161a 5", "decr");
    refused(database, "INCRBY issue9161a", "incrby");
    refused(database, "INCRBY issue9161a 1 2", "incrby");
    refused(database, "DECRBY issue9161a", "decrby");
    refused(database, "INCRBYFLOAT issue9161a", "incrbyfloat");
    refused(database, "GET", "get");
    refused(database, "GET a b", "get");
    refused(database, "GETDEL", "getdel");
    refused(database, "GETDEL a b", "getdel");
    refused(database, "SET issue9161a", "set");
    refused(database, "EXISTS", "exists");

    // nothing of the refused commands ran
    try (final ResultSet rs = database.command("redis", "GET issue9161a")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void theDocumentedFormsStillRun() {
    final Database database = getServerDatabase(0, getDatabaseName());

    assertThat(value(database, "INCR issue9161b")).isEqualTo(1L);
    assertThat(value(database, "INCRBY issue9161b 4")).isEqualTo(5L);
    assertThat(value(database, "DECR issue9161b")).isEqualTo(4L);
    assertThat(value(database, "DECRBY issue9161b 3")).isEqualTo(1L);
    assertThat(value(database, "SET issue9161c a b c")).isEqualTo("OK");
    assertThat(value(database, "GET issue9161c")).isEqualTo("a");
    assertThat(value(database, "EXISTS issue9161b issue9161c nothing")).isEqualTo(2);
  }

  @Test
  void aMultiBlockRefusesAWrongArgumentCountAndRunsNothing() {
    final Database database = getServerDatabase(0, getDatabaseName());

    assertThatThrownBy(() -> database.command("redis", "MULTI\nSET issue9161d 1\nINCR issue9161d 5\nEXEC").close())
        .hasMessageContaining("wrong number of arguments for 'incr' command");

    try (final ResultSet rs = database.command("redis", "GET issue9161d")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void hDelByRidSkipsOnlyAMissingRecord() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Doc9161");

    final RID[] rids = new RID[2];
    database.transaction(() -> {
      for (int i = 0; i < rids.length; i++) {
        final MutableDocument doc = database.newDocument("Doc9161").set("id", i);
        doc.save();
        rids[i] = doc.getIdentity();
      }
      // a RID that existed and is gone: the bucket id is valid, the position holds nothing
      database.lookupByRID(rids[1], true).delete();
    });

    assertThat(value(database, "HDEL " + rids[0] + " " + rids[1])).isEqualTo(1);
    assertThat(database.query("sql", "SELECT count(*) AS c FROM Doc9161").next().<Long>getProperty("c")).isZero();
  }

  @Test
  void hDelRefusesAMalformedRidBeforeDeletingAnything() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Doc9161b");
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc9161b").set("id", 1);
      doc.save();
      rid[0] = doc.getIdentity();
    });

    assertThatThrownBy(() -> database.command("redis", "HDEL " + rid[0] + " #abc").close()).hasMessageContaining("invalid RID");
    assertThatThrownBy(() -> database.command("redis", "HDEL " + rid[0] + " notarid").close()).hasMessageContaining("RID");
    assertThat(database.query("sql", "SELECT count(*) AS c FROM Doc9161b").next().<Long>getProperty("c")).isEqualTo(1L);
  }

  @Test
  void hDelDoesNotSwallowAFailureOfTheDelete() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Doc9161c");
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc9161c").set("id", 1);
      doc.save();
      rid[0] = doc.getIdentity();
    });

    final BeforeRecordDeleteListener veto = record -> {
      throw new ArcadeDBException("delete vetoed");
    };
    database.getSchema().getType("Doc9161c").getEvents().registerListener(veto);
    try {
      assertThatThrownBy(() -> database.command("redis", "HDEL " + rid[0]).close()).hasStackTraceContaining("delete vetoed");
    } finally {
      database.getSchema().getType("Doc9161c").getEvents().unregisterListener(veto);
    }
    assertThat(database.query("sql", "SELECT count(*) AS c FROM Doc9161c").next().<Long>getProperty("c")).isEqualTo(1L);
  }

  @Test
  void hDelNeedsAKey() {
    final Database database = getServerDatabase(0, getDatabaseName());
    assertThatThrownBy(() -> database.command("redis", "HDEL").close()).hasMessageContaining("wrong number of arguments for 'hdel' command");
    assertThatThrownBy(() -> database.command("redis", "HDEL Doc[id]").close())
        .hasMessageContaining("wrong number of arguments for 'hdel' command");
  }

  @Test
  void hDelOnAnUnknownIndexIsAnErrorAndDeletesNothing() {
    final Database database = getServerDatabase(0, getDatabaseName());
    assertThatThrownBy(() -> database.command("redis", "HDEL NoSuchType[id] 1").close()).isInstanceOf(ArcadeDBException.class);
  }

  private static void refused(final Database database, final String command, final String name) {
    assertThatThrownBy(() -> database.command("redis", command).close()).as(command)
        .hasMessageContaining("wrong number of arguments for '" + name + "' command");
  }

  private static Object value(final Database database, final String command) {
    try (final ResultSet rs = database.command("redis", command)) {
      final Object value = rs.next().getProperty("value");
      return value instanceof Number number && !(value instanceof Long) ? number.intValue() : value;
    }
  }
}
