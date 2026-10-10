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
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.query.RunningQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9689 over the Redis protocol: a command is registered in the server's running statements while it runs - so
 * {@code list queries} and {@code SHOW TRANSACTIONS} list it - and a terminate stops it without its writes. The entry is
 * captured on the command's own thread, by a record listener, so nothing depends on timing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9689RedisRunningQueryTest extends BaseRedisServerTest {
  private static final String TYPE = "Item9689";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aCommandIsListedWhileItRunsAndATerminateStopsIt() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".id INTEGER");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (id) UNIQUE");

    final AtomicReference<RunningQuery> seen = new AtomicReference<>();
    final AtomicReference<String> listedText = new AtomicReference<>();
    final AtomicReference<Boolean> terminate = new AtomicReference<>(false);
    final BeforeRecordCreateListener listener = record -> {
      final RunningQuery current = RunningQuery.current();
      seen.set(current);
      if (current != null) {
        listedText.set(current.getText());
        if (terminate.get())
          current.terminate("root");
      }
      return true;
    };
    database.getSchema().getType(TYPE).getEvents().registerListener(listener);
    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);

      jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), TYPE, "{\"id\":1}");
      final RunningQuery entry = seen.get();
      assertThat(entry).as("the command must run under its registry entry").isNotNull();
      assertThat(entry.getProtocol()).isEqualTo("redis");
      assertThat(entry.getUser()).isEqualTo("root");
      assertThat(entry.getLanguage()).isEqualTo("redis");
      assertThat(listedText.get()).isEqualTo("HSET " + getDatabaseName() + " " + TYPE + " {\"id\":1}");
      assertThat(entry.isEnded()).as("the entry goes once the command is over").isTrue();

      // Terminated while it runs: the write does not stand
      terminate.set(true);
      assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), TYPE, "{\"id\":2}"))
          .isInstanceOf(JedisDataException.class).hasMessageContaining("terminated");
      assertThat(seen.get().getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
      assertThat(database.countType(TYPE, false)).isEqualTo(1);

      // The connection serves the next command
      terminate.set(false);
      jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), TYPE, "{\"id\":3}");
      assertThat(database.countType(TYPE, false)).isEqualTo(2);
    } finally {
      database.getSchema().getType(TYPE).getEvents().unregisterListener(listener);
    }
    assertThat(getServer(0).getRunningQueries().size()).isZero();
  }
}
