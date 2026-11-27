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
package com.arcadedb.mongo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.FileUtils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The state a database wrapper keeps for the requests of a connection that stays open: a driver pool reuses its connections for days,
 * so whatever a request leaves behind on the wrapper must not grow with the number of requests. The wrapper is read by reflection,
 * because nothing of it is meant to be seen from outside: every map and collection it holds is counted, with the maps and collections
 * they hold in turn, before and after a series of requests on one open channel.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoConnectionStateTest {
  private static final String DB_PATH   = "./target/databases/MongoConnectionStateTest";
  private static final String TYPE_NAME = "Item";
  private static final int    REQUESTS  = 300;

  private Database               database;
  private MongoDBDatabaseWrapper wrapper;
  private Channel                channel;
  private int                    nextId;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    wrapper = new MongoDBDatabaseWrapper(database, null, null);
    channel = new EmbeddedChannel();
    nextId = 0;
  }

  @AfterEach
  void tearDown() {
    channel.close();
    if (database != null && database.isOpen())
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void theCountReachesTheStateOfTheWrapper() throws IllegalAccessException {
    final int before = retainedEntries(wrapper);

    insertOne();

    // the cache of the collection wrappers is a part of the state the count walks: if it stopped reaching it, the other tests would pass
    // on nothing
    assertThat(retainedEntries(wrapper)).isGreaterThan(before);
  }

  @Test
  void insertsOnOneOpenConnectionKeepTheStateOfTheWrapperConstant() throws IllegalAccessException {
    insertOne();
    final int afterTheFirst = retainedEntries(wrapper);

    for (int i = 0; i < REQUESTS; i++)
      insertOne();

    assertThat(retainedEntries(wrapper)).isEqualTo(afterTheFirst);
    assertThat(database.countType(TYPE_NAME, true)).isEqualTo(REQUESTS + 1);
  }

  @Test
  void queriesOnOneOpenConnectionKeepTheStateOfTheWrapperConstant() throws IllegalAccessException {
    insertOne();
    queryAll();
    final int afterTheFirst = retainedEntries(wrapper);

    for (int i = 0; i < REQUESTS; i++)
      queryAll();

    assertThat(retainedEntries(wrapper)).isEqualTo(afterTheFirst);
  }

  @Test
  void refusedInsertsOnOneOpenConnectionKeepTheStateOfTheWrapperConstant() throws IllegalAccessException {
    insertRefused();
    final int afterTheFirst = retainedEntries(wrapper);

    for (int i = 0; i < REQUESTS; i++) {
      final Document reply = insertRefused();
      assertThat(reply.get("writeErrors")).isNotNull();
    }

    assertThat(retainedEntries(wrapper)).isEqualTo(afterTheFirst);
  }

  @Test
  void theWrapperKeepsNothingPerConnectionThatAClosedConnectionWouldFree() throws IllegalAccessException {
    insertOne();
    queryAll();
    final int beforeTheClose = retainedEntries(wrapper);

    wrapper.handleClose(channel);

    assertThat(retainedEntries(wrapper)).isEqualTo(beforeTheClose);
  }

  @Test
  void anInsertWithoutAChannelIsAnswered() {
    final Document reply = wrapper.handleCommand(null, "insert", insertCommand(TYPE_NAME), null, null);

    assertThat(reply.get("n")).isEqualTo(1);
    assertThat(reply.get("writeErrors")).isNull();
  }

  private Document insertOne() {
    return wrapper.handleCommand(channel, "insert", insertCommand(TYPE_NAME), null, null);
  }

  /** An insert into a namespace the wrapper refuses, answered as a write error of the command. */
  private Document insertRefused() {
    return wrapper.handleCommand(channel, "insert", insertCommand("system.refused"), null, null);
  }

  private Document insertCommand(final String collection) {
    final int id = nextId++;
    return new Document("insert", collection).append("documents", List.of(new Document("_id", id).append("k", id)));
  }

  private void queryAll() {
    wrapper.handleQuery(new MongoQuery(channel, new MessageHeader(1, 0), database.getName() + "." + TYPE_NAME, 0, 0, new Document(), null));
  }

  /**
   * The entries of every map and collection the wrapper holds, with the ones of the maps and collections inside them: the number
   * a request must not make grow once the first has run.
   */
  private static int retainedEntries(final Object holder) throws IllegalAccessException {
    int total = 0;
    for (Class<?> type = holder.getClass(); type != null && type != Object.class; type = type.getSuperclass())
      for (final Field field : type.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()))
          continue;
        field.setAccessible(true);
        total += entries(field.get(holder), 0);
      }
    return total;
  }

  private static int entries(final Object value, final int depth) {
    if (depth > 4)
      return 0;
    int count = 0;
    if (value instanceof Map<?, ?> map) {
      count += map.size();
      for (final Object nested : map.values())
        count += entries(nested, depth + 1);
    } else if (value instanceof Collection<?> collection) {
      count += collection.size();
      for (final Object nested : collection)
        count += entries(nested, depth + 1);
    }
    return count;
  }
}
