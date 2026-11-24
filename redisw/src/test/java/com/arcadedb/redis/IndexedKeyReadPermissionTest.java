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

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A user who may not read a type is refused by a walk of its buckets, and by a read or a delete by the key of an index of the type
 * ({@code HGET}, {@code HMGET}, {@code HEXISTS}, {@code HDEL}, on the wire and in the {@code redis} query language), with the same
 * refusal for any key. The read permission of the type and of its subtypes is asked before the index is read.
 * <p>
 * Every command is run with a key the denied type holds and with one it does not.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class IndexedKeyReadPermissionTest {
  private static final String PATH = "target/databases/IndexedKeyReadPermissionTest";

  private DatabaseFactory  factory;
  private DatabaseInternal database;

  @BeforeEach
  void setUp() {
    // without a security manager the engine asks no file permission at all, whatever user is bound
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = (DatabaseInternal) factory.create();

    for (final String name : new String[] { "Open", "Secret", "Base" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + name + " BUCKETS 2");
      database.command("sql", "CREATE PROPERTY " + name + ".id INTEGER");
      database.command("sql", "CREATE INDEX ON " + name + " (id) UNIQUE");
    }
    database.command("sql", "CREATE DOCUMENT TYPE Sub EXTENDS Base BUCKETS 2");
    database.command("sql", "CREATE DOCUMENT TYPE Pair BUCKETS 2");
    database.command("sql", "CREATE PROPERTY Pair.a INTEGER");
    database.command("sql", "CREATE PROPERTY Pair.b INTEGER");
    database.command("sql", "CREATE INDEX ON Pair (a, b) UNIQUE");

    database.transaction(() -> {
      for (int i = 1; i <= 5; i++) {
        database.newDocument("Open").set("id", i).save();
        database.newDocument("Secret").set("id", i).save();
        database.newDocument("Pair").set("a", i).set("b", i + 10).save();
      }
      // the keys 1 to 5 of the index of Base are held by Base itself, 6 to 10 by its subtype
      for (int i = 1; i <= 5; i++)
        database.newDocument("Base").set("id", i).save();
      for (int i = 6; i <= 10; i++)
        database.newDocument("Sub").set("id", i).save();
    });
  }

  @AfterEach
  void tearDown() {
    bindUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aTypeTheUserMayNotReadRefusesTheLookupWhateverTheKeyIs() {
    deny("Secret");

    for (final String key : new String[] { "3", "100", "\"3\"" })
      assertThat(refused(() -> RedisIndexKeys.lookup(database, "Secret[id]", key))).as("lookup of " + key).isTrue();
  }

  @Test
  void aTypeTheUserMayNotReadRefusesEveryCommandWhateverTheKeyIs() {
    deny("Secret");

    final List<String> notRefused = new ArrayList<>();
    for (final String key : new String[] { "3", "100" }) {
      for (final String command : new String[] { "HGET Secret[id] " + key, "HEXISTS Secret[id] " + key,
          "HMGET Secret[id] " + key + " " + key })
        if (!refused(() -> drain(command)))
          notRefused.add(command);
      if (!refused(() -> database.command("redis", "HDEL Secret[id] " + key).close()))
        notRefused.add("HDEL Secret[id] " + key);
      if (!refused(() -> deleteByKey("Secret[id]", key)))
        notRefused.add("deleteByKey " + key);
    }
    assertThat(notRefused).as("a denied type refuses the command for any key").isEmpty();

    // nothing was deleted behind the refusals
    bindUser(null);
    assertThat(database.countType("Secret", false)).isEqualTo(5L);
  }

  @Test
  void aCompositeKeyOfATypeTheUserMayNotReadIsRefusedWhateverTheKeyIs() {
    deny("Pair");

    for (final String key : new String[] { "[3,13]", "[3,99]", "[99,99]" })
      assertThat(refused(() -> RedisIndexKeys.lookup(database, "Pair[a,b]", key))).as("lookup of " + key).isTrue();
  }

  @Test
  void aSubTypeTheUserMayNotReadRefusesTheLookupInTheIndexOfItsSuperTypeWhateverTheKeyIs() {
    deny("Sub");

    // the index of Base holds the records of Sub, and the walk of Base opens the buckets of Sub: a key only Base holds is refused too
    for (final String key : new String[] { "3", "8", "100" })
      assertThat(refused(() -> RedisIndexKeys.lookup(database, "Base[id]", key))).as("lookup of " + key).isTrue();
  }

  @Test
  void aTypeTheUserMayReadIsAnsweredWhileAnotherIsDenied() {
    deny("Secret");

    assertThat(RedisIndexKeys.lookup(database, "Open[id]", "3")).isNotNull();
    assertThat(RedisIndexKeys.lookup(database, "Open[id]", "100")).isNull();
    assertThat(RedisIndexKeys.lookup(database, "Base[id]", "3")).isNotNull();
    assertThat(RedisIndexKeys.lookup(database, "Base[id]", "8")).isNotNull();
    assertThat(RedisIndexKeys.lookup(database, "Base[id]", "100")).isNull();
    assertThat(drain("HGET Open[id] 3")).isEqualTo(1);
    assertThat(drain("HGET Open[id] 100")).isZero();
    assertThat(drain("HEXISTS Open[id] 3")).isEqualTo(1);
    assertThat(deleteByKey("Open[id]", "100")).isFalse();
  }

  @Test
  void aKeyOfATypeTheUserMayReadIsDeleted() {
    deny("Secret");

    assertThat(deleteByKey("Open[id]", "3")).isTrue();
    assertThat(RedisIndexKeys.lookup(database, "Open[id]", "3")).isNull();
  }

  @Test
  void aUserWhoMayReadEveryTypeIsAnswered() {
    deny();

    assertThat(RedisIndexKeys.lookup(database, "Secret[id]", "3")).isNotNull();
    assertThat(RedisIndexKeys.lookup(database, "Secret[id]", "100")).isNull();
    assertThat(RedisIndexKeys.lookup(database, "Pair[a,b]", "[3,13]")).isNotNull();
    assertThat(RedisIndexKeys.lookup(database, "Base[id]", "8")).isNotNull();
    assertThat(drain("HGET Secret[id] 3")).isEqualTo(1);
  }

  @Test
  void aLookupWithNoBoundUserIsAnswered() {
    // an embedded call has no user bound: nothing is denied
    bindUser(null);

    assertThat(RedisIndexKeys.lookup(database, "Secret[id]", "3")).isNotNull();
    assertThat(RedisIndexKeys.lookup(database, "Secret[id]", "100")).isNull();
  }

  @Test
  void anIndexOfNoTypeIsAnswered() {
    final RID record;
    try (final ResultSet rs = database.query("sql", "SELECT FROM Open WHERE id = 3")) {
      record = rs.next().getIdentity().get();
    }
    database.transaction(() -> {
      final Index manual = database.getSchema().buildManualIndex("manualKeys", new Type[] { Type.STRING })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
      manual.put(new Object[] { "k" }, new RID[] { record });
    });
    deny("Secret");

    // an index that belongs to no type has none to ask
    assertThat(RedisIndexKeys.lookup(database, "manualKeys", "k")).isNotNull();
    assertThat(RedisIndexKeys.lookup(database, "manualKeys", "absent")).isNull();
  }

  private static boolean refused(final Runnable command) {
    Throwable failure = catchThrowable(command::run);
    while (failure != null) {
      if (failure instanceof SecurityException)
        return true;
      failure = failure.getCause();
    }
    return false;
  }

  /** Whether the delete by key removed a record, in the transaction the wire and the query language open for it. */
  private boolean deleteByKey(final String indexName, final String key) {
    final boolean[] removed = new boolean[1];
    database.transaction(() -> {
      removed[0] = RedisRecords.deleteByKey(database, indexName, key);
    });
    return removed[0];
  }

  /** The number of rows of the query-language command. */
  private long drain(final String command) {
    long rows = 0;
    try (final ResultSet rs = database.query("redis", command)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  private void bindUser(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(user);
  }

  /** Denies the read of every record of the types, and grants everything else. */
  private void deny(final String... typeNames) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : typeNames)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    bindUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
