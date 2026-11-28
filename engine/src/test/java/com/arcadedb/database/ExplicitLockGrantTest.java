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
package com.arcadedb.database;

import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * An explicit {@code LOCK TYPE} / {@code LOCK BUCKET} (and {@code acquireLock()}) takes the commit locks of the files a write to the
 * type touches, so it is asked of a user entitled to write the type: a create, an update or a delete grant on every bucket it
 * locks. The user below resolves permissions as the server does: a file belongs to the type that involves it, a type with no entry
 * has no grant, and a file no type involves (an index file) is default-allow.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ExplicitLockGrantTest {
  private static final String PATH = "target/databases/ExplicitLockGrantTest";

  private DatabaseFactory factory;
  private Database       database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    final Schema schema = database.getSchema();
    final DocumentType account = schema.createDocumentType("Account", 2);
    account.createProperty("code", Type.STRING);
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Account", "code");
    schema.createDocumentType("Ledger");
    schema.createDocumentType("Base");
    schema.createDocumentType("Derived").addSuperType("Base");
    schema.createVertexType("Person");
  }

  @AfterEach
  void tearDown() {
    if (database == null)
      return;
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    if (database.isTransactionActive())
      database.rollback();
    database.drop();
    factory.close();
  }

  @Test
  void aUserWithReadGrantsOnlyIsRefusedTheLockOfATypeAndOfItsBucket() {
    bindUser(Map.of("Account", EnumSet.of(SecurityDatabaseUser.ACCESS.READ_RECORD)));
    final String bucketName = database.getSchema().getType("Account").getBuckets(false).getFirst().getName();

    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "LOCK TYPE Account"))).isInstanceOf(SecurityException.class);
    assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).isFalse();
    database.rollback();

    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "LOCK BUCKET " + bucketName))).isInstanceOf(SecurityException.class);
    assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).isFalse();
    database.rollback();

    database.begin();
    assertThat(catchThrowable(() -> database.acquireLock().type("Account").lock())).isInstanceOf(SecurityException.class);
    assertThat(catchThrowable(() -> database.acquireLock().bucket(bucketName).lock())).isInstanceOf(SecurityException.class);
    assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).isFalse();
    database.rollback();
  }

  @Test
  void aUserWithNoGrantsOnTheTypeIsRefusedTheLock() {
    bindUser(Map.of("Ledger", EnumSet.allOf(SecurityDatabaseUser.ACCESS.class)));

    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "LOCK TYPE Account"))).isInstanceOf(SecurityException.class);
    assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).isFalse();
    database.rollback();
  }

  @Test
  void aRefusalNamesTheTypeAndLeavesTheTransactionUsable() {
    bindUser(Map.of("Account", EnumSet.of(SecurityDatabaseUser.ACCESS.READ_RECORD), "Ledger",
        EnumSet.of(SecurityDatabaseUser.ACCESS.READ_RECORD, SecurityDatabaseUser.ACCESS.CREATE_RECORD,
            SecurityDatabaseUser.ACCESS.UPDATE_RECORD)));

    database.begin();
    final Throwable refused = catchThrowable(() -> database.command("sql", "LOCK TYPE Account"));
    assertThat(refused).isInstanceOf(SecurityException.class).hasMessageContaining("Account");

    // the files of the refused type stay out of the lock the same transaction then takes on a type it may write
    final LocalTransactionExplicitLock lock = (LocalTransactionExplicitLock) database.acquireLock().type("Ledger");
    for (final int fileId : database.getSchema().getType("Account").getBucketIds(false))
      assertThat(lock.collectsFile(fileId)).isFalse();
    for (final var typeIndex : database.getSchema().getType("Account").getAllIndexes(true))
      for (final var index : typeIndex.getIndexesOnBuckets())
        for (final int fileId : index.getFileIds())
          assertThat(lock.collectsFile(fileId)).isFalse();
    for (final int fileId : database.getSchema().getType("Ledger").getBucketIds(false))
      assertThat(lock.collectsFile(fileId)).isTrue();

    lock.lock();
    assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).isTrue();
    database.newDocument("Ledger").save();
    database.commit();

    assertThat(database.countType("Ledger", false)).isEqualTo(1);
  }

  @Test
  void aRefusedTypeAmongSeveralTakesNothingOfTheRefusedOne() {
    bindUser(Map.of("Account", EnumSet.of(SecurityDatabaseUser.ACCESS.READ_RECORD), "Ledger",
        EnumSet.of(SecurityDatabaseUser.ACCESS.CREATE_RECORD)));

    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "LOCK TYPE Ledger, Account"))).isInstanceOf(SecurityException.class);
    assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).isFalse();

    final LocalTransactionExplicitLock lock = (LocalTransactionExplicitLock) database.acquireLock();
    for (final int fileId : database.getSchema().getType("Account").getBucketIds(false))
      assertThat(lock.collectsFile(fileId)).isFalse();
    database.rollback();
  }

  @Test
  void anyWriteGrantOnTheTypeAllowsTheLock() {
    for (final SecurityDatabaseUser.ACCESS write : new SecurityDatabaseUser.ACCESS[] { SecurityDatabaseUser.ACCESS.CREATE_RECORD,
        SecurityDatabaseUser.ACCESS.UPDATE_RECORD, SecurityDatabaseUser.ACCESS.DELETE_RECORD }) {
      bindUser(Map.of("Account", EnumSet.of(SecurityDatabaseUser.ACCESS.READ_RECORD, write)));
      final String bucketName = database.getSchema().getType("Account").getBuckets(false).getFirst().getName();

      database.begin();
      database.command("sql", "LOCK TYPE Account");
      assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).as("LOCK TYPE with %s", write).isTrue();
      database.rollback();

      database.begin();
      database.command("sql", "LOCK BUCKET " + bucketName);
      assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).as("LOCK BUCKET with %s", write).isTrue();
      database.rollback();
    }
  }

  @Test
  void aLockOfAParentTypeAsksTheGrantsOfItsSubTypes() {
    bindUser(Map.of("Base", EnumSet.allOf(SecurityDatabaseUser.ACCESS.class), "Derived",
        EnumSet.of(SecurityDatabaseUser.ACCESS.READ_RECORD)));

    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "LOCK TYPE Base"))).isInstanceOf(SecurityException.class)
        .hasMessageContaining("Derived");
    assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).isFalse();
    database.rollback();

    // the other way round: the sub type's own grants lock the sub type, and the parent's lock covers the whole hierarchy
    bindUser(Map.of("Base", EnumSet.of(SecurityDatabaseUser.ACCESS.READ_RECORD), "Derived",
        EnumSet.allOf(SecurityDatabaseUser.ACCESS.class)));
    database.begin();
    database.command("sql", "LOCK TYPE Derived");
    assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).isTrue();
    database.rollback();

    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "LOCK TYPE Base"))).isInstanceOf(SecurityException.class);
    database.rollback();

    bindUser(Map.of("Base", EnumSet.allOf(SecurityDatabaseUser.ACCESS.class), "Derived",
        EnumSet.of(SecurityDatabaseUser.ACCESS.DELETE_RECORD)));
    database.begin();
    database.command("sql", "LOCK TYPE Base");
    assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).isTrue();
    database.rollback();
  }

  @Test
  void aVertexTypeIsLockedWithItsWriteGrants() {
    bindUser(Map.of("Person", EnumSet.of(SecurityDatabaseUser.ACCESS.READ_RECORD)));
    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "LOCK TYPE Person"))).isInstanceOf(SecurityException.class);
    database.rollback();

    bindUser(Map.of("Person", EnumSet.of(SecurityDatabaseUser.ACCESS.READ_RECORD, SecurityDatabaseUser.ACCESS.UPDATE_RECORD)));
    database.begin();
    database.command("sql", "LOCK TYPE Person");
    assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).isTrue();
    database.rollback();
  }

  @Test
  void aCallerWithNoBoundUserLocksAsBefore() {
    database.begin();
    database.command("sql", "LOCK TYPE Account, Base");
    assertThat(((DatabaseInternal) database).getTransaction().holdsExplicitLocks()).isTrue();
    database.newDocument("Account").set("code", "a").save();
    database.commit();

    assertThat(database.countType("Account", false)).isEqualTo(1);
  }

  /**
   * Binds a user holding {@code grants} per type name: a file takes the grants of the type that involves it, a type with no entry
   * has none, and a file no type involves is allowed.
   */
  private void bindUser(final Map<String, Set<SecurityDatabaseUser.ACCESS>> grants) {
    final Map<String, Set<SecurityDatabaseUser.ACCESS>> grantsByType = new HashMap<>(grants);
    final DatabaseInternal databaseInternal = (DatabaseInternal) database;

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "limited";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        final DocumentType type = databaseInternal.getSchema().getInvolvedTypeByBucketId(fileId);
        return type == null || grantsByType.getOrDefault(type.getName(), Set.of()).contains(access);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return grantsByType.getOrDefault(typeName, Set.of()).contains(access);
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
