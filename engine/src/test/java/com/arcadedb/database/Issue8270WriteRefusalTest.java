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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8270: a database an HA server opened before it could replicate it refuses writes, and only writes, until
 * the refusal is lifted. Each entry point a write can take into the engine is driven on its own: a SQL statement in a
 * managed transaction, an explicit transaction creating a record, one updating a record, a schema change creating files, one that only
 * rewrites the schema configuration, and a database setting - the last three being the kinds that do not end in a
 * commit.
 */
class Issue8270WriteRefusalTest extends TestHelper {

  private static final String TYPE   = "Issue8270Doc";
  private static final String REASON = "the test says so";

  private RID existing;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE);
      existing = database.newDocument(TYPE).set("name", "before").save().getIdentity();
    });
  }

  @Override
  protected void endTest() {
    local().acceptWrites();
  }

  private LocalDatabase local() {
    return (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
  }

  @Test
  void aStatementInAManagedTransactionIsRefusedAndLeavesNothingBehind() {
    local().refuseWrites(REASON);

    // The shape an HTTP command takes: the handler runs the statement inside database.transaction(), which retries a
    // NeedRetryException and rethrows it once the retries are spent.
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET name = 'refused'")))
        .isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.query("sql", "SELECT count(*) AS c FROM " + TYPE + " WHERE name = 'refused'").next()
        .<Long>getProperty("c")).isEqualTo(0L);
  }

  @Test
  void anExplicitTransactionCreatingARecordIsRefused() {
    local().refuseWrites(REASON);

    database.begin();
    database.newDocument(TYPE).set("name", "refused").save();
    assertThatThrownBy(() -> database.commit()).isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
    // Refused before the commit started, like a rollback-only transaction: the caller rolls it back.
    assertThat(database.isTransactionActive()).isTrue();
    database.rollback();

    assertThat(database.countType(TYPE, true)).isEqualTo(1L);
  }

  @Test
  void anExplicitTransactionUpdatingARecordIsRefused() {
    local().refuseWrites(REASON);

    database.begin();
    database.lookupByRID(existing, true).asDocument().modify().set("name", "refused").save();
    assertThatThrownBy(() -> database.commit()).isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
    database.rollback();

    assertThat(database.lookupByRID(existing, true).asDocument().getString("name")).isEqualTo("before");
  }

  @Test
  void aTransactionBegunWhileWritesWereRefusedIsRefusedAfterTheyAreAccepted() {
    // A handle resolved while the database waited to be wrapped may be the plain database even after the wrap: the
    // transaction it began then must not commit locally just because the refusal has been lifted meanwhile.
    local().refuseWrites(REASON);
    database.begin();
    database.newDocument(TYPE).set("name", "begun-refused").save();
    local().acceptWrites();

    assertThatThrownBy(() -> database.commit()).isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
    database.rollback();
    assertThat(database.countType(TYPE, true)).isEqualTo(1L);

    // The next transaction begins with writes accepted and commits.
    database.transaction(() -> database.newDocument(TYPE).set("name", "after").save());
    assertThat(database.countType(TYPE, true)).isEqualTo(2L);
  }

  @Test
  void aSchemaChangeIsRefused() {
    local().refuseWrites(REASON);

    assertThatThrownBy(() -> database.getSchema().createDocumentType("Issue8270Refused"))
        .isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
    assertThat(database.getSchema().existsType("Issue8270Refused")).isFalse();
  }

  @Test
  void aConfigurationOnlySchemaChangeIsRefused() {
    // ALTER PROPERTY rewrites the schema configuration and nothing else: no commit, no file created.
    database.getSchema().getType(TYPE).createProperty("name", String.class);
    local().refuseWrites(REASON);

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY " + TYPE + ".name MANDATORY true"))
        .isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
    assertThat(database.getSchema().getType(TYPE).getProperty("name").isMandatory()).isFalse();
  }

  @Test
  void aDatabaseSettingChangeIsRefused() {
    local().refuseWrites(REASON);

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'yyyy'"))
        .isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
  }

  @Test
  void aPrivilegeCheckThatIsNotAWriteIsNotRefused() {
    local().refuseWrites(REASON);

    // UPDATE_SECURITY guards read-only operations too (BACKUP DATABASE, LOAD CSV): the refusal must not block them.
    local().checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SECURITY);

    assertThatThrownBy(() -> local().checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA))
        .isInstanceOf(NeedRetryException.class).hasMessageContaining(REASON);
  }

  @Test
  void readsAndReadOnlyTransactionsAreNotRefused() {
    local().refuseWrites(REASON);

    assertThat(database.query("sql", "SELECT FROM " + TYPE).hasNext()).isTrue();
    database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(1L));
  }

  @Test
  void writesAreAcceptedOnceTheRefusalIsLifted() {
    local().refuseWrites(REASON);
    assertThat(local().getWriteRefusal()).isEqualTo(REASON);

    local().acceptWrites();
    assertThat(local().getWriteRefusal()).isNull();

    database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET name = 'accepted'"));
    database.getSchema().createDocumentType("Issue8270Accepted");
    assertThat(database.countType(TYPE, true)).isEqualTo(2L);
    assertThat(database.getSchema().existsType("Issue8270Accepted")).isTrue();
  }
}
