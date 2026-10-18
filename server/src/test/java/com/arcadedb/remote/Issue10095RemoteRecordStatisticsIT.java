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
package com.arcadedb.remote;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10095, the remote half: {@code RemoteDatabase.saveRecord} incremented {@code createRecord} before branching into
 * its UPDATE arm, so over HTTP every update was counted as a create and {@code updateRecord} stayed at zero for the whole
 * session. Each arm now counts its own statistic.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10095RemoteRecordStatisticsIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database-10095";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void anUpdateIsCountedAsAnUpdateAndNotAsACreate() throws Exception {
    testEachServer(serverIndex -> {
      final RemoteDatabase db = new RemoteDatabase("127.0.0.1", getServerHttpPort(serverIndex), DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      try {
        db.command("sql", "CREATE DOCUMENT TYPE Doc10095 IF NOT EXISTS");

        final long creates = stat(db, "createRecord");
        final long updates = stat(db, "updateRecord");

        final MutableDocument doc = db.newDocument("Doc10095").set("v", 0);
        final RID rid = doc.save().getIdentity();
        assertThat(rid).isNotNull();
        assertThat(stat(db, "createRecord")).as("the first save creates").isEqualTo(creates + 1);
        assertThat(stat(db, "updateRecord")).isEqualTo(updates);

        doc.set("v", 1).save();
        assertThat(stat(db, "createRecord")).as("saving an existing record is not a create").isEqualTo(creates + 1);
        assertThat(stat(db, "updateRecord")).as("it is an update").isEqualTo(updates + 1);

        db.newDocument("Doc10095").set("v", 2);
        assertThat(stat(db, "createRecord")).as("an instance that was not saved is not counted").isEqualTo(creates + 1);
      } finally {
        db.close();
      }
    });
  }

  @Test
  void aRefusedSaveIsNotCounted() throws Exception {
    testEachServer(serverIndex -> {
      final RemoteDatabase db = new RemoteDatabase("127.0.0.1", getServerHttpPort(serverIndex), DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      try {
        db.command("sql", "CREATE DOCUMENT TYPE Doc10095b IF NOT EXISTS");
        db.command("sql", "CREATE PROPERTY Doc10095b.name IF NOT EXISTS STRING (mandatory true)");

        final long creates = stat(db, "createRecord");
        try {
          db.newDocument("Doc10095b").save();
        } catch (final RuntimeException expected) {
          // the server refuses the document: the mandatory property is missing
        }
        assertThat(stat(db, "createRecord")).isEqualTo(creates);
      } finally {
        db.close();
      }
    });
  }

  /**
   * Issue #10141: {@code deleteRecord} counted before its own non-persistent guard and before the server answered, so a
   * delete the driver rejected locally, or one that matched no record on the server, still moved the counter.
   */
  @Test
  void onlyADeleteTheServerPerformedIsCounted() throws Exception {
    testEachServer(serverIndex -> {
      final RemoteDatabase db = new RemoteDatabase("127.0.0.1", getServerHttpPort(serverIndex), DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      try {
        db.command("sql", "CREATE DOCUMENT TYPE Doc10141 IF NOT EXISTS");

        final long deletes = stat(db, "deleteRecord");

        final MutableDocument notSaved = db.newDocument("Doc10141");
        assertThatThrownBy(() -> db.deleteRecord(notSaved)).isInstanceOf(IllegalArgumentException.class);
        assertThat(stat(db, "deleteRecord")).as("a record that never left the client is not deleted").isEqualTo(deletes);

        final MutableDocument doc = db.newDocument("Doc10141").set("v", 0);
        doc.save();
        db.deleteRecord(doc);
        assertThat(stat(db, "deleteRecord")).as("the server deleted the record").isEqualTo(deletes + 1);
        try (final ResultSet remaining = db.query("sql", "select from Doc10141")) {
          assertThat(remaining.hasNext()).isFalse();
        }

        // the record is gone: the second delete fails as the embedded engine's does (issue #10230)
        assertThatThrownBy(() -> db.deleteRecord(doc)).isInstanceOf(RecordNotFoundException.class);
        assertThat(stat(db, "deleteRecord")).as("a delete that matched no record is not counted").isEqualTo(deletes + 1);
      } finally {
        db.close();
      }
    });
  }

  private static long stat(final RemoteDatabase db, final String name) {
    return ((Number) db.getStats().get(name)).longValue();
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
