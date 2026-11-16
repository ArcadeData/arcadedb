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

import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12188: a property dropped or renamed stayed in the remote client's cached type for the life of the connection,
 * {@link RemoteSchema#reload()} included, and its declared type kept decoding the values of the records read through it.
 */
class Issue12188RemoteDroppedPropertyIT extends BaseGraphServerTest {

  @Override
  protected String getDatabaseName() {
    return "issue12188";
  }

  @Test
  void droppedAndRenamedPropertiesLeaveTheCachedType() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final Schema schema = database.getSchema();
      final DocumentType type = schema.createDocumentType("Doc12188");
      type.createProperty("created", Type.DATETIME);
      type.createProperty("oldName", Type.STRING);

      schema.getType("Doc12188").dropProperty("created");
      assertThat(schema.getType("Doc12188").existsProperty("created")).isFalse();
      assertThat(schema.getType("Doc12188").getPropertyNames()).containsExactly("oldName");

      schema.getType("Doc12188").renameProperty("oldName", "newName");
      assertThat(schema.getType("Doc12188").getPropertyNames()).containsExactly("newName");

      ((RemoteSchema) schema).reload();
      assertThat(schema.getType("Doc12188").getPropertyNames()).containsExactly("newName");

      // the stale declarations no longer decode the record: it reads as a fresh connection and the embedded API read it
      final RID rid = database.command("sql", "INSERT INTO Doc12188 SET created = 1700000000000, oldName = 42").next()
          .getIdentity().get();
      final var read = database.lookupByRID(rid, true).asDocument();
      assertThat(read.get("created")).isEqualTo(1700000000000L);
      assertThat(read.get("oldName")).isEqualTo(42);
    } finally {
      database.close();
    }
  }

  @Test
  void propertyDroppedByAnotherClientLeavesTheCacheOnReload() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    final RemoteDatabase other = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {
      database.getSchema().createDocumentType("Other12188").createProperty("gone", Type.STRING);
      assertThat(database.getSchema().getType("Other12188").existsProperty("gone")).isTrue();

      other.command("sql", "DROP PROPERTY Other12188.gone");
      ((RemoteSchema) database.getSchema()).reload();

      assertThat(database.getSchema().getType("Other12188").existsProperty("gone")).isFalse();
      assertThat(database.getSchema().getType("Other12188").getPropertyNames()).isEmpty();
    } finally {
      other.close();
      database.close();
    }
  }
}
