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

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.junit.jupiter.api.Test;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.Vertex.DIRECTION;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

/**
 * @author Pawel Maslej
 * @since 1 Jul 2024
 */
class DataEncryptionTest extends TestHelper {

  String password = "password";
  String salt = "salt";

  @Test
  void dataIsEncrypted() throws Exception {
    database.setDataEncryption(DefaultDataEncryption.useDefaults(DefaultDataEncryption.getSecretKeyFromPasswordUsingDefaults(password, salt)));

    database.command("sql", "create vertex type Person");
    database.command("sql", "create property Person.id string");
    database.command("sql", "create index on Person (id) unique");
    database.command("sql", "create property Person.name string");
    database.command("sql", "create edge type Knows");

    var v1Id = new AtomicReference<RID>(null);
    var v2Id = new AtomicReference<RID>(null);

    database.transaction(() -> {
      var v1 = database.newVertex("Person").set("name", "John").save();
      var v2 = database.newVertex("Person").set("name", "Doe").save();
      v1.newEdge("Knows", v2, "since", 2024);
      verify(v1, v2, true);
      v1Id.set(v1.getIdentity());
      v2Id.set(v2.getIdentity());
    });
    verify(v1Id.get(), v2Id.get(), true);

    database.setDataEncryption(null);
    verify(v1Id.get(), v2Id.get(), false);

    reopenDatabase();
    verify(v1Id.get(), v2Id.get(), false);

    database.setDataEncryption(DefaultDataEncryption.useDefaults(DefaultDataEncryption.getSecretKeyFromPasswordUsingDefaults(password, salt)));
    verify(v1Id.get(), v2Id.get(), true);
  }

  private void verify(RID rid1, RID rid2, boolean isEquals) {
    database.transaction(() -> {
      var p1 = database.lookupByRID(rid1, true).asVertex();
      var p2 = database.lookupByRID(rid2, true).asVertex();
      verify(p1, p2, isEquals);
    });
  }

  private void verify(Vertex p1, Vertex p2, boolean isEquals) {
    if (isEquals) {
      assertThat(p1.get("name")).isEqualTo("John");
      assertThat(p2.get("name")).isEqualTo("Doe");
      assertThat(p1.getEdges(DIRECTION.OUT, "Knows").iterator().next().get("since")).isEqualTo(2024);
    } else {
      assertThat(((String) p1.get("name")).contains("John")).isFalse();
      assertThat(((String) p2.get("name")).contains("Doe")).isFalse();
      assertThat(p1.getEdges(DIRECTION.OUT, "Knows").iterator().next().get("since").toString().contains("2024")).isFalse();
    }
  }

  // Regression coverage for issue #4137: encryption uses a fresh random IV per call,
  // so index keys must be serialized without encryption to remain deterministic.
  // Without the fix, equality lookups on a HASH-indexed property never resolve when
  // encryption is enabled.

  @Test
  void hashIndexedLongLookupWorksWithEncryption() throws Exception {
    enableRandomEncryption();
    createEntryTypeWithLongHashIndex();

    database.transaction(() -> {
      final var doc = database.newDocument("Entry");
      doc.set("vid", 1L);
      doc.set("created_at", Clock.systemUTC().instant());
      doc.set("updated_at", null);
      doc.save();
    });

    assertThat(database.countType("Entry", false)).isEqualTo(1);

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Entry WHERE vid = :vid", Map.of("vid", 1L))) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Long>getProperty("vid")).isEqualTo(1L);
        assertThat(rs.hasNext()).isFalse();
      }
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Entry WHERE vid <> :vid", Map.of("vid", 0L))) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Long>getProperty("vid")).isEqualTo(1L);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void hashIndexedStringLookupWorksWithEncryption() throws Exception {
    enableRandomEncryption();

    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType("EntryStr");
      t.createProperty("name", Type.STRING).setMandatory(true).setNotNull(true);
      t.createTypeIndex(Schema.INDEX_TYPE.HASH, true, "name");
    });
    database.transaction(() -> {
      database.newDocument("EntryStr").set("name", "alice").save();
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM EntryStr WHERE name = :name", Map.of("name", "alice"))) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("alice");
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  private void enableRandomEncryption() throws Exception {
    final KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256);
    final SecretKey key = keyGenerator.generateKey();
    database.setDataEncryption(DefaultDataEncryption.useDefaults(key));
  }

  private void createEntryTypeWithLongHashIndex() {
    database.transaction(() -> {
      final DocumentType entryType = database.getSchema().createDocumentType("Entry");
      entryType.createProperty("vid", Type.LONG).setMandatory(true).setNotNull(true);
      entryType.createProperty("created_at", Type.DATETIME_NANOS).setMandatory(true).setNotNull(true);
      entryType.createProperty("updated_at", Type.DATETIME_NANOS).setMandatory(true).setNotNull(false);
      entryType.createTypeIndex(Schema.INDEX_TYPE.HASH, true, "vid");
    });
  }
}
