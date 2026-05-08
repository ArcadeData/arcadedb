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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DefaultDataEncryption;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.time.Clock;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BrokenHashIndexTest extends TestHelper {
  private static final String ENTRY = "Entry";

  @Override
  protected void beginTest() {
    try {
      final KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
      keyGenerator.init(256);
      final SecretKey key = keyGenerator.generateKey();
      database.setDataEncryption(DefaultDataEncryption.useDefaults(key));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    database.transaction(() -> {
      final DocumentType entryType = database.getSchema().createDocumentType(ENTRY);
      entryType.createProperty("vid", Type.LONG).setMandatory(true).setNotNull(true);
      entryType.createProperty("created_at", Type.DATETIME_NANOS).setMandatory(true).setNotNull(true);
      entryType.createProperty("updated_at", Type.DATETIME_NANOS).setMandatory(true).setNotNull(false);
      entryType.createTypeIndex(Schema.INDEX_TYPE.HASH, true, "vid");
    });

    database.transaction(() -> {
      final var doc = database.newDocument(ENTRY);
      doc.set("vid", 1L);
      doc.set("created_at", Clock.systemUTC().instant());
      doc.set("updated_at", null);
      doc.save();
    });
  }

  @Test
  void countTypeReturnsTheInsertedEntry() {
    assertThat(database.countType(ENTRY, false)).isEqualTo(1);
  }

  @Test
  void selectByEqualsOnHashIndexedLongFindsTheEntry() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Entry WHERE vid = :vid", Map.of("vid", 1L))) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Long>getProperty("vid")).isEqualTo(1L);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void selectByNotEqualsOnHashIndexedLongFindsTheEntry() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Entry WHERE vid <> :vid", Map.of("vid", 0L))) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Long>getProperty("vid")).isEqualTo(1L);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void selectByEqualsOnHashIndexedStringFindsTheEntry() {
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
}
