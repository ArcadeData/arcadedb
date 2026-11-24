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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12356: reading a property of a record already loaded resolves the property name through the
 * dictionary, and used to reach it by {@code getSchema()}, which looks up (and creates) the calling thread's transaction
 * context. That lookup is a thread-local and a map read on every property of every row, 2 to 5% of a server's CPU under
 * a read-heavy load. The read needs no context: these reads must leave a thread that has none without one, and must
 * still be refused once the database is closed.
 */
class PropertyReadWithoutTransactionContextTest extends TestHelper {

  @Test
  void readingTheBytesOfALoadedRecordNeedsNoTransactionContext() {
    final RID rid = createRecord();
    final Document doc = database.lookupByRID(rid, true).asDocument();

    final String path = database.getDatabasePath();
    DatabaseContext.INSTANCE.removeContext(path);
    assertThat(DatabaseContext.INSTANCE.getContextIfExists(path)).isNull();

    assertThat(doc.getString("name")).isEqualTo("alice");
    assertThat(doc.getInteger("age")).isEqualTo(42);
    assertThat(doc.has("age")).isTrue();
    assertThat(doc.has("missing")).isFalse();
    assertThat(doc.get("missing")).isNull();
    assertThat(doc.getList("tags")).containsExactly("a", "b");
    assertThat(doc.getMap("attrs")).containsEntry("k", "v");
    assertThat(doc.getPropertyNames()).containsExactlyInAnyOrder("name", "age", "tags", "attrs", "address");

    assertThat(DatabaseContext.INSTANCE.getContextIfExists(path))
        .as("a property read of a loaded record set up a transaction context it does not use")
        .isNull();

    // An embedded document resolves its type through the schema, which is allowed to set the context up
    assertThat(doc.getEmbedded("address").getString("city")).isEqualTo("Rome");
    assertThat(doc.toMap()).containsEntry("name", "alice");
  }

  @Test
  void aPropertyReadIsStillRefusedOnceTheDatabaseIsClosed() {
    final RID rid = createRecord();
    final Document doc = database.lookupByRID(rid, true).asDocument();

    database.close();
    try {
      assertThatThrownBy(() -> doc.getString("name")).isInstanceOf(DatabaseIsClosedException.class);
      assertThatThrownBy(doc::getPropertyNames).isInstanceOf(DatabaseIsClosedException.class);
      assertThatThrownBy(() -> doc.has("name")).isInstanceOf(DatabaseIsClosedException.class);
    } finally {
      reopenDatabase();
    }
  }

  private RID createRecord() {
    database.getSchema().createDocumentType("Person").createProperty("age", Type.INTEGER);
    database.getSchema().createDocumentType("Address");
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Person").set("name", "alice", "age", 42, "tags", List.of("a", "b"),
          "attrs", Map.of("k", "v"));
      doc.newEmbeddedDocument("Address", "address").set("city", "Rome");
      rid[0] = doc.save().getIdentity();
    });
    return rid[0];
  }
}
