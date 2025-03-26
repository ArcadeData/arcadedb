/*
 * Copyright © 2024-present Arcade Data Ltd (info@arcadedata.com)
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
 * SPDX-FileCopyrightText: 2024-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.database;

import static org.assertj.core.api.Assertions.assertThat;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.NoSuchPaddingException;

import org.junit.jupiter.api.Test;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.Vertex.DIRECTION;

/**
 * @author Pawel Maslej
 * @since 1 Jul 2024
 */
class DataEncryptionTest extends TestHelper {

  String password = "password";
  String salt = "salt";

  @Test
  void dataIsEncrypted() throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException {
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
      v1.newEdge("Knows", v2, true, "since", 2024);
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
}
