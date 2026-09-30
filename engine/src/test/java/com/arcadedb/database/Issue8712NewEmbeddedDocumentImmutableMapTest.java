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

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8712: the map-keyed {@code newEmbeddedDocument} overloads trusted the stored container, so a property set to
 * an immutable {@code Map.of(...)} threw {@code UnsupportedOperationException} where the collection arm heals (#7777).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8712NewEmbeddedDocumentImmutableMapTest extends TestHelper {

  @Test
  void stringKeyedOverloadHealsAnImmutableMap() {
    database.getSchema().createDocumentType("Item8712");
    database.getSchema().createDocumentType("Holder8712");

    database.transaction(() -> {
      final MutableDocument seed = database.newDocument("Item8712").set("n", 0);
      final MutableDocument d = database.newDocument("Holder8712");
      d.set("items", Map.of("a", seed));
      d.newEmbeddedDocument("Item8712", "items", "b").set("n", 2);

      final Map<String, Object> items = d.getMap("items");
      assertThat(items).containsOnlyKeys("a", "b");
      assertThat(((EmbeddedDocument) items.get("b")).getInteger("n")).isEqualTo(2);
    });
  }

  @Test
  void objectKeyedOverloadHealsAnImmutableMap() {
    database.getSchema().createDocumentType("Item8712b");
    database.getSchema().createDocumentType("Holder8712b");

    database.transaction(() -> {
      final MutableDocument seed = database.newDocument("Item8712b").set("n", 0);
      final MutableDocument d = database.newDocument("Holder8712b");
      d.set("items", Map.of("a", seed));
      d.newEmbeddedDocument("Item8712b", "items", (Object) "b").set("n", 2);

      assertThat(d.getMap("items")).containsOnlyKeys("a", "b");
    });
  }
}
