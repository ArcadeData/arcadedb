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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8613: {@code MutableDocument.newEmbeddedDocument(type, property)} on a property the schema does not declare
 * stores the first embedded document as a single value, so the second call replaces it. That behavior
 * is a documented contract, not a bug (the scratch-property idiom relies on it), and a declared {@code LIST}
 * or a caller-created collection is the way to keep several embedded documents.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8613NewEmbeddedDocumentOverwriteTest extends TestHelper {

  @Test
  void secondEmbeddedDocumentOnAnUndeclaredPropertyReplacesTheFirstAsDocumented() {
    database.getSchema().createDocumentType("Item8613");
    database.getSchema().createDocumentType("Undeclared8613");

    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Undeclared8613");
      d.newEmbeddedDocument("Item8613", "items").set("n", 1);
      d.newEmbeddedDocument("Item8613", "items").set("n", 2);

      // single-value contract, pinned: only the last one is kept, and it is not a collection
      assertThat(d.get("items")).isInstanceOf(EmbeddedDocument.class);
      assertThat(((EmbeddedDocument) d.get("items")).getInteger("n")).isEqualTo(2);
    });
  }

  @Test
  void collectionCreatedByTheCallerStillReceivesEveryEmbeddedDocument() {
    database.getSchema().createDocumentType("Item8613b");
    database.getSchema().createDocumentType("Holder8613b");

    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Holder8613b").set("items", new ArrayList<>());
      d.newEmbeddedDocument("Item8613b", "items").set("n", 1);
      d.newEmbeddedDocument("Item8613b", "items").set("n", 2);
      assertThat(d.getList("items")).hasSize(2);
    });
  }

  @Test
  void declaredListStillAppends() {
    database.getSchema().createDocumentType("Item8613c");
    final DocumentType holder = database.getSchema().createDocumentType("Holder8613c");
    holder.createProperty("items", Type.LIST);

    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Holder8613c");
      d.newEmbeddedDocument("Item8613c", "items").set("n", 1);
      d.newEmbeddedDocument("Item8613c", "items").set("n", 2);
      assertThat((List<?>) d.getList("items")).hasSize(2);
    });
  }

  @Test
  void declaredEmbeddedPropertyKeepsReplacingItsSingleValue() {
    database.getSchema().createDocumentType("Item8613d");
    final DocumentType holder = database.getSchema().createDocumentType("Holder8613d");
    holder.createProperty("item", Type.EMBEDDED).setOfType("Item8613d");

    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Holder8613d");
      d.newEmbeddedDocument("Item8613d", "item").set("n", 1);
      d.newEmbeddedDocument("Item8613d", "item").set("n", 2);
      assertThat(((EmbeddedDocument) d.get("item")).getInteger("n")).isEqualTo(2);
    });
  }
}
