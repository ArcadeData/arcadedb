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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #4947 (pre-existing on-disk defect surfaced by the null-ordering fix): in a composite index, an entry
 * whose key has a NULL component adjacent to entries sharing the non-null prefix corrupted reads. The
 * same-key neighbor walk (findEntryOfSameKey) broke out on the stored null with the PREVIOUS component's
 * result - declaring (k,null) the "same key" as (k,v) - and the retrieve path then applied the matched
 * entry's key size to every pointer of the run: (k,null)'s key is shorter, so its value position landed
 * mid-bytes and the read failed with BufferUnderflowException (or returned garbage).
 * <p>
 * The two inserts run in SEPARATE transactions, pinning the physical put order independently of the
 * transaction overlay's comparator - this reproduced on code long before the #4947 fix.
 */
class LSMTreeCompositeNullKeyAdjacencyTest extends TestHelper {

  @Test
  void nullComponentKeyAdjacentToPlainKeyMustNotCorruptReads() {
    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType("D");
      t.createProperty("a", Integer.class);
      t.createProperty("b", Integer.class);
      database.getSchema().buildTypeIndex("D", new String[] { "a", "b" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true).create();
    });

    // The null-component key lands in the fresh index page FIRST.
    database.transaction(() -> database.newDocument("D").set("a", 0).save());
    database.transaction(() -> database.newDocument("D").set("a", 0).set("b", 0).save());

    database.transaction(() -> {
      final TypeIndex index = database.getSchema().getType("D").getIndexesByProperties("a", "b").getFirst();
      final IndexCursor c = index.get(new Object[] { 0, 0 });
      assertThat(c.hasNext())
          .as("the (0,0) entry must be readable next to the shorter (0,null) entry (#4947)").isTrue();
      assertThat(c.next().asDocument().<Integer>get("b")).isEqualTo(0);

      // The null-key entry itself resolves too.
      final IndexCursor cn = index.get(new Object[] { 0, null });
      assertThat(cn.hasNext()).as("the (0,null) entry itself must resolve (#4947)").isTrue();
    });
  }
}
