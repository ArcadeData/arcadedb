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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4935: updating the same indexed record more than once inside a single
 * transaction left a phantom index entry for every intermediate value. The eager in-transaction index
 * update diffed each save against the record's committed buffer (which never advances until commit,
 * because serialization is deferred) instead of against the previous in-transaction value, so the
 * intermediate ADD was never cancelled. On a unique index the phantom entry both resolved via lookup to
 * the wrong (final) record and blocked a later legitimate insert of the intermediate value.
 */
class DoubleUpdateInTransactionIndexTest extends TestHelper {

  private static final String TYPE_NAME = "Person";

  private void createSchema(final boolean unique) {
    final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
    type.createProperty("email", String.class);
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "email" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(unique)
        .create();
  }

  @Test
  void doubleUpdateOnUniqueIndexLeavesNoPhantom() {
    createSchema(true);

    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> holder[0] = database.newDocument(TYPE_NAME).set("email", "orig").save());
    final String rid = holder[0].getIdentity().toString();

    // Two updates of the SAME record inside a SINGLE transaction: orig -> mid -> final.
    database.transaction(() -> {
      final MutableDocument doc = holder[0].modify();
      doc.set("email", "mid");
      doc.save();
      doc.set("email", "final");
      doc.save();
    });

    // The intermediate value must not survive as a phantom index entry.
    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'mid'").hasNext())
        .as("no record should be found under the intermediate value 'mid'").isFalse();

    // The final value must resolve to the record.
    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'final'").next()
        .getIdentity().get().toString())
        .as("final value must resolve to the record").isEqualTo(rid);

    // The unique constraint must not be poisoned: inserting another record with the intermediate value must succeed.
    database.transaction(() -> database.newDocument(TYPE_NAME).set("email", "mid").save());
    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'mid'").hasNext())
        .as("a fresh record with the previously-intermediate value must be insertable and findable").isTrue();

    // Exactly two records exist overall (final + the new mid).
    assertThat(database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME).next().<Long>getProperty("c"))
        .isEqualTo(2L);
  }

  @Test
  void doubleUpdateOnNonUniqueIndexLeavesNoPhantom() {
    createSchema(false);

    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> holder[0] = database.newDocument(TYPE_NAME).set("email", "orig").save());
    final String rid = holder[0].getIdentity().toString();

    database.transaction(() -> {
      final MutableDocument doc = holder[0].modify();
      doc.set("email", "mid");
      doc.save();
      doc.set("email", "final");
      doc.save();
    });

    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'mid'").hasNext())
        .as("non-unique index must not keep a phantom 'mid' entry").isFalse();
    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'orig'").hasNext())
        .as("non-unique index must not keep the original 'orig' entry").isFalse();
    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'final'").next()
        .getIdentity().get().toString()).isEqualTo(rid);
  }

  @Test
  void tripleUpdateOnUniqueIndexLeavesNoPhantom() {
    createSchema(true);

    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> holder[0] = database.newDocument(TYPE_NAME).set("email", "v0").save());

    database.transaction(() -> {
      final MutableDocument doc = holder[0].modify();
      doc.set("email", "v1");
      doc.save();
      doc.set("email", "v2");
      doc.save();
      doc.set("email", "v3");
      doc.save();
    });

    for (final String phantom : new String[] { "v0", "v1", "v2" })
      assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = '" + phantom + "'").hasNext())
          .as("no phantom for intermediate value '" + phantom + "'").isFalse();
    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'v3'").hasNext()).isTrue();
  }
}
