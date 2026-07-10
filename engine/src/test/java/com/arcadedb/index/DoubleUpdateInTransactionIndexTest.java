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

import java.util.ArrayList;
import java.util.List;

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

  @Test
  void compositeIndexDoubleUpdateLeavesNoPhantom() {
    final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
    type.createProperty("firstName", String.class);
    type.createProperty("lastName", String.class);
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "firstName", "lastName" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false)
        .create();

    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> holder[0] = database.newDocument(TYPE_NAME)
        .set("firstName", "John").set("lastName", "Doe").save());
    final String rid = holder[0].getIdentity().toString();

    // Change one component per save: (John,Doe) -> (Jane,Doe) -> (Jane,Smith).
    database.transaction(() -> {
      final MutableDocument doc = holder[0].modify();
      doc.set("firstName", "Jane");
      doc.save();
      doc.set("lastName", "Smith");
      doc.save();
    });

    assertThat(database.query("sql",
        "SELECT FROM " + TYPE_NAME + " WHERE firstName = 'John' AND lastName = 'Doe'").hasNext())
        .as("original composite key must be gone").isFalse();
    assertThat(database.query("sql",
        "SELECT FROM " + TYPE_NAME + " WHERE firstName = 'Jane' AND lastName = 'Doe'").hasNext())
        .as("intermediate composite key must not survive as a phantom").isFalse();
    assertThat(database.query("sql",
        "SELECT FROM " + TYPE_NAME + " WHERE firstName = 'Jane' AND lastName = 'Smith'").next()
        .getIdentity().get().toString()).isEqualTo(rid);
  }

  @Test
  void firstUpdateKeepsKeyThenSecondChangesIt() {
    createSchema(true);

    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> holder[0] = database.newDocument(TYPE_NAME).set("email", "stable").save());
    final String rid = holder[0].getIdentity().toString();

    // The FIRST save leaves the indexed key untouched (only a non-indexed property changes); only the SECOND
    // save moves the key. Locks in the snapshot-vs-original selection: the second diff must see 'stable' as
    // the previous value, whichever source it comes from.
    database.transaction(() -> {
      final MutableDocument doc = holder[0].modify();
      doc.set("age", 42);
      doc.save();
      doc.set("email", "moved");
      doc.save();
    });

    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'stable'").hasNext())
        .as("old key must be removed even though the first save did not change it").isFalse();
    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'moved'").next()
        .getIdentity().get().toString()).isEqualTo(rid);

    // The unique constraint must not be poisoned for the old value.
    database.transaction(() -> database.newDocument(TYPE_NAME).set("email", "stable").save());
  }

  @Test
  void keyChangeThenNoChangeThenKeyChangeInOneTransaction() {
    createSchema(true);

    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> holder[0] = database.newDocument(TYPE_NAME).set("email", "v0").save());
    final String rid = holder[0].getIdentity().toString();

    // Save 1 changes the key (snapshot refreshed), save 2 changes only a non-indexed property (snapshot
    // refresh is SKIPPED because no index changed - the retained snapshot must still be accurate), save 3
    // changes the key again and must diff against save 1's state.
    database.transaction(() -> {
      final MutableDocument doc = holder[0].modify();
      doc.set("email", "v1");
      doc.save();
      doc.set("age", 7);
      doc.save();
      doc.set("email", "v2");
      doc.save();
    });

    for (final String gone : new String[] { "v0", "v1" })
      assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = '" + gone + "'").hasNext())
          .as("no phantom for '" + gone + "'").isFalse();
    assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'v2'").next()
        .getIdentity().get().toString()).isEqualTo(rid);

    // Unique constraint fully released for both stale values.
    database.transaction(() -> {
      database.newDocument(TYPE_NAME).set("email", "v0").save();
      database.newDocument(TYPE_NAME).set("email", "v1").save();
    });
  }

  @Test
  void listByItemIndexDoubleUpdateComputesDeltaAcrossSaves() {
    database.getSchema().createDocumentType(TYPE_NAME).createProperty("tags", List.class);
    database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (tags BY ITEM) NOTUNIQUE");

    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> holder[0] = database.newDocument(TYPE_NAME)
        .set("tags", List.of("a", "b")).save());
    final String rid = holder[0].getIdentity().toString();

    // Two list replacements in one tx: [a,b] -> [b,c] -> [c,d].
    database.transaction(() -> {
      final MutableDocument doc = holder[0].modify();
      doc.set("tags", List.of("b", "c"));
      doc.save();
      doc.set("tags", List.of("c", "d"));
      doc.save();
    });

    // Assert on the INDEX content directly (a CONTAINS query could be answered by a full scan, hiding a stale index).
    for (final String gone : new String[] { "a", "b" })
      assertThat(lookupInTagsIndex(gone)).as("removed list item '" + gone + "' must not survive in the BY ITEM index")
          .isEmpty();
    for (final String kept : new String[] { "c", "d" })
      assertThat(lookupInTagsIndex(kept)).as("list item '" + kept + "' must resolve to the record")
          .containsExactly(rid);
  }

  @Test
  void inPlaceListMutationBetweenSavesUpdatesIndex() {
    database.getSchema().createDocumentType(TYPE_NAME).createProperty("tags", List.class);
    database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (tags BY ITEM) NOTUNIQUE");

    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> holder[0] = database.newDocument(TYPE_NAME)
        .set("tags", new ArrayList<>(List.of("a"))).save());
    final String rid = holder[0].getIdentity().toString();

    // Mutate the SAME list instance in place between two saves of the same tx. The per-RID snapshot must be a
    // COPY of the indexed value: if it aliases the live list, the second diff compares the list against itself,
    // sees no change and leaves the index stale ('a' resurrected, 'c' missing).
    database.transaction(() -> {
      final MutableDocument doc = holder[0].modify();
      final List<String> tags = (List<String>) doc.get("tags");
      tags.add("b");
      doc.set("tags", tags);
      doc.save();
      tags.remove("a");
      tags.add("c");
      doc.set("tags", tags);
      doc.save();
    });

    // Assert on the INDEX content directly (a CONTAINS query could be answered by a full scan, hiding a stale index).
    assertThat(lookupInTagsIndex("a"))
        .as("item 'a' was removed in-place before the second save and must not survive in the index").isEmpty();
    for (final String kept : new String[] { "b", "c" })
      assertThat(lookupInTagsIndex(kept)).as("list item '" + kept + "' must resolve to the record")
          .containsExactly(rid);
  }

  private List<String> lookupInTagsIndex(final String value) {
    final List<String> rids = new ArrayList<>();
    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof TypeIndex ti && TYPE_NAME.equals(ti.getTypeName())) {
        final IndexCursor cursor = ti.get(new Object[] { value });
        while (cursor.hasNext())
          rids.add(cursor.next().getIdentity().toString());
      }
    return rids;
  }

  @Test
  void deleteAfterUpdateInSameTransactionLeavesNoEntry() {
    createSchema(true);

    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> holder[0] = database.newDocument(TYPE_NAME).set("email", "orig").save());

    // Update then delete the same record inside one tx: no key may survive and the per-RID snapshot must not
    // linger for the deleted record.
    database.transaction(() -> {
      final MutableDocument doc = holder[0].modify();
      doc.set("email", "mid");
      doc.save();
      doc.delete();
    });

    for (final String gone : new String[] { "orig", "mid" })
      assertThat(database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = '" + gone + "'").hasNext())
          .as("no index entry may survive for '" + gone + "' after in-tx update+delete").isFalse();

    // The unique constraint must be fully released for both values.
    database.transaction(() -> {
      database.newDocument(TYPE_NAME).set("email", "orig").save();
      database.newDocument(TYPE_NAME).set("email", "mid").save();
    });
  }
}
