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
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the {@link SchemaDelta} format: what {@code compute} emits, that {@code apply} reproduces the leader's document from the receiver's, and that the merged document keeps the leader's key order.
 */
class SchemaDeltaTest {

  // ---- #6989 ----
  // a {@code SCHEMA_ENTRY} must be able to carry what a DDL changed rather than the whole schema
  // document. These tests pin the delta format itself: what {@link SchemaDelta#compute} emits, and that
  // {@link SchemaDelta#apply} reproduces the leader's document from the receiver's.

  /** A schema document shaped like {@code LocalSchema.toJSON()}: scalars, a settings map, and named maps. */
  private static JSONObject schema(final long version, final int typeCount) {
    final JSONObject root = new JSONObject();
    root.put("schemaVersion", version);
    root.put("dbmsVersion", "26.9.1");
    root.put("dbmsBuild", "1234");

    final JSONObject settings = new JSONObject();
    settings.put("zoneId", "Europe/Rome");
    settings.put("dateFormat", "yyyy-MM-dd");
    root.put("settings", settings);

    final JSONObject types = new JSONObject();
    for (int i = 0; i < typeCount; i++)
      types.put("Type_" + i, type("Type_" + i, "id", "name", "payload"));
    root.put("types", types);

    root.put("triggers", new JSONObject());
    root.put("materializedViews", new JSONObject());
    return root;
  }

  private static JSONObject type(final String name, final String... propertyNames) {
    final JSONObject type = new JSONObject();
    type.put("name", name);
    type.put("type", "v");
    final JSONObject properties = new JSONObject();
    for (final String propertyName : propertyNames) {
      final JSONObject property = new JSONObject();
      property.put("type", "STRING");
      property.put("notNull", false);
      // Padding so a type is worth several hundred bytes, like a real one.
      property.put("custom", new JSONObject().put("comment", "property " + propertyName + " of " + name));
      properties.put(propertyName, property);
    }
    type.put("properties", properties);
    return type;
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void addingOnePropertyShipsOnlyThatType() {
    final JSONObject base = schema(41, 200);
    final JSONObject updated = schema(42, 200);
    updated.getJSONObject("types").put("Type_7", type("Type_7", "id", "name", "payload", "extra"));

    final JSONObject delta = SchemaDelta.compute(base, updated);

    assertThat(delta.getLong(SchemaDelta.FIELD_BASE)).isEqualTo(41L);
    assertThat(delta.getJSONObject(SchemaDelta.FIELD_MERGE).getJSONObject("types").keySet())
        .as("only the type that changed is carried")
        .containsExactly("Type_7");
    assertThat(delta.getJSONObject(SchemaDelta.FIELD_PUT).keySet())
        .as("only the schema version moved at root level")
        .containsExactly("schemaVersion");

    assertThat(delta.toString().length())
        .as("a one-property DDL must not cost a fraction of the schema anywhere near the whole document")
        .isLessThan(updated.toString().length() / 10);

    assertThat(SchemaDelta.apply(base, delta).toString()).isEqualTo(updated.toString());
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void addingATypeShipsOnlyThatType() {
    final JSONObject base = schema(41, 50);
    final JSONObject updated = schema(42, 50);
    updated.getJSONObject("types").put("Brand_New", type("Brand_New", "a", "b"));

    final JSONObject delta = SchemaDelta.compute(base, updated);

    assertThat(delta.getJSONObject(SchemaDelta.FIELD_MERGE).getJSONObject("types").keySet())
        .containsExactly("Brand_New");
    assertThat(SchemaDelta.apply(base, delta).toString()).isEqualTo(updated.toString());
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void droppingATypeIsCarriedByTheAuthoritativeKeySet() {
    final JSONObject base = schema(41, 30);
    final JSONObject updated = schema(42, 30);
    updated.getJSONObject("types").remove("Type_11");

    final JSONObject delta = SchemaDelta.compute(base, updated);

    assertThat(delta.has(SchemaDelta.FIELD_MERGE))
        .as("a pure removal upserts nothing")
        .isFalse();
    assertThat(delta.getJSONObject(SchemaDelta.FIELD_KEYS).getJSONArray("types").toListOfStrings())
        .doesNotContain("Type_11")
        .hasSize(29);

    final JSONObject merged = SchemaDelta.apply(base, delta);
    assertThat(merged.getJSONObject("types").has("Type_11")).isFalse();
    assertThat(merged.toString()).isEqualTo(updated.toString());
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void aRootSectionAddedAndRemovedIsCarried() {
    final JSONObject base = schema(41, 5);
    final JSONObject updated = schema(42, 5);
    updated.remove("materializedViews");
    updated.put("extensions", new JSONObject().put("ts", new JSONObject().put("enabled", true)));

    final JSONObject delta = SchemaDelta.compute(base, updated);
    final JSONObject merged = SchemaDelta.apply(base, delta);

    assertThat(merged.has("materializedViews")).isFalse();
    assertThat(merged.getJSONObject("extensions").getJSONObject("ts").getBoolean("enabled")).isTrue();
    assertThat(merged.toString()).isEqualTo(updated.toString());
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void aChangeToAnotherSectionCarriesNoTypeNamesAtAll() {
    // The blind spot the second review found: `types` is a map present in both documents, so an unconditional
    // key set put all 800 type names into a delta that only touched `settings`.
    final JSONObject base = schema(41, 800);
    final JSONObject updated = schema(42, 800);
    updated.getJSONObject("settings").put("zoneId", "UTC");

    final JSONObject delta = SchemaDelta.compute(base, updated);

    assertThat(delta.has(SchemaDelta.FIELD_KEYS))
        .as("no section's key set moved, so no key set is worth carrying")
        .isFalse();
    assertThat(delta.toString())
        .as("a settings change must not name a single type")
        .doesNotContain("Type_500");
    assertThat(delta.toString().length())
        .as("and must not be sized by the schema")
        .isLessThan(updated.toString().length() / 100);

    assertThat(SchemaDelta.apply(base, delta).toString()).isEqualTo(updated.toString());
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void aKeySetIsCarriedOnlyForTheSectionWhoseShapeMoved() {
    final JSONObject base = schema(41, 60);
    final JSONObject updated = schema(42, 60);
    updated.getJSONObject("types").put("Brand_New", type("Brand_New", "a"));
    updated.getJSONObject("triggers").put("onCreate", new JSONObject().put("sql", "SELECT 1"));

    final JSONObject delta = SchemaDelta.compute(base, updated);

    assertThat(delta.getJSONObject(SchemaDelta.FIELD_KEYS).keySet())
        .as("only the two sections that gained a child")
        .containsExactlyInAnyOrder("types", "triggers");

    assertThat(SchemaDelta.apply(base, delta).toString()).isEqualTo(updated.toString());
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void aChangeConfinedToOneTypeCarriesNoKeySetEither() {
    final JSONObject base = schema(41, 400);
    final JSONObject updated = schema(42, 400);
    updated.getJSONObject("types").put("Type_7", type("Type_7", "id", "name", "payload", "extra"));

    final JSONObject delta = SchemaDelta.compute(base, updated);

    assertThat(delta.has(SchemaDelta.FIELD_KEYS))
        .as("no type was added or dropped, so the key set says nothing apply() needs")
        .isFalse();
    assertThat(delta.toString().length())
        .as("which makes the entry proportional to the one type that changed")
        .isLessThan(updated.toString().length() / 50);

    assertThat(SchemaDelta.apply(base, delta).toString()).isEqualTo(updated.toString());
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void settingsChangeIsCarriedPerKey() {
    final JSONObject base = schema(41, 5);
    final JSONObject updated = schema(42, 5);
    updated.getJSONObject("settings").put("zoneId", "UTC");

    final JSONObject delta = SchemaDelta.compute(base, updated);

    assertThat(delta.getJSONObject(SchemaDelta.FIELD_MERGE).getJSONObject("settings").keySet())
        .containsExactly("zoneId");
    assertThat(SchemaDelta.apply(base, delta).toString()).isEqualTo(updated.toString());
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void applyDoesNotMutateTheDocumentItIsGiven() {
    final JSONObject base = schema(41, 5);
    final String beforeApply = base.toString();
    final JSONObject updated = schema(42, 5);
    updated.getJSONObject("types").put("Type_2", type("Type_2", "id", "extra"));

    SchemaDelta.apply(base, SchemaDelta.compute(base, updated));

    assertThat(base.toString()).isEqualTo(beforeApply);
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void applyIsStructureAuthoritativeOverAReceiverThatDrifted() {
    final JSONObject base = schema(41, 10);
    final JSONObject updated = schema(42, 10);
    updated.getJSONObject("types").remove("Type_3");
    updated.getJSONObject("types").put("Type_4", type("Type_4", "id", "renamed"));

    // A receiver carrying a type the leader never had, and missing one it does have.
    final JSONObject drifted = schema(41, 10);
    drifted.getJSONObject("types").put("Phantom", type("Phantom", "x"));
    drifted.getJSONObject("types").remove("Type_8");

    final JSONObject merged = SchemaDelta.apply(drifted, SchemaDelta.compute(base, updated));

    assertThat(merged.getJSONObject("types").keySet())
        .as("the result carries exactly the leader's key set")
        .doesNotContain("Phantom", "Type_3")
        .contains("Type_4");
    assertThat(merged.getJSONObject("types").getJSONObject("Type_4").toString())
        .isEqualTo(updated.getJSONObject("types").getJSONObject("Type_4").toString());
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void anEmptyChangeStillRoundTrips() {
    final JSONObject base = schema(41, 8);
    final JSONObject updated = schema(41, 8);

    final JSONObject delta = SchemaDelta.compute(base, updated);

    assertThat(delta.has(SchemaDelta.FIELD_PUT)).isFalse();
    assertThat(delta.has(SchemaDelta.FIELD_MERGE)).isFalse();
    assertThat(SchemaDelta.apply(base, delta).toString()).isEqualTo(updated.toString());
  }

  // Regression for #6989: a SCHEMA_ENTRY must be able to carry what a DDL changed rather than the whole schema document.
  @Test
  void theDeltaSurvivesSerializationRoundTrip() {
    final JSONObject base = schema(41, 40);
    final JSONObject updated = schema(42, 40);
    updated.getJSONObject("types").put("Type_9", type("Type_9", "id", "name", "payload", "extra"));

    final String wire = SchemaDelta.compute(base, updated).toString();

    assertThat(SchemaDelta.apply(base, new JSONObject(wire)).toString()).isEqualTo(updated.toString());
  }

  // ---- #8206 ----
  // a follower writes the document {@link SchemaDelta#apply} returns to its {@code schema.json} verbatim,
  // so the merged document must carry the leader's key ORDER as well as its key set. Before the fix a child the delta
  // added was appended after the receiver's own children, and a follower's schema file listed a new type last while
  // the leader's listed it in its own place.

  private static JSONObject schemaOf(final long version, final String... typeNames) {
    final JSONObject root = new JSONObject();
    root.put("schemaVersion", version);
    root.put("dbmsVersion", "26.10.1");

    final JSONObject types = new JSONObject();
    for (final String name : typeNames)
      types.put(name, new JSONObject().put("type", "v").put("name", name));
    root.put("types", types);

    root.put("triggers", new JSONObject());
    root.put("functions", new JSONObject());
    return root;
  }

  // Regression for #8206: a follower writes the document SchemaDelta#apply returns to its schema.json verbatim, so the merged document must carry the leader's key ORDER as w...
  @Test
  void aTypeAddedInTheMiddleKeepsTheLeadersPosition() {
    final JSONObject base = schemaOf(1, "E1", "Person", "V1");
    final JSONObject updated = schemaOf(2, "E1", "Person", "RaftIndexedVertex0", "V1");

    final JSONObject merged = SchemaDelta.apply(base, SchemaDelta.compute(base, updated));

    assertThat(merged.toString()).isEqualTo(updated.toString());
  }

  // Regression for #8206: a follower writes the document SchemaDelta#apply returns to its schema.json verbatim, so the merged document must carry the leader's key ORDER as w...
  @Test
  void aDropFollowedByACreateKeepsTheLeadersOrder() {
    final JSONObject base = schemaOf(1, "E1", "RaftRuntimeVertex0", "V1");
    final JSONObject updated = schemaOf(2, "E1", "RaftIndexedVertex0", "V1");

    final JSONObject merged = SchemaDelta.apply(base, SchemaDelta.compute(base, updated));

    assertThat(merged.toString()).isEqualTo(updated.toString());
  }

  // Regression for #8206: a follower writes the document SchemaDelta#apply returns to its schema.json verbatim, so the merged document must carry the leader's key ORDER as w...
  @Test
  void twoSectionsMovingTogetherWithARemovalInTheMiddleKeepTheLeadersOrder() {
    final JSONObject base = schemaOf(1, "A", "B", "C", "D");
    base.getJSONObject("triggers").put("t1", new JSONObject().put("name", "t1")).put("t3", new JSONObject().put("name", "t3"));

    final JSONObject updated = schemaOf(2, "A", "AB", "C", "D");
    updated.getJSONObject("triggers").put("t1", new JSONObject().put("name", "t1")).put("t2", new JSONObject().put("name", "t2"))
        .put("t3", new JSONObject().put("name", "t3"));

    final JSONObject merged = SchemaDelta.apply(base, SchemaDelta.compute(base, updated));

    assertThat(merged.getJSONObject("types").keySet()).as("B, unlisted by the leader, is dropped").doesNotContain("B");
    assertThat(merged.toString()).isEqualTo(updated.toString());
  }

  // Regression for #8206: a follower writes the document SchemaDelta#apply returns to its schema.json verbatim, so the merged document must carry the leader's key ORDER as w...
  @Test
  void aReceiverMissingAListedTypeKeepsTheLeadersOrderForTheRest() {
    final JSONObject base = schemaOf(1, "A", "C", "D");
    final JSONObject updated = schemaOf(2, "A", "B", "C", "D");

    // A receiver behind the leader's base: it never got C. The key set lists C, but there is nothing to backfill it
    // from, so it is skipped and the rest still follows the leader's order.
    final JSONObject receiver = schemaOf(1, "D", "A");

    final JSONObject merged = SchemaDelta.apply(receiver, SchemaDelta.compute(base, updated));

    assertThat(merged.getJSONObject("types").keySet()).containsExactly("A", "B", "D");
  }

  // Regression for #8206: a follower writes the document SchemaDelta#apply returns to its schema.json verbatim, so the merged document must carry the leader's key ORDER as w...
  @Test
  void aRootSectionAddedBeforeAnExistingOneKeepsTheLeadersPosition() {
    final JSONObject base = schemaOf(1, "V1");
    final JSONObject updated = new JSONObject();
    updated.put("schemaVersion", 2L);
    updated.put("dbmsVersion", "26.10.1");
    updated.put("extensions", new JSONObject().put("module", new JSONObject().put("on", true)));
    updated.put("types", base.getJSONObject("types").copy());
    updated.put("triggers", new JSONObject());
    updated.put("functions", new JSONObject());

    final JSONObject merged = SchemaDelta.apply(base, SchemaDelta.compute(base, updated));

    assertThat(merged.toString()).isEqualTo(updated.toString());
  }
}
