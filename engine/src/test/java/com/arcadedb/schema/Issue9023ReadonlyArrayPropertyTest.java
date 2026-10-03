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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ValidationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9023: a READONLY array-valued property (BINARY, ARRAY_OF_*) blocked every later update of its record, even of
 * other properties, because the stored value was compared with the current one by reference.
 */
class Issue9023ReadonlyArrayPropertyTest extends TestHelper {

  static Stream<Arguments> arrayTypes() {
    return Stream.of(//
        Arguments.of(Type.BINARY, (Supplier<Object>) () -> new byte[] { 1, 2, 3 }, (Supplier<Object>) () -> new byte[] { 1, 2, 4 }),//
        Arguments.of(Type.ARRAY_OF_SHORTS, (Supplier<Object>) () -> new short[] { 1, 2 }, (Supplier<Object>) () -> new short[] { 1, 3 }),//
        Arguments.of(Type.ARRAY_OF_INTEGERS, (Supplier<Object>) () -> new int[] { 1, 2 }, (Supplier<Object>) () -> new int[] { 1, 3 }),//
        Arguments.of(Type.ARRAY_OF_LONGS, (Supplier<Object>) () -> new long[] { 1L, 2L }, (Supplier<Object>) () -> new long[] { 1L, 3L }),//
        Arguments.of(Type.ARRAY_OF_FLOATS, (Supplier<Object>) () -> new float[] { 0.1f, 0.2f },
            (Supplier<Object>) () -> new float[] { 0.1f, 0.3f }),//
        Arguments.of(Type.ARRAY_OF_DOUBLES, (Supplier<Object>) () -> new double[] { 0.1d, 0.2d },
            (Supplier<Object>) () -> new double[] { 0.1d, 0.3d }));
  }

  @ParameterizedTest
  @MethodSource("arrayTypes")
  void updateOfOtherPropertyIsAcceptedThroughSql(final Type type, final Supplier<Object> value, final Supplier<Object> other) {
    final RID rid = createRecord(type, value.get());

    database.transaction(() -> database.command("sql", "UPDATE " + rid + " SET note = 'b'").close());

    assertThat(database.lookupByRID(rid, true).asDocument().getString("note")).isEqualTo("b");
  }

  @ParameterizedTest
  @MethodSource("arrayTypes")
  void updateOfOtherPropertyIsAcceptedThroughApi(final Type type, final Supplier<Object> value, final Supplier<Object> other) {
    final RID rid = createRecord(type, value.get());

    database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("note", "c").save());

    assertThat(database.lookupByRID(rid, true).asDocument().getString("note")).isEqualTo("c");
  }

  @ParameterizedTest
  @MethodSource("arrayTypes")
  void settingAContentEqualArrayIsAccepted(final Type type, final Supplier<Object> value, final Supplier<Object> other) {
    final RID rid = createRecord(type, value.get());

    database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("p", value.get()).set("note", "d").save());

    assertThat(database.lookupByRID(rid, true).asDocument().getString("note")).isEqualTo("d");
  }

  @ParameterizedTest
  @MethodSource("arrayTypes")
  void changingTheArrayContentIsStillRefused(final Type type, final Supplier<Object> value, final Supplier<Object> other) {
    final RID rid = createRecord(type, value.get());

    assertThatThrownBy(
        () -> database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("p", other.get()).save()))//
        .isInstanceOf(ValidationException.class)//
        .hasMessageContaining("is immutable");

    assertThatThrownBy(
        () -> database.transaction(() -> {
          final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
          doc.remove("p");
          doc.save();
        }))//
        .isInstanceOf(ValidationException.class)//
        .hasMessageContaining("is immutable");
  }

  @Test
  void refusalMessagePrintsTheArrayContent() {
    final RID rid = createRecord(Type.ARRAY_OF_INTEGERS, new int[] { 1, 2 });

    assertThatThrownBy(() -> database.transaction(
        () -> database.lookupByRID(rid, true).asDocument().modify().set("p", new int[] { 7, 8 }).save()))//
        .isInstanceOf(ValidationException.class)//
        .hasMessageContaining("[7, 8]");
  }

  @Test
  void readonlyListHoldingAnArrayDoesNotBlockOtherUpdates() {
    final List<Object> list = new ArrayList<>();
    list.add(new byte[] { 1, 2 });
    list.add("x");
    final RID rid = createRecord(Type.LIST, list);

    database.transaction(() -> database.command("sql", "UPDATE " + rid + " SET note = 'b'").close());
    assertThat(database.lookupByRID(rid, true).asDocument().getString("note")).isEqualTo("b");

    final List<Object> changed = new ArrayList<>();
    changed.add(new byte[] { 1, 3 });
    changed.add("x");
    assertThatThrownBy(() -> database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("p", changed).save()))//
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void readonlyMapHoldingAnArrayDoesNotBlockOtherUpdates() {
    final Map<String, Object> map = new HashMap<>();
    map.put("k", new float[] { 0.5f, 1.5f });
    final RID rid = createRecord(Type.MAP, map);

    database.transaction(() -> database.command("sql", "UPDATE " + rid + " SET note = 'b'").close());
    assertThat(database.lookupByRID(rid, true).asDocument().getString("note")).isEqualTo("b");

    final Map<String, Object> changed = new HashMap<>();
    changed.put("k", new float[] { 0.5f, 2.5f });
    assertThatThrownBy(() -> database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("p", changed).save()))//
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void readonlyEmbeddedHoldingAnArrayDoesNotBlockOtherUpdates() {
    database.getSchema().createDocumentType("Inner");
    final DocumentType type = database.getSchema().createDocumentType("T_EMBEDDED");
    type.createProperty("p", Type.EMBEDDED).setReadonly(true);

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("T_EMBEDDED");
      doc.newEmbeddedDocument("Inner", "p").set("bytes", new byte[] { 9, 8, 7 });
      doc.set("note", "a");
      rid[0] = doc.save().getIdentity();
    });

    database.transaction(() -> database.command("sql", "UPDATE " + rid[0] + " SET note = 'b'").close());
    database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().modify().set("note", "c").save());

    final Document reloaded = database.lookupByRID(rid[0], true).asDocument();
    assertThat(reloaded.getString("note")).isEqualTo("c");
  }

  @Test
  void updateOfOtherPropertyIsAcceptedThroughCypherSet() {
    database.getSchema().createVertexType("V9023").createProperty("p", Type.ARRAY_OF_FLOATS).setReadonly(true);
    database.transaction(() -> database.newVertex("V9023").set("p", new float[] { 0.1f, 0.2f }).set("note", "a").save());

    database.transaction(() -> database.command("opencypher", "MATCH (n:V9023) SET n.note = 'b'").close());

    assertThat(database.query("sql", "SELECT note FROM V9023").next().<String>getProperty("note")).isEqualTo("b");
  }

  private RID createRecord(final Type type, final Object value) {
    final String typeName = "T_" + type.name();
    database.getSchema().createDocumentType(typeName).createProperty("p", type).setReadonly(true);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(typeName).set("p", value).set("note", "a").save().getIdentity());
    return rid[0];
  }
}
