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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10905: an element row carries an {@code @props} hint only for a property the schema does not declare whose
 * value is a non-finite number (or a collection holding one), so the remote client can tell the marker string from text.
 */
class Issue10905NonFiniteElementRowHintTest extends TestHelper {

  private JSONObject serialize(final MutableDocument doc, final boolean hints) {
    return new JsonSerializer(database).setIncludeTypeHints(hints).serializeResult(database, new ResultInternal(doc));
  }

  private MutableDocument document() {
    database.getSchema().createDocumentType("Rec").createProperty("declared", Type.DOUBLE);
    return database.newDocument("Rec");
  }

  @Test
  void undeclaredNonFiniteGetsAHint() {
    final MutableDocument doc = document();
    doc.set("declared", Double.NaN).set("u", Double.POSITIVE_INFINITY).set("f", Float.NEGATIVE_INFINITY);
    final JSONObject json = serialize(doc, true);
    assertThat(json.getString("@props")).isEqualTo("u:" + Type.DOUBLE.getId() + ",f:" + Type.FLOAT.getId());
  }

  @Test
  void mixedCollectionGetsADoubleElementHint() {
    final MutableDocument doc = document();
    final List<Object> list = new ArrayList<>();
    list.add(Float.NaN);
    list.add(1.5D);
    list.add(2);
    doc.set("l", list);
    assertThat(serialize(doc, true).getString("@props")).isEqualTo("l:" + Type.LIST.getId() + "(" + Type.DOUBLE.getId() + ")");
  }

  @Test
  void collectionWithAnElementADoubleCannotHoldGetsNoHint() {
    final MutableDocument doc = document();
    final List<Object> list = new ArrayList<>();
    list.add(Double.NaN);
    list.add("text");
    doc.set("l", list);
    assertThat(serialize(doc, true).has("@props")).isFalse();
  }

  @Test
  void collectionWithALongADoubleCannotHoldExactlyGetsNoHint() {
    // (double) Long.MAX_VALUE is 2^63, which the cast back to long saturates to Long.MAX_VALUE: the round trip only looks exact
    final MutableDocument doc = document();
    final List<Object> list = new ArrayList<>();
    list.add(Double.NaN);
    list.add(Long.MAX_VALUE);
    doc.set("l", list);
    assertThat(serialize(doc, true).has("@props")).isFalse();
  }

  @Test
  void integerKindsADoubleHoldsExactlyKeepTheDoubleElementHint() {
    final MutableDocument doc = document();
    final List<Object> list = new ArrayList<>();
    list.add(Double.NaN);
    list.add((byte) 1);
    list.add((short) 2);
    list.add(3);
    list.add(4L);
    list.add(Long.MIN_VALUE);
    doc.set("l", list);
    assertThat(serialize(doc, true).getString("@props")).isEqualTo("l:" + Type.LIST.getId() + "(" + Type.DOUBLE.getId() + ")");
  }

  @Test
  void rowWithoutAnUndeclaredNonFiniteValueKeepsItsShape() {
    final MutableDocument doc = document();
    doc.set("declared", Double.NaN).set("u", 1.5D).set("s", "NaN");
    assertThat(serialize(doc, true).has("@props")).isFalse();
  }

  @Test
  void noHintWhenHintsAreOff() {
    final MutableDocument doc = document();
    doc.set("u", Double.NaN);
    assertThat(serialize(doc, false).has("@props")).isFalse();
  }
}
