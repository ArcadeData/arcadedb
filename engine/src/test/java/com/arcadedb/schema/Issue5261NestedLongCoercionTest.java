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
import com.arcadedb.database.MutableDocument;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #5261: a property declared as {@code MAP OF LONG} (or {@code LIST OF LONG}) rejects nested {@code Integer}
 * values that reach the write path from JSON (e.g. the remote client re-serializes a full record with {@code UPDATE ... CONTENT}
 * and small {@code Long} values arrive as untyped JSON integers parsed as {@link Integer}). The engine must coerce the nested
 * values to the declared {@code ofType} before validation, mirroring the top-level scalar coercion.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5261NestedLongCoercionTest extends TestHelper {

  @Test
  void mapOfLongCoercesNestedIntegerValues() {
    database.getSchema().createDocumentType("MapLong5261").createProperty("values", Type.MAP).setOfType("LONG");

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("MapLong5261");

      final Map<String, Object> values = new LinkedHashMap<>();
      // Small values that fit the 32-bit range: this is exactly what JSON parsing produces as Integer.
      values.put("first", 1001);
      values.put("second", 2002);
      doc.set("values", values);

      // No exception must be thrown by validation.
      doc.save();

      final Map<String, Object> stored = (Map<String, Object>) doc.get("values");
      assertThat(stored.get("first")).isInstanceOf(Long.class).isEqualTo(1001L);
      assertThat(stored.get("second")).isInstanceOf(Long.class).isEqualTo(2002L);
    });
  }

  @Test
  void listOfLongCoercesNestedIntegerValues() {
    database.getSchema().createDocumentType("ListLong5261").createProperty("numbers", Type.LIST).setOfType("LONG");

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ListLong5261");
      doc.set("numbers", List.of(10, 20, 30));
      doc.save();

      final List<Object> stored = (List<Object>) doc.get("numbers");
      for (final Object item : stored)
        assertThat(item).isInstanceOf(Long.class);
      assertThat(stored).containsExactly(10L, 20L, 30L);
    });
  }

  @Test
  void mapOfLongViaFromMapCoercesNestedIntegerValues() {
    database.getSchema().createDocumentType("MapLongFromMap5261").createProperty("values", Type.MAP).setOfType("LONG");

    database.transaction(() -> {
      final Map<String, Object> values = new LinkedHashMap<>();
      values.put("first", 1001);
      values.put("second", 2002);

      final Map<String, Object> record = new LinkedHashMap<>();
      record.put("values", values);

      final MutableDocument doc = database.newDocument("MapLongFromMap5261").fromMap(record);
      doc.save();

      final Map<String, Object> stored = (Map<String, Object>) doc.get("values");
      assertThat(stored.get("first")).isInstanceOf(Long.class).isEqualTo(1001L);
      assertThat(stored.get("second")).isInstanceOf(Long.class).isEqualTo(2002L);
    });
  }
}
