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
package com.arcadedb;

import com.arcadedb.database.DetachedDocument;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;


public class DocumentTest extends TestHelper {
  @Override
  public void beginTest() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("ConversionTest");

      type.createProperty("string", Type.STRING);
      type.createProperty("int", Type.INTEGER);
      type.createProperty("long", Type.LONG);
      type.createProperty("float", Type.FLOAT);
      type.createProperty("double", Type.DOUBLE);
      type.createProperty("decimal", Type.DECIMAL);
      type.createProperty("date", Type.DATE);
      type.createProperty("datetime_second", Type.DATETIME_SECOND);
      type.createProperty("datetime_millis", Type.DATETIME);
      type.createProperty("datetime_micros", Type.DATETIME_MICROS);
      type.createProperty("datetime_nanos", Type.DATETIME_NANOS);
    });
  }

  @Test
  public void testDetached() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ConversionTest");
      doc.set("name", "Tim");
      final EmbeddedDocument embeddedObj = (EmbeddedDocument) doc.newEmbeddedDocument("ConversionTest", "embeddedObj").set("embeddedObj", true);

      final List<EmbeddedDocument> embeddedList = new ArrayList<>();
      doc.set("embeddedList", embeddedList);
      doc.newEmbeddedDocument("ConversionTest", "embeddedList").set("embeddedList", true);

      final Map<String, EmbeddedDocument> embeddedMap = new HashMap<>();
      doc.set("embeddedMap", embeddedMap);
      doc.newEmbeddedDocument("ConversionTest", "embeddedMap", "first").set("embeddedMap", true);

      final DetachedDocument detached = doc.detach();
      doc.save();
      doc.reload();
      doc.detach();

      assertThat(detached.getString("name")).isEqualTo("Tim");
      assertThat(detached.get("embeddedObj")).isEqualTo(embeddedObj);
      assertThat(detached.get("embeddedList")).isEqualTo(embeddedList);
      assertThat(detached.get("embeddedMap")).isEqualTo(embeddedMap);
      assertThat(detached.getString("lastname")).isNull();

      final Set<String> props = detached.getPropertyNames();
      assertThat(props).hasSize(4);
      assertThat(props.contains("name")).isTrue();
      assertThat(props.contains("embeddedObj")).isTrue();
      assertThat(props.contains("embeddedList")).isTrue();
      assertThat(props.contains("embeddedMap")).isTrue();

      final Map<String, Object> map = detached.toMap();
      assertThat(map).hasSize(6);

      assertThat(map.get("name")).isEqualTo("Tim");
      assertThat(map.get("embeddedObj")).isEqualTo(embeddedObj);
      assertThat(((DetachedDocument) map.get("embeddedObj")).getBoolean("embeddedObj")).isTrue();
      assertThat(map.get("embeddedList")).isEqualTo(embeddedList);
      assertThat(((List<DetachedDocument>) map.get("embeddedList")).get(0).getBoolean("embeddedList")).isTrue();
      assertThat(map.get("embeddedMap")).isEqualTo(embeddedMap);
      assertThat(((Map<String, DetachedDocument>) map.get("embeddedMap")).get("first").getBoolean("embeddedMap")).isTrue();

      assertThat(detached.toJSON().get("name")).isEqualTo("Tim");

      detached.toString();

      try {
        detached.modify();
        fail("modify");
      } catch (final UnsupportedOperationException e) {
      }

      detached.reload();

      try {
        detached.setBuffer(null);
        fail("setBuffer");
      } catch (final UnsupportedOperationException e) {
      }

      assertThat(detached.getString("name")).isNull();
      assertThat(detached.getString("lastname")).isNull();
    });
  }
}
