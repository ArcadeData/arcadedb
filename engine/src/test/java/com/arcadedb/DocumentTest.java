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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class DocumentTest extends TestHelper {
  @Override
  public void beginTest() {
    database.transaction(() -> {
      DocumentType type = database.getSchema().createDocumentType("ConversionTest");

      type.createProperty("string", Type.STRING);
      type.createProperty("int", Type.INTEGER);
      type.createProperty("long", Type.LONG);
      type.createProperty("float", Type.FLOAT);
      type.createProperty("double", Type.DOUBLE);
      type.createProperty("decimal", Type.DECIMAL);
      type.createProperty("date", Type.DATE);
      type.createProperty("datetime", Type.DATETIME);
    });
  }

  @Test
  public void testNoConversion() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ConversionTest");

      final Date now = new Date();

      doc.set("string", "test");
      doc.set("int", 33);
      doc.set("long", 33l);
      doc.set("float", 33.33f);
      doc.set("double", 33.33d);
      doc.set("decimal", new BigDecimal("33.33"));
      doc.set("date", now);
      doc.set("datetime", now);

      Assertions.assertEquals(33, doc.get("int"));
      Assertions.assertEquals(33l, doc.get("long"));
      Assertions.assertEquals(33.33f, doc.get("float"));
      Assertions.assertEquals(33.33d, doc.get("double"));
      Assertions.assertEquals(new BigDecimal("33.33"), doc.get("decimal"));
      Assertions.assertEquals(now, doc.get("date"));
      Assertions.assertEquals(now, doc.get("datetime"));
    });
  }

  @Test
  public void testConversionDecimals() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ConversionTest");

      final Date now = new Date();

      doc.set("decimal", "33.33");
      Assertions.assertEquals(new BigDecimal("33.33"), doc.get("decimal"));

      doc.set("decimal", 33.33f);
      Assertions.assertEquals(new BigDecimal("33.33"), doc.get("decimal"));

      doc.set("decimal", 33.33d);
      Assertions.assertEquals(new BigDecimal("33.33"), doc.get("decimal"));
    });
  }

  @Test
  public void testConversionDates() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ConversionTest");

      final Date now = new Date();

      doc.set("date", now.getTime());
      doc.set("datetime", now.getTime());
      Assertions.assertEquals(now, doc.get("date"));
      Assertions.assertEquals(now, doc.get("datetime"));

      doc.set("date", "" + now.getTime());
      doc.set("datetime", "" + now.getTime());
      Assertions.assertEquals(now, doc.get("date"));
      Assertions.assertEquals(now, doc.get("datetime"));

      final SimpleDateFormat df = new SimpleDateFormat(database.getSchema().getDateTimeFormat());

      doc.set("date", df.format(now));
      doc.set("datetime", df.format(now));
      Assertions.assertEquals(df.format(now), df.format(doc.get("date")));
      Assertions.assertEquals(df.format(now), df.format(doc.get("datetime")));
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

      Assertions.assertEquals("Tim", detached.getString("name"));
      Assertions.assertEquals(embeddedObj, detached.get("embeddedObj"));
      Assertions.assertEquals(embeddedList, detached.get("embeddedList"));
      Assertions.assertEquals(embeddedMap, detached.get("embeddedMap"));
      Assertions.assertNull(detached.getString("lastname"));

      Set<String> props = detached.getPropertyNames();
      Assertions.assertEquals(4, props.size());
      Assertions.assertTrue(props.contains("name"));
      Assertions.assertTrue(props.contains("embeddedObj"));
      Assertions.assertTrue(props.contains("embeddedList"));
      Assertions.assertTrue(props.contains("embeddedMap"));

      final Map<String, Object> map = detached.toMap();
      Assertions.assertEquals(4, map.size());

      Assertions.assertEquals("Tim", map.get("name"));
      Assertions.assertEquals(embeddedObj, map.get("embeddedObj"));
      Assertions.assertTrue(((DetachedDocument) map.get("embeddedObj")).getBoolean("embeddedObj"));
      Assertions.assertEquals(embeddedList, map.get("embeddedList"));
      Assertions.assertTrue(((List<DetachedDocument>) map.get("embeddedList")).get(0).getBoolean("embeddedList"));
      Assertions.assertEquals(embeddedMap, map.get("embeddedMap"));
      Assertions.assertTrue(((Map<String, DetachedDocument>) map.get("embeddedMap")).get("first").getBoolean("embeddedMap"));

      Assertions.assertEquals("Tim", detached.toJSON().get("name"));

      detached.toString();

      try {
        detached.modify();
        Assertions.fail("modify");
      } catch (UnsupportedOperationException e) {
      }

      detached.reload();

      try {
        detached.setBuffer(null);
        Assertions.fail("setBuffer");
      } catch (UnsupportedOperationException e) {
      }

      Assertions.assertNull(detached.getString("name"));
      Assertions.assertNull(detached.getString("lastname"));
    });
  }
}
