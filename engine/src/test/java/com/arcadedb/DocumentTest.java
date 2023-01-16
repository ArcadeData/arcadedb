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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class DocumentTest extends TestHelper {

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

      final Set<String> props = detached.getPropertyNames();
      Assertions.assertEquals(4, props.size());
      Assertions.assertTrue(props.contains("name"));
      Assertions.assertTrue(props.contains("embeddedObj"));
      Assertions.assertTrue(props.contains("embeddedList"));
      Assertions.assertTrue(props.contains("embeddedMap"));

      final Map<String, Object> map = detached.toMap();
      Assertions.assertEquals(6, map.size());

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
      } catch (final UnsupportedOperationException e) {
      }

      detached.reload();

      try {
        detached.setBuffer(null);
        Assertions.fail("setBuffer");
      } catch (final UnsupportedOperationException e) {
      }

      Assertions.assertNull(detached.getString("name"));
      Assertions.assertNull(detached.getString("lastname"));
    });
  }
}
