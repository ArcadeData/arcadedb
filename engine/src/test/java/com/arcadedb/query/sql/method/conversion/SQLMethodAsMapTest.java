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
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.schema.LocalVertexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the "asMap()" method implemented by the OSQLMethodAsMap class. Note that the only input to
 * the execute() method from the OSQLMethod interface that is used is the ioResult argument (the 4th
 * argument).
 *
 * @author Michael MacFadden
 */
public class SQLMethodAsMapTest {

  private SQLMethod function;

  @BeforeEach
  public void setup() {
    function = new SQLMethodAsMap();
  }

  @Test
  public void testNull() {
    // The expected behavior is to return an empty map.
    final Object result = function.execute(null, null, null, null);
    assertThat(new HashMap<Object, Object>()).isEqualTo(result);
  }

  @Test
  public void testMap() {
    // The expected behavior is to return the map itself.
    final HashMap<Object, Object> aMap = new HashMap<Object, Object>();
    aMap.put("p1", 1);
    aMap.put("p2", 2);
    final Object result = function.execute(aMap, null, null, null);
    assertThat(aMap).isEqualTo(result);
  }

  @Test
  public void testDocument() {
    // The expected behavior is to return a map that has the field names mapped
    // to the field values of the ODocument.
    final MutableDocument doc = new MutableDocument(null, new LocalVertexType(null, "Test"), null) {
    };

    doc.set("f1", 1);
    doc.set("f2", 2);

    final Object result = function.execute(doc, null, null, null);

    assertThat(result).isEqualTo(doc.toMap(false));
  }

  @Test
  public void testIterable() {
    // The expected behavior is to return a map where the even values (0th,
    // 2nd, 4th, etc) are keys and the odd values (1st, 3rd, etc.) are
    // property values.
    final ArrayList<Object> aCollection = new ArrayList<Object>();
    aCollection.add("p1");
    aCollection.add(1);
    aCollection.add("p2");
    aCollection.add(2);

    final Object result = function.execute(aCollection, null, null, null);

    final HashMap<Object, Object> expected = new HashMap<Object, Object>();
    expected.put("p1", 1);
    expected.put("p2", 2);
    assertThat(expected).isEqualTo(result);
  }

  @Test
  public void testIterator() {
    // The expected behavior is to return a map where the even values (0th,
    // 2nd, 4th, etc) are keys and the odd values (1st, 3rd, etc.) are
    // property values.
    final ArrayList<Object> aCollection = new ArrayList<Object>();
    aCollection.add("p1");
    aCollection.add(1);
    aCollection.add("p2");
    aCollection.add(2);

    final Object result = function.execute(aCollection.iterator(), null, null, null);

    final HashMap<Object, Object> expected = new HashMap<Object, Object>();
    expected.put("p1", 1);
    expected.put("p2", 2);
    assertThat(expected).isEqualTo(result);
  }

  @Test
  public void testOtherValue() {
    // The expected behavior is to return null.
    final Object result = function.execute(Integer.valueOf(4), null, null, null);
    assertThat(result).isNull();
  }
}
