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
 * Tests the "asSet()" method implemented by the OSQLMethodAsSet class. Note that the only input to
 * the execute() method from the OSQLMethod interface that is used is the ioResult argument (the 4th
 * argument).
 *
 * @author Michael MacFadden
 */
public class SQLMethodAsSetTest {

  private SQLMethod function;

  @BeforeEach
  public void setup() {
    function = new SQLMethodAsSet();
  }

  @Test
  public void testSet() {
    // The expected behavior is to return the set itself.
    final HashSet<Object> aSet = new HashSet<Object>();
    aSet.add(1);
    aSet.add("2");
    final Object result = function.execute(aSet, null, null, null);
    assertThat(aSet).isEqualTo(result);
  }

  @Test
  public void testNull() {
    // The expected behavior is to return an empty set.
    final Object result = function.execute(null, null, null, null);
    assertThat(new HashSet<Object>()).isEqualTo(result);
  }

  @Test
  public void testCollection() {
    // The expected behavior is to return a set with all of the elements
    // of the collection in it.
    final ArrayList<Object> aCollection = new ArrayList<Object>();
    aCollection.add(1);
    aCollection.add("2");
    final Object result = function.execute(aCollection, null, null, null);

    final HashSet<Object> expected = new HashSet<Object>();
    expected.add(1);
    expected.add("2");
    assertThat(expected).isEqualTo(result);
  }

  @Test
  public void testIterable() {
    // The expected behavior is to return a set with all of the elements
    // of the iterable in it.
    final ArrayList<Object> values = new ArrayList<Object>();
    values.add(1);
    values.add("2");

    final TestIterable<Object> anIterable = new TestIterable<Object>(values);
    final Object result = function.execute(anIterable, null, null, null);

    final HashSet<Object> expected = new HashSet<Object>();
    expected.add(1);
    expected.add("2");

    assertThat(expected).isEqualTo(result);
  }

  @Test
  public void testIterator() {
    // The expected behavior is to return a set with all of the elements
    // of the iterator in it.
    final ArrayList<Object> values = new ArrayList<Object>();
    values.add(1);
    values.add("2");

    final TestIterable<Object> anIterable = new TestIterable<Object>(values);
    final Object result = function.execute(anIterable.iterator(), null, null, null);

    final HashSet<Object> expected = new HashSet<Object>();
    expected.add(1);
    expected.add("2");

    assertThat(expected).isEqualTo(result);
  }

  @Test
  public void testDocument() {
    // The expected behavior is to return a set with only the single
    // ODocument in it.
    final MutableDocument doc = new MutableDocument(null, new LocalVertexType(null, "Test"), null) {
    };

    doc.set("f1", 1);
    doc.set("f2", 2);

    final Object result = function.execute(doc, null, null, null);

    final HashSet<Object> expected = new HashSet<Object>();
    expected.add(doc);

    assertThat(expected).isEqualTo(result);
  }

  @Test
  public void testOtherSingleValue() {
    // The expected behavior is to return a set with only the single
    // element in it.

    final Object result = function.execute(Integer.valueOf(4), null, null, null);
    final HashSet<Object> expected = new HashSet<Object>();
    expected.add(Integer.valueOf(4));
    assertThat(expected).isEqualTo(result);
  }
}
