/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.query.sql.method;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.method.misc.SQLMethodAsList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Tests the "asList()" method implemented by the OSQLMethodAsList class. Note that the only input
 * to the execute() method from the OSQLMethod interface that is used is the ioResult argument (the
 * 4th argument).
 *
 * @author Michael MacFadden
 */
public class SQLMethodAsListTest {

  private SQLMethodAsList function;

  @BeforeEach
  public void setup() {
    function = new SQLMethodAsList();
  }

  @Test
  public void testList() {
    // The expected behavior is to return the list itself.
    ArrayList<Object> aList = new ArrayList<Object>();
    aList.add(1);
    aList.add("2");
    Object result = function.execute(null, null, null, aList, null);
    Assertions.assertEquals(result, aList);
  }

  @Test
  public void testNull() {
    // The expected behavior is to return an empty list.
    Object result = function.execute(null, null, null, null, null);
    Assertions.assertEquals(result, new ArrayList<Object>());
  }

  @Test
  public void testCollection() {
    // The expected behavior is to return a list with all of the elements
    // of the collection in it.
    Set<Object> aCollection = new LinkedHashSet<Object>();
    aCollection.add(1);
    aCollection.add("2");
    Object result = function.execute(null, null, null, aCollection, null);

    ArrayList<Object> expected = new ArrayList<Object>();
    expected.add(1);
    expected.add("2");
    Assertions.assertEquals(result, expected);
  }

  public void testIterable() {
    // The expected behavior is to return a list with all of the elements
    // of the iterable in it, in order of the collecitons iterator.
    ArrayList<Object> expected = new ArrayList<Object>();
    expected.add(1);
    expected.add("2");

    TestIterable<Object> anIterable = new TestIterable<Object>(expected);
    Object result = function.execute(null, null, null, anIterable, null);

    Assertions.assertEquals(result, expected);
  }

  public void testIterator() {
    // The expected behavior is to return a list with all of the elements
    // of the iterator in it, in order of the iterator.
    ArrayList<Object> expected = new ArrayList<Object>();
    expected.add(1);
    expected.add("2");

    TestIterable<Object> anIterable = new TestIterable<Object>(expected);
    Object result = function.execute(null, null, null, anIterable.iterator(), null);

    Assertions.assertEquals(result, expected);
  }

  public void testODocument() {
    // The expected behavior is to return a list with only the single
    // ODocument in it.
    MutableDocument doc = new MutableDocument(null, null, null) {
    };

    doc.set("f1", 1);
    doc.set("f2", 2);

    Object result = function.execute(null, null, null, doc, null);

    ArrayList<Object> expected = new ArrayList<Object>();
    expected.add(doc);

    Assertions.assertEquals(result, expected);
  }

  public void testOtherSingleValue() {
    // The expected behavior is to return a list with only the single
    // element in it.

    Object result = function.execute(null, null, null, Integer.valueOf(4), null);
    ArrayList<Object> expected = new ArrayList<Object>();
    expected.add(Integer.valueOf(4));
    Assertions.assertEquals(result, expected);
  }
}
