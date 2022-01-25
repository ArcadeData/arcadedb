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
package com.arcadedb.query.sql.method.misc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

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
    public void testMap() {
        // The expected behavior is to return the map itself.
        HashMap<Object, Object> aMap = new HashMap<Object, Object>();
        aMap.put("p1", 1);
        aMap.put("p2", 2);
        Object result = function.execute(null, null, null, aMap, null);
        assertEquals(result, aMap);
    }

    @Test
    public void testNull() {
        // The expected behavior is to return an empty map.
        Object result = function.execute(null, null, null, null, null);
        assertEquals(result, new HashMap<Object, Object>());
    }

    public void testODocument() {
        // The expected behavior is to return a map that has the field names mapped
        // to the field values of the ODocument.
        MutableDocument doc = new MutableDocument(null, null, null) {
        };

        doc.set("f1", 1);
        doc.set("f2", 2);

        Object result = function.execute(null, null, null, doc, null);

        assertEquals(result, doc.toMap());
    }

    @Test
    public void testIterable() {
        // The expected behavior is to return a map where the even values (0th,
        // 2nd, 4th, etc) are keys and the odd values (1st, 3rd, etc.) are
        // property values.
        ArrayList<Object> aCollection = new ArrayList<Object>();
        aCollection.add("p1");
        aCollection.add(1);
        aCollection.add("p2");
        aCollection.add(2);

        Object result = function.execute(null, null, null, aCollection, null);

        HashMap<Object, Object> expected = new HashMap<Object, Object>();
        expected.put("p1", 1);
        expected.put("p2", 2);
        assertEquals(result, expected);
    }

    @Test
    public void testIterator() {
        // The expected behavior is to return a map where the even values (0th,
        // 2nd, 4th, etc) are keys and the odd values (1st, 3rd, etc.) are
        // property values.
        ArrayList<Object> aCollection = new ArrayList<Object>();
        aCollection.add("p1");
        aCollection.add(1);
        aCollection.add("p2");
        aCollection.add(2);

        Object result = function.execute(null, null, null, aCollection.iterator(), null);

        HashMap<Object, Object> expected = new HashMap<Object, Object>();
        expected.put("p1", 1);
        expected.put("p2", 2);
        assertEquals(result, expected);
    }

    @Test
    public void testOtherValue() {
        // The expected behavior is to return null.
        Object result = function.execute(null, null, null, Integer.valueOf(4), null);
        assertEquals(result, null);
    }
}
