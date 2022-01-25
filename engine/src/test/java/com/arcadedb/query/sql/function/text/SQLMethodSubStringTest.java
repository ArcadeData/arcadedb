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
package com.arcadedb.query.sql.function.text;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the "asList()" method implemented by the OSQLMethodAsList class. Note that the only input
 * to the execute() method from the OSQLMethod interface that is used is the ioResult argument (the
 * 4th argument).
 *
 * @author Michael MacFadden
 */
public class SQLMethodSubStringTest {

  private SQLMethodSubString function;

  @BeforeEach
  public void setup() {
    function = new SQLMethodSubString();
  }

  @Test
  public void testRange() {

    Object result = function.execute("foobar", null, null, null, new Object[] { 1, 3 });
    assertEquals(result, "foobar".substring(1, 3));

    result = function.execute("foobar", null, null, null, new Object[] { 0, 0 });
    assertEquals(result, "foobar".substring(0, 0));

    result = function.execute("foobar", null, null, null, new Object[] { 0, 1000 });
    assertEquals(result, "foobar");

    result = function.execute("foobar", null, null, null, new Object[] { 0, -1 });
    assertEquals(result, "");

    result = function.execute("foobar", null, null, null, new Object[] { 6, 6 });
    assertEquals(result, "foobar".substring(6, 6));

    result = function.execute("foobar", null, null, null, new Object[] { 1, 9 });
    assertEquals(result, "foobar".substring(1, 6));

    result = function.execute("foobar", null, null, null, new Object[] { -7, 4 });
    assertEquals(result, "foobar".substring(0, 4));
  }

  @Test
  public void testFrom() {
    Object result = function.execute("foobar", null, null, null, new Object[] { 1 });
    assertEquals(result, "foobar".substring(1));

    result = function.execute("foobar", null, null, null, new Object[] { 0 });
    assertEquals(result, "foobar");

    result = function.execute("foobar", null, null, null, new Object[] { 6 });
    assertEquals(result, "foobar".substring(6));

    result = function.execute("foobar", null, null, null, new Object[] { 12 });
    assertEquals(result, "");

    result = function.execute("foobar", null, null, null, new Object[] { -7 });
    assertEquals(result, "foobar");
  }

  @Test
  public void testNull() {

    Object result = function.execute(null, null, null, null, null);
    Assertions.assertNull(result);
  }
}
