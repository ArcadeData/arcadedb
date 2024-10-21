/*
 * Copyright 2023 Arcade Data Ltd
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
package com.arcadedb.query.sql.method.string;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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

    Object result = function.execute("foobar", null, null, new Object[] { 1, 3 });
    assertThat("foobar".substring(1, 3)).isEqualTo(result);

    result = function.execute("foobar", null, null, new Object[] { 0, 0 });
    assertThat("foobar".substring(0, 0)).isEqualTo(result);

    result = function.execute("foobar", null, null, new Object[] { 0, 1000 });
    assertThat(result).isEqualTo("foobar");

    result = function.execute("foobar", null, null, new Object[] { 0, -1 });
    assertThat(result).isEqualTo("");

    result = function.execute("foobar", null, null, new Object[] { 6, 6 });
    assertThat("foobar".substring(6, 6)).isEqualTo(result);

    result = function.execute("foobar", null, null, new Object[] { 1, 9 });
    assertThat("foobar".substring(1, 6)).isEqualTo(result);

    result = function.execute("foobar", null, null, new Object[] { -7, 4 });
    assertThat("foobar".substring(0, 4)).isEqualTo(result);
  }

  @Test
  public void testFrom() {
    Object result = function.execute("foobar", null, null, new Object[] { 1 });
    assertThat("foobar".substring(1)).isEqualTo(result);

    result = function.execute("foobar", null, null, new Object[] { 0 });
    assertThat(result).isEqualTo("foobar");

    result = function.execute("foobar", null, null, new Object[] { 6 });
    assertThat("foobar".substring(6)).isEqualTo(result);

    result = function.execute("foobar", null, null, new Object[] { 12 });
    assertThat(result).isEqualTo("");

    result = function.execute("foobar", null, null, new Object[] { -7 });
    assertThat(result).isEqualTo("foobar");
  }

  @Test
  public void testNull() {

    final Object result = function.execute(null, null, null, null);
    assertThat(result).isNull();
  }
}
