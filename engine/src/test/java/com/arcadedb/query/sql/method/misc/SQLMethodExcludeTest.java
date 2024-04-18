/*
 * Copyright 2022 Arcade Data Ltd
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
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

class SQLMethodExcludeTest {

  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodExclude();
  }

  @Test
  void testFieldValue() {

    final ResultInternal resultInternal = new ResultInternal();
    resultInternal.setProperty("name", "Foo");
    resultInternal.setProperty("surname", "Bar");

    final Object result = method.execute(resultInternal, null, null, new Object[] { "name" });
    Assertions.assertNotNull(result);
    Assertions.assertTrue(((Map) result).containsKey("surname"));
    Assertions.assertFalse(((Map) result).containsKey("name"));
    Assertions.assertEquals("Bar", ((Map) result).get("surname"));
  }
}
