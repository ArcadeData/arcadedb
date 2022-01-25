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
package com.arcadedb.query.sql.parser.operators;

import com.arcadedb.query.sql.parser.ContainsValueOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com) */
public class ContainsValueOperatorTest {
  @Test
  public void test() {
    ContainsValueOperator op = new ContainsValueOperator(-1);

    Assertions.assertFalse(op.execute(null,null, null));
    Assertions.assertFalse(op.execute(null,null, "foo"));

    Map<Object, Object> originMap = new HashMap<Object, Object>();
    Assertions.assertFalse(op.execute(null,originMap, "bar"));
    Assertions.assertFalse(op.execute(null,originMap, null));

    originMap.put("foo", "bar");
    originMap.put(1, "baz");
    originMap.put(2, 12);

    Assertions.assertTrue(op.execute(null,originMap, "bar"));
    Assertions.assertTrue(op.execute(null,originMap, "baz"));
    Assertions.assertTrue(op.execute(null,originMap, 12));
    Assertions.assertFalse(op.execute(null,originMap, "asdfafsd"));
  }
}
