/*
 *
 *  *  Copyright 2021 Arcade Data Ltd
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: https://arcadedb.com
 *
 */
package com.arcadedb.query.sql.parser.operators;

import com.arcadedb.query.sql.parser.ContainsKeyOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com) */
public class ContainsKeyOperatorTest {

  @Test
  public void test() {
    ContainsKeyOperator op = new ContainsKeyOperator(-1);

    Assertions.assertFalse(op.execute(null,null, null));
    Assertions.assertFalse(op.execute(null,null, "foo"));

    Map<Object, Object> originMap = new HashMap<Object, Object>();
    Assertions.assertFalse(op.execute(null,originMap, "foo"));
    Assertions.assertFalse(op.execute(null,originMap, null));

    originMap.put("foo", "bar");
    originMap.put(1, "baz");

    Assertions.assertTrue(op.execute(null,originMap, "foo"));
    Assertions.assertTrue(op.execute(null,originMap, 1));
    Assertions.assertFalse(op.execute(null,originMap, "fooz"));
  }
}
