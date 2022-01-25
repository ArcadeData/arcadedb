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

import com.arcadedb.query.sql.parser.InOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class InOperatorTest {
  @Test
  public void test() {
    InOperator op = new InOperator(-1);

    Assertions.assertFalse(op.execute(null, null, null));
    Assertions.assertFalse(op.execute(null, null, "foo"));
    Assertions.assertFalse(op.execute(null, "foo", null));
    Assertions.assertFalse(op.execute(null, "foo", "foo"));

    List<Object> list1 = new ArrayList<Object>();
    Assertions.assertFalse(op.execute(null, "foo", list1));
    Assertions.assertFalse(op.execute(null, null, list1));
    Assertions.assertTrue(op.execute(null, list1, list1));

    list1.add("a");
    list1.add(1);

    Assertions.assertFalse(op.execute(null, "foo", list1));
    Assertions.assertTrue(op.execute(null, "a", list1));
    Assertions.assertTrue(op.execute(null, 1, list1));

    // TODO
  }
}
