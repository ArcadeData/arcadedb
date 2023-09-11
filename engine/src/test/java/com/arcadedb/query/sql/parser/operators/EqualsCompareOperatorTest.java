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

import com.arcadedb.database.RID;
import com.arcadedb.query.sql.parser.EqualsCompareOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.*;
import java.util.*;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class EqualsCompareOperatorTest {
  @Test
  public void test() {
    final EqualsCompareOperator op = new EqualsCompareOperator(-1);

    Assertions.assertFalse(op.execute(null, null, 1));
    Assertions.assertFalse(op.execute(null, 1, null));
    Assertions.assertFalse(op.execute(null, null, null));

    Assertions.assertTrue(op.execute(null, 1, 1));
    Assertions.assertFalse(op.execute(null, 1, 0));
    Assertions.assertFalse(op.execute(null, 0, 1));

    Assertions.assertFalse(op.execute(null, "aaa", "zzz"));
    Assertions.assertFalse(op.execute(null, "zzz", "aaa"));
    Assertions.assertTrue(op.execute(null, "aaa", "aaa"));

    Assertions.assertFalse(op.execute(null, 1, 1.1));
    Assertions.assertFalse(op.execute(null, 1.1, 1));

    Assertions.assertTrue(op.execute(null, BigDecimal.ONE, 1));
    Assertions.assertTrue(op.execute(null, 1, BigDecimal.ONE));

    Assertions.assertFalse(op.execute(null, 1.1, BigDecimal.ONE));
    Assertions.assertFalse(op.execute(null, 2, BigDecimal.ONE));

    Assertions.assertFalse(op.execute(null, BigDecimal.ONE, 0.999999));
    Assertions.assertFalse(op.execute(null, BigDecimal.ONE, 0));

    Assertions.assertFalse(op.execute(null, BigDecimal.ONE, 2));
    Assertions.assertFalse(op.execute(null, BigDecimal.ONE, 1.0001));

    Assertions.assertTrue(op.execute(null, new RID(null, 1, 10), new RID(null, (short) 1, 10)));
    Assertions.assertFalse(op.execute(null, new RID(null, 1, 10), new RID(null, (short) 1, 20)));

    Assertions.assertFalse(op.execute(null, new Object(), new Object()));

    // MAPS
    Assertions.assertTrue(op.execute(null, Map.of("a", "b"), Map.of("a", "b")));

    Assertions.assertTrue(op.execute(null, Map.of("a", "b", "c", 3), Map.of("a", "b", "c", 3)));
    Assertions.assertFalse(op.execute(null, Map.of("a", "b", "c", 3), Map.of("a", "b")));
  }
}
