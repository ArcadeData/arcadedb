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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arcadedb.query.sql.parser.ContainsCondition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class ContainsConditionTest {
    @Test
    public void test() {
        ContainsCondition op = new ContainsCondition(-1);

        assertFalse(op.execute(null, null));
        assertFalse(op.execute(null, "foo"));

        List<Object> left = new ArrayList<Object>();
        assertFalse(op.execute(left, "foo"));
        assertFalse(op.execute(left, null));

        left.add("foo");
        left.add("bar");

        assertTrue(op.execute(left, "foo"));
        assertTrue(op.execute(left, "bar"));
        assertFalse(op.execute(left, "fooz"));

        left.add(null);
        assertTrue(op.execute(left, null));
    }

    @Test
    public void testIterable() {
        Iterable left = new Iterable() {
            private final List<Integer> ls = Arrays.asList(3, 1, 2);

            @Override
            public Iterator iterator() {
                return ls.iterator();
            }
        };

        Iterable right = new Iterable() {
            private final List<Integer> ls = Arrays.asList(2, 3);

            @Override
            public Iterator iterator() {
                return ls.iterator();
            }
        };

        ContainsCondition op = new ContainsCondition(-1);
        assertTrue(op.execute(left, right));
    }
}
