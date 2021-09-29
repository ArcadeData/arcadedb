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
 */
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SQLMethodKeysTest {

    private SQLMethod function;

    @BeforeEach
    public void setup() {
        function = new SQLMethodKeys();
    }

    @Test
    public void testWithResult() {

        ResultInternal resultInternal = new ResultInternal();
        resultInternal.setProperty("name", "Foo");
        resultInternal.setProperty("surname", "Bar");

        Object result = function.execute(null, null, null, resultInternal, null);
        assertEquals(new LinkedHashSet(Arrays.asList("name", "surname")), result);
    }
}
