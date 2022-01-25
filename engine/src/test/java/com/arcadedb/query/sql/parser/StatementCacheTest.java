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
package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StatementCacheTest {

  @Test
  public void testInIsNotAReservedWord() {
    StatementCache cache = new StatementCache(null,2);
    cache.get("select from foo");
    cache.get("select from bar");
    cache.get("select from baz");

    Assertions.assertTrue(cache.contains("select from bar"));
    Assertions.assertTrue(cache.contains("select from baz"));
    Assertions.assertFalse(cache.contains("select from foo"));

    cache.get("select from bar");
    cache.get("select from foo");

    Assertions.assertTrue(cache.contains("select from bar"));
    Assertions.assertTrue(cache.contains("select from foo"));
    Assertions.assertFalse(cache.contains("select from baz"));
  }
}
