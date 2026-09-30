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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8286: a stream of one-off Cypher texts (values embedded in the query) must not evict the parameterized statement the
 * application keeps running.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherStatementCacheChurnTest extends TestHelper {

  @Test
  void oneOffStatementsDoNotEvictAHotStatement() {
    final CypherStatementCache cache = new CypherStatementCache(database, 50);
    final String hot = "MATCH (p:Person {id: $id}) RETURN p.name";
    final Object first = cache.getParsed(hot);
    assertThat(cache.getParsed(hot)).isSameAs(first);

    for (int i = 0; i < 300; i++)
      cache.getParsed("MATCH (p:Person {id: " + i + "}) RETURN p.name");

    assertThat(cache.contains(hot)).isTrue();
    assertThat(cache.getParsed(hot)).isSameAs(first);
    assertThat(cache.size()).isEqualTo(50);
  }
}
