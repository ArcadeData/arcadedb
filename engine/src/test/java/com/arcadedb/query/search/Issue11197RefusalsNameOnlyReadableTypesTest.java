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
package com.arcadedb.query.search;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11197: the census over the compiled classes of every module, the engine included, found two refusals that named
 * types the caller may not read - the hybrid search's "Available edge types" list and the algorithm procedures' hint that a
 * name is a property of an edge type - and the second also told a denied type apart from a missing one ("is not a
 * relationship type, it is an edge type"). A type the caller may not read is now answered as one that does not exist.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11197RefusalsNameOnlyReadableTypesTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.transaction(() -> {
      database.getSchema().createVertexType("City");
      database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);
      database.getSchema().createEdgeType("SECRET_LINK").createProperty("secretWeight", Type.DOUBLE);
    });
  }

  @AfterEach
  void unbind() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  @Test
  void theHybridSearchListsOnlyTheReadableEdgeTypes() {
    // control: unbound, every edge type is listed
    assertThat(catchThrowable(() -> HybridSearch.validatedEdgeTypes(database, new JSONArray().put("NOPE"))))
        .hasMessageContaining("SECRET_LINK").hasMessageContaining("ROAD");

    bindUserDenying(Set.of("SECRET_LINK"));

    final Throwable unknown = catchThrowable(() -> HybridSearch.validatedEdgeTypes(database, new JSONArray().put("NOPE")));
    assertThat(unknown).hasMessageContaining("ROAD").hasMessageNotContaining("SECRET_LINK");

    // the denied type is answered exactly as a missing one
    assertThatThrownBy(() -> HybridSearch.validatedEdgeTypes(database, new JSONArray().put("SECRET_LINK")))
        .hasMessageContaining("Edge type 'SECRET_LINK' does not exist").hasMessageNotContaining("SECRET_LINK]");
    assertThat(HybridSearch.validatedEdgeTypes(database, new JSONArray().put("ROAD"))).containsExactly("ROAD");
  }

  @Test
  void theAlgorithmRefusalNamesNoEdgeTypeTheCallerMayNotRead() {
    // control: unbound, the hint points at the edge type declaring the property
    assertThatThrownBy(() -> rows("CALL algo.mst('secretWeight') YIELD source RETURN source"))
        .hasStackTraceContaining("'secretWeight' is a property of the edge type 'SECRET_LINK'");

    bindUserDenying(Set.of("SECRET_LINK"));

    final Throwable hint = catchThrowable(() -> rows("CALL algo.mst('secretWeight') YIELD source RETURN source"));
    assertThat(hint).hasStackTraceContaining("relationship type 'secretWeight' does not exist");
    assertThat(stackTraceOf(hint)).doesNotContain("SECRET_LINK");

    // the denied type itself is answered as one that does not exist
    assertThatThrownBy(() -> rows("CALL algo.mst('SECRET_LINK') YIELD source RETURN source"))
        .hasStackTraceContaining("relationship type 'SECRET_LINK' does not exist");

    // a readable type still passes, and a readable property still gets its hint
    rows("CALL algo.mst('ROAD') YIELD source RETURN source");
    assertThatThrownBy(() -> rows("CALL algo.mst('w') YIELD source RETURN source"))
        .hasStackTraceContaining("'w' is a property of the edge type 'ROAD'");
  }

  private int rows(final String cypher) {
    int n = 0;
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext()) {
        rs.next();
        ++n;
      }
    }
    return n;
  }

  private static String stackTraceOf(final Throwable e) {
    final StringBuilder out = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause())
      out.append(t).append('\n');
    return out.toString();
  }

  private void bindUserDenying(final Set<String> deniedTypes) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : deniedTypes)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "scoped";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return !deniedTypes.contains(typeName);
      }

      @Override
      public long getResultSetLimit() {
        return -1;
      }

      @Override
      public long getReadTimeout() {
        return -1;
      }
    });
  }
}
