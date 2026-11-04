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
package com.arcadedb.query.opencypher.procedures;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code meta.*} procedures walked every type of the schema without asking the bound principal, while {@code db.labels()}
 * and the rest of the {@code db.*} family hide a type the caller may not read: {@code meta.schema} and the
 * {@code *TypeProperties} pair handed its properties to a user denied it, and {@code meta.stats} / {@code meta.graph} counted
 * it, which the per-bucket gate then refused, failing the whole call. They describe the readable types only.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMetaProceduresPerTypeAclTest extends TestHelper {

  private static final List<String> PROCEDURES = List.of("meta.schema", "meta.stats", "meta.graph", "meta.nodeTypeProperties",
      "meta.relTypeProperties");

  @AfterEach
  void unbind() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  @Test
  void theMetaProceduresDescribeOnlyTheTypesThePrincipalMayRead() {
    database.transaction(() -> {
      database.getSchema().createVertexType("PublicNode").createProperty("title", Type.STRING);
      database.getSchema().createVertexType("SecretNode").createProperty("cardNumber", Type.STRING);
      database.getSchema().createEdgeType("SECRET_LINK").createProperty("secretWeight", Type.DOUBLE);
      database.command("cypher", "CREATE (:PublicNode {title: 'a'}), (:SecretNode {cardNumber: '1'})");
    });

    // control: unbound, everything is described
    assertThat(render("meta.schema")).contains("SecretNode", "cardNumber");

    bindUserDenying(Set.of("SecretNode", "SECRET_LINK"));

    for (final String procedure : PROCEDURES)
      assertThat(render(procedure)).as(procedure).doesNotContain("SecretNode", "cardNumber", "SECRET_LINK", "secretWeight");
    assertThat(render("meta.schema")).contains("PublicNode");
    assertThat(render("meta.stats")).contains("PublicNode");
  }

  private String render(final String procedure) {
    final StringBuilder out = new StringBuilder();
    try (final ResultSet rs = database.query("cypher", "CALL " + procedure + "()")) {
      while (rs.hasNext())
        out.append(rs.next().toJSON()).append('\n');
    }
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
