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
package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * GraphQL introspection read the schema without asking whether the bound principal may read a type, so a user denied a type
 * by its per-type permissions got its name and every field through {@code __schema { types }} and {@code __type(name:)},
 * while {@code SELECT FROM schema:types} hid it. A type the principal may not read is now absent from introspection, a
 * {@code __type} of it is "not found" exactly as a name that does not exist, and a field of the Query root typed by it goes
 * with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphQLIntrospectionPerTypeAclTest extends AbstractGraphQLTest {

  @Test
  void introspectionDescribesOnlyTheTypesThePrincipalMayRead() {
    executeTest(database -> {
      database.getSchema().createDocumentType("PublicDoc").createProperty("title", Type.STRING);
      final DocumentType secret = database.getSchema().createDocumentType("SecretDoc");
      secret.createProperty("cardNumber", Type.STRING);
      secret.createProperty("ssn", Type.STRING);
      // a readable type embedding the hidden one
      database.getSchema().getType("PublicDoc").createProperty("secretPart", Type.EMBEDDED, "SecretDoc");

      database.command("graphql", """
          type Query {
            publicDocs: [PublicDoc]
            secretDocs: [SecretDoc]
          }

          type SecretDoc {
            cardNumber: String
            ssn: String
          }""");

      // control: unbound, everything is described
      assertThat(schemaTypes(database)).contains("SecretDoc", "cardNumber");

      try {
        bindUserDenying(database, Set.of("SecretDoc"));

        final String types = schemaTypes(database);
        assertThat(types).contains("PublicDoc", "publicDocs").doesNotContain("SecretDoc", "cardNumber", "ssn", "secretDocs");

        final Throwable hidden = catchThrowable(() -> database.query("graphql", "{ __type(name: \"SecretDoc\") { name } }").close());
        final Throwable missing = catchThrowable(() -> database.query("graphql", "{ __type(name: \"NoSuchType\") { name } }").close());
        assertThat(hidden).as("a hidden type is not found").isNotNull();
        assertThat(missing).isNotNull();
        assertThat(hidden.getClass()).isEqualTo(missing.getClass());
        assertThat(hidden.getMessage().replace("SecretDoc", "<t>")).isEqualTo(missing.getMessage().replace("NoSuchType", "<t>"));

        // the embedded property of a readable type does not describe the hidden one either
        try (final ResultSet rs = database.query("graphql",
            "{ __type(name: \"PublicDoc\") { fields { name type { kind name fields { name } } } } }")) {
          assertThat(rs.next().toJSON().toString()).contains("secretPart").doesNotContain("SecretDoc", "cardNumber");
        }
      } finally {
        DatabaseContext.INSTANCE.getContext(((DatabaseInternal) database).getDatabasePath()).setCurrentUser(null);
      }
      return null;
    });
  }

  private static String schemaTypes(final Database database) {
    try (final ResultSet rs = database.query("graphql", "{ __schema { types { name fields { name } } queryType { fields { name } } } }")) {
      return rs.next().toJSON().toString();
    }
  }

  private static void bindUserDenying(final Database database, final Set<String> deniedTypes) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : deniedTypes)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    DatabaseContext.INSTANCE.getContext(((DatabaseInternal) database).getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
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
