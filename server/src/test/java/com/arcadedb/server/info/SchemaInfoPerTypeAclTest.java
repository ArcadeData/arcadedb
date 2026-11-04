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
package com.arcadedb.server.info;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ai.AiSchemaDigest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The schema document the MCP {@code get_schema} tool, the {@code arcadedb://{database}/schema} resource and the Studio AI
 * assistant render did not ask whether the bound principal may read a type, while its two callers checked only database
 * access: a user denied a type by its per-type permissions got its properties and its UNIQUE indexes. It now renders the
 * types {@link SecurityHelper#readableTypes} answers, and leaves an unreadable parent out of a readable type's parents.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SchemaInfoPerTypeAclTest {
  private static final String DATABASE_PATH = "./target/databases/SchemaInfoPerTypeAclTest";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      schema.createDocumentType("PublicDoc").createProperty("title", Type.STRING);
      schema.createDocumentType("SecretDoc").createProperty("cardNumber", Type.STRING);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "SecretDoc", "cardNumber");
      schema.createDocumentType("SecretBase").createProperty("internalCode", Type.STRING);
      schema.createDocumentType("PublicChild").addSuperType("SecretBase");
    });
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen()) {
      unbind();
      database.drop();
    }
  }

  @Test
  void theSchemaDocumentRendersOnlyTheTypesThePrincipalMayRead() {
    // control: unbound, everything is rendered
    assertThat(SchemaInfo.toJSON(database, "probe").toString()).contains("SecretDoc", "cardNumber", "SecretBase");

    bindUserDenying(Set.of("SecretDoc", "SecretBase"));

    final String rendered = SchemaInfo.toJSON(database, "probe").toString();
    assertThat(rendered).contains("PublicDoc", "PublicChild").doesNotContain("SecretDoc", "cardNumber", "SecretBase");

    assertThat(SecurityHelper.readableTypes((DatabaseInternal) database)).extracting(t -> t.getName())
        .containsExactlyInAnyOrder("PublicDoc", "PublicChild");
  }

  @Test
  void theAssistantDescribesAnUnreadableTypeAsOneThatDoesNotExist() {
    bindUserDenying(Set.of("SecretDoc", "SecretBase"));

    final Throwable hidden = catchThrowable(() -> AiSchemaDigest.typeDetail(database, "SecretDoc"));
    final Throwable missing = catchThrowable(() -> AiSchemaDigest.typeDetail(database, "NoSuchDoc"));
    assertThat(hidden).isInstanceOf(IllegalArgumentException.class);
    assertThat(missing).isInstanceOf(IllegalArgumentException.class);
    assertThat(hidden.getMessage().replace("SecretDoc", "<t>")).isEqualTo(missing.getMessage().replace("NoSuchDoc", "<t>"));

    // a readable type does not name the parent it may not read
    final JSONObject child = AiSchemaDigest.typeDetail(database, "PublicChild");
    assertThat(child.toString()).doesNotContain("SecretBase");

    assertThat(AiSchemaDigest.build(database, "probe")).contains("PublicDoc").doesNotContain("SecretDoc", "SecretBase");
  }

  private void unbind() {
    DatabaseContext.INSTANCE.getContext(((DatabaseInternal) database).getDatabasePath()).setCurrentUser(null);
  }

  private void bindUserDenying(final Set<String> deniedTypes) {
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
