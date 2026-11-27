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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The search services (vector, full-text and hybrid) name only the indexes of the types the caller may read, in the lists their
 * refusals carry, and answer the name of an index of a type the caller may not read exactly as the name of an index that does not
 * exist.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class SearchIndexVisibilityTest extends TestHelper {
  private static final String   OPEN       = "Catalog";
  private static final String   RESTRICTED = "Ledger";
  private static final JSONArray QUERY      = new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f });

  @BeforeEach
  void createSchema() {
    database.transaction(() -> {
      for (final String type : List.of(OPEN, RESTRICTED)) {
        database.command("sql", "CREATE DOCUMENT TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE PROPERTY " + type + ".dims ARRAY_OF_INTEGERS");
        database.command("sql", "CREATE PROPERTY " + type + ".weights ARRAY_OF_FLOATS");
        database.command("sql", "CREATE PROPERTY " + type + ".body STRING");
        database.command("sql", "CREATE PROPERTY " + type + ".title STRING");
        database.command("sql", "CREATE INDEX ON " + type + " (embedding) LSM_VECTOR METADATA { dimensions: 3, similarity: 'COSINE' }");
        database.command("sql", "CREATE INDEX ON " + type + " (dims, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");
        database.command("sql", "CREATE INDEX ON " + type + " (body) FULL_TEXT");
        database.command("sql", "CREATE INDEX ON " + type + " (title) FULL_TEXT");

        database.newDocument(type).set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).set("dims", new int[] { 1, 5 })
            .set("weights", new float[] { 0.5f, 0.25f }).set("body", "alpha beta").set("title", "gamma").save();
      }
    });
  }

  @AfterEach
  void unbind() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  @Test
  void vectorSearchListsOnlyTheIndexesTheCallerMayRead() {
    final String everything = message(() -> VectorSearch.search(database, vectorArgs("Missing[embedding]", false)));
    assertThat(everything).contains(OPEN + "[embedding]", RESTRICTED + "[embedding]");

    bindUserDenying(Set.of(RESTRICTED));

    final String visible = message(() -> VectorSearch.search(database, vectorArgs("Missing[embedding]", false)));
    assertThat(visible).contains(OPEN + "[embedding]").doesNotContain(RESTRICTED);
  }

  @Test
  void vectorSearchOfTheOtherKindListsOnlyTheIndexesTheCallerMayRead() {
    bindUserDenying(Set.of(RESTRICTED));

    // a full-text index addressed as a dense one, and a dense one addressed as a sparse one
    final String dense = message(() -> VectorSearch.search(database, vectorArgs(OPEN + "[body]", false)));
    assertThat(dense).contains(OPEN + "[embedding]").doesNotContain(RESTRICTED);

    final String sparse = message(() -> VectorSearch.search(database, vectorArgs(OPEN + "[embedding]", true)));
    assertThat(sparse).contains(OPEN + "[dims,weights]").doesNotContain(RESTRICTED);
  }

  @Test
  void sparseVectorSearchListsOnlyTheIndexesTheCallerMayRead() {
    final String everything = message(() -> VectorSearch.search(database, vectorArgs("Missing[dims,weights]", true)));
    assertThat(everything).contains(OPEN + "[dims,weights]", RESTRICTED + "[dims,weights]");

    bindUserDenying(Set.of(RESTRICTED));

    final String visible = message(() -> VectorSearch.search(database, vectorArgs("Missing[dims,weights]", true)));
    assertThat(visible).contains(OPEN + "[dims,weights]").doesNotContain(RESTRICTED);
  }

  @Test
  void vectorSearchAnswersAnIndexOfAnUnreadableTypeAsOneThatDoesNotExist() {
    final String missing = message(() -> VectorSearch.search(database, vectorArgs("Missing[embedding]", false)));
    assertThat(missing).contains("does not exist");

    bindUserDenying(Set.of(RESTRICTED));

    final String missingForTheCaller = message(() -> VectorSearch.search(database, vectorArgs("Missing[embedding]", false)));
    for (final String name : List.of(RESTRICTED + "[embedding]", RESTRICTED + "[body]"))
      assertThat(message(() -> VectorSearch.search(database, vectorArgs(name, false)))).as(name)
          .isEqualTo(missingForTheCaller.replace("Missing[embedding]", name));

    final String missingSparse = message(() -> VectorSearch.search(database, vectorArgs("Missing[dims,weights]", true)));
    assertThat(message(() -> VectorSearch.search(database, vectorArgs(RESTRICTED + "[dims,weights]", true))))
        .isEqualTo(missingSparse.replace("Missing[dims,weights]", RESTRICTED + "[dims,weights]"));
  }

  @Test
  void vectorSearchOfAnIndexOfAReadableTypeStillRuns() {
    bindUserDenying(Set.of(RESTRICTED));

    assertThat(VectorSearch.search(database, vectorArgs(OPEN + "[embedding]", false)).getInt("count")).isEqualTo(1);
  }

  @Test
  void fullTextSearchListsOnlyTheIndexesTheCallerMayRead() {
    assertThat(FullTextSearch.listFullTextIndexes(database)).contains(OPEN + "[body]", RESTRICTED + "[body]", RESTRICTED + "[title]");

    final String everything = message(() -> FullTextQuery.search(database, fullTextArgs("indexName", "Missing[body]")));
    assertThat(everything).contains(OPEN + "[body]", RESTRICTED + "[body]");

    bindUserDenying(Set.of(RESTRICTED));

    assertThat(FullTextSearch.listFullTextIndexes(database)).contains(OPEN + "[body]", OPEN + "[title]").noneMatch(n -> n.startsWith(RESTRICTED));

    final String visible = message(() -> FullTextQuery.search(database, fullTextArgs("indexName", "Missing[body]")));
    assertThat(visible).contains(OPEN + "[body]").doesNotContain(RESTRICTED);

    // a dense index addressed as a full-text one
    final String otherKind = message(() -> FullTextQuery.search(database, fullTextArgs("indexName", OPEN + "[embedding]")));
    assertThat(otherKind).contains(OPEN + "[body]").doesNotContain(RESTRICTED);
  }

  @Test
  void fullTextSearchByTypeNamesNoIndexOfAnUnreadableType() {
    // control: the caller sees the candidates of a type with several indexes
    assertThat(message(() -> FullTextQuery.search(database, fullTextArgs("typeName", RESTRICTED))))
        .contains(RESTRICTED + "[body]", RESTRICTED + "[title]");

    bindUserDenying(Set.of(RESTRICTED));

    final String absent = message(() -> FullTextQuery.search(database, fullTextArgs("typeName", "Nothing")));
    final String denied = message(() -> FullTextQuery.search(database, fullTextArgs("typeName", RESTRICTED)));
    assertThat(denied).doesNotContain(RESTRICTED + "[").isEqualTo(absent.replace("Nothing", RESTRICTED));

    // the type the caller may read is still answered with its candidates
    assertThat(message(() -> FullTextQuery.search(database, fullTextArgs("typeName", OPEN))))
        .contains(OPEN + "[body]", OPEN + "[title]");
  }

  @Test
  void fullTextSearchAnswersAnIndexOfAnUnreadableTypeAsOneThatDoesNotExist() {
    bindUserDenying(Set.of(RESTRICTED));

    final String missing = message(() -> FullTextQuery.search(database, fullTextArgs("indexName", "Missing[body]")));
    assertThat(missing).contains("does not exist");
    for (final String name : List.of(RESTRICTED + "[body]", RESTRICTED + "[embedding]"))
      assertThat(message(() -> FullTextQuery.search(database, fullTextArgs("indexName", name)))).as(name)
          .isEqualTo(missing.replace("Missing[body]", name));

    // the type and its properties given apart address the same index
    final Throwable derived = catchThrowable(() -> FullTextQuery.search(database,
        fullTextArgs("typeName", RESTRICTED).put("properties", new JSONArray(List.of("body")))));
    final Throwable derivedMissing = catchThrowable(() -> FullTextQuery.search(database,
        fullTextArgs("typeName", "Nothing").put("properties", new JSONArray(List.of("body")))));
    assertThat(derived).isInstanceOf(SchemaException.class).hasMessageContaining("does not exist");
    assertThat(derived.getMessage()).isEqualTo(derivedMissing.getMessage().replace("Nothing[body]", RESTRICTED + "[body]"));
  }

  @Test
  void fullTextSearchOfAnIndexOfAReadableTypeStillRuns() {
    bindUserDenying(Set.of(RESTRICTED));

    assertThat(FullTextQuery.search(database, fullTextArgs("indexName", OPEN + "[body]")).getInt("count")).isEqualTo(1);
    assertThat(FullTextQuery.search(database, fullTextArgs("typeName", OPEN).put("properties", new JSONArray(List.of("body"))))
        .getInt("count")).isEqualTo(1);
  }

  @Test
  void hybridSearchListsOnlyTheFullTextIndexesTheCallerMayRead() {
    final String everything = message(() -> HybridSearch.search(database, hybridArgs("Missing[body]")));
    assertThat(everything).contains(OPEN + "[body]", RESTRICTED + "[body]");

    bindUserDenying(Set.of(RESTRICTED));

    final String visible = message(() -> HybridSearch.search(database, hybridArgs("Missing[body]")));
    assertThat(visible).contains(OPEN + "[body]").doesNotContain(RESTRICTED);

    // a dense index addressed as a full-text one
    final String otherKind = message(() -> HybridSearch.search(database, hybridArgs(OPEN + "[embedding]")));
    assertThat(otherKind).contains(OPEN + "[body]").doesNotContain(RESTRICTED);
  }

  @Test
  void hybridSearchAnswersAnIndexOfAnUnreadableTypeAsOneThatDoesNotExist() {
    bindUserDenying(Set.of(RESTRICTED));

    final String missing = message(() -> HybridSearch.search(database, hybridArgs("Missing[body]")));
    assertThat(missing).contains("does not exist");
    for (final String name : List.of(RESTRICTED + "[body]", RESTRICTED + "[embedding]"))
      assertThat(message(() -> HybridSearch.search(database, hybridArgs(name)))).as(name).isEqualTo(missing.replace("Missing[body]", name));

    // the vector leg
    final JSONObject restrictedVectorLeg = hybridArgs(OPEN + "[body]").put("vectorIndexName", RESTRICTED + "[embedding]");
    final JSONObject missingVectorLeg = hybridArgs(OPEN + "[body]").put("vectorIndexName", "Missing[embedding]");
    assertThat(message(() -> HybridSearch.search(database, restrictedVectorLeg)))
        .isEqualTo(message(() -> HybridSearch.search(database, missingVectorLeg)).replace("Missing[embedding]", RESTRICTED + "[embedding]"));
  }

  @Test
  void hybridSearchOfIndexesOfReadableTypesStillRuns() {
    bindUserDenying(Set.of(RESTRICTED));

    final JSONObject response = HybridSearch.search(database, hybridArgs(OPEN + "[body]").put("fulltextQuery", "alpha"));
    assertThat(response.getJSONArray("results").length()).isEqualTo(1);
  }

  private static JSONObject vectorArgs(final String indexName, final boolean sparse) {
    final JSONObject args = new JSONObject().put("indexName", indexName).put("queryVector", QUERY).put("k", 3);
    if (sparse)
      args.put("sparse", true).put("queryIndices", new JSONArray(new Object[] { 1 })).put("queryVector", new JSONArray(new Object[] { 1.0f }));
    return args;
  }

  private static JSONObject fullTextArgs(final String addressing, final String value) {
    return new JSONObject().put(addressing, value).put("queryText", "alpha");
  }

  private static JSONObject hybridArgs(final String fullTextIndexName) {
    return new JSONObject().put("vectorIndexName", OPEN + "[embedding]").put("queryVector", QUERY).put("k", 3)
        .put("fulltextIndexName", fullTextIndexName).put("fulltextQuery", "alpha");
  }

  private static String message(final Runnable call) {
    final Throwable thrown = catchThrowable(call::run);
    assertThat(thrown).isNotNull();
    return thrown.getMessage();
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
