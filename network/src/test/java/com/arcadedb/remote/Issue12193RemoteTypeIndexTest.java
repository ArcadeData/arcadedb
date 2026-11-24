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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12193: what the remote client makes of the rows of {@code schema:indexes}, against a server that is a list of rows (no
 * server, no mock): the lookups of a {@link RemoteDocumentType}, the index a create answers, and the members of the
 * {@link RemoteTypeIndex} that are not on the {@link com.arcadedb.index.Index} interface and would answer from the (empty) list of
 * bucket indexes the client does not hold.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12193RemoteTypeIndexTest {

  /** A database whose server is a list of rows: the schema:types and schema:indexes listings, and the CREATE / DROP INDEX commands. */
  private static final class ListedServer extends RemoteDatabase {
    final List<ResultInternal> types    = new ArrayList<>();
    final List<ResultInternal> indexes  = new ArrayList<>();
    final List<String>         commands = new ArrayList<>();
    int indexListings;

    ListedServer() {
      super("localhost", 2480, "issue12193", "root", "unused", new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // NO SERVER
    }

    @Override
    protected ResultSet commandUncounted(final String language, final String command, final Map<String, Object> params) {
      commands.add(command);
      final InternalResultSet answer = new InternalResultSet();
      if (command.contains("schema:types"))
        types.forEach(answer::add);
      else if (command.contains("schema:indexes where typeName")) {
        ++indexListings;
        for (final ResultInternal row : indexes)
          if (params.get("name").equals(row.getProperty("typeName")))
            answer.add(row);
      } else if (command.contains("schema:indexes where name")) {
        ++indexListings;
        for (final ResultInternal row : indexes)
          if (params.get("name").equals(row.getProperty("name")))
            answer.add(row);
      } else if (command.startsWith("create index")) {
        // THE INDEX THE SERVER NAMES: THE ONE OF THE FIRST LISTED TYPE INDEX WHOSE PROPERTIES ARE THOSE ASKED FOR
        final ResultInternal created = new ResultInternal(new LinkedHashMap<>());
        created.setProperty("operation", "create index");
        created.setProperty("name", "Parent[code]");
        answer.add(created);
      }
      return answer;
    }
  }

  private ListedServer server;

  @BeforeEach
  void setUp() {
    server = new ListedServer();
    server.types.add(typeRow("Parent", List.of()));
    server.types.add(typeRow("Child", List.of("Parent")));
  }

  private static ResultInternal typeRow(final String name, final List<String> parents) {
    final Map<String, Object> content = new LinkedHashMap<>();
    content.put("name", name);
    content.put("type", "document");
    content.put("records", 0);
    content.put("buckets", List.of(name + "_0"));
    content.put("bucketSelectionStrategy", "round-robin");
    content.put("parentTypes", parents);
    content.put("properties", List.of());
    return new ResultInternal(content);
  }

  /** A row of schema:indexes as FetchFromSchemaIndexesStep writes it, for a type index (no fileId) or a bucket index (a fileId). */
  private static ResultInternal row(final String name, final String typeName, final Schema.INDEX_TYPE indexType, final boolean unique,
      final Integer fileId, final String... propertyNames) {
    final Map<String, Object> content = new LinkedHashMap<>();
    content.put("name", name);
    content.put("indexType", indexType.name());
    content.put("typeName", typeName);
    content.put("properties", List.of(List.of(propertyNames)));
    content.put("keyTypes", List.of("STRING"));
    content.put("unique", unique);
    content.put("automatic", true);
    content.put("compacting", false);
    content.put("valid", true);
    if (fileId != null)
      content.put("fileId", fileId);
    content.put("supportsOrderedIterations", indexType != Schema.INDEX_TYPE.COLUMNAR);
    content.put("nullStrategy", "SKIP");
    return new ResultInternal(content);
  }

  private DocumentType type(final String name) {
    return server.getSchema().getType(name);
  }

  @Test
  void bucketIndexesListedNextToTheTypeIndexAreLeftOut() {
    server.indexes.add(row("Parent[code]", "Parent", Schema.INDEX_TYPE.LSM_TREE, false, null, "code"));
    server.indexes.add(row("Parent_0_123", "Parent", Schema.INDEX_TYPE.LSM_TREE, false, 12, "code"));

    assertThat(type("Parent").getAllIndexes(false)).extracting(TypeIndex::getName).containsExactly("Parent[code]");
  }

  @Test
  void aTypeAnswersTheIndexesDeclaredOnItselfAndTheSuperTypesOnlyWhenPolymorphic() {
    server.indexes.add(row("Parent[code]", "Parent", Schema.INDEX_TYPE.LSM_TREE, false, null, "code"));

    final DocumentType child = type("Child");
    assertThat(child.getAllIndexes(false)).isEmpty();
    assertThat(child.getAllIndexes(true)).extracting(TypeIndex::getName).containsExactly("Parent[code]");
    assertThat(child.getIndexByProperties("code")).isNull();
    assertThat(child.getPolymorphicIndexByProperties("code")).isNotNull();
    assertThat(child.getPolymorphicIndexByProperties("code").getName()).isEqualTo("Parent[code]");
    assertThat(child.getPolymorphicIndexByProperties("other")).isNull();
  }

  @Test
  void aLookupIsOneListingOfTheServerAndAPolymorphicOneIsOnePerTypeOfTheHierarchy() {
    server.indexes.add(row("Parent[code]", "Parent", Schema.INDEX_TYPE.LSM_TREE, false, null, "code"));

    type("Parent").getIndexByProperties("code");
    assertThat(server.indexListings).isEqualTo(1);

    server.indexListings = 0;
    type("Child").getPolymorphicIndexByProperties("code");
    assertThat(server.indexListings).isEqualTo(2);
  }

  @Test
  void theExactListOfPropertiesIsTheKeyAndAnyListedPropertyMatchesTheCollectionLookup() {
    server.indexes.add(row("Parent[a,b]", "Parent", Schema.INDEX_TYPE.LSM_TREE, true, null, "a", "b"));
    server.indexes.add(row("Parent[c]", "Parent", Schema.INDEX_TYPE.LSM_TREE, false, null, "c"));

    final DocumentType parent = type("Parent");
    assertThat(parent.getIndexByProperties("a", "b").getName()).isEqualTo("Parent[a,b]");
    assertThat(parent.getIndexByProperties(List.of("a", "b")).getName()).isEqualTo("Parent[a,b]");
    assertThat(parent.getIndexByProperties("b", "a")).isNull();
    assertThat(parent.getIndexByProperties("a")).isNull();
    assertThat(parent.getIndexesByProperties(List.of("b", "c"))).extracting(TypeIndex::getName)
        .containsExactlyInAnyOrder("Parent[a,b]", "Parent[c]");
    assertThat(parent.getIndexesByProperties("c", "zzz")).extracting(TypeIndex::getName).containsExactly("Parent[c]");
    assertThat(parent.getIndexesByProperties(List.of("zzz"))).isEmpty();
  }

  @Test
  void aColumnarIndexIsNotAKeyLookupAndIsFoundByValueBeforeByItem() {
    server.indexes.add(row("Parent[tags by item]_columnar", "Parent", Schema.INDEX_TYPE.COLUMNAR, false, null, "tags by item"));
    server.indexes.add(row("Parent[tags]_columnar", "Parent", Schema.INDEX_TYPE.COLUMNAR, false, null, "tags"));

    final DocumentType parent = type("Parent");
    assertThat(parent.getIndexByProperties("tags")).isNull();
    assertThat(parent.getColumnarIndex("tags").getName()).isEqualTo("Parent[tags]_columnar");
    assertThat(type("Child").getPolymorphicColumnarIndex("tags").getName()).isEqualTo("Parent[tags]_columnar");

    server.indexes.remove(1);
    assertThat(parent.getColumnarIndex("tags").getName()).isEqualTo("Parent[tags by item]_columnar");
    assertThat(parent.getColumnarIndex("other")).isNull();
  }

  @Test
  void theIndexACreateAnswersIsTheOneTheServerListsUnderTheTypeThatDeclaresIt() {
    server.indexes.add(row("Parent[code]", "Parent", Schema.INDEX_TYPE.LSM_TREE, true, null, "code"));

    // CREATE INDEX IF NOT EXISTS on the sub type is answered by the index of its super type
    final TypeIndex index = server.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Child", "code");

    assertThat(index).isNotNull();
    assertThat(index.getName()).isEqualTo("Parent[code]");
    assertThat(index.getTypeName()).isEqualTo("Parent");
    assertThat(index.isUnique()).isTrue();
    assertThat(index.getPropertyNames()).containsExactly("code");
  }

  @Test
  void aCreatedIndexTheServerDoesNotListAnswersNullInsteadOfAnIndexThatIsNotThere() {
    // dropped by somebody else between the create and the listing, or hidden from a user who cannot read the type
    assertThat(server.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Parent", "code")).isNull();
  }

  @Test
  void aTypeIndexAnswersTheMetadataTheServerListedAndNothingFromTheBucketIndexesItDoesNotHold() {
    server.indexes.add(row("Parent[code]", "Parent", Schema.INDEX_TYPE.LSM_TREE, true, null, "code"));
    server.indexes.add(row("Parent[other]", "Parent", Schema.INDEX_TYPE.LSM_TREE, false, null, "other"));

    final TypeIndex unique = type("Parent").getIndexByProperties("code");
    assertThat(unique.isUnique()).isTrue();
    assertThat(unique.isUniqueIfPresent()).isTrue();
    assertThat(unique.isValid()).isTrue();
    assertThat(unique.isDropped()).isFalse();
    assertThat(unique.isOnline()).isTrue();
    assertThat(unique.isComplete()).isTrue();
    assertThat(unique.getBucketsWithoutIndex()).isEmpty();
    assertThat(unique.isReadyForQueries()).isFalse();

    assertThat(type("Parent").getIndexByProperties("other").isUniqueIfPresent()).isFalse();
  }

  @Test
  void anIndexTheServerListsAsNotValidIsNotValidOrOnline() {
    final ResultInternal notValid = row("Parent[code]", "Parent", Schema.INDEX_TYPE.LSM_TREE, false, null, "code");
    notValid.setProperty("valid", false);
    server.indexes.add(notValid);

    final TypeIndex index = type("Parent").getIndexByProperties("code");
    assertThat(index.isValid()).isFalse();
    assertThat(index.isOnline()).isFalse();
  }

  @Test
  void aMemberTheClientCannotAnswerSaysSoInsteadOfAnsweringFromNoBucketIndex() {
    server.indexes.add(row("Parent[code]", "Parent", Schema.INDEX_TYPE.LSM_TREE, true, null, "code"));
    final TypeIndex index = type("Parent").getIndexByProperties("code");

    assertThatThrownBy(index::getMetadata).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(index::getMetadataForNewFile).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(index::getPageSize).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(index::getPageSizeForNewFile).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(index::toJSON).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(index::getMostRecentFileName).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(index::getBinaryKeyTypes).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> index.convertLookupKeys(new Object[] { "x" })).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> index.getIndexesByKeys(new Object[] { "x" })).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(index::scheduleCompaction).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(index::recomputeStatistics).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(index::compact).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(index::countEntries).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void dropRunsDropIndexOnTheServerAndTheIndexIsNoLongerValid() {
    server.indexes.add(row("Parent[code]", "Parent", Schema.INDEX_TYPE.LSM_TREE, false, null, "code"));
    final TypeIndex index = type("Parent").getIndexByProperties("code");

    index.drop();

    assertThat(server.commands).anyMatch(command -> command.startsWith("drop index") && command.contains("Parent[code]"));
    assertThat(index.isDropped()).isTrue();
    assertThat(index.isValid()).isFalse();
    assertThat(index.isUniqueIfPresent()).isFalse();

    // as the embedded index: dropping a dropped index leaves nothing to do, the server is not asked to drop it twice
    index.drop();
    assertThat(server.commands).filteredOn(command -> command.startsWith("drop index")).hasSize(1);
  }
}
