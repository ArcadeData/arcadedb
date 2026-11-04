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
package com.arcadedb.index.fulltext;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A user who may read a type but not one of its subtypes searches, and reads the statistics of, the buckets it may read: the
 * corpus counters stay whole, because they are the index's own, but nothing the user is answered is derived from what it may
 * not read. Every title has two tokens, so the average document length is the same with and without the subtype.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullTextCorpusStatisticsPerTypeAclTest {
  private static final String PATH  = "target/databases/FullTextCorpusStatisticsPerTypeAclTest";
  private static final String INDEX = "Asset[title]";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void createCorpus() {
    // Without a security manager the engine asks no file permission at all, whatever user is bound
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 2");
    database.command("sql", "CREATE PROPERTY Asset.title STRING");
    database.command("sql", "CREATE DOCUMENT TYPE Image EXTENDS Asset BUCKETS 2");
    database.command("sql", "CREATE INDEX ON Asset (title) FULL_TEXT");

    database.transaction(() -> {
      for (final String title : List.of("public cat", "public dog", "public bird", "confidential memo"))
        database.command("sql", "INSERT INTO Asset SET title = ?", title);
      for (final String title : List.of("confidential photo", "confidential scan", "confidential draft", "zebra cat", "zebra dog"))
        database.command("sql", "INSERT INTO Image SET title = ?", title);
    });
  }

  @AfterEach
  void dropDatabase() {
    unbind();
    database.drop();
    factory.close();
  }

  private void unbind() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  private void reopenDatabase() {
    database.close();
    database = factory.open();
  }

  @Test
  void theFirstSearchAfterAnOpenAnswersAUserDeniedASubtype() {
    reopenDatabase();
    bindUserDenying(Set.of("Image"));

    assertThat(titlesMatching("cat")).containsExactly("public cat");

    // the check ran as the index, not as the user: it is not left to fail for the next one either
    unbind();
    assertThat(titlesMatching("cat")).containsExactlyInAnyOrder("public cat", "zebra cat");
  }

  @Test
  void aRescanRunByADeniedUserCountsTheWholeCorpusAndGivesTheUserBack() {
    reopenDatabase();
    LSMTreeFullTextIndex.metadataOf(typeIndex()).requireRescan();
    final SecurityDatabaseUser user = bindUserDenying(Set.of("Image"));

    assertThat(titlesMatching("cat")).containsExactly("public cat");
    assertThat(DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).getCurrentUser()).isSameAs(user);

    assertThat(LSMTreeFullTextIndex.metadataOf(typeIndex()).getTotalDocs()).isEqualTo(9L);
    assertThat(LSMTreeFullTextIndex.metadataOf(typeIndex()).isRescanRequired()).isFalse();
  }

  @Test
  void theExplanationCountsOnlyTheBucketsTheUserMayRead() {
    final JSONObject whole = FullTextSearch.explainScoring(typeIndex(), "confidential");
    assertThat(whole.getLong("totalDocs")).isEqualTo(9L);
    assertThat(whole.getJSONArray("terms").getJSONObject(0).getLong("df")).isEqualTo(4L);
    assertThat(whole.getString("corpusScope")).isEqualTo("type");
    assertThat(whole.has("avgDocLength")).isTrue();

    bindUserDenying(Set.of("Image"));

    final JSONObject readable = FullTextSearch.explainScoring(typeIndex(), "confidential");
    assertThat(readable.getLong("totalDocs")).isEqualTo(4L);
    assertThat(readable.getJSONArray("terms").getJSONObject(0).getLong("df")).isEqualTo(1L);
    assertThat(readable.getInt("bucketCount")).isEqualTo(2);
    assertThat(readable.getString("corpusScope")).isEqualTo("readable");
    assertThat(readable.has("avgDocLength")).isFalse();
  }

  @Test
  void aWildcardIsNotExpandedOverTermsOnlyAnUnreadableBucketHolds() {
    assertThat(FullTextSearch.explainScoring(typeIndex(), "zeb*").getJSONArray("terms").length()).isEqualTo(1);

    bindUserDenying(Set.of("Image"));

    assertThat(FullTextSearch.explainScoring(typeIndex(), "zeb*").getJSONArray("terms").length()).isZero();
  }

  @Test
  void aScoreDoesNotMoveWithTheDocumentsOfAnUnreadableBucket() {
    final float wholeBefore = scoreOf("confidential");
    bindUserDenying(Set.of("Image"));
    final float readableBefore = scoreOf("confidential");
    unbind();

    database.transaction(() -> database.command("sql", "INSERT INTO Image SET title = 'confidential archive'"));

    // control: the same insert moves the score of whoever may read it
    assertThat(scoreOf("confidential")).isNotEqualTo(wholeBefore);

    bindUserDenying(Set.of("Image"));
    assertThat(scoreOf("confidential")).isEqualTo(readableBefore);
  }

  @Test
  void theLiteralLookupAnswersOnlyTheReadableBuckets() {
    bindUserDenying(Set.of("Image"));

    final Set<RID> found = new HashSet<>();
    try (final IndexCursor cursor = FullTextSearch.searchSimple(typeIndex(), new Object[] { "cat" }, -1)) {
      while (cursor.hasNext())
        found.add(cursor.next().getIdentity());
    }

    assertThat(found).hasSize(1);
    assertThat(database.getSchema().getType("Asset").getBucketIds(false)).contains(found.iterator().next().getBucketId());
  }

  @Test
  void theIndexDetailShowsTheCountersOnlyToWhoMayReadTheWholeCorpus() {
    assertThat(indexDetail().<Object>getProperty("bm25")).isNotNull();

    bindUserDenying(Set.of("Image"));

    assertThat(indexDetail().hasProperty("bm25")).isFalse();
  }

  @Test
  void moreLikeThisAnswersAUserDeniedASubtype() {
    final RID source;
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Asset WHERE title = 'public cat'")) {
      source = rs.next().getProperty("rid");
    }

    bindUserDenying(Set.of("Image"));

    final MoreLikeThisConfig config = new MoreLikeThisConfig().setMinTermFreq(1).setMinDocFreq(1).setMaxDocFreqPercent(null);
    final Set<Integer> buckets = new HashSet<>();
    int found = 0;
    for (final IndexInternal bucketIndex : typeIndex().getIndexesOnBuckets())
      try (final IndexCursor cursor = ((LSMTreeFullTextIndex) bucketIndex).searchMoreLikeThis(Set.of(source), config)) {
        while (cursor.hasNext()) {
          buckets.add(cursor.next().getIdentity().getBucketId());
          ++found;
        }
      }

    assertThat(found).isEqualTo(2);
    assertThat(database.getSchema().getType("Asset").getBucketIds(false)).containsAll(buckets);
  }

  @Test
  void aUserDeniedEveryBucketFindsNothing() {
    bindUserDenying(Set.of("Asset", "Image"));

    final Map<RID, Float> found = FullTextSearch.search(typeIndex(), "cat", -1);

    assertThat(found).isEmpty();
    assertThat(FullTextSearch.explainScoring(typeIndex(), "cat")).isNull();
  }

  private TypeIndex typeIndex() {
    return (TypeIndex) database.getSchema().getIndexByName(INDEX);
  }

  private Result indexDetail() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:index:`" + INDEX + "`")) {
      return rs.next();
    }
  }

  private List<String> titlesMatching(final String text) {
    final List<String> titles = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT title FROM Asset WHERE SEARCH_INDEX('" + INDEX + "', ?) = true", text)) {
      while (rs.hasNext())
        titles.add(rs.next().getProperty("title"));
    }
    return titles;
  }

  /** The score of the one readable document that holds the term. */
  private float scoreOf(final String text) {
    try (final ResultSet rs = database.query("sql",
        "SELECT $score AS score FROM Asset WHERE SEARCH_INDEX('" + INDEX + "', ?) = true AND title = 'confidential memo'", text)) {
      return rs.next().<Float>getProperty("score");
    }
  }

  private SecurityDatabaseUser bindUserDenying(final Set<String> deniedTypes) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : deniedTypes)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    final SecurityDatabaseUser user = new SecurityDatabaseUser() {
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
    };
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(user);
    return user;
  }
}
