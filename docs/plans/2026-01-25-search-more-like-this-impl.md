# SEARCH_MORE Like This Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement `SEARCH_INDEX_MORE()` and `SEARCH_FIELDS_MORE()` SQL functions for similar document search using Lucene's "More Like This" algorithm.

**Architecture:** Extract terms from source documents via their indexed tokens, filter by configurable thresholds (minTermFreq, minDocFreq, etc.), build an OR query from top terms, execute against the full-text index, and normalize scores to produce both `$score` (raw) and `$similarity` (0.0-1.0).

**Tech Stack:** Java 21, Lucene analyzers (already in use), ArcadeDB SQL function framework, LSMTreeFullTextIndex.

---

## Task 1: Add $similarity to ResultInternal

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/ResultInternal.java`
- Test: `engine/src/test/java/com/arcadedb/query/sql/executor/ResultInternalTest.java`

**Step 1: Write the failing test**

Create test file if it doesn't exist, or add to existing:

```java
// In ResultInternalTest.java
@Test
void testSimilarityProperty() {
    final ResultInternal result = new ResultInternal();
    result.setSimilarity(0.85f);

    assertThat(result.getSimilarity()).isEqualTo(0.85f);
    assertThat(result.<Float>getProperty("$similarity")).isEqualTo(0.85f);
    assertThat(result.hasProperty("$similarity")).isTrue();
    assertThat(result.getPropertyNames()).contains("$similarity");
}

@Test
void testSimilarityDefaultsToZero() {
    final ResultInternal result = new ResultInternal();

    assertThat(result.getSimilarity()).isEqualTo(0f);
    assertThat(result.<Float>getProperty("$similarity")).isEqualTo(0f);
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=ResultInternalTest#testSimilarityProperty -pl engine -q`

Expected: FAIL with "cannot find symbol: method setSimilarity"

**Step 3: Write minimal implementation**

In `ResultInternal.java`, add after line 42 (after `protected float score = 0f;`):

```java
  protected float similarity = 0f;
```

Add after the `setScore` method (around line 131):

```java
  /**
   * Gets the similarity score for this result (normalized 0.0 to 1.0).
   * Used for SEARCH_INDEX_MORE/SEARCH_FIELDS_MORE functions.
   *
   * @return the similarity, or 0 if not set
   */
  public float getSimilarity() {
    return similarity;
  }

  /**
   * Sets the similarity score for this result (normalized 0.0 to 1.0).
   * Used for SEARCH_INDEX_MORE/SEARCH_FIELDS_MORE functions.
   *
   * @param similarity the normalized similarity score
   */
  public void setSimilarity(final float similarity) {
    this.similarity = similarity;
  }
```

In `getProperty` method (around line 164), add after the `$score` handling:

```java
    // If $similarity not found in content/element, fall back to similarity field
    if (result == null && "$similarity".equals(name))
      return (T) Float.valueOf(similarity);
```

In `getPropertyNames` method (around line 239), add after the `$score` handling:

```java
    // Include $similarity in property names if similarity is set
    if (similarity > 0)
      result.add("$similarity");
```

In `hasProperty` method (around line 252), add after the `$score` check:

```java
    // $similarity is always available as a special property
    if ("$similarity".equals(propName))
      return true;
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=ResultInternalTest -pl engine -q`

Expected: PASS

**Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/executor/ResultInternal.java
git add engine/src/test/java/com/arcadedb/query/sql/executor/ResultInternalTest.java
git commit -m "feat: add \$similarity property to ResultInternal

Adds similarity field alongside existing score for SEARCH_INDEX_MORE
and SEARCH_FIELDS_MORE functions. The similarity is a normalized
0.0-1.0 score where 1.0 means most similar.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 2: Create MoreLikeThisConfig Class

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/lsm/MoreLikeThisConfig.java`
- Test: `engine/src/test/java/com/arcadedb/index/lsm/MoreLikeThisConfigTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.index.lsm;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MoreLikeThisConfigTest {

  @Test
  void testDefaultValues() {
    final MoreLikeThisConfig config = new MoreLikeThisConfig();

    assertThat(config.getMinTermFreq()).isEqualTo(2);
    assertThat(config.getMinDocFreq()).isEqualTo(5);
    assertThat(config.getMaxDocFreqPercent()).isNull();
    assertThat(config.getMaxQueryTerms()).isEqualTo(25);
    assertThat(config.getMinWordLen()).isEqualTo(0);
    assertThat(config.getMaxWordLen()).isEqualTo(0);
    assertThat(config.isBoostByScore()).isTrue();
    assertThat(config.isExcludeSource()).isTrue();
    assertThat(config.getMaxSourceDocs()).isEqualTo(25);
  }

  @Test
  void testFromJSON() {
    final JSONObject json = new JSONObject();
    json.put("minTermFreq", 1);
    json.put("minDocFreq", 3);
    json.put("maxDocFreqPercent", 0.5);
    json.put("maxQueryTerms", 50);
    json.put("excludeSource", false);

    final MoreLikeThisConfig config = MoreLikeThisConfig.fromJSON(json);

    assertThat(config.getMinTermFreq()).isEqualTo(1);
    assertThat(config.getMinDocFreq()).isEqualTo(3);
    assertThat(config.getMaxDocFreqPercent()).isEqualTo(0.5f);
    assertThat(config.getMaxQueryTerms()).isEqualTo(50);
    assertThat(config.isExcludeSource()).isFalse();
  }

  @Test
  void testFromNullJSON() {
    final MoreLikeThisConfig config = MoreLikeThisConfig.fromJSON(null);

    // Should use defaults
    assertThat(config.getMinTermFreq()).isEqualTo(2);
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=MoreLikeThisConfigTest -pl engine -q`

Expected: FAIL with "cannot find symbol: class MoreLikeThisConfig"

**Step 3: Write minimal implementation**

```java
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
package com.arcadedb.index.lsm;

import com.arcadedb.serializer.json.JSONObject;

/**
 * Configuration for More Like This queries.
 * Holds parameters that control term selection and filtering.
 */
public class MoreLikeThisConfig {
  private int minTermFreq = 2;
  private int minDocFreq = 5;
  private Float maxDocFreqPercent = null;
  private int maxQueryTerms = 25;
  private int minWordLen = 0;
  private int maxWordLen = 0;
  private boolean boostByScore = true;
  private boolean excludeSource = true;
  private int maxSourceDocs = 25;

  public MoreLikeThisConfig() {
  }

  public static MoreLikeThisConfig fromJSON(final JSONObject json) {
    final MoreLikeThisConfig config = new MoreLikeThisConfig();
    if (json == null)
      return config;

    if (json.has("minTermFreq"))
      config.minTermFreq = json.getInt("minTermFreq");
    if (json.has("minDocFreq"))
      config.minDocFreq = json.getInt("minDocFreq");
    if (json.has("maxDocFreqPercent"))
      config.maxDocFreqPercent = json.getFloat("maxDocFreqPercent");
    if (json.has("maxQueryTerms"))
      config.maxQueryTerms = json.getInt("maxQueryTerms");
    if (json.has("minWordLen"))
      config.minWordLen = json.getInt("minWordLen");
    if (json.has("maxWordLen"))
      config.maxWordLen = json.getInt("maxWordLen");
    if (json.has("boostByScore"))
      config.boostByScore = json.getBoolean("boostByScore");
    if (json.has("excludeSource"))
      config.excludeSource = json.getBoolean("excludeSource");
    if (json.has("maxSourceDocs"))
      config.maxSourceDocs = json.getInt("maxSourceDocs");

    return config;
  }

  public int getMinTermFreq() { return minTermFreq; }
  public int getMinDocFreq() { return minDocFreq; }
  public Float getMaxDocFreqPercent() { return maxDocFreqPercent; }
  public int getMaxQueryTerms() { return maxQueryTerms; }
  public int getMinWordLen() { return minWordLen; }
  public int getMaxWordLen() { return maxWordLen; }
  public boolean isBoostByScore() { return boostByScore; }
  public boolean isExcludeSource() { return excludeSource; }
  public int getMaxSourceDocs() { return maxSourceDocs; }
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=MoreLikeThisConfigTest -pl engine -q`

Expected: PASS

**Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/index/lsm/MoreLikeThisConfig.java
git add engine/src/test/java/com/arcadedb/index/lsm/MoreLikeThisConfigTest.java
git commit -m "feat: add MoreLikeThisConfig for MLT parameter handling

Configuration class for More Like This queries with all Lucene MLT
parameters: minTermFreq, minDocFreq, maxDocFreqPercent, maxQueryTerms,
minWordLen, maxWordLen, boostByScore, excludeSource, maxSourceDocs.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 3: Create MoreLikeThisQueryBuilder Class

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/lsm/MoreLikeThisQueryBuilder.java`
- Test: `engine/src/test/java/com/arcadedb/index/lsm/MoreLikeThisQueryBuilderTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.index.lsm;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MoreLikeThisQueryBuilderTest {

  @Test
  void testSelectTopTerms() {
    final MoreLikeThisConfig config = new MoreLikeThisConfig();
    final MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(config);

    // Simulate term frequencies from source docs
    final Map<String, Integer> termFreqs = new HashMap<>();
    termFreqs.put("java", 5);
    termFreqs.put("database", 3);
    termFreqs.put("the", 10);  // Common word, high freq
    termFreqs.put("system", 2);

    // Simulate document frequencies (how many docs contain each term)
    final Map<String, Integer> docFreqs = new HashMap<>();
    docFreqs.put("java", 10);
    docFreqs.put("database", 8);
    docFreqs.put("the", 1000);  // Very common
    docFreqs.put("system", 6);

    final List<String> topTerms = builder.selectTopTerms(termFreqs, docFreqs, 100);

    // Should select terms that meet minTermFreq (2) and minDocFreq (5)
    assertThat(topTerms).contains("java", "database", "system");
    // "the" should be included too since we didn't set maxDocFreqPercent
    assertThat(topTerms).contains("the");
  }

  @Test
  void testSelectTopTermsWithMaxQueryTerms() {
    final MoreLikeThisConfig config = new MoreLikeThisConfig();
    // Default maxQueryTerms is 25, let's set it to 2
    final MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(
        MoreLikeThisConfig.fromJSON(new com.arcadedb.serializer.json.JSONObject().put("maxQueryTerms", 2)));

    final Map<String, Integer> termFreqs = new HashMap<>();
    termFreqs.put("java", 5);
    termFreqs.put("database", 3);
    termFreqs.put("system", 2);

    final Map<String, Integer> docFreqs = new HashMap<>();
    docFreqs.put("java", 10);
    docFreqs.put("database", 8);
    docFreqs.put("system", 6);

    final List<String> topTerms = builder.selectTopTerms(termFreqs, docFreqs, 100);

    // Should only return top 2 terms by score
    assertThat(topTerms).hasSize(2);
  }

  @Test
  void testFilterByMinTermFreq() {
    final MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(
        MoreLikeThisConfig.fromJSON(new com.arcadedb.serializer.json.JSONObject().put("minTermFreq", 3)));

    final Map<String, Integer> termFreqs = new HashMap<>();
    termFreqs.put("java", 5);      // passes
    termFreqs.put("database", 2);  // fails minTermFreq

    final Map<String, Integer> docFreqs = new HashMap<>();
    docFreqs.put("java", 10);
    docFreqs.put("database", 8);

    final List<String> topTerms = builder.selectTopTerms(termFreqs, docFreqs, 100);

    assertThat(topTerms).contains("java");
    assertThat(topTerms).doesNotContain("database");
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=MoreLikeThisQueryBuilderTest -pl engine -q`

Expected: FAIL with "cannot find symbol: class MoreLikeThisQueryBuilder"

**Step 3: Write minimal implementation**

```java
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
package com.arcadedb.index.lsm;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Builds "More Like This" queries by selecting and scoring terms from source documents.
 * Implements the MLT algorithm: extract terms, filter by thresholds, rank by TF-IDF, select top N.
 */
public class MoreLikeThisQueryBuilder {
  private final MoreLikeThisConfig config;

  public MoreLikeThisQueryBuilder(final MoreLikeThisConfig config) {
    this.config = config;
  }

  /**
   * Selects top terms from source documents based on configured thresholds.
   *
   * @param termFreqs  map of term -> frequency in source documents
   * @param docFreqs   map of term -> number of documents containing the term
   * @param totalDocs  total number of documents in the index
   * @return list of selected terms, ordered by score descending
   */
  public List<String> selectTopTerms(final Map<String, Integer> termFreqs,
                                     final Map<String, Integer> docFreqs,
                                     final long totalDocs) {
    final List<TermScore> scored = new ArrayList<>();

    for (final Map.Entry<String, Integer> entry : termFreqs.entrySet()) {
      final String term = entry.getKey();
      final int termFreq = entry.getValue();
      final int docFreq = docFreqs.getOrDefault(term, 0);

      // Filter by minTermFreq
      if (termFreq < config.getMinTermFreq())
        continue;

      // Filter by minDocFreq
      if (docFreq < config.getMinDocFreq())
        continue;

      // Filter by maxDocFreqPercent
      if (config.getMaxDocFreqPercent() != null && totalDocs > 0) {
        final float docFreqPercent = (float) docFreq / totalDocs;
        if (docFreqPercent > config.getMaxDocFreqPercent())
          continue;
      }

      // Filter by word length
      if (config.getMinWordLen() > 0 && term.length() < config.getMinWordLen())
        continue;
      if (config.getMaxWordLen() > 0 && term.length() > config.getMaxWordLen())
        continue;

      // Calculate TF-IDF score
      final float score;
      if (config.isBoostByScore() && docFreq > 0 && totalDocs > 0) {
        // TF-IDF: termFreq * log(totalDocs / docFreq)
        score = termFreq * (float) Math.log((double) totalDocs / docFreq);
      } else {
        score = termFreq;
      }

      scored.add(new TermScore(term, score));
    }

    // Sort by score descending
    scored.sort(Comparator.comparingDouble((TermScore t) -> t.score).reversed());

    // Limit to maxQueryTerms
    final int limit = Math.min(config.getMaxQueryTerms(), scored.size());
    final List<String> result = new ArrayList<>(limit);
    for (int i = 0; i < limit; i++) {
      result.add(scored.get(i).term);
    }

    return result;
  }

  private static class TermScore {
    final String term;
    final float score;

    TermScore(final String term, final float score) {
      this.term = term;
      this.score = score;
    }
  }
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=MoreLikeThisQueryBuilderTest -pl engine -q`

Expected: PASS

**Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/index/lsm/MoreLikeThisQueryBuilder.java
git add engine/src/test/java/com/arcadedb/index/lsm/MoreLikeThisQueryBuilderTest.java
git commit -m "feat: add MoreLikeThisQueryBuilder for term selection

Implements the MLT algorithm: extract terms from source documents,
filter by thresholds (minTermFreq, minDocFreq, word length, etc.),
calculate TF-IDF scores, and select top N terms for the query.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 4: Add searchMoreLikeThis to LSMTreeFullTextIndex

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/index/lsm/LSMTreeFullTextIndex.java`
- Test: `engine/src/test/java/com/arcadedb/index/lsm/LSMTreeFullTextIndexMLTTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class LSMTreeFullTextIndexMLTTest extends TestHelper {

  private RID javaDocRid;
  private RID pythonDocRid;
  private RID javaDatabaseRid;

  @BeforeEach
  void setup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      // Create test documents
      Document doc1 = database.newDocument("Article")
          .set("title", "Java Guide")
          .set("content", "java programming language tutorial guide")
          .save();
      javaDocRid = doc1.getIdentity();

      Document doc2 = database.newDocument("Article")
          .set("title", "Python Guide")
          .set("content", "python programming language tutorial")
          .save();
      pythonDocRid = doc2.getIdentity();

      Document doc3 = database.newDocument("Article")
          .set("title", "Java Database")
          .set("content", "java database connection jdbc programming")
          .save();
      javaDatabaseRid = doc3.getIdentity();
    });
  }

  @Test
  void testSearchMoreLikeThis() {
    database.transaction(() -> {
      final TypeIndex typeIndex = database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0];

      final MoreLikeThisConfig config = new MoreLikeThisConfig();
      final Set<RID> sourceRids = Set.of(javaDocRid);

      final IndexCursor cursor = ftIndex.searchMoreLikeThis(sourceRids, config);

      final Set<RID> results = new HashSet<>();
      while (cursor.hasNext()) {
        results.add(cursor.next().getIdentity());
      }

      // Should find javaDatabaseRid (shares "java", "programming") and pythonDocRid (shares "programming", "language", "tutorial")
      assertThat(results).contains(javaDatabaseRid, pythonDocRid);
    });
  }

  @Test
  void testSearchMoreLikeThisExcludesSource() {
    database.transaction(() -> {
      final TypeIndex typeIndex = database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0];

      final MoreLikeThisConfig config = new MoreLikeThisConfig();  // excludeSource = true by default
      final Set<RID> sourceRids = Set.of(javaDocRid);

      final IndexCursor cursor = ftIndex.searchMoreLikeThis(sourceRids, config);

      final Set<RID> results = new HashSet<>();
      while (cursor.hasNext()) {
        results.add(cursor.next().getIdentity());
      }

      // Source document should be excluded
      assertThat(results).doesNotContain(javaDocRid);
    });
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=LSMTreeFullTextIndexMLTTest -pl engine -q`

Expected: FAIL with "cannot find symbol: method searchMoreLikeThis"

**Step 3: Write minimal implementation**

Add to `LSMTreeFullTextIndex.java` after the `analyzeText` method (around line 613):

```java
  /**
   * Searches for documents similar to the given source documents using More Like This algorithm.
   *
   * @param sourceRids the RIDs of source documents to find similar documents to
   * @param config     the MLT configuration parameters
   * @return cursor with matching documents, sorted by similarity score descending
   */
  public IndexCursor searchMoreLikeThis(final Set<RID> sourceRids, final MoreLikeThisConfig config) {
    if (sourceRids == null || sourceRids.isEmpty())
      throw new IndexException("SEARCH_INDEX_MORE requires at least one source RID");

    if (sourceRids.size() > config.getMaxSourceDocs())
      throw new IndexException("Source RIDs (" + sourceRids.size() + ") exceeds maxSourceDocs limit (" + config.getMaxSourceDocs() + ")");

    final DatabaseInternal db = underlyingIndex.mutable.getDatabase();

    // Step 1: Extract terms from source documents
    final Map<String, Integer> termFreqs = new HashMap<>();
    final List<String> propertyNames = getPropertyNames();

    for (final RID rid : sourceRids) {
      if (!db.existsRecord(rid))
        throw new IndexException("Record " + rid + " not found");

      final Document doc = rid.getRecord().asDocument();
      final String typeName = getTypeName();
      if (!doc.getTypeName().equals(typeName))
        throw new IndexException("Record " + rid + " is not of type '" + typeName + "'");

      // Extract and analyze text from indexed properties
      for (final String propName : propertyNames) {
        final Object value = doc.get(propName);
        if (value != null) {
          final List<String> tokens = analyzeText(queryAnalyzer, new Object[] { value });
          for (final String token : tokens) {
            termFreqs.merge(token, 1, Integer::sum);
          }
        }
      }
    }

    if (termFreqs.isEmpty())
      return new TempIndexCursor(Collections.emptyList());

    // Step 2: Get document frequencies for each term
    final Map<String, Integer> docFreqs = new HashMap<>();
    for (final String term : termFreqs.keySet()) {
      final IndexCursor cursor = underlyingIndex.get(new String[] { term });
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      docFreqs.put(term, count);
    }

    // Step 3: Select top terms using the query builder
    final MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(config);
    final long totalDocs = countEntries();  // Approximation
    final List<String> topTerms = builder.selectTopTerms(termFreqs, docFreqs, totalDocs);

    if (topTerms.isEmpty())
      return new TempIndexCursor(Collections.emptyList());

    // Step 4: Execute OR query with selected terms and score results
    final Map<RID, AtomicInteger> scoreMap = new HashMap<>();
    for (final String term : topTerms) {
      final IndexCursor cursor = underlyingIndex.get(new String[] { term });
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).incrementAndGet();
      }
    }

    // Step 5: Exclude source documents if configured
    if (config.isExcludeSource()) {
      for (final RID rid : sourceRids) {
        scoreMap.remove(rid);
      }
    }

    // Step 6: Build result cursor sorted by score
    final ArrayList<IndexCursorEntry> list = new ArrayList<>(scoreMap.size());
    for (final Map.Entry<RID, AtomicInteger> entry : scoreMap.entrySet()) {
      list.add(new IndexCursorEntry(new Object[] {}, entry.getKey(), entry.getValue().get()));
    }

    // Sort by score descending
    list.sort((o1, o2) -> Integer.compare(o2.score, o1.score));

    return new TempIndexCursor(list);
  }
```

Add necessary imports at the top of the file:

```java
import java.util.Collections;
import java.util.Set;
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=LSMTreeFullTextIndexMLTTest -pl engine -q`

Expected: PASS

**Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/index/lsm/LSMTreeFullTextIndex.java
git add engine/src/test/java/com/arcadedb/index/lsm/LSMTreeFullTextIndexMLTTest.java
git commit -m "feat: add searchMoreLikeThis to LSMTreeFullTextIndex

Implements the More Like This search algorithm:
1. Extract terms from source documents
2. Calculate document frequencies
3. Select top terms using MoreLikeThisQueryBuilder
4. Execute OR query and score results
5. Optionally exclude source documents

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 5: Create SQLFunctionSearchIndexMore

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndexMore.java`
- Test: `engine/src/test/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndexMoreTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.query.sql.function.text;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLFunctionSearchIndexMoreTest extends TestHelper {

  private RID javaDocRid;
  private RID pythonDocRid;
  private RID javaDatabaseRid;

  @BeforeEach
  void setup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      javaDocRid = database.command("sql",
          "INSERT INTO Article SET title = 'Java Guide', content = 'java programming language tutorial guide'")
          .next().getIdentity().get();

      pythonDocRid = database.command("sql",
          "INSERT INTO Article SET title = 'Python Guide', content = 'python programming language tutorial'")
          .next().getIdentity().get();

      javaDatabaseRid = database.command("sql",
          "INSERT INTO Article SET title = 'Java Database', content = 'java database connection jdbc programming'")
          .next().getIdentity().get();
    });
  }

  @Test
  void testBasicSearchIndexMore() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title, $score, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[content]', [" + javaDocRid + "]) = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        final Result r = result.next();
        titles.add(r.getProperty("title"));
        assertThat(r.<Float>getProperty("$score")).isGreaterThan(0f);
        assertThat(r.<Float>getProperty("$similarity")).isBetween(0f, 1f);
      }

      // Should find similar docs but not the source
      assertThat(titles).contains("Java Database", "Python Guide");
      assertThat(titles).doesNotContain("Java Guide");  // Source excluded
    });
  }

  @Test
  void testSearchIndexMoreWithConfig() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX_MORE('Article[content]', [" + javaDocRid + "], {'excludeSource': false}) = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Source should be included when excludeSource is false
      assertThat(titles).contains("Java Guide");
    });
  }

  @Test
  void testEmptyRIDArray() {
    database.transaction(() -> {
      assertThatThrownBy(() ->
          database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[content]', []) = true")
      ).isInstanceOf(CommandExecutionException.class)
       .hasMessageContaining("at least one source RID");
    });
  }

  @Test
  void testInvalidRID() {
    database.transaction(() -> {
      assertThatThrownBy(() ->
          database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[content]', [#999:999]) = true")
      ).isInstanceOf(CommandExecutionException.class)
       .hasMessageContaining("not found");
    });
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=SQLFunctionSearchIndexMoreTest -pl engine -q`

Expected: FAIL with "No function with name 'search_index_more'"

**Step 3: Write minimal implementation**

```java
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
package com.arcadedb.query.sql.function.text;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
import com.arcadedb.index.lsm.MoreLikeThisConfig;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL function to find similar documents using More Like This algorithm.
 *
 * Usage: SEARCH_INDEX_MORE('indexName', [rid1, rid2, ...] [, {config}])
 * Example: SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[content]', [#10:3, #10:4])
 */
public class SQLFunctionSearchIndexMore extends SQLFunctionAbstract {
  public static final String NAME = "search_index_more";

  public SQLFunctionSearchIndexMore() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    if (iParams.length < 2)
      throw new CommandExecutionException("SEARCH_INDEX_MORE() requires at least 2 parameters: indexName and sourceRIDs");

    if (iParams[0] == null || iParams[1] == null)
      throw new CommandExecutionException("SEARCH_INDEX_MORE() parameters cannot be null");

    final String indexName = iParams[0].toString();
    final Set<RID> sourceRids = extractRIDs(iParams[1]);

    if (sourceRids.isEmpty())
      throw new CommandExecutionException("SEARCH_INDEX_MORE requires at least one source RID");

    // Parse optional config
    final MoreLikeThisConfig config;
    if (iParams.length >= 3 && iParams[2] != null) {
      if (iParams[2] instanceof Map) {
        config = MoreLikeThisConfig.fromJSON(new JSONObject((Map<?, ?>) iParams[2]));
      } else if (iParams[2] instanceof JSONObject) {
        config = MoreLikeThisConfig.fromJSON((JSONObject) iParams[2]);
      } else {
        config = new MoreLikeThisConfig();
      }
    } else {
      config = new MoreLikeThisConfig();
    }

    // Cache key for this specific search
    final String cacheKey = "search_index_more:" + indexName + ":" + sourceRids.hashCode() + ":" + config.hashCode();

    // Try to get cached results
    @SuppressWarnings("unchecked")
    Map<RID, float[]> allResults = (Map<RID, float[]>) iContext.getVariable(cacheKey);

    if (allResults == null) {
      allResults = new HashMap<>();

      final Database database = iContext.getDatabase();
      final Index index = database.getSchema().getIndexByName(indexName);

      if (index == null)
        throw new CommandExecutionException("Index '" + indexName + "' not found");

      if (!(index instanceof TypeIndex))
        throw new CommandExecutionException("Index '" + indexName + "' is not a type index");

      final TypeIndex typeIndex = (TypeIndex) index;
      final Index[] bucketIndexes = typeIndex.getIndexesOnBuckets();
      if (bucketIndexes.length == 0 || !(bucketIndexes[0] instanceof LSMTreeFullTextIndex))
        throw new CommandExecutionException("Index '" + indexName + "' is not a full-text index");

      // Execute MLT search across all bucket indexes
      float maxScore = 0f;
      for (final Index bucketIndex : bucketIndexes) {
        if (bucketIndex instanceof LSMTreeFullTextIndex) {
          final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) bucketIndex;
          final IndexCursor cursor = ftIndex.searchMoreLikeThis(sourceRids, config);

          while (cursor.hasNext()) {
            final Identifiable match = cursor.next();
            final int score = cursor.getScore();
            final RID rid = match.getIdentity();

            // Store score, will normalize later
            final float[] scores = allResults.computeIfAbsent(rid, k -> new float[] { 0f, 0f });
            scores[0] += score;  // Accumulate raw score
            if (scores[0] > maxScore)
              maxScore = scores[0];
          }
        }
      }

      // Normalize scores to compute similarity
      if (maxScore > 0) {
        for (final float[] scores : allResults.values()) {
          scores[1] = scores[0] / maxScore;  // similarity = score / maxScore
        }
      }

      iContext.setVariable(cacheKey, allResults);
    }

    // Check if current record matches
    if (iCurrentRecord != null) {
      final RID rid = iCurrentRecord.getIdentity();
      final boolean matches = allResults.containsKey(rid);

      if (matches) {
        final float[] scores = allResults.get(rid);
        iContext.setVariable("$score", scores[0]);
        iContext.setVariable("$similarity", scores[1]);
      } else {
        iContext.setVariable("$score", 0f);
        iContext.setVariable("$similarity", 0f);
      }

      return matches;
    }

    // Return cursor with all results
    final List<IndexCursorEntry> entries = new ArrayList<>();
    for (final Map.Entry<RID, float[]> entry : allResults.entrySet()) {
      entries.add(new IndexCursorEntry(new Object[] {}, entry.getKey(), (int) entry.getValue()[0]));
    }
    entries.sort((a, b) -> Integer.compare(b.score, a.score));

    return new TempIndexCursor(entries);
  }

  @SuppressWarnings("unchecked")
  private Set<RID> extractRIDs(final Object param) {
    final Set<RID> rids = new HashSet<>();
    if (param instanceof Collection) {
      for (final Object item : (Collection<?>) param) {
        if (item instanceof RID)
          rids.add((RID) item);
        else if (item instanceof Identifiable)
          rids.add(((Identifiable) item).getIdentity());
      }
    } else if (param instanceof RID) {
      rids.add((RID) param);
    } else if (param instanceof Identifiable) {
      rids.add(((Identifiable) param).getIdentity());
    }
    return rids;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX_MORE(<index-name>, <source-rids> [, <config>])";
  }
}
```

**Step 4: Register the function**

In `engine/src/main/java/com/arcadedb/query/sql/function/DefaultSQLFunctionFactory.java`, add after line 197 (after the existing search functions):

```java
    register(SQLFunctionSearchIndexMore.NAME, SQLFunctionSearchIndexMore.class);
```

And add the import:

```java
import com.arcadedb.query.sql.function.text.SQLFunctionSearchIndexMore;
```

**Step 5: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=SQLFunctionSearchIndexMoreTest -pl engine -q`

Expected: PASS

**Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndexMore.java
git add engine/src/main/java/com/arcadedb/query/sql/function/DefaultSQLFunctionFactory.java
git add engine/src/test/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndexMoreTest.java
git commit -m "feat: add SEARCH_INDEX_MORE SQL function

SQL function for More Like This search by index name.
Supports optional configuration via JSON metadata parameter.
Populates both \$score and \$similarity for result ranking.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 6: Create SQLFunctionSearchFieldsMore

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchFieldsMore.java`
- Test: `engine/src/test/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchFieldsMoreTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.query.sql.function.text;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SQLFunctionSearchFieldsMoreTest extends TestHelper {

  private RID javaDocRid;

  @BeforeEach
  void setup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      javaDocRid = database.command("sql",
          "INSERT INTO Article SET title = 'Java Guide', content = 'java programming language tutorial guide'")
          .next().getIdentity().get();

      database.command("sql",
          "INSERT INTO Article SET title = 'Python Guide', content = 'python programming language tutorial'");

      database.command("sql",
          "INSERT INTO Article SET title = 'Java Database', content = 'java database connection jdbc programming'");
    });
  }

  @Test
  void testBasicSearchFieldsMore() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title, $similarity FROM Article WHERE SEARCH_FIELDS_MORE(['content'], [" + javaDocRid + "]) = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        final Result r = result.next();
        titles.add(r.getProperty("title"));
        assertThat(r.<Float>getProperty("$similarity")).isBetween(0f, 1f);
      }

      assertThat(titles).contains("Java Database", "Python Guide");
      assertThat(titles).doesNotContain("Java Guide");  // Source excluded
    });
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=SQLFunctionSearchFieldsMoreTest -pl engine -q`

Expected: FAIL with "No function with name 'search_fields_more'"

**Step 3: Write minimal implementation**

```java
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
package com.arcadedb.query.sql.function.text;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
import com.arcadedb.index.lsm.MoreLikeThisConfig;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL function to find similar documents by field names using More Like This algorithm.
 *
 * Usage: SEARCH_FIELDS_MORE(['field1', 'field2'], [rid1, rid2, ...] [, {config}])
 * Example: SELECT FROM Article WHERE SEARCH_FIELDS_MORE(['title', 'content'], [#10:3])
 */
public class SQLFunctionSearchFieldsMore extends SQLFunctionAbstract {
  public static final String NAME = "search_fields_more";

  public SQLFunctionSearchFieldsMore() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    if (iParams.length < 2)
      throw new CommandExecutionException("SEARCH_FIELDS_MORE() requires at least 2 parameters: fieldNames and sourceRIDs");

    if (iParams[0] == null || iParams[1] == null)
      throw new CommandExecutionException("SEARCH_FIELDS_MORE() parameters cannot be null");

    final List<String> fieldNames = extractFieldNames(iParams[0]);
    final Set<RID> sourceRids = extractRIDs(iParams[1]);

    if (fieldNames.isEmpty())
      throw new CommandExecutionException("SEARCH_FIELDS_MORE requires at least one field name");

    if (sourceRids.isEmpty())
      throw new CommandExecutionException("SEARCH_FIELDS_MORE requires at least one source RID");

    // Parse optional config
    final MoreLikeThisConfig config;
    if (iParams.length >= 3 && iParams[2] != null) {
      if (iParams[2] instanceof Map) {
        config = MoreLikeThisConfig.fromJSON(new JSONObject((Map<?, ?>) iParams[2]));
      } else if (iParams[2] instanceof JSONObject) {
        config = MoreLikeThisConfig.fromJSON((JSONObject) iParams[2]);
      } else {
        config = new MoreLikeThisConfig();
      }
    } else {
      config = new MoreLikeThisConfig();
    }

    // Resolve index from field names
    final Database database = iContext.getDatabase();
    final String indexName = resolveIndexName(database, sourceRids.iterator().next(), fieldNames);

    // Delegate to SEARCH_INDEX_MORE logic
    final SQLFunctionSearchIndexMore delegate = new SQLFunctionSearchIndexMore();
    final Object[] delegateParams = iParams.length >= 3
        ? new Object[] { indexName, sourceRids, iParams[2] }
        : new Object[] { indexName, sourceRids };

    return delegate.execute(iThis, iCurrentRecord, iCurrentResult, delegateParams, iContext);
  }

  private String resolveIndexName(final Database database, final RID sampleRid, final List<String> fieldNames) {
    // Get the type from the sample RID
    final String typeName = sampleRid.getRecord().asDocument().getTypeName();
    final DocumentType type = database.getSchema().getType(typeName);

    // Look for a full-text index on the specified fields
    for (final TypeIndex index : type.getAllIndexes(false)) {
      if (index.getType() != Schema.INDEX_TYPE.FULL_TEXT)
        continue;

      final List<String> indexFields = index.getPropertyNames();
      if (indexFields.size() == fieldNames.size() && indexFields.containsAll(fieldNames)) {
        return index.getName();
      }
    }

    // Try single-field indexes if looking for single field
    if (fieldNames.size() == 1) {
      for (final TypeIndex index : type.getAllIndexes(false)) {
        if (index.getType() == Schema.INDEX_TYPE.FULL_TEXT) {
          final List<String> indexFields = index.getPropertyNames();
          if (indexFields.size() == 1 && indexFields.get(0).equals(fieldNames.get(0))) {
            return index.getName();
          }
        }
      }
    }

    throw new CommandExecutionException(
        "No full-text index found on type '" + typeName + "' for fields " + fieldNames);
  }

  @SuppressWarnings("unchecked")
  private List<String> extractFieldNames(final Object param) {
    final List<String> names = new ArrayList<>();
    if (param instanceof Collection) {
      for (final Object item : (Collection<?>) param) {
        names.add(item.toString());
      }
    } else {
      names.add(param.toString());
    }
    return names;
  }

  @SuppressWarnings("unchecked")
  private Set<RID> extractRIDs(final Object param) {
    final Set<RID> rids = new HashSet<>();
    if (param instanceof Collection) {
      for (final Object item : (Collection<?>) param) {
        if (item instanceof RID)
          rids.add((RID) item);
        else if (item instanceof Identifiable)
          rids.add(((Identifiable) item).getIdentity());
      }
    } else if (param instanceof RID) {
      rids.add((RID) param);
    } else if (param instanceof Identifiable) {
      rids.add(((Identifiable) param).getIdentity());
    }
    return rids;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_FIELDS_MORE(<field-names>, <source-rids> [, <config>])";
  }
}
```

**Step 4: Register the function**

In `engine/src/main/java/com/arcadedb/query/sql/function/DefaultSQLFunctionFactory.java`, add after the SEARCH_INDEX_MORE registration:

```java
    register(SQLFunctionSearchFieldsMore.NAME, SQLFunctionSearchFieldsMore.class);
```

And add the import:

```java
import com.arcadedb.query.sql.function.text.SQLFunctionSearchFieldsMore;
```

**Step 5: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=SQLFunctionSearchFieldsMoreTest -pl engine -q`

Expected: PASS

**Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchFieldsMore.java
git add engine/src/main/java/com/arcadedb/query/sql/function/DefaultSQLFunctionFactory.java
git add engine/src/test/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchFieldsMoreTest.java
git commit -m "feat: add SEARCH_FIELDS_MORE SQL function

SQL function for More Like This search by field names.
Automatically resolves the appropriate full-text index.
Delegates to SEARCH_INDEX_MORE for actual execution.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 7: Integration Tests

**Files:**
- Create: `engine/src/test/java/com/arcadedb/index/lsm/FullTextMoreLikeThisIT.java`

**Step 1: Write comprehensive integration tests**

```java
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for SEARCH_INDEX_MORE and SEARCH_FIELDS_MORE functions.
 */
class FullTextMoreLikeThisIT extends TestHelper {

  private RID javaGuideRid;
  private RID pythonGuideRid;
  private RID javaDatabaseRid;
  private RID javaWebRid;
  private RID unrelatedRid;

  @BeforeEach
  void setup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      // Create diverse test documents
      javaGuideRid = database.command("sql",
          "INSERT INTO Article SET title = 'Java Programming Guide', content = 'java programming language tutorial guide basics introduction'")
          .next().getIdentity().get();

      pythonGuideRid = database.command("sql",
          "INSERT INTO Article SET title = 'Python Programming Guide', content = 'python programming language tutorial guide basics scripting'")
          .next().getIdentity().get();

      javaDatabaseRid = database.command("sql",
          "INSERT INTO Article SET title = 'Java Database Tutorial', content = 'java database jdbc connection programming sql queries'")
          .next().getIdentity().get();

      javaWebRid = database.command("sql",
          "INSERT INTO Article SET title = 'Java Web Development', content = 'java web development servlet jsp programming tutorial'")
          .next().getIdentity().get();

      unrelatedRid = database.command("sql",
          "INSERT INTO Article SET title = 'Cooking Recipes', content = 'cooking recipes food kitchen ingredients meals breakfast'")
          .next().getIdentity().get();
    });
  }

  @Test
  void testSimilarDocumentsRankedByRelevance() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title, $score, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[content]', [" + javaGuideRid + "]) = true");

      final List<String> titles = new ArrayList<>();
      final List<Float> similarities = new ArrayList<>();

      while (result.hasNext()) {
        final Result r = result.next();
        titles.add(r.getProperty("title"));
        similarities.add(r.getProperty("$similarity"));
      }

      // Results should be ordered by similarity (descending)
      for (int i = 1; i < similarities.size(); i++) {
        assertThat(similarities.get(i)).isLessThanOrEqualTo(similarities.get(i - 1));
      }

      // Python guide should be highly similar (shares: programming, language, tutorial, guide, basics)
      assertThat(titles).contains("Python Programming Guide");

      // Unrelated doc should not appear or be very low
      if (titles.contains("Cooking Recipes")) {
        final int idx = titles.indexOf("Cooking Recipes");
        assertThat(similarities.get(idx)).isLessThan(0.3f);
      }
    });
  }

  @Test
  void testMultipleSourceDocuments() {
    database.transaction(() -> {
      // Use both Java Guide and Java Database as sources
      final ResultSet result = database.query("sql",
          "SELECT title, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[content]', [" +
              javaGuideRid + ", " + javaDatabaseRid + "]) = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Should find Java Web (shares java, programming, tutorial with both sources)
      assertThat(titles).contains("Java Web Development");

      // Source documents should be excluded
      assertThat(titles).doesNotContain("Java Programming Guide", "Java Database Tutorial");
    });
  }

  @Test
  void testSimilarityScoreNormalization() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[content]', [" + javaGuideRid + "]) = true");

      float maxSimilarity = 0f;
      while (result.hasNext()) {
        final float sim = result.next().getProperty("$similarity");
        assertThat(sim).isBetween(0f, 1f);
        if (sim > maxSimilarity)
          maxSimilarity = sim;
      }

      // The most similar document should have similarity = 1.0
      assertThat(maxSimilarity).isEqualTo(1f);
    });
  }

  @Test
  void testSearchFieldsMoreEquivalence() {
    database.transaction(() -> {
      // Both should return the same results
      final ResultSet indexResult = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX_MORE('Article[content]', [" + javaGuideRid + "]) = true ORDER BY title");

      final ResultSet fieldsResult = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_FIELDS_MORE(['content'], [" + javaGuideRid + "]) = true ORDER BY title");

      final Set<String> indexTitles = new HashSet<>();
      final Set<String> fieldsTitles = new HashSet<>();

      while (indexResult.hasNext())
        indexTitles.add(indexResult.next().getProperty("title"));
      while (fieldsResult.hasNext())
        fieldsTitles.add(fieldsResult.next().getProperty("title"));

      assertThat(indexTitles).isEqualTo(fieldsTitles);
    });
  }

  @Test
  void testConfigExcludeSourceFalse() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX_MORE('Article[content]', [" + javaGuideRid + "], {'excludeSource': false}) = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext())
        titles.add(result.next().getProperty("title"));

      // Source should be included and probably score highest
      assertThat(titles).contains("Java Programming Guide");
    });
  }

  @Test
  void testWithLimitClause() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[content]', [" + javaGuideRid + "]) = true LIMIT 2");

      int count = 0;
      float lastSim = Float.MAX_VALUE;
      while (result.hasNext()) {
        final float sim = result.next().getProperty("$similarity");
        assertThat(sim).isLessThanOrEqualTo(lastSim);
        lastSim = sim;
        count++;
      }

      assertThat(count).isEqualTo(2);
    });
  }
}
```

**Step 2: Run integration tests**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest=FullTextMoreLikeThisIT -pl engine -q`

Expected: PASS

**Step 3: Commit**

```bash
git add engine/src/test/java/com/arcadedb/index/lsm/FullTextMoreLikeThisIT.java
git commit -m "test: add comprehensive More Like This integration tests

Tests cover:
- Similarity ranking by relevance
- Multiple source documents
- Score normalization (0.0-1.0)
- SEARCH_FIELDS_MORE equivalence
- excludeSource configuration
- LIMIT clause interaction

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 8: Final Verification and Cleanup

**Step 1: Run all related tests**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -Dtest="*MoreLikeThis*,*SearchIndex*,*SearchFields*,ResultInternalTest" -pl engine`

Expected: All tests PASS

**Step 2: Build the entire project**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn clean install -DskipTests -pl engine`

Expected: BUILD SUCCESS

**Step 3: Run full test suite**

Run: `cd /Users/frank/projects/arcade/worktrees/improve-fulltext && mvn test -pl engine`

Expected: All tests PASS

**Step 4: Update design document status**

Edit `docs/plans/2026-01-25-search-more-like-this-design.md` to change status from "Draft" to "Implemented".

**Step 5: Final commit**

```bash
git add docs/plans/2026-01-25-search-more-like-this-design.md
git commit -m "docs: mark SEARCH_MORE design as implemented

All components implemented and tested:
- MoreLikeThisConfig for parameter handling
- MoreLikeThisQueryBuilder for term selection algorithm
- LSMTreeFullTextIndex.searchMoreLikeThis() method
- SEARCH_INDEX_MORE SQL function
- SEARCH_FIELDS_MORE SQL function
- \$similarity projection variable

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Summary

| Task | Component | Purpose |
|------|-----------|---------|
| 1 | ResultInternal | Add `$similarity` property |
| 2 | MoreLikeThisConfig | Configuration parameter handling |
| 3 | MoreLikeThisQueryBuilder | Term selection algorithm |
| 4 | LSMTreeFullTextIndex | `searchMoreLikeThis()` method |
| 5 | SQLFunctionSearchIndexMore | `SEARCH_INDEX_MORE()` function |
| 6 | SQLFunctionSearchFieldsMore | `SEARCH_FIELDS_MORE()` function |
| 7 | Integration tests | End-to-end validation |
| 8 | Verification | Final testing and cleanup |

Each task follows TDD (test first, implement, verify, commit) and builds on previous tasks.
