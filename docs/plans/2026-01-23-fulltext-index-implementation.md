# Full-Text Index Improvements Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enhance ArcadeDB's full-text index with configurable analyzers, Lucene query syntax, SEARCH_INDEX/SEARCH_FIELDS functions, multi-property indexes, and $score exposure in SQL.

**Architecture:** Extend the existing LSMTreeFullTextIndex to support configurable analyzers via metadata, use Lucene's QueryParser for advanced query syntax, add SQL functions that query the index and return scored results, and propagate scores through the SQL execution pipeline.

**Tech Stack:** Java 21, Apache Lucene 10.x (QueryParser, Analyzers), ArcadeDB SQL engine

---

## Phase 1: Infrastructure

### Task 1.1: Create FullTextIndexMetadata Class

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/FullTextIndexMetadata.java`
- Test: `engine/src/test/java/com/arcadedb/schema/FullTextIndexMetadataTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.schema;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FullTextIndexMetadataTest {

  @Test
  void defaultValues() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Article", new String[]{"title"}, 0);

    assertThat(metadata.analyzerClass).isEqualTo("org.apache.lucene.analysis.standard.StandardAnalyzer");
    assertThat(metadata.allowLeadingWildcard).isFalse();
    assertThat(metadata.defaultOperator).isEqualTo("OR");
  }

  @Test
  void fromJSON() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Article", new String[]{"title"}, 0);

    final JSONObject json = new JSONObject();
    json.put("analyzer", "org.apache.lucene.analysis.en.EnglishAnalyzer");
    json.put("allowLeadingWildcard", true);
    json.put("defaultOperator", "AND");

    metadata.fromJSON(json);

    assertThat(metadata.analyzerClass).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
    assertThat(metadata.allowLeadingWildcard).isTrue();
    assertThat(metadata.defaultOperator).isEqualTo("AND");
  }

  @Test
  void perFieldAnalyzers() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Article", new String[]{"title", "body"}, 0);

    final JSONObject json = new JSONObject();
    json.put("analyzer", "org.apache.lucene.analysis.standard.StandardAnalyzer");
    json.put("title_analyzer", "org.apache.lucene.analysis.en.EnglishAnalyzer");

    metadata.fromJSON(json);

    assertThat(metadata.getAnalyzerClass("title")).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
    assertThat(metadata.getAnalyzerClass("body")).isEqualTo("org.apache.lucene.analysis.standard.StandardAnalyzer");
  }
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=FullTextIndexMetadataTest -pl engine`
Expected: FAIL with "cannot find symbol: class FullTextIndexMetadata"

**Step 3: Write minimal implementation**

```java
package com.arcadedb.schema;

import com.arcadedb.serializer.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Metadata for full-text indexes, supporting configurable Lucene analyzers.
 */
public class FullTextIndexMetadata extends IndexMetadata {
  public static final String DEFAULT_ANALYZER = "org.apache.lucene.analysis.standard.StandardAnalyzer";

  public String analyzerClass = DEFAULT_ANALYZER;
  public String indexAnalyzerClass = null;  // If set, used for indexing (analyzerClass used for queries)
  public String queryAnalyzerClass = null;  // If set, used for queries (analyzerClass used for indexing)
  public boolean allowLeadingWildcard = false;
  public String defaultOperator = "OR";

  // Per-field analyzer overrides: fieldName -> analyzerClass
  private final Map<String, String> fieldAnalyzers = new HashMap<>();

  public FullTextIndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    super(typeName, propertyNames, bucketId);
  }

  @Override
  public void fromJSON(final JSONObject json) {
    if (json.has("typeName"))
      super.fromJSON(json);

    if (json.has("analyzer"))
      this.analyzerClass = json.getString("analyzer");

    if (json.has("index_analyzer"))
      this.indexAnalyzerClass = json.getString("index_analyzer");

    if (json.has("query_analyzer"))
      this.queryAnalyzerClass = json.getString("query_analyzer");

    if (json.has("allowLeadingWildcard"))
      this.allowLeadingWildcard = json.getBoolean("allowLeadingWildcard");

    if (json.has("defaultOperator"))
      this.defaultOperator = json.getString("defaultOperator");

    // Parse per-field analyzers (fieldName_analyzer)
    for (final String key : json.keySet()) {
      if (key.endsWith("_analyzer") && !key.equals("index_analyzer") && !key.equals("query_analyzer")) {
        final String fieldName = key.substring(0, key.length() - "_analyzer".length());
        fieldAnalyzers.put(fieldName, json.getString(key));
      }
    }
  }

  /**
   * Gets the analyzer class for a specific field.
   * Returns field-specific analyzer if configured, otherwise the default analyzer.
   */
  public String getAnalyzerClass(final String fieldName) {
    return fieldAnalyzers.getOrDefault(fieldName, analyzerClass);
  }

  /**
   * Gets the analyzer class to use for indexing.
   */
  public String getIndexAnalyzerClass() {
    return indexAnalyzerClass != null ? indexAnalyzerClass : analyzerClass;
  }

  /**
   * Gets the analyzer class to use for queries.
   */
  public String getQueryAnalyzerClass() {
    return queryAnalyzerClass != null ? queryAnalyzerClass : analyzerClass;
  }
}
```

**Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=FullTextIndexMetadataTest -pl engine`
Expected: PASS

**Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/schema/FullTextIndexMetadata.java engine/src/test/java/com/arcadedb/schema/FullTextIndexMetadataTest.java
git commit -m "feat(fulltext): add FullTextIndexMetadata for analyzer configuration"
```

---

### Task 1.2: Create TypeFullTextIndexBuilder Class

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/TypeFullTextIndexBuilder.java`
- Test: `engine/src/test/java/com/arcadedb/schema/TypeFullTextIndexBuilderTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TypeFullTextIndexBuilderTest extends TestHelper {

  @Test
  void withMetadataFromJSON() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
      database.getSchema().getType("Article").createProperty("body", String.class);
    });

    database.transaction(() -> {
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Article", new String[]{"title"});
      final TypeFullTextIndexBuilder ftBuilder = builder.withType(Schema.INDEX_TYPE.FULL_TEXT).withFullTextType();

      final JSONObject json = new JSONObject();
      json.put("analyzer", "org.apache.lucene.analysis.en.EnglishAnalyzer");
      ftBuilder.withMetadata(json);

      assertThat(ftBuilder).isNotNull();
      assertThat(((FullTextIndexMetadata) ftBuilder.metadata).analyzerClass)
          .isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
    });
  }
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=TypeFullTextIndexBuilderTest -pl engine`
Expected: FAIL with "cannot find symbol: method withFullTextType()"

**Step 3: Write minimal implementation**

```java
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONObject;

/**
 * Builder class for full-text indexes with analyzer configuration.
 */
public class TypeFullTextIndexBuilder extends TypeIndexBuilder {

  protected TypeFullTextIndexBuilder(final TypeIndexBuilder copyFrom) {
    super(copyFrom.database, copyFrom.metadata.typeName, copyFrom.metadata.propertyNames.toArray(new String[0]));

    this.metadata = new FullTextIndexMetadata(
        copyFrom.metadata.typeName,
        copyFrom.metadata.propertyNames.toArray(new String[0]),
        copyFrom.metadata.associatedBucketId);

    this.indexType = Schema.INDEX_TYPE.FULL_TEXT;
    this.unique = copyFrom.unique;
    this.pageSize = copyFrom.pageSize;
    this.nullStrategy = copyFrom.nullStrategy;
    this.callback = copyFrom.callback;
    this.ignoreIfExists = copyFrom.ignoreIfExists;
    this.indexName = copyFrom.indexName;
    this.filePath = copyFrom.filePath;
    this.keyTypes = copyFrom.keyTypes;
    this.batchSize = copyFrom.batchSize;
    this.maxAttempts = copyFrom.maxAttempts;
  }

  protected TypeFullTextIndexBuilder(final DatabaseInternal database, final String typeName, final String[] propertyNames) {
    super(database, typeName, propertyNames);
    this.indexType = Schema.INDEX_TYPE.FULL_TEXT;
    this.metadata = new FullTextIndexMetadata(typeName, propertyNames, -1);
  }

  /**
   * Sets the analyzer class name.
   */
  public TypeFullTextIndexBuilder withAnalyzer(final String analyzerClass) {
    ((FullTextIndexMetadata) metadata).analyzerClass = analyzerClass;
    return this;
  }

  /**
   * Sets whether leading wildcards are allowed in queries.
   */
  public TypeFullTextIndexBuilder withAllowLeadingWildcard(final boolean allow) {
    ((FullTextIndexMetadata) metadata).allowLeadingWildcard = allow;
    return this;
  }

  /**
   * Sets the default boolean operator (AND or OR).
   */
  public TypeFullTextIndexBuilder withDefaultOperator(final String operator) {
    ((FullTextIndexMetadata) metadata).defaultOperator = operator;
    return this;
  }

  @Override
  public TypeFullTextIndexBuilder withMetadata(final IndexMetadata metadata) {
    this.metadata = (FullTextIndexMetadata) metadata;
    return this;
  }

  /**
   * Configures the builder from a JSON metadata object.
   */
  public void withMetadata(final JSONObject json) {
    ((FullTextIndexMetadata) metadata).fromJSON(json);
  }
}
```

**Step 4: Add withFullTextType() to TypeIndexBuilder**

Modify `engine/src/main/java/com/arcadedb/schema/TypeIndexBuilder.java`:

Add after line 61 (after the withType method):

```java
/**
 * Returns this builder as a TypeFullTextIndexBuilder for full-text specific configuration.
 * Only valid after withType(FULL_TEXT) has been called.
 */
public TypeFullTextIndexBuilder withFullTextType() {
  if (this instanceof TypeFullTextIndexBuilder)
    return (TypeFullTextIndexBuilder) this;
  if (indexType != Schema.INDEX_TYPE.FULL_TEXT)
    throw new IllegalStateException("withFullTextType() can only be called after withType(FULL_TEXT)");
  return new TypeFullTextIndexBuilder(this);
}
```

And modify the withType method to return TypeFullTextIndexBuilder for FULL_TEXT:

```java
@Override
public TypeIndexBuilder withType(final Schema.INDEX_TYPE indexType) {
  if (indexType == Schema.INDEX_TYPE.LSM_VECTOR && !(this instanceof TypeLSMVectorIndexBuilder))
    return new TypeLSMVectorIndexBuilder(this);
  if (indexType == Schema.INDEX_TYPE.FULL_TEXT && !(this instanceof TypeFullTextIndexBuilder))
    return new TypeFullTextIndexBuilder(this);
  super.withType(indexType);
  return this;
}
```

**Step 5: Run test to verify it passes**

Run: `mvn test -Dtest=TypeFullTextIndexBuilderTest -pl engine`
Expected: PASS

**Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/schema/TypeFullTextIndexBuilder.java engine/src/main/java/com/arcadedb/schema/TypeIndexBuilder.java engine/src/test/java/com/arcadedb/schema/TypeFullTextIndexBuilderTest.java
git commit -m "feat(fulltext): add TypeFullTextIndexBuilder for metadata configuration"
```

---

### Task 1.3: Update CreateIndexStatement for FULL_TEXT Metadata

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/parser/CreateIndexStatement.java:149-166`
- Test: `engine/src/test/java/com/arcadedb/index/LSMTreeFullTextIndexTest.java`

**Step 1: Write the failing test**

Add to `LSMTreeFullTextIndexTest.java`:

```java
@Test
void createIndexWithMetadata() {
  database.transaction(() -> {
    database.command("sql", "CREATE DOCUMENT TYPE Article");
    database.command("sql", "CREATE PROPERTY Article.title STRING");
    database.command("sql", "CREATE INDEX ON Article (title) FULL_TEXT METADATA {\"analyzer\": \"org.apache.lucene.analysis.en.EnglishAnalyzer\", \"allowLeadingWildcard\": true}");

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[title]");
    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);

    // Verify metadata was applied
    final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
    assertThat(ftIndex.getAnalyzer()).isInstanceOf(org.apache.lucene.analysis.en.EnglishAnalyzer.class);
  });
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=LSMTreeFullTextIndexTest#createIndexWithMetadata -pl engine`
Expected: FAIL (metadata not being passed to full-text index)

**Step 3: Update CreateIndexStatement.executeDDL()**

Modify `CreateIndexStatement.java` around line 149-166. Replace the LSM_VECTOR handling and add FULL_TEXT handling:

```java
    // Handle vector-specific metadata
    if (indexType == Schema.INDEX_TYPE.LSM_VECTOR) {
      if (metadata == null)
        throw new CommandSQLParsingException(
            "LSM_VECTOR index requires METADATA with dimensions, similarity, maxConnections, and beamWidth");

      final Map<String, Object> metadataMap = metadata.toMap((Result) null, context);
      final JSONObject jsonMetadata = new JSONObject(metadataMap);

      // Builder is now an LSMVectorIndexBuilder after withType(LSM_VECTOR)
      final TypeLSMVectorIndexBuilder vectorBuilder = builder.withLSMVectorType();
      vectorBuilder.withMetadata(jsonMetadata);
      vectorBuilder.create();

    } else if (indexType == Schema.INDEX_TYPE.FULL_TEXT && metadata != null) {
      // Handle full-text index metadata
      final Map<String, Object> metadataMap = metadata.toMap((Result) null, context);
      final JSONObject jsonMetadata = new JSONObject(metadataMap);

      final TypeFullTextIndexBuilder ftBuilder = builder.withFullTextType();
      ftBuilder.withMetadata(jsonMetadata);
      ftBuilder.create();

    } else {
      builder.create();
    }
```

Add import at top of file:
```java
import com.arcadedb.schema.TypeFullTextIndexBuilder;
```

**Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=LSMTreeFullTextIndexTest#createIndexWithMetadata -pl engine`
Expected: Still FAIL - need to update LSMTreeFullTextIndex to use metadata

**Step 5: Commit partial progress**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/parser/CreateIndexStatement.java
git commit -m "feat(fulltext): pass METADATA to full-text index builder in SQL"
```

---

### Task 1.4: Update LSMTreeFullTextIndex to Use Configurable Analyzer

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/index/lsm/LSMTreeFullTextIndex.java`
- Test: `engine/src/test/java/com/arcadedb/index/LSMTreeFullTextIndexTest.java`

**Step 1: Update the test expectations**

The test from Task 1.3 should pass after this implementation.

**Step 2: Update IndexFactoryHandler in LSMTreeFullTextIndex**

Modify `LSMTreeFullTextIndex.java` IndexFactoryHandler (around line 71-87):

```java
public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
  @Override
  public IndexInternal create(final IndexBuilder builder) {
    if (builder.isUnique())
      throw new IllegalArgumentException("Full text index cannot be unique");

    // Allow multiple STRING properties for multi-property indexes
    for (final Type keyType : builder.getKeyTypes()) {
      if (keyType != Type.STRING)
        throw new IllegalArgumentException(
            "Full text index can only be defined on STRING properties, found: " + keyType);
    }

    // Get metadata if available
    FullTextIndexMetadata ftMetadata = null;
    if (builder.getMetadata() instanceof FullTextIndexMetadata) {
      ftMetadata = (FullTextIndexMetadata) builder.getMetadata();
    }

    return new LSMTreeFullTextIndex(builder.getDatabase(), builder.getIndexName(), builder.getFilePath(),
        ComponentFile.MODE.READ_WRITE, builder.getPageSize(), builder.getNullStrategy(),
        builder.getKeyTypes().length, ftMetadata);
  }
}
```

**Step 3: Update constructor and add analyzer creation**

Add new fields and modify constructors:

```java
public class LSMTreeFullTextIndex implements Index, IndexInternal {
  private final LSMTreeIndex underlyingIndex;
  private final Analyzer     indexAnalyzer;
  private final Analyzer     queryAnalyzer;
  private final FullTextIndexMetadata ftMetadata;
  private final int propertyCount;
  private       TypeIndex    typeIndex;

  // ... existing IndexFactoryHandler ...

  /**
   * Called at load time. The Full Text index is just a wrapper of an LSMTree Index.
   */
  public LSMTreeFullTextIndex(final LSMTreeIndex index) {
    this(index, null);
  }

  public LSMTreeFullTextIndex(final LSMTreeIndex index, final FullTextIndexMetadata metadata) {
    this.underlyingIndex = index;
    this.ftMetadata = metadata;
    this.propertyCount = 1; // Will be set properly when loading
    this.indexAnalyzer = createAnalyzer(metadata, true);
    this.queryAnalyzer = createAnalyzer(metadata, false);
  }

  /**
   * Creation time.
   */
  public LSMTreeFullTextIndex(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final int propertyCount, final FullTextIndexMetadata metadata) {
    this.ftMetadata = metadata;
    this.propertyCount = propertyCount;
    this.indexAnalyzer = createAnalyzer(metadata, true);
    this.queryAnalyzer = createAnalyzer(metadata, false);
    underlyingIndex = new LSMTreeIndex(database, name, false, filePath, mode, new Type[] { Type.STRING }, pageSize, nullStrategy);
  }

  /**
   * Loading time.
   */
  public LSMTreeFullTextIndex(final DatabaseInternal database, final String name, final String filePath, final int fileId,
      final ComponentFile.MODE mode, final int pageSize, final int version) {
    try {
      underlyingIndex = new LSMTreeIndex(database, name, false, filePath, fileId, mode, pageSize, version);
    } catch (final IOException e) {
      throw new IndexException("Cannot create search engine (error=" + e + ")", e);
    }
    this.ftMetadata = null;
    this.propertyCount = 1;
    this.indexAnalyzer = new StandardAnalyzer();
    this.queryAnalyzer = new StandardAnalyzer();
  }

  /**
   * Creates an analyzer from the metadata configuration.
   */
  private static Analyzer createAnalyzer(final FullTextIndexMetadata metadata, final boolean forIndexing) {
    if (metadata == null)
      return new StandardAnalyzer();

    final String analyzerClass = forIndexing ? metadata.getIndexAnalyzerClass() : metadata.getQueryAnalyzerClass();

    try {
      final Class<?> clazz = Class.forName(analyzerClass);
      return (Analyzer) clazz.getDeclaredConstructor().newInstance();
    } catch (final Exception e) {
      throw new IndexException("Cannot instantiate analyzer: " + analyzerClass, e);
    }
  }

  public Analyzer getAnalyzer() {
    return queryAnalyzer;
  }

  public Analyzer getIndexAnalyzer() {
    return indexAnalyzer;
  }

  public FullTextIndexMetadata getFullTextMetadata() {
    return ftMetadata;
  }
```

**Step 4: Update put() to use indexAnalyzer**

```java
@Override
public void put(final Object[] keys, final RID[] rids) {
  final List<String> keywords = analyzeText(indexAnalyzer, keys);
  for (final String k : keywords)
    underlyingIndex.put(new String[] { k }, rids);
}
```

**Step 5: Update get() to use queryAnalyzer**

```java
@Override
public IndexCursor get(final Object[] keys, final int limit) {
  final List<String> keywords = analyzeText(queryAnalyzer, keys);
  // ... rest of method unchanged
}
```

**Step 6: Add import for FullTextIndexMetadata**

```java
import com.arcadedb.schema.FullTextIndexMetadata;
```

**Step 7: Run test to verify it passes**

Run: `mvn test -Dtest=LSMTreeFullTextIndexTest#createIndexWithMetadata -pl engine`
Expected: PASS

**Step 8: Commit**

```bash
git add engine/src/main/java/com/arcadedb/index/lsm/LSMTreeFullTextIndex.java
git commit -m "feat(fulltext): support configurable Lucene analyzers via metadata"
```

---

## Phase 2: Multi-Property Support

### Task 2.1: Update Token Storage for Multi-Property Indexes

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/index/lsm/LSMTreeFullTextIndex.java`
- Test: `engine/src/test/java/com/arcadedb/index/LSMTreeFullTextIndexTest.java`

**Step 1: Write the failing test**

Add to `LSMTreeFullTextIndexTest.java`:

```java
@Test
void multiPropertyIndex() {
  database.transaction(() -> {
    database.command("sql", "CREATE DOCUMENT TYPE Article");
    database.command("sql", "CREATE PROPERTY Article.title STRING");
    database.command("sql", "CREATE PROPERTY Article.body STRING");
    database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");

    database.command("sql", "INSERT INTO Article SET title = 'Java Programming', body = 'Learn Java basics'");
    database.command("sql", "INSERT INTO Article SET title = 'Python Tutorial', body = 'Python programming guide'");
  });

  database.transaction(() -> {
    // Search should find documents matching in either field
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[title,body]");
    final IndexCursor cursor = index.get(new Object[] { "java", "programming" });

    int count = 0;
    while (cursor.hasNext()) {
      cursor.next();
      count++;
    }
    // Both documents should match (Java appears in title of doc1, programming in both)
    assertThat(count).isEqualTo(2);
  });
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=LSMTreeFullTextIndexTest#multiPropertyIndex -pl engine`
Expected: FAIL with "Full text index can only be defined on one only string property"

**Step 3: Update put() method for multi-property support**

The key change is to prefix tokens with field names for multi-property indexes:

```java
@Override
public void put(final Object[] keys, final RID[] rids) {
  if (propertyCount == 1) {
    // Single property - existing behavior
    final List<String> keywords = analyzeText(indexAnalyzer, keys);
    for (final String k : keywords)
      underlyingIndex.put(new String[] { k }, rids);
  } else {
    // Multi-property - prefix tokens with field index
    final List<String> propertyNames = getPropertyNames();
    for (int i = 0; i < keys.length && i < propertyNames.size(); i++) {
      if (keys[i] == null)
        continue;
      final String fieldName = propertyNames.get(i);
      final List<String> keywords = analyzeText(indexAnalyzer, new Object[] { keys[i] });
      for (final String k : keywords) {
        // Store with field prefix for field-specific queries
        underlyingIndex.put(new String[] { fieldName + ":" + k }, rids);
        // Also store without prefix for unqualified queries
        underlyingIndex.put(new String[] { k }, rids);
      }
    }
  }
}
```

**Step 4: Update get() for multi-property support**

The get method needs to handle both prefixed and unprefixed lookups:

```java
@Override
public IndexCursor get(final Object[] keys, final int limit) {
  final List<String> keywords = analyzeText(queryAnalyzer, keys);

  final HashMap<RID, AtomicInteger> scoreMap = new HashMap<>();

  for (final String k : keywords) {
    // Check if this is a field-qualified search (field:term)
    final IndexCursor rids;
    if (k.contains(":")) {
      // Field-specific search
      rids = underlyingIndex.get(new String[] { k });
    } else {
      // Unqualified search - search without prefix
      rids = underlyingIndex.get(new String[] { k });
    }

    while (rids.hasNext()) {
      final RID rid = rids.next().getIdentity();

      final AtomicInteger score = scoreMap.get(rid);
      if (score == null)
        scoreMap.put(rid, new AtomicInteger(1));
      else
        score.incrementAndGet();
    }
  }

  // ... rest unchanged
}
```

**Step 5: Run test to verify it passes**

Run: `mvn test -Dtest=LSMTreeFullTextIndexTest#multiPropertyIndex -pl engine`
Expected: PASS

**Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/index/lsm/LSMTreeFullTextIndex.java engine/src/test/java/com/arcadedb/index/LSMTreeFullTextIndexTest.java
git commit -m "feat(fulltext): support multi-property full-text indexes"
```

---

## Phase 3: Query Functions

### Task 3.1: Create SQLFunctionSearchIndex

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndex.java`
- Modify: `engine/src/main/java/com/arcadedb/query/sql/function/DefaultSQLFunctionFactory.java`
- Test: `engine/src/test/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndexTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.query.sql.function.text;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLFunctionSearchIndexTest extends TestHelper {

  @BeforeEach
  void setup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java database system'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'python scripting'");
    });
  }

  @Test
  void basicSearch() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'java')");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.getProperty("title").toString()).isIn("Doc1", "Doc2");
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void booleanSearch() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', '+java +programming')");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.getProperty("title")).isEqualTo("Doc1");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=SQLFunctionSearchIndexTest -pl engine`
Expected: FAIL with "Unknown function 'SEARCH_INDEX'"

**Step 3: Create SQLFunctionSearchIndex**

```java
package com.arcadedb.query.sql.function.text;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import java.util.*;

/**
 * SQL function to search a full-text index using Lucene query syntax.
 *
 * Usage: SEARCH_INDEX('indexName', 'query')
 * Example: SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', '+java -python')
 */
public class SQLFunctionSearchIndex extends SQLFunctionAbstract {
  public static final String NAME = "search_index";

  public SQLFunctionSearchIndex() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    if (iParams.length < 2)
      throw new CommandExecutionException("SEARCH_INDEX() requires 2 parameters: indexName and query");

    final String indexName = iParams[0].toString();
    final String queryString = iParams[1].toString();

    final Database database = iContext.getDatabase();
    final Index index = database.getSchema().getIndexByName(indexName);

    if (index == null)
      throw new CommandExecutionException("Index '" + indexName + "' not found");

    if (!(index instanceof TypeIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a type index");

    final TypeIndex typeIndex = (TypeIndex) index;

    // Get the underlying full-text index
    final Index[] bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes.length == 0 || !(bucketIndexes[0] instanceof LSMTreeFullTextIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a full-text index");

    // For now, simple keyword search - Lucene QueryParser integration in next task
    final IndexCursor cursor = typeIndex.get(new Object[] { queryString });

    // Check if current record matches
    if (iCurrentRecord != null) {
      final RID currentRid = iCurrentRecord.getIdentity();
      while (cursor.hasNext()) {
        final Identifiable match = cursor.next();
        if (match.getIdentity().equals(currentRid)) {
          return true;
        }
      }
      return false;
    }

    return cursor;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX(<index-name>, <query>)";
  }
}
```

**Step 4: Register in DefaultSQLFunctionFactory**

Add to `DefaultSQLFunctionFactory.java` after line 194 (in the Text section):

```java
import com.arcadedb.query.sql.function.text.SQLFunctionSearchIndex;

// In constructor, add after SQLFunctionStrcmpci:
register(SQLFunctionSearchIndex.NAME, SQLFunctionSearchIndex.class);
```

**Step 5: Run test to verify it passes**

Run: `mvn test -Dtest=SQLFunctionSearchIndexTest#basicSearch -pl engine`
Expected: PASS

**Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndex.java engine/src/main/java/com/arcadedb/query/sql/function/DefaultSQLFunctionFactory.java engine/src/test/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndexTest.java
git commit -m "feat(fulltext): add SEARCH_INDEX() SQL function"
```

---

### Task 3.2: Integrate Lucene QueryParser

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/index/lsm/LSMTreeFullTextIndex.java`
- Create: `engine/src/main/java/com/arcadedb/index/lsm/FullTextQueryExecutor.java`
- Test: `engine/src/test/java/com/arcadedb/index/FullTextQueryExecutorTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.index.lsm.FullTextQueryExecutor;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class FullTextQueryExecutorTest extends TestHelper {

  @Test
  void parseBooleanQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET content = 'java database'");
      database.command("sql", "INSERT INTO Article SET content = 'python programming'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];

      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // +java +programming should only match first document
      final IndexCursor cursor = executor.search("+java +programming", -1);

      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void parseExclusionQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java database'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];

      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // java -programming should only match second document
      final IndexCursor cursor = executor.search("java -programming", -1);

      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=FullTextQueryExecutorTest -pl engine`
Expected: FAIL with "cannot find symbol: class FullTextQueryExecutor"

**Step 3: Create FullTextQueryExecutor**

```java
package com.arcadedb.index.lsm;

import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executes Lucene queries against an LSMTreeFullTextIndex.
 * Translates parsed Lucene Query objects into LSM-Tree lookups.
 */
public class FullTextQueryExecutor {
  private final LSMTreeFullTextIndex index;
  private final Analyzer analyzer;
  private final QueryParser queryParser;

  public FullTextQueryExecutor(final LSMTreeFullTextIndex index) {
    this.index = index;
    this.analyzer = index.getAnalyzer();

    final FullTextIndexMetadata metadata = index.getFullTextMetadata();
    this.queryParser = new QueryParser("content", analyzer);

    if (metadata != null) {
      queryParser.setAllowLeadingWildcard(metadata.allowLeadingWildcard);
      if ("AND".equalsIgnoreCase(metadata.defaultOperator)) {
        queryParser.setDefaultOperator(QueryParser.Operator.AND);
      }
    }
  }

  /**
   * Searches the index using Lucene query syntax.
   */
  public IndexCursor search(final String queryString, final int limit) {
    try {
      final Query query = queryParser.parse(queryString);
      return executeQuery(query, limit);
    } catch (final ParseException e) {
      throw new IndexException("Invalid search query: " + queryString, e);
    }
  }

  private IndexCursor executeQuery(final Query query, final int limit) {
    final Map<RID, AtomicInteger> scoreMap = new HashMap<>();
    final Set<RID> excluded = new HashSet<>();

    collectMatches(query, scoreMap, excluded, true);

    // Remove excluded RIDs
    for (final RID rid : excluded) {
      scoreMap.remove(rid);
    }

    final int maxElements = limit > -1 ? Math.min(limit, scoreMap.size()) : scoreMap.size();
    final ArrayList<IndexCursorEntry> list = new ArrayList<>(maxElements);

    for (final Map.Entry<RID, AtomicInteger> entry : scoreMap.entrySet()) {
      list.add(new IndexCursorEntry(new Object[] { }, entry.getKey(), entry.getValue().get()));
    }

    // Sort by score descending
    list.sort((o1, o2) -> Integer.compare(o2.score, o1.score));

    if (limit > 0 && list.size() > limit) {
      return new TempIndexCursor(list.subList(0, limit));
    }
    return new TempIndexCursor(list);
  }

  private void collectMatches(final Query query, final Map<RID, AtomicInteger> scoreMap,
      final Set<RID> excluded, final boolean required) {

    if (query instanceof BooleanQuery) {
      final BooleanQuery bq = (BooleanQuery) query;
      for (final BooleanClause clause : bq.clauses()) {
        if (clause.getOccur() == BooleanClause.Occur.MUST_NOT) {
          collectMatches(clause.getQuery(), scoreMap, excluded, false);
          // Mark all matches from MUST_NOT as excluded
          final Map<RID, AtomicInteger> tempMap = new HashMap<>();
          collectTermMatches(clause.getQuery(), tempMap);
          excluded.addAll(tempMap.keySet());
        } else {
          collectMatches(clause.getQuery(), scoreMap, excluded,
              clause.getOccur() == BooleanClause.Occur.MUST);
        }
      }
    } else if (query instanceof TermQuery) {
      collectTermMatches(query, scoreMap);
    } else if (query instanceof PrefixQuery) {
      collectPrefixMatches((PrefixQuery) query, scoreMap);
    } else if (query instanceof WildcardQuery) {
      collectWildcardMatches((WildcardQuery) query, scoreMap);
    } else if (query instanceof PhraseQuery) {
      collectPhraseMatches((PhraseQuery) query, scoreMap);
    }
  }

  private void collectTermMatches(final Query query, final Map<RID, AtomicInteger> scoreMap) {
    if (query instanceof TermQuery) {
      final String term = ((TermQuery) query).getTerm().text();
      final IndexCursor cursor = index.get(new Object[] { term });
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).incrementAndGet();
      }
    }
  }

  private void collectPrefixMatches(final PrefixQuery query, final Map<RID, AtomicInteger> scoreMap) {
    // For prefix queries, we need to scan - simplified implementation
    final String prefix = query.getPrefix().text();
    // This would need index iteration support - for now, treat as term
    final IndexCursor cursor = index.get(new Object[] { prefix });
    while (cursor.hasNext()) {
      final RID rid = cursor.next().getIdentity();
      scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).incrementAndGet();
    }
  }

  private void collectWildcardMatches(final WildcardQuery query, final Map<RID, AtomicInteger> scoreMap) {
    // Simplified - treat as term for now
    final String term = query.getTerm().text().replace("*", "").replace("?", "");
    if (!term.isEmpty()) {
      final IndexCursor cursor = index.get(new Object[] { term });
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).incrementAndGet();
      }
    }
  }

  private void collectPhraseMatches(final PhraseQuery query, final Map<RID, AtomicInteger> scoreMap) {
    // For phrase queries, all terms must match in the same document
    // Simplified: just require all terms to match (not checking order)
    final org.apache.lucene.index.Term[] terms = query.getTerms();
    if (terms.length == 0) return;

    Map<RID, AtomicInteger> intersection = null;

    for (final org.apache.lucene.index.Term term : terms) {
      final Map<RID, AtomicInteger> termMatches = new HashMap<>();
      final IndexCursor cursor = index.get(new Object[] { term.text() });
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        termMatches.put(rid, new AtomicInteger(1));
      }

      if (intersection == null) {
        intersection = termMatches;
      } else {
        intersection.keySet().retainAll(termMatches.keySet());
      }
    }

    if (intersection != null) {
      for (final RID rid : intersection.keySet()) {
        scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).addAndGet(terms.length);
      }
    }
  }
}
```

**Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=FullTextQueryExecutorTest -pl engine`
Expected: PASS

**Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/index/lsm/FullTextQueryExecutor.java engine/src/test/java/com/arcadedb/index/FullTextQueryExecutorTest.java
git commit -m "feat(fulltext): add FullTextQueryExecutor for Lucene query parsing"
```

---

### Task 3.3: Update SEARCH_INDEX to Use QueryExecutor

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndex.java`
- Test: `engine/src/test/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndexTest.java`

**Step 1: Update test to verify boolean queries work**

Run existing test: `mvn test -Dtest=SQLFunctionSearchIndexTest#booleanSearch -pl engine`
Expected: FAIL (not using QueryExecutor yet)

**Step 2: Update SQLFunctionSearchIndex to use FullTextQueryExecutor**

Replace the execute method:

```java
@Override
public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
    final Object[] iParams, final CommandContext iContext) {

  if (iParams.length < 2)
    throw new CommandExecutionException("SEARCH_INDEX() requires 2 parameters: indexName and query");

  final String indexName = iParams[0].toString();
  final String queryString = iParams[1].toString();

  final Database database = iContext.getDatabase();
  final Index index = database.getSchema().getIndexByName(indexName);

  if (index == null)
    throw new CommandExecutionException("Index '" + indexName + "' not found");

  if (!(index instanceof TypeIndex))
    throw new CommandExecutionException("Index '" + indexName + "' is not a type index");

  final TypeIndex typeIndex = (TypeIndex) index;

  // Get the underlying full-text index from first bucket
  final Index[] bucketIndexes = typeIndex.getIndexesOnBuckets();
  if (bucketIndexes.length == 0 || !(bucketIndexes[0] instanceof LSMTreeFullTextIndex))
    throw new CommandExecutionException("Index '" + indexName + "' is not a full-text index");

  final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) bucketIndexes[0];
  final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

  // Execute the search across all bucket indexes
  final Map<RID, Integer> allResults = new HashMap<>();

  for (final Index bucketIndex : bucketIndexes) {
    if (bucketIndex instanceof LSMTreeFullTextIndex) {
      final FullTextQueryExecutor bucketExecutor = new FullTextQueryExecutor((LSMTreeFullTextIndex) bucketIndex);
      final IndexCursor cursor = bucketExecutor.search(queryString, -1);

      while (cursor.hasNext()) {
        final Identifiable match = cursor.next();
        final int score = cursor.getScore();
        allResults.merge(match.getIdentity(), score, Integer::sum);
      }
    }
  }

  // Check if current record matches
  if (iCurrentRecord != null) {
    return allResults.containsKey(iCurrentRecord.getIdentity());
  }

  // Return cursor with all results
  final List<IndexCursorEntry> entries = new ArrayList<>();
  for (final Map.Entry<RID, Integer> entry : allResults.entrySet()) {
    entries.add(new IndexCursorEntry(new Object[] { queryString }, entry.getKey(), entry.getValue()));
  }
  entries.sort((a, b) -> Integer.compare(b.score, a.score));

  return new TempIndexCursor(entries);
}
```

Add imports:

```java
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.lsm.FullTextQueryExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
```

**Step 3: Run test to verify it passes**

Run: `mvn test -Dtest=SQLFunctionSearchIndexTest -pl engine`
Expected: PASS

**Step 4: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndex.java
git commit -m "feat(fulltext): integrate Lucene QueryParser in SEARCH_INDEX function"
```

---

### Task 3.4: Create SQLFunctionSearchFields

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchFields.java`
- Modify: `engine/src/main/java/com/arcadedb/query/sql/function/DefaultSQLFunctionFactory.java`
- Test: `engine/src/test/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchFieldsTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.query.sql.function.text;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLFunctionSearchFieldsTest extends TestHelper {

  @BeforeEach
  void setup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'python scripting'");
    });
  }

  @Test
  void searchByFieldNames() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_FIELDS(['content'], 'java')");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.getProperty("title")).isEqualTo("Doc1");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=SQLFunctionSearchFieldsTest -pl engine`
Expected: FAIL with "Unknown function 'SEARCH_FIELDS'"

**Step 3: Create SQLFunctionSearchFields**

```java
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
import com.arcadedb.index.lsm.FullTextQueryExecutor;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;

/**
 * SQL function to search full-text indexes by field names.
 * Automatically finds the appropriate full-text index for the given fields.
 *
 * Usage: SEARCH_FIELDS(['field1', 'field2'], 'query')
 * Example: SELECT FROM Article WHERE SEARCH_FIELDS(['title', 'content'], 'java')
 */
public class SQLFunctionSearchFields extends SQLFunctionAbstract {
  public static final String NAME = "search_fields";

  public SQLFunctionSearchFields() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    if (iParams.length < 2)
      throw new CommandExecutionException("SEARCH_FIELDS() requires 2 parameters: fields array and query");

    // Parse field names from first parameter
    final List<String> fieldNames = new ArrayList<>();
    final Object fieldsParam = iParams[0];

    if (fieldsParam instanceof Collection) {
      for (final Object f : (Collection<?>) fieldsParam) {
        fieldNames.add(f.toString());
      }
    } else if (fieldsParam.getClass().isArray()) {
      for (final Object f : (Object[]) fieldsParam) {
        fieldNames.add(f.toString());
      }
    } else {
      fieldNames.add(fieldsParam.toString());
    }

    final String queryString = iParams[1].toString();
    final Database database = iContext.getDatabase();

    // Find the full-text index that covers these fields
    // First, determine the type from the current record or query context
    String typeName = null;
    if (iCurrentRecord != null) {
      typeName = iCurrentRecord.asDocument().getTypeName();
    }

    if (typeName == null)
      throw new CommandExecutionException("SEARCH_FIELDS() requires a type context (use in WHERE clause with FROM)");

    final DocumentType type = database.getSchema().getType(typeName);

    // Find full-text index matching the fields
    TypeIndex matchingIndex = null;
    for (final TypeIndex typeIndex : type.getAllIndexes(true)) {
      if (typeIndex.getType() == Schema.INDEX_TYPE.FULL_TEXT) {
        final List<String> indexFields = typeIndex.getPropertyNames();
        if (indexFields.containsAll(fieldNames)) {
          matchingIndex = typeIndex;
          break;
        }
      }
    }

    if (matchingIndex == null)
      throw new CommandExecutionException("No full-text index found for fields: " + fieldNames);

    // Execute search using the found index
    final Map<RID, Integer> allResults = new HashMap<>();

    for (final Index bucketIndex : matchingIndex.getIndexesOnBuckets()) {
      if (bucketIndex instanceof LSMTreeFullTextIndex) {
        final FullTextQueryExecutor executor = new FullTextQueryExecutor((LSMTreeFullTextIndex) bucketIndex);
        final IndexCursor cursor = executor.search(queryString, -1);

        while (cursor.hasNext()) {
          final Identifiable match = cursor.next();
          final int score = cursor.getScore();
          allResults.merge(match.getIdentity(), score, Integer::sum);
        }
      }
    }

    // Check if current record matches
    if (iCurrentRecord != null) {
      return allResults.containsKey(iCurrentRecord.getIdentity());
    }

    // Return cursor with all results
    final List<IndexCursorEntry> entries = new ArrayList<>();
    for (final Map.Entry<RID, Integer> entry : allResults.entrySet()) {
      entries.add(new IndexCursorEntry(new Object[] { queryString }, entry.getKey(), entry.getValue()));
    }
    entries.sort((a, b) -> Integer.compare(b.score, a.score));

    return new TempIndexCursor(entries);
  }

  @Override
  public String getSyntax() {
    return "SEARCH_FIELDS(<fields-array>, <query>)";
  }
}
```

**Step 4: Register in DefaultSQLFunctionFactory**

Add after SQLFunctionSearchIndex registration:

```java
import com.arcadedb.query.sql.function.text.SQLFunctionSearchFields;

register(SQLFunctionSearchFields.NAME, SQLFunctionSearchFields.class);
```

**Step 5: Run test to verify it passes**

Run: `mvn test -Dtest=SQLFunctionSearchFieldsTest -pl engine`
Expected: PASS

**Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchFields.java engine/src/main/java/com/arcadedb/query/sql/function/DefaultSQLFunctionFactory.java engine/src/test/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchFieldsTest.java
git commit -m "feat(fulltext): add SEARCH_FIELDS() SQL function"
```

---

## Phase 4: Score Exposure

### Task 4.1: Add $score to ResultInternal

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/ResultInternal.java`
- Test: `engine/src/test/java/com/arcadedb/query/sql/executor/ResultInternalTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResultInternalScoreTest {

  @Test
  void scoreProperty() {
    final ResultInternal result = new ResultInternal(null);
    result.setProperty("title", "Test");
    result.setScore(0.95f);

    assertThat(result.getScore()).isEqualTo(0.95f);
    assertThat(result.getProperty("$score")).isEqualTo(0.95f);
  }

  @Test
  void scoreDefaultsToZero() {
    final ResultInternal result = new ResultInternal(null);
    assertThat(result.getScore()).isEqualTo(0f);
  }
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=ResultInternalScoreTest -pl engine`
Expected: FAIL with "cannot find symbol: method getScore()"

**Step 3: Add score support to ResultInternal**

In `ResultInternal.java`, add:

```java
private float score = 0f;

public float getScore() {
  return score;
}

public void setScore(final float score) {
  this.score = score;
}

// In getProperty method, add special handling for $score:
@Override
public <T> T getProperty(final String name) {
  if ("$score".equals(name))
    return (T) Float.valueOf(score);
  // ... existing code
}

// In hasProperty method, add:
@Override
public boolean hasProperty(final String name) {
  if ("$score".equals(name))
    return true;
  // ... existing code
}

// In getPropertyNames, include $score if score > 0:
@Override
public Set<String> getPropertyNames() {
  final Set<String> names = new LinkedHashSet<>();
  if (score > 0)
    names.add("$score");
  // ... existing code
}
```

**Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=ResultInternalScoreTest -pl engine`
Expected: PASS

**Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/executor/ResultInternal.java engine/src/test/java/com/arcadedb/query/sql/executor/ResultInternalScoreTest.java
git commit -m "feat(fulltext): add score support to ResultInternal"
```

---

### Task 4.2: Propagate Score Through Query Execution

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/function/text/SQLFunctionSearchIndex.java`
- Test: `engine/src/test/java/com/arcadedb/index/LSMTreeFullTextIndexTest.java`

**Step 1: Update test to check $score in SQL results**

Add to `LSMTreeFullTextIndexTest.java`:

```java
@Test
void scoreInSQLProjection() {
  database.transaction(() -> {
    database.command("sql", "CREATE DOCUMENT TYPE Article");
    database.command("sql", "CREATE PROPERTY Article.title STRING");
    database.command("sql", "CREATE PROPERTY Article.content STRING");
    database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

    database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
    database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java database'");
  });

  database.transaction(() -> {
    final ResultSet result = database.query("sql",
        "SELECT title, $score FROM Article WHERE SEARCH_INDEX('Article[content]', 'java programming')");

    final Map<String, Float> scores = new HashMap<>();
    while (result.hasNext()) {
      final Result r = result.next();
      final String title = r.getProperty("title");
      final Float score = r.getProperty("$score");
      assertThat(score).isNotNull();
      assertThat(score).isGreaterThan(0f);
      scores.put(title, score);
    }

    assertThat(scores).hasSize(2);
    // Doc1 matches both 'java' and 'programming', Doc2 only 'java'
    assertThat(scores.get("Doc1")).isGreaterThan(scores.get("Doc2"));
  });
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=LSMTreeFullTextIndexTest#scoreInSQLProjection -pl engine`
Expected: FAIL ($score not being set)

**Step 3: Update search functions to set score on results**

This requires modifying how the search function results are processed in the query execution pipeline. The score needs to be set on the ResultInternal objects as they pass through.

The implementation depends on understanding the query execution pipeline better. For now, we'll mark this as requiring deeper integration work.

**Step 4: Commit partial progress**

```bash
git add engine/src/test/java/com/arcadedb/index/LSMTreeFullTextIndexTest.java
git commit -m "test(fulltext): add test for $score in SQL projection (pending implementation)"
```

---

## Phase 5: Testing & Documentation

### Task 5.1: Integration Tests

**Files:**
- Create: `engine/src/test/java/com/arcadedb/index/FullTextIndexIntegrationTest.java`

Create comprehensive integration tests covering all features.

### Task 5.2: Performance Tests

**Files:**
- Create: `engine/src/test/java/performance/FullTextIndexPerformanceTest.java`

Create performance benchmarks for large datasets.

### Task 5.3: Documentation

Update manual documentation to describe new features.

---

## Summary

This plan covers:

| Phase | Tasks | Estimated Steps |
|-------|-------|-----------------|
| 1. Infrastructure | 4 tasks | ~30 steps |
| 2. Multi-Property | 1 task | ~6 steps |
| 3. Query Functions | 4 tasks | ~25 steps |
| 4. Score Exposure | 2 tasks | ~12 steps |
| 5. Testing & Docs | 3 tasks | ~15 steps |

Total: ~14 tasks, ~88 steps

Each step is designed to be 2-5 minutes of work following TDD principles.
