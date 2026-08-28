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
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.serializer.json.JSONObject;

import java.util.List;

/**
 * Builder class for index types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class IndexBuilder<T extends Index> {
  public static final int BUILD_BATCH_SIZE = 5_000;

  /**
   * Value of {@link #pageSize} meaning "the caller did not ask for a page size", so each index implementation is free
   * to pick its own default.
   * <p>
   * This used to be expressed by initialising the field to {@link LSMTreeIndexAbstract#DEF_PAGE_SIZE} and having
   * {@code HashIndex} read that exact value back as "unset". That conflated the two, and made 262144 the one page size
   * a hash index could never actually be given - see issue #5713.
   */
  public static final int PAGE_SIZE_UNSET = -1;

  final DatabaseInternal       database;
  final Class<? extends Index> indexImplementation;
  Schema.INDEX_TYPE                  indexType;
  boolean                            unique;
  int                                pageSize              = PAGE_SIZE_UNSET;
  LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy          = LSMTreeIndexAbstract.NULL_STRATEGY.SKIP;
  Index.BuildIndexCallback           callback;
  boolean                            ignoreIfExists        = false;
  /**
   * Explicit permission to take away an index that does not provide what was asked for - see
   * {@link #withReplaceIfIncompatible(boolean)}. Kept separate from {@link #ignoreIfExists} on purpose: "I can live
   * with what is there" and "replace what is there" are different intents, and conflating them is what made a guarded
   * {@code CREATE INDEX} silently rebuild an index the caller only meant to skip (issue #5675).
   */
  boolean                            replaceIfIncompatible = false;
  String                             indexName             = null;
  String                             filePath              = null;
  Type[]                             keyTypes;
  int                                batchSize             = BUILD_BATCH_SIZE;
  int                                maxAttempts           = 1;
  /**
   * The one and only metadata slot of the builder hierarchy. {@link TypeIndexBuilder} used to declare a field of the
   * same name, which SHADOWED this one: {@code withMetadata()} wrote here while {@code create()} read there, so every
   * index type without a dedicated builder subclass (GEOSPATIAL, see #5478) silently lost the metadata it was given.
   * Keep it single - and reach it through {@link #withMetadata(IndexMetadata)} / {@link #getMetadata()} from outside
   * this package, so a second slot cannot be reintroduced unnoticed.
   */
  IndexMetadata                      metadata;
  /**
   * The {@code METADATA} clause exactly as the caller wrote it, or null when none was given. Kept BESIDE the parsed
   * {@link #metadata} rather than derived from it because the two answer different questions: the parsed form says
   * what the index would be configured with, this one says which settings the caller actually NAMED - the only ones
   * an {@code IF NOT EXISTS} request compares against an existing index. See
   * {@link IndexMetadata#findUserSettingMismatches}.
   */
  JSONObject                         userMetadata;

  protected IndexBuilder(final DatabaseInternal database, final Class<? extends Index> indexImplementation) {
    this.database = database;
    this.indexImplementation = indexImplementation;
  }

  /**
   * Carries every setting THIS class declares over to a builder that replaces {@code this} mid-configuration - the
   * builder swap of {@link TypeIndexBuilder#withType}, which hands back a specialised subclass once the index type is
   * known and leaves everything configured so far behind unless it is copied across.
   * <p>
   * The point of it being one method is that it is the ONE place a new field of this class has to be added. The four
   * specialised builders used to hand-copy the same eleven fields each, which is a copy that cannot fail to compile
   * and cannot be missed by review: {@code replaceIfIncompatible} (#5675) and {@code userMetadata} (#5723) were both
   * added to this class, wired through a {@code withX()} setter, and silently dropped by every one of them - working
   * perfectly for {@code LSM_TREE} and {@code HASH}, which never swap builders, and evaporating for FULL_TEXT,
   * GEOSPATIAL and the two vector types (issue #5606). The shadowed {@code metadata} field of #5478 was the same class
   * of bug one level down.
   * <p>
   * {@link #indexType}, {@link #metadata}, {@link #database} and {@link #indexImplementation} are deliberately NOT
   * here: they are what the swap is FOR - the target builder is constructed around the new index type and its own
   * metadata subclass, and gets the database from the source. See
   * {@link TypeIndexBuilder#TypeIndexBuilder(TypeIndexBuilder, Schema.INDEX_TYPE, IndexMetadata)}, which sets those
   * three and then calls this.
   */
  protected void copyBaseFieldsFrom(final IndexBuilder<?> source) {
    this.unique = source.unique;
    this.pageSize = source.pageSize;
    this.nullStrategy = source.nullStrategy;
    this.callback = source.callback;
    this.ignoreIfExists = source.ignoreIfExists;
    this.replaceIfIncompatible = source.replaceIfIncompatible;
    this.indexName = source.indexName;
    this.filePath = source.filePath;
    this.keyTypes = source.keyTypes;
    this.batchSize = source.batchSize;
    this.maxAttempts = source.maxAttempts;
    // The clause as written: shared rather than re-copied, because withUserMetadata() already deep-copied it on the
    // way in and neither builder mutates it afterwards.
    this.userMetadata = source.userMetadata;
  }

  /**
   * Answers whether {@code existing} already provides everything the caller asked for, which is the only thing that
   * makes an {@code IF NOT EXISTS} / {@link #withIgnoreIfExists(boolean)} request a legitimate no-op.
   * <p>
   * The kind of index has to match: a {@code FULL_TEXT} index on a property is not a range index on it, so answering
   * success to a request for the other one leaves the caller with something it cannot use. Uniqueness is directional
   * instead: a {@code UNIQUE} index indexes exactly the keys a {@code NOTUNIQUE} one would, so it covers a
   * {@code NOTUNIQUE} request, while a {@code NOTUNIQUE} index does not carry the constraint a {@code UNIQUE} request
   * is about - and reporting success there is what let a schema migration proceed believing a uniqueness constraint
   * protected its data (issue #5675).
   * <p>
   * The null strategy is deliberately not part of this: unlike the kind and the uniqueness it is not structural - it
   * is settable on an existing index through {@code ALTER} - so a mismatch is not a reason to refuse the statement.
   * Note that a satisfied request stays a plain no-op: the requested strategy is NOT applied to the existing index,
   * which is what {@code IF NOT EXISTS} asks for.
   * <p>
   * This answers the STRUCTURAL half of the question only. The settings an index type keeps of its own - vector
   * {@code dimensions}, full-text analyzers, geospatial precision - are compared separately by
   * {@link IndexMetadata#findUserSettingMismatches}, which needs the {@code METADATA} clause as written and so cannot
   * be folded into this static signature. A caller that guards its request must consult both; {@link TypeIndexBuilder}
   * does, and {@link #conflictWithExistingIndex} puts the two answers into one message.
   */
  public static boolean satisfiesRequest(final Index existing, final Schema.INDEX_TYPE requestedType,
      final boolean requestedUnique) {
    if (existing.getType() != requestedType)
      return false;
    return existing.isUnique() || !requestedUnique;
  }

  /**
   * The other half of {@link #satisfiesRequest}: which of the per-index-type settings {@code requested} NAMES the
   * existing index does not provide. Empty when the request carried no {@code METADATA} clause, which is what keeps a
   * guarded statement without one the plain no-op it has always been.
   * <p>
   * Kept here rather than inlined at the two call sites - {@link TypeIndexBuilder#create()} and the {@code IF NOT
   * EXISTS} shortcut of {@code CreateIndexStatement}, which answers before a builder ever exists - so both ask the
   * same question of the same accessor. {@code getMetadataForNewFile()} is the one to ask: on a wrapper index
   * {@code getMetadata()} answers what the wrapper stores rather than the type-specific configuration (issue #5723).
   *
   * @see IndexMetadata#findUserSettingMismatches
   */
  public static List<String> findUnsatisfiedSettings(final Index existing, final JSONObject requested,
      final Schema.INDEX_TYPE requestedType) {
    if (requested == null || requested.isEmpty() || !(existing instanceof IndexInternal internal))
      return List.of();

    final IndexMetadata existingMetadata = internal.getMetadataForNewFile();
    return existingMetadata == null ? List.of() : existingMetadata.findUserSettingMismatches(requested, requestedType);
  }

  /**
   * Builds the error reported when an index on the same properties exists but {@link #satisfiesRequest} rejected it.
   * <p>
   * Refusing is the answer in every case, including the one the request could technically be granted by rebuilding:
   * dropping the existing index to recreate it with the requested definition is not recoverable if the rebuild then
   * fails on the data already stored - the type is left with no index at all - and on an inherited index it silently
   * takes away the parent type's index (issue #4083). An explicit {@code DROP INDEX} is one statement away and leaves
   * the operator in control of when the index disappears.
   * <p>
   * {@link IllegalArgumentException} on purpose: the HTTP layer maps it to a 400, which is what a schema request the
   * server can read but cannot honour is.
   */
  public static IllegalArgumentException conflictWithExistingIndex(final Index existing,
      final Schema.INDEX_TYPE requestedType, final boolean requestedUnique, final String requestedTypeName,
      final List<String> requestedProperties) {
    return conflictWithExistingIndex(existing, requestedType, requestedUnique, requestedTypeName, requestedProperties,
        List.of());
  }

  /**
   * Same, naming the per-index-type settings the existing index does not provide - the answer of
   * {@link IndexMetadata#findUserSettingMismatches}. Empty when the request was refused on its structural definition
   * alone, which is the only case the overload above covers.
   */
  public static IllegalArgumentException conflictWithExistingIndex(final Index existing,
      final Schema.INDEX_TYPE requestedType, final boolean requestedUnique, final String requestedTypeName,
      final List<String> requestedProperties, final List<String> settingMismatches) {
    final boolean inherited = !existing.getTypeName().equals(requestedTypeName);
    return new IllegalArgumentException(
        "Cannot create the index on type '" + requestedTypeName + "' properties " + requestedProperties + " as "
            + describeDefinition(requestedType, requestedUnique) + " because the index '" + existing.getName()
            + "' already exists on " + (inherited ? "the parent type '" + existing.getTypeName() + "' and " : "")
            + "the same properties as " + describeDefinition(existing.getType(), existing.isUnique())
            + (settingMismatches.isEmpty() ?
            "" :
            " with a different configuration: " + String.join(", ", settingMismatches))
            + (inherited ?
            ". Drop the parent index or align the definition: it is not replaced implicitly because that would take "
                + "the index away from the parent type" :
            ". Drop the existing index first: it is not replaced implicitly because a rebuild that the stored data "
                + "cannot satisfy would leave the type with no index at all"));
  }

  private static String describeDefinition(final Schema.INDEX_TYPE indexType, final boolean unique) {
    return indexType + " (unique=" + unique + ")";
  }

  public abstract T create();

  public IndexBuilder<T> withType(final Schema.INDEX_TYPE indexType) {
    this.indexType = indexType;
    return this;
  }

  public TypeLSMVectorIndexBuilder withLSMVectorType() {
    if (this instanceof TypeLSMVectorIndexBuilder v)
      return v;

    return new TypeLSMVectorIndexBuilder((TypeIndexBuilder) this);
  }

  public TypeLSMSparseVectorIndexBuilder withSparseVectorType() {
    if (this instanceof TypeLSMSparseVectorIndexBuilder v)
      return v;

    return new TypeLSMSparseVectorIndexBuilder((TypeIndexBuilder) this);
  }

  public IndexBuilder<T> withUnique(final boolean unique) {
    this.unique = unique;
    return this;
  }

  public IndexBuilder<T> withIgnoreIfExists(final boolean ignoreIfExists) {
    this.ignoreIfExists = ignoreIfExists;
    return this;
  }

  /**
   * Allows the index already defined on the same properties to be replaced when it does not provide what this builder
   * asks for - see {@link #satisfiesRequest}. Without it such a request is refused, because a rebuild the stored data
   * cannot satisfy would leave the type with no index at all.
   * <p>
   * Only for callers whose statement genuinely means "make this the definition", not merely "create it if missing":
   * the Cypher {@code CREATE CONSTRAINT ... IS UNIQUE} over a property that already carries a plain index is the one
   * in the engine today. Neo4j keeps the range index and the constraint as two separate objects there; ArcadeDB has a
   * single index per property set, and a unique one indexes exactly the same keys, so upgrading it is the equivalent
   * end state.
   * <p>
   * An index the type only INHERITS is never replaced even with this set - taking it away would silently remove the
   * parent type's index (issue #4083). The replacement is also undone if the new index cannot be built, so a failed
   * upgrade leaves the previous definition in place.
   */
  public IndexBuilder<T> withReplaceIfIncompatible(final boolean replaceIfIncompatible) {
    this.replaceIfIncompatible = replaceIfIncompatible;
    return this;
  }

  /**
   * Requests an explicit page size for the index file. Any value below 1 means "unset", leaving the choice to the
   * index implementation - see {@link #getPageSize(int)}, which is the single place that resolves it. Normalising
   * here as well would be redundant, and the builder subclasses that copy the field verbatim
   * ({@code TypeLSMVectorIndexBuilder}, {@code TypeLSMSparseVectorIndexBuilder}) would bypass it anyway.
   */
  public IndexBuilder<T> withPageSize(final int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  /**
   * Requests a null strategy for the index. {@code null} means "the caller did not ask for one" and leaves the
   * default in place, the same convention {@link #withPageSize(int)} follows: the deprecated {@code create*Index}
   * overloads all take a nullable strategy, and passing it straight through used to install a null the index
   * constructor rejects with "Index null strategy is null" (issue #5765).
   */
  public IndexBuilder<T> withNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    if (nullStrategy != null)
      this.nullStrategy = nullStrategy;
    return this;
  }

  public IndexBuilder<T> withCallback(final Index.BuildIndexCallback callback) {
    this.callback = callback;
    return this;
  }

  public DatabaseInternal getDatabase() {
    return database;
  }

  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return nullStrategy;
  }

  /**
   * Returns the requested page size, or the LSM default when none was requested. Kept for the index implementations
   * whose default IS the LSM one; anything with a different default must use {@link #getPageSize(int)} so it can tell
   * "the caller asked for 262144" from "the caller asked for nothing".
   */
  public int getPageSize() {
    return getPageSize(LSMTreeIndexAbstract.DEF_PAGE_SIZE);
  }

  /**
   * Returns the page size the caller explicitly requested, or {@code defaultIfUnset} when none was requested.
   */
  public int getPageSize(final int defaultIfUnset) {
    return pageSize > 0 ? pageSize : defaultIfUnset;
  }

  public Schema.INDEX_TYPE getIndexType() {
    return indexType;
  }

  public Class<? extends Index> getIndexImplementation() {
    return indexImplementation;
  }

  public Index.BuildIndexCallback getCallback() {
    return callback;
  }

  public boolean isUnique() {
    return unique;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getFilePath() {
    return filePath;
  }

  public Type[] getKeyTypes() {
    return keyTypes;
  }

  public IndexMetadata getMetadata() {
    return metadata;
  }

  public IndexBuilder<T> withIndexName(final String indexName) {
    this.indexName = indexName;
    return this;
  }

  public IndexBuilder<T> withFilePath(final String path) {
    this.filePath = path;
    return this;
  }

  public IndexBuilder<T> withKeyTypes(final Type[] keyTypes) {
    this.keyTypes = keyTypes;
    return this;
  }

  public IndexBuilder<T> withBatchSize(final int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public IndexBuilder<T> withMaxAttempts(final int maxAttempts) {
    this.maxAttempts = maxAttempts;
    return this;
  }

  public IndexBuilder<T> withMetadata(final IndexMetadata metadata) {
    this.metadata = metadata;
    return this;
  }

  /**
   * Records the {@code METADATA} clause as written, so a guarded request can be compared against an existing index on
   * the settings it NAMED - see {@link IndexMetadata#findUserSettingMismatches}. Only callers that have a literal user
   * clause set it; a builder configured through the typed {@code with*} setters names nothing, and its request is
   * satisfied by any index of the right kind and uniqueness, exactly as before.
   */
  public IndexBuilder<T> withUserMetadata(final JSONObject userMetadata) {
    // Deep-copied on the way in. The callers hand the SAME instance to withMetadata(JSONObject) and to this setter,
    // and the two are read at different times: withMetadata reads it now, this one is read at create(). No
    // withMetadata overload mutates the clause today, but a copy is a DDL-time cost of nothing and it means a future
    // one could not silently change what a guarded request ends up comparing against.
    this.userMetadata = userMetadata == null ? null : userMetadata.copy();
    return this;
  }

  public JSONObject getUserMetadata() {
    return userMetadata;
  }

  /**
   * Whether the build about to run should share the transaction already open on this thread - the decision issue
   * #6324 item 1 is about, written once here so a third {@link IndexBuilder} subclass cannot get it subtly different.
   * <p>
   * Two conditions, and the second is the one that is easy to get wrong. The index family has to be able to use a
   * shared transaction at all ({@link Schema.INDEX_TYPE#buildCanShareCallerTransaction}); and the open transaction has
   * to actually HOLD uncommitted work, because that is the entire reason for sharing it. An EMPTY transaction has
   * nothing for the scan to see, so joining it buys nothing and costs the build its chunked commit - and empty is
   * exactly what a transaction opened as scaffolding looks like. {@code DatabaseAsyncExecutorImpl.runCommand} opens
   * one around every dispatched command because {@code requiresActiveTx()} defaults to true, so without this test a
   * {@code CREATE INDEX} sent with {@code awaitResponse=false} - the case issue #6303 item 3 and #6324 item 5 exist
   * to make work - would build the whole index in one uncommitted transaction, whatever batch size it was given.
   * <p>
   * Asked BEFORE the build opens anything, because afterwards there is always a transaction and the answer is always
   * yes.
   */
  protected boolean buildSharesCallerTransaction() {
    return indexType != null && indexType.buildCanShareCallerTransaction() && callerTransactionHasChanges(database);
  }

  /**
   * The half of the decision that is about the TRANSACTION rather than about the index family, exposed on its own for
   * the call site that does not go through a builder at all: {@link LocalDocumentType#addSuperType} propagates a whole
   * set of a super type's indexes by calling {@link LocalSchema#createBucketIndex} directly, so it asks this once,
   * before it opens anything, and pairs it with {@code buildCanShareCallerTransaction()} per index. Issue #6359 item 1
   * is that call site arriving back at the defect #6324 item 1 fixed, and the predicate lives here so a third spelling
   * of it cannot drift from the other two.
   * <p>
   * Timing is the whole trap: inside the component-creation transaction there is ALWAYS one open, it is EMPTY, and the
   * answer would always be no.
   */
  static boolean callerTransactionHasChanges(final DatabaseInternal database) {
    if (!database.isTransactionActive())
      return false;
    final TransactionContext tx = database.getTransactionIfExists();
    return tx != null && tx.hasChanges();
  }

  /**
   * Populates an index that was created EMPTY - the second half of the two-transaction split of issue #6324, item 1.
   * The comment at the build loop in {@link TypeIndexBuilder#create()} says why the two halves cannot share one
   * transaction.
   * <p>
   * The status step is what the split costs. Creating an index without building it parks it at {@code UNAVAILABLE},
   * so nothing can read a half-populated index, while {@code build()} insists on starting from {@code AVAILABLE} and
   * puts it back there itself. The window in between is invisible: both halves run under the schema's write lock,
   * inside {@code recordFileChanges}.
   */
  protected void buildCreatedIndex(final Index index, final boolean sharesCallerTransaction) {
    buildCreatedIndex(index, batchSize, sharesCallerTransaction, callback);
  }

  /** @see #buildCreatedIndex(Index, boolean) - the builder-less form, for the same reason as the predicate above. */
  static void buildCreatedIndex(final Index index, final int batchSize, final boolean sharesCallerTransaction,
      final Index.BuildIndexCallback callback) {
    final IndexInternal internal = (IndexInternal) index;
    if (!internal.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.UNAVAILABLE },
        IndexInternal.INDEX_STATUS.AVAILABLE))
      throw new IndexException("Cannot build the index '" + index.getName() + "' because it is not available");
    internal.build(batchSize, sharesCallerTransaction, callback);
  }

  /**
   * Takes away an index that could not be built. The FULL removal, not just the component's own {@code drop()}: on the
   * two-transaction path the component is already committed and attached to its type, so leaving it behind would
   * answer lookups with an empty index. This is the cleanup {@code LocalSchema.createBucketIndex} did while the build
   * still ran inside it (issue #6324, item 1) - an index that could not be built must be GONE, and the caller told so.
   */
  static void dropPartiallyBuiltIndex(final LocalSchema schema, final Index index) {
    if (index != null && schema.existsIndex(index.getName()))
      schema.dropIndex(index.getName());
  }
}
