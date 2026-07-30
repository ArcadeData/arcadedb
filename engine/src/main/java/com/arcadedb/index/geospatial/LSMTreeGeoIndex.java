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
package com.arcadedb.index.geospatial;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.function.sql.geo.GeoUtils;
import com.arcadedb.index.EmptyIndexCursor;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexFactoryHandler;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.GeoIndexMetadata;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Geospatial index implementation based on LSM-Tree index.
 * <p>
 * Uses Lucene's {@link GeohashPrefixTree} and {@link RecursivePrefixTreeStrategy} to decompose
 * WKT geometry strings into GeoHash cell tokens. Each token is stored as a key in the underlying
 * {@link LSMTreeIndex} with the document RID as value.
 * <p>
 * Two storage layouts exist, see {@link GeoIndexMetadata.TOKENIZATION}:
 * <ul>
 *   <li>{@link GeoIndexMetadata.TOKENIZATION#FRONTIER} (default since 26.8.1, issue #5478) stores only the deepest
 *       cells of the decomposition - one single token for a point - and answers a query with a GeoHash prefix RANGE
 *       SCAN over each covering cell of the search shape, plus an exact lookup on the covering cells that still have
 *       children (they can only match a shape indexed at a coarser resolution). The underlying LSM-Tree is a sorted
 *       store, so "every cell below C" is literally the key range {@code [C, C+\uFFFF]}.</li>
 *   <li>{@link GeoIndexMetadata.TOKENIZATION#FULL} is the original layout: the whole ancestor chain of every covering
 *       cell, resolved with one exact lookup per covering cell of the search shape. Kept for indexes created before
 *       26.8.1, whose entries are already written that way.</li>
 * </ul>
 * Both layouts return a SUPERSET of the matching records - the grid is an approximation of the shape - which the SQL
 * geo.* predicates post-filter ({@code shouldExecuteAfterSearch}).
 */
public class LSMTreeGeoIndex implements Index, IndexInternal {

  /**
   * Upper bound of a GeoHash prefix range scan. GeoHash tokens are ASCII (the base-32 alphabet plus Lucene's {@code '+'}
   * leaf marker), and the LSM-Tree compares STRING keys as UNSIGNED UTF-8 bytes, so a character encoding to 0xEF 0xBF
   * 0xBF sorts after every possible descendant of a cell and before its next sibling.
   */
  private static final String PREFIX_SCAN_UPPER_BOUND = "\uFFFF";

  private final LSMTreeIndex                     underlyingIndex;
  private final int                              precision;
  private final GeoIndexMetadata.TOKENIZATION    tokenization;
  private final GeohashPrefixTree                grid;
  private final RecursivePrefixTreeStrategy      strategy;
  private       TypeIndex                        typeIndex;

  /**
   * Factory handler for creating LSMTreeGeoIndex instances.
   */
  public static class GeoIndexFactoryHandler implements IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder builder) {
      if (builder.isUnique())
        throw new IllegalArgumentException("Geospatial index cannot be unique");
      for (final Type keyType : builder.getKeyTypes())
        if (keyType != Type.STRING)
          throw new IllegalArgumentException(
              "Geospatial index can only be defined on STRING properties, found: " + keyType);

      int precision = GeoIndexMetadata.DEFAULT_PRECISION;
      GeoIndexMetadata.TOKENIZATION tokenization = GeoIndexMetadata.DEFAULT_TOKENIZATION;
      if (builder.getMetadata() instanceof GeoIndexMetadata geoMeta) {
        precision = geoMeta.getPrecision();
        tokenization = geoMeta.getTokenization();
      }

      return new LSMTreeGeoIndex(builder.getDatabase(), builder.getIndexName(),
          builder.getFilePath(), ComponentFile.MODE.READ_WRITE,
          builder.getPageSize(), builder.getNullStrategy(), precision, tokenization);
    }
  }

  /**
   * Called at load time. Uses the default precision and the LEGACY layout, because a caller with no persisted
   * definition to read from can only be looking at a file written before the layout existed.
   */
  public LSMTreeGeoIndex(final LSMTreeIndex index) {
    this(index, GeoIndexMetadata.DEFAULT_PRECISION, GeoIndexMetadata.LEGACY_TOKENIZATION);
  }

  /**
   * Called at load time with explicit precision and tokenization, both read back from the persisted index definition.
   */
  public LSMTreeGeoIndex(final LSMTreeIndex index, final int precision, final GeoIndexMetadata.TOKENIZATION tokenization) {
    this.underlyingIndex = index;
    this.precision = precision;
    this.tokenization = tokenization;
    this.grid = new GeohashPrefixTree(GeoUtils.getSpatialContext(), precision);
    this.strategy = new RecursivePrefixTreeStrategy(grid, "geo");
  }

  /**
   * Creation time constructor (used by factory handler and tests).
   */
  public LSMTreeGeoIndex(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final int precision) {
    this(database, name, filePath, mode, pageSize, nullStrategy, precision, GeoIndexMetadata.DEFAULT_TOKENIZATION);
  }

  /**
   * Creation time constructor (used by factory handler and tests).
   */
  public LSMTreeGeoIndex(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final int precision, final GeoIndexMetadata.TOKENIZATION tokenization) {
    this.precision = precision;
    this.tokenization = tokenization;
    this.grid = new GeohashPrefixTree(GeoUtils.getSpatialContext(), precision);
    this.strategy = new RecursivePrefixTreeStrategy(grid, "geo");
    this.underlyingIndex = new LSMTreeIndex(database, name, false, filePath, mode, new Type[]{Type.STRING}, pageSize, nullStrategy);
  }

  /**
   * Loading time constructor from an existing file.
   */
  public LSMTreeGeoIndex(final DatabaseInternal database, final String name, final String filePath, final int fileId,
      final ComponentFile.MODE mode, final int pageSize, final int version) {
    this.precision = GeoIndexMetadata.DEFAULT_PRECISION;
    this.tokenization = GeoIndexMetadata.LEGACY_TOKENIZATION;
    this.grid = new GeohashPrefixTree(GeoUtils.getSpatialContext(), precision);
    this.strategy = new RecursivePrefixTreeStrategy(grid, "geo");
    try {
      this.underlyingIndex = new LSMTreeIndex(database, name, false, filePath, fileId, mode, pageSize, version);
    } catch (final IOException e) {
      throw new IndexException("Cannot create geospatial index (error=" + e + ")", e);
    }
  }

  @Override
  public void put(final Object[] keys, final RID[] rids) {
    if (keys == null || keys.length == 0 || keys[0] == null)
      return;

    // Always treats keys[0] as a Shape or a WKT String. The transaction commit replay uses
    // {@link #putReplay}, which forwards already-tokenized GeoHash cells to the underlying
    // LSM-Tree unchanged (issue #4073, replaces the prior looksLikeGeoHashToken heuristic).
    final Object key0 = keys[0];

    if (key0 instanceof Shape shape) {
      indexShape(shape, rids);
      return;
    }

    final String wkt = key0.toString();
    final Shape shape;
    try {
      shape = GeoUtils.getSpatialContext().getFormats().getWktReader().read(wkt);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Geospatial index: skipping invalid WKT '%s': %s", wkt, e.getMessage());
      return;
    }

    indexShape(shape, rids);
  }

  /**
   * Tokenizes a shape using the geohash prefix tree strategy and stores each token in the
   * underlying LSM index. Inside a transaction the underlying LSM-Tree queues the per-token
   * write onto {@code TransactionIndexContext}; commit replay then re-enters via
   * {@link #putReplay}, which skips re-tokenization.
   */
  private void indexShape(final Shape shape, final RID[] rids) {
    for (final String token : extractTokens(shape))
      underlyingIndex.put(new Object[] { token }, rids);
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    if (keys == null || keys.length == 0 || keys[0] == null)
      return new EmptyIndexCursor();

    final Shape searchShape = toShape(keys[0]);
    if (searchShape == null)
      return new EmptyIndexCursor();

    // Determine the detail level for the query (same heuristic as RecursivePrefixTreeStrategy)
    final SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, searchShape);
    final double distErr = args.resolveDistErr(GeoUtils.getSpatialContext(), strategy.getDistErrPct());
    final int detailLevel = grid.getLevelForDistance(distErr);

    // TODO: For large regions at high precision (up to 12 levels) this materialises the candidate RID set in memory.
    //       A streaming/lazy cursor chaining cells on demand (similar to LSMTreeFullTextIndex) would reduce GC
    //       pressure on production datasets with dense or wide-area queries.
    final LinkedHashSet<RID> seen = new LinkedHashSet<>();
    final int maxElements = limit > -1 ? limit : Integer.MAX_VALUE;

    forEachCoveringCell(searchShape, detailLevel, (token, frontier) -> {
      if (seen.size() >= maxElements)
        return false;

      final IndexCursor cursor;
      if (tokenization == GeoIndexMetadata.TOKENIZATION.FULL || !frontier)
        // A cell that still has children can only match a shape whose OWN decomposition stopped there, so an exact
        // lookup is enough - and it is the only thing the legacy layout can do, having stored every ancestor.
        cursor = underlyingIndex.get(new Object[] { token });
      else
        // Frontier cell: every indexed cell at or below it starts with its token, which on a sorted store is one
        // range scan instead of one lookup per descendant level.
        cursor = underlyingIndex.range(true, new Object[] { token }, true,
            new Object[] { token + PREFIX_SCAN_UPPER_BOUND }, true);

      while (cursor.hasNext() && seen.size() < maxElements) {
        // A range cursor answers hasNext() optimistically and returns null once a run of tombstones leaves nothing
        // to emit, so the result must be checked rather than dereferenced.
        final Identifiable next = cursor.next();
        if (next != null)
          seen.add(next.getIdentity());
      }
      return true;
    });

    final List<IndexCursorEntry> entries = new ArrayList<>(seen.size());
    for (final RID rid : seen)
      entries.add(new IndexCursorEntry(keys, rid, 1));
    return new TempIndexCursor(entries);
  }

  @Override
  public void remove(final Object[] keys) {
    if (keys == null || keys.length == 0 || keys[0] == null)
      return;
    for (final String token : extractTokens(keys[0]))
      underlyingIndex.remove(new Object[] { token });
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    if (keys == null || keys.length == 0 || keys[0] == null)
      return;
    for (final String token : extractTokens(keys[0]))
      underlyingIndex.remove(new Object[] { token }, rid);
  }

  /**
   * Replay entry point invoked by {@code TransactionIndexContext.applyChanges} at commit time
   * (issue #4073). The {@code keys} are already tokenized GeoHash cells (queued by the
   * underlying LSM-Tree at original-call time), so the wrapper must NOT attempt to re-parse
   * them as WKT. Forwards directly to the underlying index, removing the prior
   * {@code looksLikeGeoHashToken} character-class heuristic that would otherwise misclassify
   * any short alphanumeric input.
   */
  @Override
  public void putReplay(final Object[] keys, final RID[] rids) {
    underlyingIndex.put(keys, rids);
  }

  @Override
  public void removeReplay(final Object[] keys, final Identifiable rid) {
    underlyingIndex.remove(keys, rid);
  }

  @Override
  public void updateTypeName(final String newTypeName) {
    underlyingIndex.updateTypeName(newTypeName);
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    return null;
  }

  @Override
  public long countEntries() {
    return underlyingIndex.countEntries();
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    return underlyingIndex.compact();
  }

  @Override
  public IndexMetadata getMetadata() {
    return underlyingIndex.getMetadata();
  }

  @Override
  public boolean isCompacting() {
    return underlyingIndex.isCompacting();
  }

  @Override
  public boolean scheduleCompaction() {
    return underlyingIndex.scheduleCompaction();
  }

  @Override
  public String getMostRecentFileName() {
    return underlyingIndex.getMostRecentFileName();
  }

  @Override
  public void setMetadata(final IndexMetadata metadata) {
    underlyingIndex.setMetadata(metadata);
  }

  @Override
  public boolean setStatus(final INDEX_STATUS[] expectedStatuses, final INDEX_STATUS newStatus) {
    return underlyingIndex.setStatus(expectedStatuses, newStatus);
  }

  @Override
  public void setMetadata(final JSONObject indexJSON) {
    underlyingIndex.setMetadata(indexJSON);
  }

  @Override
  public String getTypeName() {
    return underlyingIndex.getTypeName();
  }

  @Override
  public List<String> getPropertyNames() {
    return underlyingIndex.getPropertyNames();
  }

  @Override
  public void close() {
    underlyingIndex.close();
  }

  @Override
  public void drop() {
    underlyingIndex.drop();
  }

  @Override
  public String getName() {
    return underlyingIndex.getName();
  }

  @Override
  public Map<String, Long> getStats() {
    return underlyingIndex.getStats();
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return underlyingIndex.getNullStrategy();
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    underlyingIndex.setNullStrategy(nullStrategy);
  }

  @Override
  public int getFileId() {
    return underlyingIndex.getFileId();
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  public PaginatedComponent getComponent() {
    return underlyingIndex.getComponent();
  }

  @Override
  public Type[] getKeyTypes() {
    return underlyingIndex.getKeyTypes();
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return underlyingIndex.getBinaryKeyTypes();
  }

  @Override
  public int getAssociatedBucketId() {
    final int bucketId = underlyingIndex.getAssociatedBucketId();
    // When no bucket is associated (bucketId == -1), return the index's own file ID so that
    // the transaction locking machinery does not attempt to lock a non-existent file (-1).
    return bucketId >= 0 ? bucketId : underlyingIndex.getFileId();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean isAutomatic() {
    return underlyingIndex.getPropertyNames() != null;
  }

  @Override
  public int getPageSize() {
    return underlyingIndex.getPageSize();
  }

  @Override
  public List<Integer> getFileIds() {
    return underlyingIndex.getFileIds();
  }

  @Override
  public void setTypeIndex(final TypeIndex typeIndex) {
    this.typeIndex = typeIndex;
  }

  @Override
  public TypeIndex getTypeIndex() {
    return typeIndex;
  }

  @Override
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    // Must NOT delegate to underlyingIndex.build(), because that would pass the raw LSMTreeIndex
    // to DocumentIndexer.addToIndex(), bypassing GeoHash tokenization and storing raw WKT keys.
    // Instead, scan the bucket and call this.put() through the indexer so tokenization runs.
    final DatabaseInternal db = underlyingIndex.getComponent().getDatabase();
    final int bucketId = underlyingIndex.getAssociatedBucketId();
    if (bucketId < 0)
      return 0;

    final String bucketName = db.getSchema().getBucketById(bucketId).getName();
    final AtomicLong total = new AtomicLong();
    final long startTime = System.currentTimeMillis();

    LogManager.instance().log(this, Level.INFO, "Building geospatial index '%s'...", getName());

    db.scanBucket(bucketName, record -> {
      db.getIndexer().addToIndex(LSMTreeGeoIndex.this, record.getIdentity(), (Document) record);
      total.incrementAndGet();

      if (total.get() % buildIndexBatchSize == 0) {
        db.getWrappedDatabaseInstance().commit();
        db.getWrappedDatabaseInstance().begin();
      }

      if (callback != null)
        callback.onDocumentIndexed((Document) record, total.get());

      return true;
    });

    LogManager.instance().log(this, Level.INFO, "Completed building geospatial index '%s': processed %d records in %dms",
        getName(), total.get(), System.currentTimeMillis() - startTime);

    return total.get();
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.GEOSPATIAL;
  }

  @Override
  public boolean isValid() {
    return underlyingIndex.isValid();
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("type", getType());
    final int bucketId = underlyingIndex.getAssociatedBucketId();
    json.put("bucket", underlyingIndex.getComponent().getDatabase().getSchema().getBucketById(bucketId).getName());
    json.put("properties", getPropertyNames());
    json.put("precision", precision);
    json.put("tokenization", tokenization.name());
    json.put("nullStrategy", getNullStrategy());
    json.put("unique", isUnique());
    return json;
  }

  /**
   * Returns the precision level used for the GeohashPrefixTree.
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Returns the cell tokenization layout the index entries are stored in.
   */
  public GeoIndexMetadata.TOKENIZATION getTokenization() {
    return tokenization;
  }

  // ---- Private helpers ----

  private Shape toShape(final Object obj) {
    if (obj instanceof Shape s)
      return s;
    try {
      return GeoUtils.getSpatialContext().getFormats().getWktReader().read(obj.toString());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Geospatial index: cannot parse shape '%s'", obj);
      return null;
    }
  }

  private List<String> extractTokens(final Object wktOrShape) {
    final Shape shape = toShape(wktOrShape);
    if (shape == null)
      return List.of();

    if (tokenization == GeoIndexMetadata.TOKENIZATION.FULL)
      return extractFullChainTokens(shape);

    // FRONTIER: only the cells the decomposition stops at. A point yields exactly one token instead of `precision` of
    // them (issue #5478), and no continent-sized cell collects a posting per indexed record.
    final int detailLevel = grid.getLevelForDistance(
        SpatialArgs.calcDistanceFromErrPct(shape, strategy.getDistErrPct(), GeoUtils.getSpatialContext()));

    final List<String> tokens = new ArrayList<>();
    forEachCoveringCell(shape, detailLevel, (token, frontier) -> {
      if (frontier)
        tokens.add(token);
      return true;
    });
    return tokens;
  }

  /**
   * Legacy tokenization: the whole ancestor chain, exactly as Lucene writes it into an inverted index (leaf cells carry
   * the {@code '+'} leaf marker). Only used by indexes created before 26.8.1.
   */
  private List<String> extractFullChainTokens(final Shape shape) {
    final List<String> tokens = new ArrayList<>();
    final Field[] fields = strategy.createIndexableFields(shape);
    for (final Field field : fields) {
      try {
        final TokenStream ts = field.tokenStream(null, null);
        if (ts == null)
          continue;
        final TermToBytesRefAttribute bytesAttr = ts.addAttribute(TermToBytesRefAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
          final String token = bytesAttr.getBytesRef().utf8ToString();
          if (!token.isEmpty())
            tokens.add(token);
        }
        ts.end();
        ts.close();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING,
            "Geospatial index: token error for shape '%s': %s", shape, e.getMessage());
      }
    }
    return tokens;
  }

  /**
   * Walks the GeoHash cells covering {@code shape} down to {@code detailLevel}, telling the visitor whether each one is
   * a FRONTIER cell - one the decomposition stops at, with no deeper cell of its own below it.
   * <p>
   * {@link org.apache.lucene.spatial.prefix.tree.CellIterator} is a depth-first pre-order walk, so a cell is a frontier
   * exactly when the cell that follows it is not deeper; the last cell always is. This is derived from the traversal
   * rather than from {@link Cell#isLeaf()} so that it holds for every shape and grid, including the boundary cells at
   * {@code detailLevel} that are emitted without the leaf flag.
   *
   * @param visitor returns false to stop the walk
   */
  private void forEachCoveringCell(final Shape shape, final int detailLevel, final CellVisitor visitor) {
    final CellIterator cellIter = grid.getTreeCellIterator(shape, detailLevel);

    String pendingToken = null;
    int pendingLevel = -1;

    while (cellIter.hasNext()) {
      final Cell cell = cellIter.next();
      final int level = cell.getLevel();
      final String token = cell.getTokenBytesNoLeaf(null).utf8ToString();

      if (pendingToken != null && !visitor.visit(pendingToken, level <= pendingLevel))
        return;

      pendingToken = token.isEmpty() ? null : token;
      pendingLevel = level;
    }

    if (pendingToken != null)
      visitor.visit(pendingToken, true);
  }

  @FunctionalInterface
  private interface CellVisitor {
    /**
     * @return false to stop the walk
     */
    boolean visit(String token, boolean frontier);
  }
}
