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
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexFactoryHandler;
import com.arcadedb.index.IndexInternal;
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
import org.apache.lucene.spatial.prefix.tree.CellCanPrune;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.util.BytesRef;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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
 *       store, so "every cell below C" is literally the key range {@code [C, C+\uFFFF]}. On an area shape a complete
 *       set of sibling cells additionally collapses into its parent, see {@link #forEachPrunedFrontierCell}.</li>
 *   <li>{@link GeoIndexMetadata.TOKENIZATION#FULL} is the original layout: the whole ancestor chain of every covering
 *       cell, resolved with one exact lookup per covering cell of the search shape. Kept for indexes created before
 *       26.8.1, whose entries are already written that way.</li>
 * </ul>
 * Both layouts return a SUPERSET of the matching records - the grid is an approximation of the shape - which the SQL
 * geo.* predicates post-filter ({@code shouldExecuteAfterSearch}).
 */
public class LSMTreeGeoIndex implements Index, IndexInternal {

  /**
   * Upper bound of a GeoHash prefix range scan. The scan only ever runs over FRONTIER tokens, which come from
   * {@code getTokenBytesNoLeaf} and are therefore the plain base-32 GeoHash alphabet - the {@code '+'} leaf marker
   * belongs to the FULL layout, whose tokens are only ever read by exact lookup. Since the LSM-Tree compares STRING
   * keys as UNSIGNED UTF-8 bytes, a character encoding to 0xEF 0xBF 0xBF sorts after every possible descendant of a
   * cell and before its next sibling.
   */
  private static final String PREFIX_SCAN_UPPER_BOUND = "\uFFFF";

  private final LSMTreeIndex                     underlyingIndex;
  private final int                              precision;
  private final GeoIndexMetadata.TOKENIZATION    tokenization;
  /** Built once at construction, see {@link #getUpgradeWarning()}. Null on an index already in the current layout. */
  private final String                           upgradeWarning;
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
    this.upgradeWarning = buildUpgradeWarning(tokenization, precision);
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
    this.upgradeWarning = buildUpgradeWarning(tokenization, precision);
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
    this.upgradeWarning = buildUpgradeWarning(tokenization, precision);
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

    // #5601: the covering cells are walked LAZILY, one underlying scan open at a time, instead of draining every cell
    // into a candidate set before the caller sees the first row. A wide-area query resolves into thousands of cells,
    // and a consumer that stops early no longer pays for the ones it never reached.
    // `limit` is DELIBERATELY IGNORED, see isResultApproximate(): what this index returns is a superset of the match
    // that the SQL geo.* predicate re-checks, so truncating it here would drop rows that would have survived the
    // filter - silently, and only on some queries. The caller applies the limit to the FILTERED rows instead, which
    // now stops this cursor at the right place rather than after the fact.
    return new GeoIndexCursor(keys, newCoveringCellWalk(searchShape, detailLevel), this::openCellCursor);
  }

  /**
   * Opens the underlying scan answering one covering cell. Package-private: the only caller is {@link GeoIndexCursor},
   * which owns the cursor and closes it as soon as it is drained.
   */
  IndexCursor openCellCursor(final String token, final boolean frontier) {
    if (tokenization == GeoIndexMetadata.TOKENIZATION.FULL || !frontier)
      // A cell that still has children can only match a shape whose OWN decomposition stopped there, so an exact
      // lookup is enough - and it is the only thing the legacy layout can do, having stored every ancestor.
      return underlyingIndex.get(new Object[] { token });

    // Frontier cell: every indexed cell at or below it starts with its token, which on a sorted store is one
    // range scan instead of one lookup per descendant level. This is the site the ASCII invariant actually
    // protects - the bound is appended to THIS token - so assert it here as well as where tokens are written.
    assert isAsciiToken(token) : "a FRONTIER token must be ASCII for the prefix range scan bound to hold: " + token;
    return underlyingIndex.range(true, new Object[] { token }, true,
        new Object[] { token + PREFIX_SCAN_UPPER_BOUND }, true);
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

  /**
   * The geohash resolution and the storage layout are held here as plain fields, not on the underlying LSM-Tree, so a
   * site carrying this definition into a new index file has to read them from this instance: through
   * {@link #getMetadata()} a copy would silently drop to the default precision (issue #5723).
   */
  @Override
  public IndexMetadata getMetadataForNewFile() {
    // Unlike the full-text and sparse-vector wrappers, there is no stored GeoIndexMetadata to hand back: this class
    // holds the two settings as plain fields. GeoIndexMetadata.from() reassembles the definition, keeping the
    // base-field copy inside the metadata hierarchy where copyCommonTo() lives.
    return GeoIndexMetadata.from(underlyingIndex.getMetadata(), precision, tokenization);
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
  public boolean isResultApproximate() {
    // The GeoHash grid approximates a shape with cells, so a cell hit is a candidate the geo.* predicate re-checks.
    return true;
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

  @Override
  public String getUpgradeWarning() {
    // Built once in the constructor: this is called per index on every schema:indexes / schema:types listing, and the
    // contract on IndexInternal#getUpgradeWarning is that it stays cheap.
    // The LSM index the cells are stored in is asked FIRST, and deliberately so. A geohash cell is ASCII, so the
    // key-order mismatch of #5802 should never arise here - but "should never" is the invariant, not the mechanism.
    // If it ever did arise it would be a correctness fault (lookups under-returning), while the layout advisory
    // below is about cost, so returning the layout one first would mask the worse of the two - the very hiding this
    // delegation exists to prevent.
    final String underlyingWarning = underlyingIndex.getUpgradeWarning();
    if (underlyingWarning != null)
      return underlyingWarning;

    return upgradeWarning;
  }

  // ---- Private helpers ----

  private static String buildUpgradeWarning(final GeoIndexMetadata.TOKENIZATION tokenization, final int precision) {
    if (tokenization != GeoIndexMetadata.TOKENIZATION.FULL)
      return null;

    return ("This geospatial index uses the legacy %s cell layout: it stores one entry per GeoHash level, so every "
        + "indexed point costs %d entries instead of 1, and a query has to read the coarse cells shared by the whole "
        + "dataset. It keeps working as it always did, and rebuilding it switches it to the compact %s layout "
        + "(issue #5478)").formatted(GeoIndexMetadata.TOKENIZATION.FULL, precision,
        GeoIndexMetadata.TOKENIZATION.FRONTIER);
  }

  /**
   * Holds the invariant {@link #PREFIX_SCAN_UPPER_BOUND} depends on: a FRONTIER token must be pure ASCII, so that
   * appending U+FFFF (0xEF 0xBF 0xBF in UTF-8) produces a key sorting after every descendant of the cell and before
   * its next sibling. GeoHash cell tokens are base-32, so this holds for every grid we use - but a tokenizer change
   * that broke it would silently truncate range scans instead of failing, which is why it is asserted rather than
   * left to the comment. Compiled out unless assertions are enabled (tests do).
   * <p>
   * Asserted on BOTH sides: where tokens are written, and where a query token has the bound appended to it. The two
   * come from the same grid today, so one implies the other - but the read site is where the truncation would
   * actually happen, and a future divergence would otherwise be caught only on the side that cannot fail.
   */
  private static boolean isAsciiToken(final String token) {
    for (int i = 0; i < token.length(); i++)
      if (token.charAt(i) > 0x7F)
        return false;
    return true;
  }

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
    forEachPrunedFrontierCell(grid, shape, detailLevel, token -> {
      assert isAsciiToken(token) : "a FRONTIER token must be ASCII for the prefix range scan bound to hold: " + token;
      tokens.add(token);
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
   * Pull-style walk of the GeoHash cells covering {@code shape} down to {@code detailLevel}, driven by
   * {@link GeoIndexCursor} one cell at a time as its consumer asks for rows.
   */
  private GeoCoveringCellWalk newCoveringCellWalk(final Shape shape, final int detailLevel) {
    return new GeoCoveringCellWalk(grid.getTreeCellIterator(shape, detailLevel));
  }

  /**
   * Walks the same cells as {@link #newCoveringCellWalk} but emits only the FRONTIER ones, collapsing a COMPLETE set of
   * sibling frontier cells into their parent, recursively. This is what Lucene calls {@code pruneLeafyBranches} and
   * enables by default on the indexing path: the parent rectangle is the union of its children, so the cover can only
   * grow and a match can never be lost, while an area shape costs measurably fewer index entries - measured at 57% and
   * 74% fewer on a small square and on a jagged polygon respectively (issue #5600).
   * <p>
   * Deliberately NOT shared with the query path: {@link #get} needs every ancestor cell of the search shape so it can
   * look up the shallower cells an indexed shape may have stopped at.
   * <p>
   * Unlike {@link org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy#createCellIteratorToIndex}, which
   * materialises every cell of the decomposition in an {@code ArrayList} - the reason its javadoc warns against the
   * option for high precision shapes - this streams: only the frontier tokens on the current root-to-leaf path can
   * still be revoked by an ancestor pruning, which bounds what is held to {@code subCellsSize * detailLevel} tokens.
   *
   * @param emit receives each token to index (order is unspecified: a pruned parent is decided only once all of
   *             its children have been visited)
   */
  static void forEachPrunedFrontierCell(final SpatialPrefixTree grid, final Shape shape, final int detailLevel,
      final Consumer<String> emit) {
    if (shape instanceof Point) {
      // The hot ingest path of #5478. A point decomposes into a chain of SINGLE-child cells, so no cell can ever hold a
      // complete set of siblings and the frontier is simply the deepest cell: skip the bookkeeping and its allocations.
      // Lucene takes the same shortcut, as isGridAlignedShape().
      final CellIterator pointIter = grid.getTreeCellIterator(shape, detailLevel);
      final BytesRef pointScratch = new BytesRef();
      String deepest = null;
      while (pointIter.hasNext()) {
        final String token = pointIter.next().getTokenBytesNoLeaf(pointScratch).utf8ToString();
        if (!token.isEmpty())
          deepest = token;
      }
      if (deepest != null)
        emit.accept(deepest);
      return;
    }

    final CellIterator cellIter = grid.getTreeCellIterator(shape, detailLevel);

    // One frame per open cell of the current path. The loop below pops every frame whose level is >= the incoming
    // one, so the levels on the stack are STRICTLY INCREASING and the depth can never exceed the number of distinct
    // levels, detailLevel - which is what makes this bound safe no matter how the traversal descends.
    final PruneFrame[] path = new PruneFrame[detailLevel + 1];
    int depth = 0;

    // One BytesRef retargeted per cell instead of one allocated per cell: this is the ingest path.
    final BytesRef scratch = new BytesRef();

    while (cellIter.hasNext()) {
      final Cell cell = cellIter.next();
      final String token = cell.getTokenBytesNoLeaf(scratch).utf8ToString();
      if (token.isEmpty())
        // The world cell: it is not a storable token and has no parent to account it to.
        continue;

      final int level = cell.getLevel();

      // A cell at this level closes every still-open cell at the same level or deeper: the walk is pre-order.
      while (depth > 0 && path[depth].level >= level)
        closeFrame(path, depth--, emit);

      ++depth;
      // A GeoHash tree adds one base-32 character per level, so the walk descends exactly one level at a time and the
      // stack depth equals the cell level. Nothing here NEEDS that - a frame's parent is the frame below it, found by
      // pop order rather than by level arithmetic, and the array bound holds on strictly increasing levels alone - so
      // this documents the grid's contract rather than guarding the algorithm.
      assert depth == level : "unexpected GeoHash traversal: level " + level + " reached at depth " + depth;

      final PruneFrame frame = path[depth] != null ? path[depth] : (path[depth] = new PruneFrame());
      frame.reset(token, level, cell instanceof CellCanPrune p ? p.getSubCellsSize() : -1);
    }

    while (depth > 0)
      closeFrame(path, depth--, emit);
  }

  /**
   * Resolves the cell held in {@code path[depth]} into either a frontier token accounted to its parent, or a flush of
   * the frontier tokens its subtree accumulated.
   */
  private static void closeFrame(final PruneFrame[] path, final int depth, final Consumer<String> emit) {
    final PruneFrame frame = path[depth];
    final PruneFrame parent = depth > 1 ? path[depth - 1] : null;

    // A cell with no visited child is where the decomposition stopped; one whose children ALL turned out to be
    // frontier cells covers exactly what they cover, so it replaces them.
    final boolean frontier = frame.childCount == 0
        || (frame.subCellsSize > 0 && frame.frontierChildren == frame.subCellsSize);

    if (frontier) {
      // Whatever the children contributed is subsumed by this cell's own token.
      frame.pendingCount = 0;
      if (parent == null)
        emit.accept(frame.token);
      else
        parent.addFrontierChild(frame.token);
    } else {
      for (int i = 0; i < frame.pendingCount; i++)
        emit.accept(frame.pending[i]);
      frame.pendingCount = 0;
      if (parent != null)
        // A non-frontier child means the parent can no longer be a complete set of leaves.
        parent.childCount++;
    }
  }

  /**
   * Per-level bookkeeping of {@link #forEachPrunedFrontierCell}. Reused across cells of the same level so a walk
   * allocates at most one frame per level.
   */
  private static final class PruneFrame {
    private String   token;
    private int      level;
    /** Number of sub-cells the grid gives this cell, or -1 when the cell cannot be pruned into. */
    private int      subCellsSize;
    private int      childCount;
    private int      frontierChildren;
    private String[] pending = new String[0];
    private int      pendingCount;

    private void reset(final String token, final int level, final int subCellsSize) {
      this.token = token;
      this.level = level;
      this.subCellsSize = subCellsSize;
      this.childCount = 0;
      this.frontierChildren = 0;
      this.pendingCount = 0;
    }

    private void addFrontierChild(final String childToken) {
      childCount++;
      frontierChildren++;
      if (pendingCount == pending.length)
        pending = Arrays.copyOf(pending, Math.max(8, pending.length * 2));
      pending[pendingCount++] = childToken;
    }
  }
}
