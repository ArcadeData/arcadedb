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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * Geospatial index implementation based on LSM-Tree index.
 * <p>
 * Uses Lucene's {@link GeohashPrefixTree} and {@link RecursivePrefixTreeStrategy} to decompose
 * WKT geometry strings into GeoHash cell tokens. Each token is stored as a key in the underlying
 * {@link LSMTreeIndex} with the document RID as value.
 * <p>
 * Querying generates covering GeoHash tokens for the search shape and performs set-union
 * lookups in the LSM index, returning deduplicated RIDs.
 */
public class LSMTreeGeoIndex implements Index, IndexInternal {

  /** Default geohash precision level (same as GeoIndexMetadata default, ~2.4 m cell resolution). */
  public static final int DEFAULT_PRECISION = GeoIndexMetadata.DEFAULT_PRECISION;

  private final LSMTreeIndex               underlyingIndex;
  private final int                        precision;
  private final GeohashPrefixTree          grid;
  private final RecursivePrefixTreeStrategy strategy;
  private       TypeIndex                  typeIndex;

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
      if (builder.getMetadata() instanceof GeoIndexMetadata geoMeta)
        precision = geoMeta.getPrecision();

      return new LSMTreeGeoIndex(builder.getDatabase(), builder.getIndexName(),
          builder.getFilePath(), ComponentFile.MODE.READ_WRITE,
          builder.getPageSize(), builder.getNullStrategy(), precision);
    }
  }

  /**
   * Called at load time. Uses the default precision.
   */
  public LSMTreeGeoIndex(final LSMTreeIndex index) {
    this(index, DEFAULT_PRECISION);
  }

  /**
   * Called at load time with explicit precision.
   */
  public LSMTreeGeoIndex(final LSMTreeIndex index, final int precision) {
    this.underlyingIndex = index;
    this.precision = precision;
    this.grid = new GeohashPrefixTree(GeoUtils.getSpatialContext(), precision);
    this.strategy = new RecursivePrefixTreeStrategy(grid, "geo");
  }

  /**
   * Creation time constructor (used by factory handler and tests).
   */
  public LSMTreeGeoIndex(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final int precision) {
    this.precision = precision;
    this.grid = new GeohashPrefixTree(GeoUtils.getSpatialContext(), precision);
    this.strategy = new RecursivePrefixTreeStrategy(grid, "geo");
    this.underlyingIndex = new LSMTreeIndex(database, name, false, filePath, mode, new Type[]{Type.STRING}, pageSize, nullStrategy);
  }

  /**
   * Loading time constructor from an existing file.
   */
  public LSMTreeGeoIndex(final DatabaseInternal database, final String name, final String filePath, final int fileId,
      final ComponentFile.MODE mode, final int pageSize, final int version) {
    this.precision = DEFAULT_PRECISION;
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

    // If keys[0] is already a Shape object, it came from a direct query — not valid for indexing.
    // If keys[0] is a String, it may be either WKT (from the original put) or a pre-tokenized
    // GeoHash string (from transaction commit replay after a transactional put).
    // We detect the replay case by attempting WKT parse: if it fails but the value looks like
    // a GeoHash token (short alphanumeric), pass it directly to the underlying index.
    final Object key0 = keys[0];

    if (key0 instanceof Shape) {
      // Direct Shape input — extract tokens and index them
      indexShape((Shape) key0, rids);
      return;
    }

    final String wkt = key0.toString();
    final Shape shape;
    try {
      shape = GeoUtils.getSpatialContext().getFormats().getWktReader().read(wkt);
    } catch (final Exception e) {
      // WKT parse failed. Check if this looks like a pre-tokenized GeoHash string
      // (commit replay scenario: transaction re-applies individual tokens via put()).
      if (looksLikeGeoHashToken(wkt)) {
        underlyingIndex.put(keys, rids);
        return;
      }
      LogManager.instance().log(this, Level.WARNING,
          "Geospatial index: skipping invalid WKT '%s': %s", wkt, e.getMessage());
      return;
    }

    indexShape(shape, rids);
  }

  /**
   * Tokenizes a shape using the geohash prefix tree strategy and stores each token in the
   * underlying LSM index.
   */
  private void indexShape(final Shape shape, final RID[] rids) {
    final Field[] fields = strategy.createIndexableFields(shape);
    for (final Field field : fields) {
      try {
        final TokenStream ts = field.tokenStream(null, null);
        if (ts == null)
          continue;
        // Spatial token streams emit binary GeoHash bytes via TermToBytesRefAttribute,
        // not via CharTermAttribute.
        final TermToBytesRefAttribute bytesAttr = ts.addAttribute(TermToBytesRefAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
          final String token = bytesAttr.getBytesRef().utf8ToString();
          if (!token.isEmpty())
            underlyingIndex.put(new Object[]{token}, rids);
        }
        ts.end();
        ts.close();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING,
            "Geospatial index: token error for shape '%s': %s", shape, e.getMessage());
      }
    }
  }

  /**
   * Returns true if the string looks like a GeoHash token (lowercase alphanumeric, max precision
   * length). Used to detect transaction commit replay where individual pre-tokenized GeoHash
   * strings are re-passed to put() by the TransactionIndexContext.
   */
  private boolean looksLikeGeoHashToken(final String s) {
    if (s == null || s.isEmpty() || s.length() > precision)
      return false;
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      // GeoHash alphabet: 0-9, b-z (excluding a, i, l, o)
      if (!((c >= '0' && c <= '9') || (c >= 'b' && c <= 'z' && c != 'i' && c != 'l' && c != 'o')))
        return false;
    }
    return true;
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

    // Iterate all tree cells that cover the search shape and collect their GeoHash tokens
    final CellIterator cellIter = grid.getTreeCellIterator(searchShape, detailLevel);
    final Map<RID, Integer> seen = new LinkedHashMap<>();
    while (cellIter.hasNext()) {
      final Cell cell = cellIter.next();
      if (cell.getShapeRel() == null)
        continue;
      final String token = cell.getTokenBytesNoLeaf(null).utf8ToString();
      if (token.isEmpty())
        continue;
      final IndexCursor cursor = underlyingIndex.get(new Object[]{token});
      while (cursor.hasNext())
        seen.put(cursor.next().getIdentity(), 1);
    }

    final List<IndexCursorEntry> entries = new ArrayList<>(seen.size());
    for (final RID rid : seen.keySet())
      entries.add(new IndexCursorEntry(keys, rid, 1));
    return new TempIndexCursor(entries);
  }

  @Override
  public void remove(final Object[] keys) {
    if (keys == null || keys.length == 0 || keys[0] == null)
      return;
    for (final String token : extractTokens(keys[0]))
      underlyingIndex.remove(new Object[]{token});
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    if (keys == null || keys.length == 0 || keys[0] == null)
      return;
    for (final String token : extractTokens(keys[0]))
      underlyingIndex.remove(new Object[]{token}, rid);
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
    return underlyingIndex.build(buildIndexBatchSize, callback);
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    // Placeholder — will return Schema.INDEX_TYPE.GEOSPATIAL after Task 4
    throw new UnsupportedOperationException("GEOSPATIAL index type not yet registered in schema — Task 4 pending");
  }

  @Override
  public boolean isValid() {
    return underlyingIndex.isValid();
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("type", "GEOSPATIAL");
    json.put("properties", getPropertyNames());
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
        // skip
      }
    }
    return tokens;
  }
}
