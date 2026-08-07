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

import com.arcadedb.serializer.json.JSONObject;

import java.util.Arrays;
import java.util.Set;

/**
 * Metadata class for geospatial indexes, storing the precision level for the
 * GeohashPrefixTree spatial strategy.
 * <p>
 * Precision level controls cell resolution:
 * <ul>
 *   <li>Precision 1  → ~5,000 km</li>
 *   <li>Precision 6  → ~1.2 km</li>
 *   <li>Precision 11 → ~2.4 m (default)</li>
 *   <li>Precision 12 → ~0.6 m</li>
 * </ul>
 *
 * @author Arcade Data Ltd
 */
public class GeoIndexMetadata extends IndexMetadata {

  /**
   * How a shape is decomposed into the GeoHash cell tokens stored in the underlying LSM-Tree.
   */
  public enum TOKENIZATION {
    /**
     * Legacy layout, written by every geospatial index created before 26.8.1. Stores the WHOLE ancestor chain of the
     * decomposition, so a point costs {@code precision} index entries (11 by default) and the continent-sized cells at
     * the top of the tree collect one posting per indexed record. Queries resolve it with an exact lookup per covering
     * cell of the search shape.
     */
    FULL,
    /**
     * Stores only the deepest cells of the decomposition - exactly ONE for a point - and resolves queries with a GeoHash
     * prefix range scan over the covering cells plus an exact lookup on their ancestors (issue #5478). Same results,
     * {@code precision} times fewer entries to write, replicate and compact, and no hot key.
     * <p>
     * On an AREA shape a COMPLETE set of sibling cells is additionally collapsed into its parent, recursively - the
     * reduction Lucene calls {@code pruneLeafyBranches} and applies by default on its own indexing path (issue #5600).
     * A parent covers the union of its children, so the cover can only grow and a match is never lost; the SQL geo.*
     * predicate post-filters the superset either way. A point decomposes into a chain of single-child cells and is
     * therefore never affected.
     */
    FRONTIER
  }

  /** Default geohash precision level (~2.4 m cell resolution). */
  public static final int DEFAULT_PRECISION = 11;

  /** Layout of a newly created geospatial index. */
  public static final TOKENIZATION DEFAULT_TOKENIZATION = TOKENIZATION.FRONTIER;

  /**
   * Layout of an index whose persisted definition predates {@link #DEFAULT_TOKENIZATION}. A missing field can only mean
   * a database written before 26.8.1, whose entries are in the {@link TOKENIZATION#FULL} layout; reading it as anything
   * else would make put/remove stop matching what is already on disk.
   */
  public static final TOKENIZATION LEGACY_TOKENIZATION = TOKENIZATION.FULL;

  /** The only keys a user may write in {@code METADATA}: anything else is a typo worth reporting. */
  private static final Set<String> USER_METADATA_KEYS = Set.of("precision", "tokenization");

  private int          precision    = DEFAULT_PRECISION;
  private TOKENIZATION tokenization = DEFAULT_TOKENIZATION;

  /**
   * Creates a new GeoIndexMetadata instance.
   *
   * @param typeName      the name of the type this index belongs to
   * @param propertyNames the property names indexed
   * @param bucketId      the associated bucket ID
   */
  public GeoIndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    super(typeName, propertyNames, bucketId);
  }

  @Override
  public void fromJSON(final JSONObject metadata) {
    if (metadata.has("typeName"))
      super.fromJSON(metadata);
    this.precision = metadata.getInt("precision", DEFAULT_PRECISION);
    this.tokenization = readTokenization(metadata);
  }

  /**
   * Reassembles a geospatial definition from the common index metadata plus the two settings a geospatial index keeps
   * outside it. {@code LSMTreeGeoIndex} holds the resolution and the layout as plain fields and its underlying LSM-Tree
   * carries a base {@link IndexMetadata}, so answering "the definition to create a new file with" means putting the two
   * halves back together. It belongs here rather than on the index because copying the base fields is
   * {@link #copyCommonTo}'s job, and that is only reachable from inside this hierarchy.
   *
   * @param base         common metadata of the underlying index, or {@code null} when there is none
   * @param precision    geohash resolution of the index
   * @param tokenization storage layout of the index
   */
  public static GeoIndexMetadata from(final IndexMetadata base, final int precision, final TOKENIZATION tokenization) {
    final GeoIndexMetadata metadata = base != null ?
        base.copyCommonTo(new GeoIndexMetadata(base.typeName, base.propertyNames.toArray(new String[0]),
            base.associatedBucketId)) :
        new GeoIndexMetadata(null, new String[0], -1);
    metadata.setPrecision(precision);
    metadata.setTokenization(tokenization);
    return metadata;
  }

  /**
   * Carries the geohash resolution AND the storage layout over. The layout is a property of what is written in the
   * file, not a preference, so a copy that is going to be populated from the same definition keeps it; a caller that
   * knows it is re-tokenizing every record from scratch - a rebuild - overrides it with
   * {@link #DEFAULT_TOKENIZATION} afterwards.
   */
  @Override
  public GeoIndexMetadata copy(final String typeName, final String[] propertyNames, final int bucketId) {
    final GeoIndexMetadata copy = copyCommonTo(new GeoIndexMetadata(typeName, propertyNames, bucketId));
    copy.precision = precision;
    copy.tokenization = tokenization;
    return copy;
  }

  @Override
  public Set<String> getUserMetadataKeys() {
    return USER_METADATA_KEYS;
  }

  @Override
  protected void applyUserMetadata(final JSONObject json) {
    // A GeoHash precision is a tree LEVEL, so 6.9 is not "6": metadataInt refuses a fractional value rather than
    // truncating it, which would drop the very kind of typo this guard exists to report.
    if (json.has("precision"))
      setPrecision(metadataInt(json, "precision"));

    if (json.has("tokenization")) {
      final Object tokenization = json.get("tokenization");
      if (!(tokenization instanceof String name))
        throw new IllegalArgumentException("Geospatial index tokenization must be a string, got: " + tokenization);
      try {
        setTokenization(TOKENIZATION.valueOf(name.toUpperCase()));
      } catch (final IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid geospatial index tokenization '" + name + "'. Supported values: "
            + Arrays.toString(TOKENIZATION.values()), e);
      }
    }
  }

  @Override
  protected Object getUserMetadataValue(final String key) {
    return switch (key) {
      case "precision" -> precision;
      case "tokenization" -> tokenization;
      default -> null;
    };
  }

  /**
   * Reads the tokenization layout from a PERSISTED index definition, defaulting to {@link #LEGACY_TOKENIZATION} when the
   * field is absent.
   */
  public static TOKENIZATION readTokenization(final JSONObject indexJSON) {
    final String value = indexJSON.getString("tokenization", LEGACY_TOKENIZATION.name());
    try {
      return TOKENIZATION.valueOf(value.toUpperCase());
    } catch (final IllegalArgumentException e) {
      return LEGACY_TOKENIZATION;
    }
  }

  /**
   * Serializes geospatial-specific metadata into the provided JSON object.
   *
   * @param json the JSON object to write metadata into
   */
  public void toJSON(final JSONObject json) {
    json.put("precision", precision);
    json.put("tokenization", tokenization.name());
  }

  /**
   * Returns the geohash precision level.
   *
   * @return the precision level
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Sets the geohash precision level.
   *
   * @param precision the precision level (1–12)
   */
  public void setPrecision(final int precision) {
    if (precision < 1 || precision > 12)
      throw new IllegalArgumentException("Geospatial index precision must be between 1 and 12, got: " + precision);
    this.precision = precision;
  }

  /**
   * Returns the cell tokenization layout of the index.
   */
  public TOKENIZATION getTokenization() {
    return tokenization;
  }

  /**
   * Sets the cell tokenization layout. Only meaningful at creation time: changing it on an index that already holds
   * entries makes put/remove disagree with what is stored, so an existing index must be rebuilt instead.
   */
  public void setTokenization(final TOKENIZATION tokenization) {
    if (tokenization == null)
      throw new IllegalArgumentException("Geospatial index tokenization cannot be null");
    this.tokenization = tokenization;
  }
}
