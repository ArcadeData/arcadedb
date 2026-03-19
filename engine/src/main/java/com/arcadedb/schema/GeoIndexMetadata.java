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

  /** Default geohash precision level (~2.4 m cell resolution). */
  public static final int DEFAULT_PRECISION = 11;

  private int precision = DEFAULT_PRECISION;

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
  }

  /**
   * Serializes geospatial-specific metadata into the provided JSON object.
   *
   * @param json the JSON object to write metadata into
   */
  public void toJSON(final JSONObject json) {
    json.put("precision", precision);
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
}
