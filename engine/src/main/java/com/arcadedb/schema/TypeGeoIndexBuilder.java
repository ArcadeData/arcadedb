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
 * Builder for GEOSPATIAL indexes: carries the {@link GeoIndexMetadata} that drives the GeoHash cell resolution
 * ({@code precision}) and the on-disk cell layout ({@code tokenization}).
 * <p>
 * Before this existed, {@code withType(GEOSPATIAL)} left the builder holding a plain {@link IndexMetadata}, so
 * {@code CREATE INDEX ... GEOSPATIAL METADATA {...}} had nowhere to put its settings and dropped them (issue #5600).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeGeoIndexBuilder extends TypeIndexBuilder {
  protected TypeGeoIndexBuilder(final TypeIndexBuilder copyFrom) {
    super(copyFrom.database, copyFrom.metadata.typeName, copyFrom.metadata.propertyNames.toArray(new String[0]));

    this.metadata = new GeoIndexMetadata(
        copyFrom.metadata.typeName,
        copyFrom.metadata.propertyNames.toArray(new String[0]),
        copyFrom.metadata.associatedBucketId);
    this.metadata.collations = copyFrom.metadata.collations;
    this.metadata.typeIndexName = copyFrom.metadata.typeIndexName;

    this.indexType = Schema.INDEX_TYPE.GEOSPATIAL;
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

  /**
   * Sets the GeoHash precision level (1-12). Higher means finer cells: 6 is about 1.2 km, the default 11 about 2.4 m.
   *
   * @return this builder for chaining
   */
  public TypeGeoIndexBuilder withPrecision(final int precision) {
    geoMetadata().setPrecision(precision);
    return this;
  }

  /**
   * Sets the cell layout the index entries are written in. Only meaningful at creation time.
   *
   * @return this builder for chaining
   */
  public TypeGeoIndexBuilder withTokenization(final GeoIndexMetadata.TOKENIZATION tokenization) {
    geoMetadata().setTokenization(tokenization);
    return this;
  }

  @Override
  public TypeGeoIndexBuilder withMetadata(final IndexMetadata metadata) {
    if (metadata != null && !(metadata instanceof GeoIndexMetadata))
      throw new IllegalArgumentException(
          "A GEOSPATIAL index requires GeoIndexMetadata but got " + metadata.getClass().getName());
    this.metadata = metadata;
    return this;
  }

  /**
   * Configures the builder from the {@code METADATA} clause of {@code CREATE INDEX}. Unknown keys are rejected rather
   * than dropped: a silently ignored {@code METADATA} is exactly what made the missing forwarding invisible (#5600).
   *
   * @param json the JSON object containing the metadata configuration
   *
   * @return this builder for chaining
   */
  public TypeGeoIndexBuilder withMetadata(final JSONObject json) {
    geoMetadata().fromUserMetadata(json, Schema.INDEX_TYPE.GEOSPATIAL);
    return this;
  }

  /**
   * Returns the builder's metadata as {@link GeoIndexMetadata}. The constructors always create one, but guard the cast
   * so that, if the metadata were ever replaced with a non-geospatial instance, callers get an actionable error instead
   * of a {@link ClassCastException}.
   */
  private GeoIndexMetadata geoMetadata() {
    if (metadata instanceof GeoIndexMetadata m)
      return m;
    throw new IllegalStateException(
        "Geospatial index metadata expected but was " + (metadata == null ? "null" : metadata.getClass().getName()));
  }
}
