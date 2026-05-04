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
 * Metadata for the {@link Schema.INDEX_TYPE#LSM_SPARSE_VECTOR LSM_SPARSE_VECTOR} index type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMSparseVectorIndexMetadata extends IndexMetadata {

  public static final String MODIFIER_NONE = "NONE";
  public static final String MODIFIER_IDF  = "IDF";

  public int    dimensions;
  public String modifier = MODIFIER_NONE;

  public LSMSparseVectorIndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    super(typeName, propertyNames, bucketId);
  }

  /**
   * Populate the metadata from the JSON entry written by {@code LSMSparseVectorIndex.toJSON()}.
   * <p>
   * The bucket-index JSON written by the wrapper does NOT carry {@code typeName} or
   * {@code associatedBucketId}: those are stored at the outer {@code types.<typeName>} key in
   * {@code schema.json} and are passed to this metadata via the constructor when
   * {@code LocalSchema.readConfiguration()} reconstructs the wrapper. {@code IndexMetadata.fromJSON}
   * would throw if called on such a JSON because it unconditionally reads {@code typeName} as a
   * required field. The {@code if (metadata.has("typeName"))} guard preserves backward
   * compatibility for any callers that pass the full type-level JSON, while the load path
   * intentionally skips it because every parent field (typeName, propertyNames, bucketId) is
   * already set, and the only optional field {@code IndexMetadata.fromJSON} would populate -
   * {@code collations} - is not meaningful for a sparse vector index whose keys are
   * {@code (int, RID, float)} composites rather than strings.
   */
  @Override
  public void fromJSON(final JSONObject metadata) {
    if (metadata.has("typeName"))
      super.fromJSON(metadata);
    this.dimensions = metadata.getInt("dimensions", 0);
    this.modifier = metadata.getString("modifier", MODIFIER_NONE).toUpperCase();
  }
}
