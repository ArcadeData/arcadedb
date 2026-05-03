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

  @Override
  public void fromJSON(final JSONObject metadata) {
    if (metadata.has("typeName"))
      super.fromJSON(metadata);
    this.dimensions = metadata.getInt("dimensions", 0);
    this.modifier = metadata.getString("modifier", MODIFIER_NONE).toUpperCase();
  }
}
