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
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

public class LSMVectorIndexMetadata extends IndexMetadata {
  public int                      dimensions;
  public VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.COSINE;
  public int                      maxConnections     = 16;
  public int                      beamWidth          = 100;
  public String                   idPropertyName     = "id";

  public LSMVectorIndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    super(typeName, propertyNames, bucketId);
  }

  public void fromJSON(final JSONObject metadata) {
    super.fromJSON(metadata);

    if (metadata.has("dimensions"))
      this.dimensions = metadata.getInt("dimensions");

    if (metadata.has("similarity"))
      this.similarityFunction = VectorSimilarityFunction.valueOf(metadata.getString("similarity"));

    if (metadata.has("maxConnections"))
      this.maxConnections = metadata.getInt("maxConnections");

    if (metadata.has("beamWidth"))
      this.beamWidth = metadata.getInt("beamWidth");

    if (metadata.has("idPropertyName"))
      this.idPropertyName = metadata.getString("idPropertyName");
  }
}
