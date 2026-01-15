package com.arcadedb.schema;

import com.arcadedb.index.vector.VectorQuantizationType;
import com.arcadedb.serializer.json.JSONObject;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

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
 */
public class LSMVectorIndexMetadata extends IndexMetadata {
  public int                      dimensions;
  public VectorSimilarityFunction similarityFunction       = VectorSimilarityFunction.COSINE;
  public VectorQuantizationType   quantizationType         = VectorQuantizationType.NONE;
  public int                      maxConnections           = 16;
  public int                      beamWidth                = 100;
  public int                      efSearch                 = 100;  // Search beam width (higher = better recall but slower)
  public float                    neighborOverflowFactor   = 1.2f;
  public float                    alphaDiversityRelaxation = 1.2f;
  public String                   idPropertyName           = "id";
  public int                      locationCacheSize        = -1;  // -1 = use global default
  public int                      graphBuildCacheSize      = -1; // -1 = use global default
  public int                      mutationsBeforeRebuild   = -1; // -1 = use global default
  public boolean                  storeVectorsInGraph      = false; // Phase 2: Store vectors inline in graph file
  public boolean                  addHierarchy             = false;
  public String                   buildState               = "READY"; // BUILDING, READY, or INVALID

  // Product Quantization (PQ) configuration - used when quantizationType=PRODUCT
  public int                      pqSubspaces              = -1;    // Number of subspaces (M), -1 = auto (dimensions/4, capped at 512)
  public int                      pqClusters               = 256;   // Clusters per subspace (K), typically 256 for byte-sized codes
  public boolean                  pqCenterGlobally         = true;  // Whether to globally center vectors before PQ encoding
  public int                      pqTrainingLimit          = 128000; // Max vectors for PQ training (128K is JVector's recommended max)

  public LSMVectorIndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    super(typeName, propertyNames, bucketId);
  }

  @Override
  public void fromJSON(final JSONObject metadata) {
    super.fromJSON(metadata);

    if (metadata.has("dimensions"))
      this.dimensions = metadata.getInt("dimensions");

    if (metadata.has("similarity"))
      this.similarityFunction = VectorSimilarityFunction.valueOf(metadata.getString("similarity"));

    if (metadata.has("quantization"))
      this.quantizationType = VectorQuantizationType.valueOf(metadata.getString("quantization"));

    if (metadata.has("maxConnections"))
      this.maxConnections = metadata.getInt("maxConnections");

    if (metadata.has("beamWidth"))
      this.beamWidth = metadata.getInt("beamWidth");

    if (metadata.has("efSearch"))
      this.efSearch = metadata.getInt("efSearch");

    if (metadata.has("neighborOverflowFactor"))
      this.neighborOverflowFactor = ((Number) metadata.get("neighborOverflowFactor")).floatValue();

    if (metadata.has("alphaDiversityRelaxation"))
      this.alphaDiversityRelaxation = ((Number) metadata.get("alphaDiversityRelaxation")).floatValue();

    if (metadata.has("idPropertyName"))
      this.idPropertyName = metadata.getString("idPropertyName");

    if (metadata.has("locationCacheSize"))
      this.locationCacheSize = metadata.getInt("locationCacheSize");

    if (metadata.has("graphBuildCacheSize"))
      this.graphBuildCacheSize = metadata.getInt("graphBuildCacheSize");

    if (metadata.has("mutationsBeforeRebuild"))
      this.mutationsBeforeRebuild = metadata.getInt("mutationsBeforeRebuild");

    if (metadata.has("storeVectorsInGraph"))
      this.storeVectorsInGraph = metadata.getBoolean("storeVectorsInGraph");

    if (metadata.has("addHierarchy"))
      this.addHierarchy = metadata.getBoolean("addHierarchy");

    if (metadata.has("buildState"))
      this.buildState = metadata.getString("buildState");

    // Product Quantization (PQ) configuration
    if (metadata.has("pqSubspaces"))
      this.pqSubspaces = metadata.getInt("pqSubspaces");

    if (metadata.has("pqClusters"))
      this.pqClusters = metadata.getInt("pqClusters");

    if (metadata.has("pqCenterGlobally"))
      this.pqCenterGlobally = metadata.getBoolean("pqCenterGlobally");

    if (metadata.has("pqTrainingLimit"))
      this.pqTrainingLimit = metadata.getInt("pqTrainingLimit");

  }
}
