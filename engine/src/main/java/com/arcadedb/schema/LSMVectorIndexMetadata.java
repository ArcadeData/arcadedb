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

import com.arcadedb.index.IndexException;
import com.arcadedb.index.vector.VectorEncoding;
import com.arcadedb.index.vector.VectorLocationIndex;
import com.arcadedb.index.vector.VectorQuantizationType;
import com.arcadedb.serializer.json.JSONObject;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import java.util.Set;

/**
 * Metadata of an {@link Schema.INDEX_TYPE#LSM_VECTOR LSM_VECTOR} index, and the single list of its settings: the
 * builders, the index and the persisted definition all read this class rather than keeping parallel copies of the
 * field list. Four settings were unreachable from SQL precisely because that list had been duplicated four times and
 * two copies had fallen behind (issue #5639).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexMetadata extends IndexMetadata {
  /**
   * Keys a user may write in {@code METADATA}. Everything on this class except {@code buildState}, which is the
   * index's own lifecycle marker and not a setting.
   * <p>
   * {@code locationCacheSize} is still listed even though {@link #setLocationCacheSize(int)} refuses any positive
   * value: this set only decides whether a key is RECOGNISED, so dropping it would turn the explanation into a bare
   * "unknown metadata key".
   */
  private static final Set<String> USER_METADATA_KEYS = Set.of("dimensions", "similarity", "quantization", "encoding",
      "maxConnections", "beamWidth", "efSearch", "neighborOverflowFactor", "alphaDiversityRelaxation", "idPropertyName",
      "locationCacheSize", "graphBuildCacheSize", "mutationsBeforeRebuild", "inactivityRebuildTimeoutMs",
      "storeVectorsInGraph", "addHierarchy", "pqSubspaces", "pqClusters", "pqCenterGlobally", "pqTrainingLimit");

  public int                      dimensions;
  public VectorSimilarityFunction similarityFunction       = VectorSimilarityFunction.COSINE;
  public VectorQuantizationType   quantizationType         = VectorQuantizationType.NONE;
  /**
   * Application-side encoding of the vector property (FLOAT32 default, INT8 opt-in). Distinct
   * from {@link #quantizationType}: encoding is what the document column stores; quantization
   * is what the index does internally on top. INT8 ingest skips a client-side
   * {@code int8 → float32} round-trip and shrinks the HTTP payload + document bucket 4x. See
   * {@link VectorEncoding}.
   */
  public VectorEncoding           encoding                 = VectorEncoding.FLOAT32;
  /**
   * Maximum number of graph connections (edges) kept per node, <b>per layer</b>. This is JVector's
   * Vamana {@code M} degree and is applied verbatim to every layer, including the base layer: it is
   * <b>not</b> doubled at layer 0 the way hnswlib (Chroma and most HNSW tools) treats its own {@code M}.
   * <p>
   * Consequence for anyone porting a configuration from an hnswlib-based system: hnswlib allocates
   * {@code 2*M} links at the base layer, so to reproduce hnswlib {@code M} density here set
   * {@code maxConnections = 2 * M} (e.g. hnswlib {@code M=16} → {@code maxConnections=32}). Leaving this
   * at half the intended value silently halves base-layer graph density and measurably lowers recall.
   * <p>
   * Default {@code 32} (raised from the historical 16 in issue #5352) matches hnswlib {@code M=16} density
   * and gives better out-of-the-box recall and query latency; the cost is higher build-time heap
   * (roughly proportional to the degree). Typical range: 16-64.
   */
  public int                      maxConnections           = 32;
  public int                      beamWidth                = 100;
  public int                      efSearch                 = 100;  // Search beam width (higher = better recall but slower)
  public float                    neighborOverflowFactor   = 1.2f;
  public float                    alphaDiversityRelaxation = 1.2f;
  public String                   idPropertyName           = "id";
  public int                      locationCacheSize        = -1;  // -1 = use global default
  public int                      graphBuildCacheSize      = -1; // -1 = use global default
  public int                      mutationsBeforeRebuild   = -1; // -1 = use global default
  public int                      inactivityRebuildTimeoutMs = -1; // -1 = use global default
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

  /**
   * Returns a fresh instance carrying every setting of this one, for the given type/properties/bucket. Used wherever an
   * index definition has to be reproduced (TRUNCATE TYPE rebuilding an index, the per-bucket index taking its
   * configuration from the type-level builder): the copy leaves the source's mutable runtime state behind - notably
   * {@code buildState}, which is per-index - while making it impossible to forget a field, since there is no second
   * field list to keep in sync.
   *
   * @param typeName      type the copy belongs to
   * @param propertyNames indexed properties of the copy
   * @param bucketId      associated bucket, or -1 when not bound yet
   */
  @Override
  public LSMVectorIndexMetadata copy(final String typeName, final String[] propertyNames, final int bucketId) {
    final LSMVectorIndexMetadata copy = copyCommonTo(new LSMVectorIndexMetadata(typeName, propertyNames, bucketId));
    copy.dimensions = dimensions;
    copy.similarityFunction = similarityFunction;
    copy.quantizationType = quantizationType;
    copy.encoding = encoding;
    copy.maxConnections = maxConnections;
    copy.beamWidth = beamWidth;
    copy.efSearch = efSearch;
    copy.neighborOverflowFactor = neighborOverflowFactor;
    copy.alphaDiversityRelaxation = alphaDiversityRelaxation;
    copy.idPropertyName = idPropertyName;
    copy.locationCacheSize = locationCacheSize;
    copy.graphBuildCacheSize = graphBuildCacheSize;
    copy.mutationsBeforeRebuild = mutationsBeforeRebuild;
    copy.inactivityRebuildTimeoutMs = inactivityRebuildTimeoutMs;
    copy.storeVectorsInGraph = storeVectorsInGraph;
    copy.addHierarchy = addHierarchy;
    copy.pqSubspaces = pqSubspaces;
    copy.pqClusters = pqClusters;
    copy.pqCenterGlobally = pqCenterGlobally;
    copy.pqTrainingLimit = pqTrainingLimit;
    return copy;
  }

  @Override
  public Set<String> getUserMetadataKeys() {
    return USER_METADATA_KEYS;
  }

  /**
   * Applies the {@code METADATA} clause of {@code CREATE INDEX}. Every key is read through the validating setter it
   * shares with the Java builders, and an absent key leaves the current value alone - unlike
   * {@link #fromJSON(JSONObject)}, which reads a complete persisted definition.
   */
  @Override
  protected void applyUserMetadata(final JSONObject json) {
    if (json.has("dimensions"))
      this.dimensions = metadataInt(json, "dimensions");

    if (json.has("similarity"))
      setSimilarity(json.getString("similarity"));

    if (json.has("quantization"))
      setQuantization(json.getString("quantization"));

    if (json.has("encoding"))
      setEncoding(json.getString("encoding"));

    if (json.has("maxConnections"))
      setMaxConnections(metadataInt(json, "maxConnections"));

    if (json.has("beamWidth"))
      setBeamWidth(metadataInt(json, "beamWidth"));

    if (json.has("efSearch"))
      setEfSearch(metadataInt(json, "efSearch"));

    if (json.has("neighborOverflowFactor"))
      setNeighborOverflowFactor(metadataFloat(json, "neighborOverflowFactor"));

    if (json.has("alphaDiversityRelaxation"))
      setAlphaDiversityRelaxation(metadataFloat(json, "alphaDiversityRelaxation"));

    if (json.has("idPropertyName"))
      this.idPropertyName = json.getString("idPropertyName");

    if (json.has("locationCacheSize"))
      setLocationCacheSize(metadataInt(json, "locationCacheSize"));

    if (json.has("graphBuildCacheSize"))
      this.graphBuildCacheSize = metadataInt(json, "graphBuildCacheSize");

    if (json.has("mutationsBeforeRebuild"))
      this.mutationsBeforeRebuild = metadataInt(json, "mutationsBeforeRebuild");

    if (json.has("inactivityRebuildTimeoutMs"))
      this.inactivityRebuildTimeoutMs = metadataInt(json, "inactivityRebuildTimeoutMs");

    if (json.has("storeVectorsInGraph"))
      this.storeVectorsInGraph = metadataBoolean(json, "storeVectorsInGraph");

    if (json.has("addHierarchy"))
      this.addHierarchy = metadataBoolean(json, "addHierarchy");

    if (json.has("pqSubspaces"))
      setPQSubspaces(metadataInt(json, "pqSubspaces"));

    if (json.has("pqClusters"))
      setPQClusters(metadataInt(json, "pqClusters"));

    if (json.has("pqCenterGlobally"))
      this.pqCenterGlobally = metadataBoolean(json, "pqCenterGlobally");

    if (json.has("pqTrainingLimit"))
      setPQTrainingLimit(metadataInt(json, "pqTrainingLimit"));
  }

  @Override
  protected Object getUserMetadataValue(final String key) {
    return switch (key) {
      case "dimensions" -> dimensions;
      case "similarity" -> similarityFunction;
      case "quantization" -> quantizationType;
      case "encoding" -> encoding;
      case "maxConnections" -> maxConnections;
      case "beamWidth" -> beamWidth;
      case "efSearch" -> efSearch;
      case "neighborOverflowFactor" -> neighborOverflowFactor;
      case "alphaDiversityRelaxation" -> alphaDiversityRelaxation;
      case "idPropertyName" -> idPropertyName;
      case "locationCacheSize" -> locationCacheSize;
      case "graphBuildCacheSize" -> graphBuildCacheSize;
      case "mutationsBeforeRebuild" -> mutationsBeforeRebuild;
      case "inactivityRebuildTimeoutMs" -> inactivityRebuildTimeoutMs;
      case "storeVectorsInGraph" -> storeVectorsInGraph;
      case "addHierarchy" -> addHierarchy;
      case "pqSubspaces" -> pqSubspaces;
      case "pqClusters" -> pqClusters;
      case "pqCenterGlobally" -> pqCenterGlobally;
      case "pqTrainingLimit" -> pqTrainingLimit;
      default -> null;
    };
  }

  /**
   * Sets the similarity function from its name (COSINE, DOT_PRODUCT, EUCLIDEAN), case-insensitive.
   */
  public void setSimilarity(final String similarity) {
    try {
      this.similarityFunction = VectorSimilarityFunction.valueOf(similarity.toUpperCase());
    } catch (final IllegalArgumentException e) {
      throw new IndexException(
          "Invalid similarity function: " + similarity + ". Supported values: COSINE, DOT_PRODUCT, EUCLIDEAN");
    }
  }

  /**
   * Refuses a location cache limit (issues #5559 and #5568), which is what every user-facing entrance to this
   * setting - the {@code METADATA} clause of {@code CREATE INDEX} and
   * {@link TypeLSMVectorIndexBuilder#withLocationCacheSize(int)} - now goes through.
   * <p>
   * The setting cannot be honoured. A vector location is the only mapping from a vector id to its record and to the
   * offset of its entry in the index file; nothing on disk maps a vector id back to an offset, so evicting a
   * location destroys it rather than spilling it to a slower tier, and every reader reads a missing location as
   * "deleted". A capped index therefore under-reported {@code countEntries()} and dropped the evicted vectors from
   * its searches, silently. Accepting the value and ignoring it would leave the same lie in the schema, where
   * {@code schema:indexes} would keep echoing a bound that is not in force, so the statement is refused instead.
   * <p>
   * {@code -1} (and the historical {@code 0}) mean "no limit" and are accepted: they are what a metadata copy or an
   * unset builder carries, and they ask for exactly what the index does. A definition persisted by an older version
   * is read by {@link #fromJSON(JSONObject)}, which does not come through here - refusing there would make an
   * existing database unopenable.
   *
   * @param locationCacheSize the requested limit; anything positive is refused
   */
  public void setLocationCacheSize(final int locationCacheSize) {
    if (locationCacheSize > 0)
      throw new IndexException("'locationCacheSize' is no longer supported (issues #5559 and #5568): a vector "
          + "location is the only mapping from a vector id to its record, so capping the location index drops "
          + "vectors from searches and from countEntries() instead of spilling them to disk. Remove the setting and "
          + "size the heap for ~" + VectorLocationIndex.APPROX_RETAINED_BYTES_PER_LOCATION
          + " bytes per live vector (~90MB per million)");

    this.locationCacheSize = locationCacheSize;
  }

  /**
   * Sets the index-internal quantization from its name (NONE, INT8, BINARY, PRODUCT), case-insensitive.
   */
  public void setQuantization(final String quantization) {
    try {
      this.quantizationType = VectorQuantizationType.valueOf(quantization.toUpperCase());
    } catch (final IllegalArgumentException e) {
      throw new IndexException("Invalid quantization type: " + quantization + ". Supported values: NONE, INT8, BINARY, PRODUCT");
    }
  }

  /**
   * Sets the wire / storage encoding of the vector property from its name (FLOAT32, INT8).
   */
  public void setEncoding(final String encoding) {
    try {
      this.encoding = VectorEncoding.fromString(encoding);
    } catch (final IllegalArgumentException e) {
      throw new IndexException(e.getMessage(), e);
    }
  }

  /** Sets the Vamana per-layer graph degree; see {@link #maxConnections}. */
  public void setMaxConnections(final int maxConnections) {
    if (maxConnections < 1)
      throw new IllegalArgumentException("maxConnections must be at least 1");
    this.maxConnections = maxConnections;
  }

  /** Sets the build-time beam width. Higher values improve recall but increase build time. */
  public void setBeamWidth(final int beamWidth) {
    if (beamWidth < 1)
      throw new IllegalArgumentException("beamWidth must be at least 1");
    this.beamWidth = beamWidth;
  }

  /** Sets the search-time beam width. Higher values improve recall at the cost of latency. */
  public void setEfSearch(final int efSearch) {
    if (efSearch < 1)
      throw new IllegalArgumentException("efSearch must be at least 1");
    this.efSearch = efSearch;
  }

  /** Sets the neighbor overflow factor used while building the graph. Typical range 1.0-1.5. */
  public void setNeighborOverflowFactor(final float neighborOverflowFactor) {
    if (neighborOverflowFactor < 1.0f)
      throw new IllegalArgumentException("neighborOverflowFactor must be at least 1.0");
    this.neighborOverflowFactor = neighborOverflowFactor;
  }

  /** Sets the alpha diversity relaxation factor used while building the graph. Typical range 1.0-1.5. */
  public void setAlphaDiversityRelaxation(final float alphaDiversityRelaxation) {
    if (alphaDiversityRelaxation < 1.0f)
      throw new IllegalArgumentException("alphaDiversityRelaxation must be at least 1.0");
    this.alphaDiversityRelaxation = alphaDiversityRelaxation;
  }

  /** Sets the number of PQ subspaces (M). Only applicable when {@code quantizationType=PRODUCT}. */
  public void setPQSubspaces(final int pqSubspaces) {
    if (pqSubspaces < 1)
      throw new IllegalArgumentException("pqSubspaces must be at least 1");
    this.pqSubspaces = pqSubspaces;
  }

  /**
   * Sets the number of PQ clusters per subspace (K). A PQ code is one byte per subspace, so more than 256 clusters
   * cannot be encoded: reject it at index creation instead of letting the graph build fail later and leave the index
   * without a graph (issue #5417).
   */
  public void setPQClusters(final int pqClusters) {
    if (pqClusters < 1)
      throw new IllegalArgumentException("pqClusters must be at least 1");
    if (pqClusters > 256)
      throw new IllegalArgumentException("pqClusters cannot exceed 256 (PQ codes are one byte per subspace)");
    this.pqClusters = pqClusters;
  }

  /** Sets the maximum number of vectors used to train the PQ codebooks. */
  public void setPQTrainingLimit(final int pqTrainingLimit) {
    if (pqTrainingLimit < 1)
      throw new IllegalArgumentException("pqTrainingLimit must be at least 1");
    this.pqTrainingLimit = pqTrainingLimit;
  }

  @Override
  public void fromJSON(final JSONObject metadata) {
    super.fromJSON(metadata);

    if (metadata.has("dimensions"))
      this.dimensions = metadata.getInt("dimensions");

    // "similarity" is the name of the METADATA key; "similarityFunction" is the name LSMVectorIndex.toJSON() has always
    // written into the persisted definition. Accept both, or a reopened EUCLIDEAN index comes back up as COSINE and
    // every search after the restart scores with the wrong metric (issue #5639). Only one of the two ever appears in a
    // definition this engine wrote; should a hand-edited one carry both, "similarity" wins because it is the spelling a
    // human would have reached for.
    if (metadata.has("similarity"))
      setSimilarity(metadata.getString("similarity"));
    else if (metadata.has("similarityFunction"))
      setSimilarity(metadata.getString("similarityFunction"));

    // Through the setters, like "similarity" above: a value that cannot be read here comes from a corrupted or
    // hand-edited schema.json and surfaces while OPENING the database, so it is worth the setter's message naming the
    // supported values instead of a bare enum constant name.
    if (metadata.has("quantization"))
      setQuantization(metadata.getString("quantization"));

    if (metadata.has("encoding"))
      setEncoding(metadata.getString("encoding"));

    if (metadata.has("maxConnections"))
      this.maxConnections = metadata.getInt("maxConnections");

    if (metadata.has("beamWidth"))
      this.beamWidth = metadata.getInt("beamWidth");

    if (metadata.has("efSearch"))
      this.efSearch = metadata.getInt("efSearch");

    // metadataFloat, not a cast to Number: the cast raised ClassCastException on a quoted value, which for a
    // hand-edited or hand-restored schema.json meant a failure while OPENING the database. The integer keys above
    // need no equivalent change - getInt() already parses a quoted number through Gson's lazy Number - so the reader
    // here is deliberately not mirrored onto them.
    if (metadata.has("neighborOverflowFactor"))
      this.neighborOverflowFactor = metadataFloat(metadata, "neighborOverflowFactor");

    if (metadata.has("alphaDiversityRelaxation"))
      this.alphaDiversityRelaxation = metadataFloat(metadata, "alphaDiversityRelaxation");

    if (metadata.has("idPropertyName"))
      this.idPropertyName = metadata.getString("idPropertyName");

    if (metadata.has("locationCacheSize"))
      this.locationCacheSize = metadata.getInt("locationCacheSize");

    if (metadata.has("graphBuildCacheSize"))
      this.graphBuildCacheSize = metadata.getInt("graphBuildCacheSize");

    if (metadata.has("mutationsBeforeRebuild"))
      this.mutationsBeforeRebuild = metadata.getInt("mutationsBeforeRebuild");

    if (metadata.has("inactivityRebuildTimeoutMs"))
      this.inactivityRebuildTimeoutMs = metadata.getInt("inactivityRebuildTimeoutMs");

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
