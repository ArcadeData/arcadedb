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
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Locale;
import java.util.Set;

/**
 * Metadata for the {@link Schema.INDEX_TYPE#LSM_SPARSE_VECTOR LSM_SPARSE_VECTOR} index type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMSparseVectorIndexMetadata extends IndexMetadata {

  public static final String MODIFIER_NONE = "NONE";
  public static final String MODIFIER_IDF  = "IDF";

  /**
   * Default posting-weight quantization. INT8 keeps segments compact (1 byte/weight) with
   * near-exact recall; FP16 (2 bytes) and FP32 (4 bytes, exact) are available for workloads that
   * need higher fidelity. This mirrors the dense index's {@code quantization} knob.
   */
  public static final WeightQuantization DEFAULT_WEIGHT_QUANTIZATION = WeightQuantization.INT8;

  /**
   * {@link #rescoreOversample} value meaning "pick by quantization": {@link #DEFAULT_RESCORE_OVERSAMPLE} for the lossy
   * INT8 and FP16 weights, no rescoring for FP32, whose index scores are already exact.
   */
  public static final int RESCORE_OVERSAMPLE_AUTO    = -1;
  /**
   * Candidates fetched per requested result before the exact rescoring, when the index does not say (issue #8576).
   * Measured on the BigANN sparse track (SPLADE, 100k documents, top-10): INT8 recall 0.9948 without rescoring and
   * 1.0000 with 2, FP16 0.9991 and 1.0000.
   */
  public static final int DEFAULT_RESCORE_OVERSAMPLE = 2;
  /** Upper bound of {@link #rescoreOversample}: past it the record loads cost more than any recall left to win. */
  public static final int MAX_RESCORE_OVERSAMPLE     = 100;

  /** The only keys a user may write in {@code METADATA}: anything else is a typo worth reporting (issue #5639). */
  private static final Set<String> USER_METADATA_KEYS = Set.of("dimensions", "modifier", "weightQuantization",
      "rescoreOversample");

  public int                dimensions;
  public String             modifier           = MODIFIER_NONE;
  public WeightQuantization weightQuantization = DEFAULT_WEIGHT_QUANTIZATION;
  /**
   * Exact rescoring of the top-K (issue #8576): a search fetches {@code k * rescoreOversample} candidates from the
   * quantized index, recomputes each one's score from the record's own full-precision weights and keeps the best
   * {@code k}. {@code 0} turns it off, {@code 1} makes the returned scores exact without widening the candidate set,
   * {@link #RESCORE_OVERSAMPLE_AUTO} (the default) picks by quantization - see {@link #effectiveRescoreOversample()}.
   */
  public int                rescoreOversample  = RESCORE_OVERSAMPLE_AUTO;

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
    this.modifier = metadata.getString("modifier", MODIFIER_NONE).toUpperCase(Locale.ROOT);
    this.weightQuantization = parseWeightQuantization(
        metadata.getString("weightQuantization", DEFAULT_WEIGHT_QUANTIZATION.name()));
    this.rescoreOversample = metadata.getInt("rescoreOversample", RESCORE_OVERSAMPLE_AUTO);
  }

  @Override
  public LSMSparseVectorIndexMetadata copy(final String typeName, final String[] propertyNames, final int bucketId) {
    final LSMSparseVectorIndexMetadata copy = copyCommonTo(
        new LSMSparseVectorIndexMetadata(typeName, propertyNames, bucketId));
    copy.dimensions = dimensions;
    copy.modifier = modifier;
    copy.weightQuantization = weightQuantization;
    copy.rescoreOversample = rescoreOversample;
    return copy;
  }

  @Override
  public Set<String> getUserMetadataKeys() {
    return USER_METADATA_KEYS;
  }

  @Override
  protected void applyUserMetadata(final JSONObject json) {
    // Read key by key rather than delegating to fromJSON(): there, an absent key resets the field to the sparse
    // default because it reads a complete persisted definition; here an absent key means the user did not ask for
    // anything, so whatever the builder was already configured with must stand.
    if (json.has("dimensions"))
      setDimensions(metadataInt(json, "dimensions"));

    if (json.has("modifier"))
      setModifier(json.getString("modifier"));

    if (json.has("weightQuantization"))
      this.weightQuantization = parseWeightQuantization(json.getString("weightQuantization"));

    if (json.has("rescoreOversample"))
      setRescoreOversample(metadataInt(json, "rescoreOversample"));
  }

  @Override
  protected Object getUserMetadataValue(final String key) {
    return switch (key) {
      case "dimensions" -> dimensions;
      case "modifier" -> modifier;
      case "weightQuantization" -> weightQuantization;
      case "rescoreOversample" -> rescoreOversample;
      default -> null;
    };
  }

  /**
   * Sets the maximum dimensionality of the sparse vectors. A value of 0 means dimensions are inferred from the data.
   */
  public void setDimensions(final int dimensions) {
    if (dimensions < 0)
      throw new IllegalArgumentException("dimensions must be >= 0");
    this.dimensions = dimensions;
  }

  /**
   * Sets how many candidates per requested result a search rescores exactly: {@code 0} disables the rescoring,
   * {@link #RESCORE_OVERSAMPLE_AUTO} restores the per-quantization default. See {@link #rescoreOversample}.
   */
  public void setRescoreOversample(final int rescoreOversample) {
    if (rescoreOversample != RESCORE_OVERSAMPLE_AUTO && (rescoreOversample < 0 || rescoreOversample > MAX_RESCORE_OVERSAMPLE))
      throw new IndexException("Invalid sparse vector index rescoreOversample: " + rescoreOversample
          + ". Supported values: 0 (off) to " + MAX_RESCORE_OVERSAMPLE + ", or " + RESCORE_OVERSAMPLE_AUTO + " (default by weightQuantization)");
    this.rescoreOversample = rescoreOversample;
  }

  /**
   * The oversampling a search applies: the configured {@link #rescoreOversample}, or when it is
   * {@link #RESCORE_OVERSAMPLE_AUTO} {@link #DEFAULT_RESCORE_OVERSAMPLE} for INT8 and FP16 weights and {@code 0} (off)
   * for FP32, whose index scores are already the exact ones.
   */
  public int effectiveRescoreOversample() {
    if (rescoreOversample != RESCORE_OVERSAMPLE_AUTO)
      return rescoreOversample;
    return weightQuantization == WeightQuantization.FP32 ? 0 : DEFAULT_RESCORE_OVERSAMPLE;
  }

  /**
   * Sets the scoring modifier: {@link #MODIFIER_NONE} (default) or {@link #MODIFIER_IDF}, case-insensitive.
   */
  public void setModifier(final String modifier) {
    final String normalized = modifier == null ? MODIFIER_NONE : modifier.toUpperCase(Locale.ROOT);
    if (!MODIFIER_NONE.equals(normalized) && !MODIFIER_IDF.equals(normalized))
      throw new IndexException("Invalid sparse vector index modifier: " + modifier + ". Supported values: NONE, IDF");
    this.modifier = normalized;
  }

  /**
   * Parses a user-supplied quantization name into a {@link WeightQuantization}, tolerating case and
   * surrounding whitespace, and producing a clear error listing the supported values instead of the
   * bare {@code IllegalArgumentException} from {@code valueOf}.
   */
  public static WeightQuantization parseWeightQuantization(final String value) {
    if (value == null)
      return DEFAULT_WEIGHT_QUANTIZATION;
    final String normalized = value.trim().toUpperCase(Locale.ROOT);
    try {
      return WeightQuantization.valueOf(normalized);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid sparse vector index weightQuantization: '" + value + "'. Supported values: FP32, FP16, INT8");
    }
  }
}
