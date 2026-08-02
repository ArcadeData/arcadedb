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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class IndexMetadata {
  public static final String COLLATION_CI      = "CI";
  public static final String COLLATION_DEFAULT = "DEFAULT";

  public String       typeName;
  public List<String> propertyNames;
  public List<String> collations;
  public int          associatedBucketId;

  /**
   * User-supplied name of the {@link com.arcadedb.index.TypeIndex} aggregating this bucket-level
   * index, or {@code null} when no manual name was provided (then the auto-derived
   * {@code typeName + "[" + propertyNames + "]"} form is used). Persisted in the bucket-level
   * indexJSON so the manual name survives schema reload (issue #4139).
   */
  public String       typeIndexName;

  public IndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    this.typeName = typeName;
    this.propertyNames = propertyNames != null ? List.of(propertyNames) : List.of();
    this.associatedBucketId = bucketId;
  }

  /**
   * Returns a fresh instance carrying every SETTING of this definition, retargeted to the given type, properties and
   * bucket. This is what "carry the index configuration over into a new index file" means: a rebuild, a truncate, the
   * propagation to a freshly added bucket or sub type, a {@code copyType()}.
   * <p>
   * Two things the copy deliberately leaves behind. Anything that is per-index RUNTIME state rather than a setting -
   * the dense vector index's {@code buildState}, the full-text corpus counters - because the new file starts empty and
   * would otherwise inherit statistics describing a different set of records. And the association to a bucket, which
   * the caller re-establishes by passing the target {@code bucketId} (or -1 when the per-bucket builder will bind it
   * during {@code create()}).
   * <p>
   * Every subclass overrides this so its own settings ride along; there is no second field list to keep in sync, which
   * is the point. Missing overrides are how a page size (issue #5713), a null strategy, a collation and a whole
   * type-specific configuration all ended up being replaced by defaults on {@code copyType()} (issue #5723).
   *
   * @param typeName      type the copy belongs to
   * @param propertyNames indexed properties of the copy
   * @param bucketId      associated bucket, or -1 when not bound yet
   */
  public IndexMetadata copy(final String typeName, final String[] propertyNames, final int bucketId) {
    return copyCommonTo(new IndexMetadata(typeName, propertyNames, bucketId));
  }

  /**
   * Copies the settings held by this base class onto a copy an override has just instantiated, and hands it back so the
   * override can keep chaining. {@code collations} is shared rather than cloned: it is assigned wholesale and never
   * mutated in place.
   */
  protected final <T extends IndexMetadata> T copyCommonTo(final T copy) {
    copy.collations = collations;
    copy.typeIndexName = typeIndexName;
    return copy;
  }

  public void fromJSON(final JSONObject metadata) {
    typeName = metadata.getString("typeName");
    propertyNames = metadata.getJSONArray("properties").toListOfStrings();
    associatedBucketId = metadata.getInt("associatedBucketId", -1);
    final JSONArray collationsJSON = metadata.getJSONArray("collations", null);
    if (collationsJSON != null)
      collations = collationsJSON.toListOfStrings();
    typeIndexName = metadata.getString("typeIndexName", null);
  }

  /**
   * Applies the {@code METADATA} clause of {@code CREATE INDEX}, reporting a key this index type does not understand
   * instead of dropping it: a silently ignored METADATA is what kept two missing forwardings invisible (issues #5600
   * and #5639).
   * <p>
   * Deliberately distinct from {@link #fromJSON(JSONObject)}, which reads a PERSISTED definition. There, an absent key
   * can mean "written by an older version" and carries its own backward-compatible default (a geospatial index with no
   * {@code tokenization} is a pre-26.8.1 index and therefore FULL); here it just means the user did not ask for
   * anything, so the creation-time default must stand. A persisted definition also carries structural keys
   * ({@code type}, {@code bucket}, {@code version}, ...) that a user has no business writing, which is why the two key
   * spaces cannot be merged into one reader.
   *
   * @param json      the METADATA clause, or {@code null} when the statement carried none
   * @param indexType the index type, named in the error message
   *
   * @throws IllegalArgumentException if a key is not one this index type understands
   */
  public final void fromUserMetadata(final JSONObject json, final Schema.INDEX_TYPE indexType) {
    if (json == null)
      return;

    for (final String key : json.keySet())
      if (!isUserMetadataKey(key))
        throw new IllegalArgumentException("Unsupported metadata key '" + key + "' for a " + indexType
            + " index. Supported keys: " + describeUserMetadataKeys());

    applyUserMetadata(json);
  }

  /**
   * Keys the {@code METADATA} clause of {@code CREATE INDEX} may carry for this index type. Empty (the default) means
   * the index type has no user-facing setting at all, so any METADATA is a mistake.
   */
  public Set<String> getUserMetadataKeys() {
    return Set.of();
  }

  /**
   * Recognises one key of the {@code METADATA} clause. The default answers from {@link #getUserMetadataKeys()};
   * override where the key space is open-ended, such as the full-text per-field {@code <field>_analyzer}.
   */
  protected boolean isUserMetadataKey(final String key) {
    return getUserMetadataKeys().contains(key);
  }

  /**
   * Describes the accepted key space for the unknown-key message. Sorted so the list is stable and easy to scan;
   * override to document keys that {@link #getUserMetadataKeys()} cannot enumerate.
   */
  protected String describeUserMetadataKeys() {
    return new TreeSet<>(getUserMetadataKeys()).toString();
  }

  /**
   * Applies the recognised keys of the {@code METADATA} clause. Called only after every key has been validated, and
   * only for the keys actually present: an absent key must leave the creation-time default alone.
   * <p>
   * Not atomic, deliberately: a key that fails its own validation leaves the keys read before it already applied. That
   * is safe because the metadata being written belongs to a builder whose {@code create()} the failure prevents, so the
   * half-applied state is discarded with it - no index is ever built from it. A builder reused after a rejected clause
   * would carry those earlier values, which is why the callers construct one per statement.
   */
  protected void applyUserMetadata(final JSONObject json) {
    // no user-facing setting on a plain index
  }

  /**
   * Reads back the current value of one {@code METADATA} key, the inverse of the corresponding branch of
   * {@link #applyUserMetadata}. Answers {@code null} for a key this index type does not have.
   * <p>
   * Every key {@link #getUserMetadataKeys()} declares must be readable here. {@code IndexMetadataUserSettingComparisonTest}
   * enforces it, so a setting added to one of the two lists and not the other fails a test instead of silently
   * becoming invisible to {@link #findUserSettingMismatches}.
   */
  protected Object getUserMetadataValue(final String key) {
    return null;
  }

  /**
   * Answers which of the settings {@code requested} NAMES this definition does not already carry, so an
   * {@code IF NOT EXISTS} request can tell "the index that is there is the one I asked for" from "the index that is
   * there happens to sit on the same properties".
   * <p>
   * Only the keys the clause actually wrote are compared. A setting the caller did not name is one they expressed no
   * opinion about, so the existing index satisfies it by definition - which is what keeps a guarded statement with no
   * {@code METADATA} the plain no-op it has always been. Everything named IS compared, including the tuning knobs a
   * rebuild would not be needed to change ({@code efSearch}, {@code inactivityRebuildTimeoutMs}): writing a value into
   * a statement and having it silently discarded is the surprise this exists to remove, and the caller who meant the
   * no-op simply leaves the key out.
   * <p>
   * The comparison runs on the INTERNAL representation, not on the JSON: the request is first read through this index
   * type's own {@link #applyUserMetadata} onto a copy, so {@code "384"} and {@code 384}, {@code "cosine"} and
   * {@code COSINE} compare as the one value each denotes rather than as the two spellings they are. It also means an
   * unreadable value is reported by the reader that owns it, with its own message, instead of surfacing here as a
   * spurious mismatch.
   *
   * @param requested the {@code METADATA} clause of the request, or {@code null} when it carried none
   * @param indexType the requested index type, named in the reader's error messages
   *
   * @return one human-readable line per differing setting, empty when the request is already satisfied
   */
  public final List<String> findUserSettingMismatches(final JSONObject requested, final Schema.INDEX_TYPE indexType) {
    if (requested == null || requested.isEmpty())
      return List.of();

    final IndexMetadata asRequested = copy(typeName,
        propertyNames == null ? null : propertyNames.toArray(new String[0]), associatedBucketId);
    asRequested.fromUserMetadata(requested, indexType);

    final List<String> mismatches = new ArrayList<>();
    for (final String key : requested.keySet()) {
      final Object current = getUserMetadataValue(key);
      final Object wanted = asRequested.getUserMetadataValue(key);
      if (!Objects.equals(current, wanted))
        mismatches.add(key + "=" + current + " (requested " + wanted + ")");
    }
    // Stable order: a JSONObject key set is unordered, and an error message that reshuffles between two identical
    // statements is one nobody can match on.
    Collections.sort(mismatches);
    return mismatches;
  }

  /**
   * Reads an integer-valued key of the {@code METADATA} clause.
   * <p>
   * Every value in that clause comes from the statement, so a value of the wrong shape is a client mistake and must
   * answer HTTP 400. Reading it straight off the JSON getters does not achieve that: a cast to {@code Number} raises
   * {@code ClassCastException} and a JSON object raises {@code UnsupportedOperationException}, neither of which the SQL
   * layer turns into a parsing error, so they escaped as a 500 (issue #5639).
   * <p>
   * A quoted number is accepted - {@code {"dimensions": "8"}} means 8, the same as everywhere else in the clause - but
   * a fractional value is refused rather than truncated: these keys are counts and limits, so silently reading
   * {@code 8.5} as {@code 8} would drop the very kind of typo this reader exists to report.
   */
  protected static int metadataInt(final JSONObject json, final String key) {
    final Number number = metadataNumber(json, key, "a whole number");
    final double value = number.doubleValue();
    if (value != Math.rint(value))
      throw new IllegalArgumentException(
          "Index metadata '" + key + "' must be a whole number, got: " + json.get(key));
    // Report an overflow as an overflow. Sharing the "must be a whole number" message here would be technically true
    // of 3000000000 and useless to whoever wrote it.
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
      throw new IllegalArgumentException("Index metadata '" + key + "' must be between " + Integer.MIN_VALUE + " and "
          + Integer.MAX_VALUE + ", got: " + json.get(key));
    return number.intValue();
  }

  /**
   * Reads a decimal-valued key of the {@code METADATA} clause, accepting the quoted form. See {@link #metadataInt} for
   * why the raw JSON getters are not enough.
   */
  protected static float metadataFloat(final JSONObject json, final String key) {
    final float value = metadataNumber(json, key, "a number").floatValue();
    // A magnitude no float can hold arrives as an infinity, and an infinite tuning factor is not a value the caller can
    // have meant. Report it for the same reason metadataInt reports an overflow instead of truncating.
    if (!Float.isFinite(value))
      throw new IllegalArgumentException(
          "Index metadata '" + key + "' must be a finite number, got: " + json.get(key));
    return value;
  }

  /**
   * Reads a boolean-valued key of the {@code METADATA} clause. Only a real boolean, or the strings {@code "true"} /
   * {@code "false"}, are accepted: {@code JSONObject.getBoolean()} answers {@code false} for any other string, so
   * {@code {"addHierarchy": "yes"}} used to disable the setting the user was asking for (issue #5639).
   */
  protected static boolean metadataBoolean(final JSONObject json, final String key) {
    final Object value = json.get(key);
    if (value instanceof Boolean b)
      return b;
    if (value instanceof String s) {
      if ("true".equalsIgnoreCase(s.trim()))
        return true;
      if ("false".equalsIgnoreCase(s.trim()))
        return false;
    }
    throw new IllegalArgumentException("Index metadata '" + key + "' must be true or false, got: " + value);
  }

  private static Number metadataNumber(final JSONObject json, final String key, final String expected) {
    final Object value = json.get(key);
    if (value instanceof Number n)
      return n;
    if (value instanceof String s)
      try {
        return new BigDecimal(s.trim());
      } catch (final NumberFormatException e) {
        throw new IllegalArgumentException("Index metadata '" + key + "' must be " + expected + ", got: " + value, e);
      }
    throw new IllegalArgumentException("Index metadata '" + key + "' must be " + expected + ", got: " + value);
  }

  /**
   * Returns true if the property at the given index has case-insensitive collation.
   */
  public boolean isCaseInsensitive(final int propertyIndex) {
    return collations != null && propertyIndex < collations.size()
        && COLLATION_CI.equals(collations.get(propertyIndex));
  }

  /**
   * Returns true if any property in this index has case-insensitive collation.
   */
  public boolean hasAnyCaseInsensitive() {
    if (collations == null)
      return false;
    for (final String c : collations)
      if (COLLATION_CI.equals(c))
        return true;
    return false;
  }
}
