package com.arcadedb.schema;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
   */
  protected void applyUserMetadata(final JSONObject json) {
    // no user-facing setting on a plain index
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
