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
package com.arcadedb.database.bucketselectionstrategy;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Select the bucket using a partition algorithm computed as the hashed value of the properties values. This allows to predetermine in which bucket is contained
 * a key(s) and therefore a document. There are some limitations on using this implementation: (1) field identified as partition key cannot be modified. (This
 * could be solved in the future by removing and recreating the document in a different bucket. If the record is part of a graph, then the edges will be updated
 * accordingly.)
 * <p>
 * <b>The two sides must hash the same object.</b> Placement ({@link #getBucketIdByRecord}) hashes the value the schema
 * coerced and stored; a lookup ({@link #getBucketIdByKeys}) is handed the caller's raw key. Since {@code hashCode()} is
 * type-dependent, the lookup side normalises its key to the declared property type before hashing (issue #5595), and
 * declines to prune - answering -1, which callers read as "search every bucket" - whenever it cannot reproduce the
 * stored form: an undeclared property, a value that will not coerce, or a case-insensitive partition index, whose two
 * spellings are one index key but were placed in two different buckets. Placement itself is never altered, so no
 * existing database needs a repartition.
 *
 * @author Luca Garulli
 */
public class PartitionedBucketSelectionStrategy extends RoundRobinBucketSelectionStrategy {
  /**
   * Sentinel for "this value's stored form cannot be reproduced", which forces the lookup to fan out. A distinct
   * object rather than {@code null}, which is a legitimate conversion result.
   */
  private static final Object UNKNOWN_STORED_FORM = new Object();

  private       LocalDocumentType type;
  private final List<String>      propertyNames;

  public PartitionedBucketSelectionStrategy(final List<String> propertyNames) {
    this.propertyNames = Collections.unmodifiableList(propertyNames);
  }

  public PartitionedBucketSelectionStrategy(final JSONObject json) {
    final JSONArray array = json.getJSONArray("properties");
    final List<String> pn = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      pn.add(array.getString(i));
    this.propertyNames = Collections.unmodifiableList(pn);
  }

  @Override
  public BucketSelectionStrategy copy() {
    final PartitionedBucketSelectionStrategy copy = new PartitionedBucketSelectionStrategy(propertyNames);
    copy.total = total;
    copy.type = type;
    return copy;
  }

  @Override
  public void setType(final LocalDocumentType type) {
    super.setType(type);
    this.type = type;

    final TypeIndex index = type.getPolymorphicIndexByProperties(propertyNames);
    if (index == null || !index.isAutomatic() || !index.isUnique())
      throw new IllegalArgumentException("Cannot find a unique index on properties " + propertyNames);
  }

  @Override
  public int getBucketIdByRecord(final Document record, final boolean async) {
    if (propertyNames != null) {
      final DocumentType documentType = record.getType();
      if (!this.type.equals(documentType))
        throw new IllegalArgumentException(
            "Record of type '" + documentType.getName() + "' is not supported by partitioned bucket selection strategy built on type '" + type.getName() + "'");

      int hash = 0;
      for (int i = 0; i < propertyNames.size(); i++) {
        final Object value = record.get(propertyNames.get(i));
        if (value != null)
          hash += value.hashCode();
      }
      return (hash & 0x7fffffff) % total;
    }

    return super.getBucketIdByRecord(record, async);
  }

  @Override
  public int getBucketIdByKeys(final List<String> lookupProperties, final Object[] keyValues, final boolean async) {
    // A record is placed by hashing THIS strategy's properties (see getBucketIdByRecord), so hashing the lookup
    // key only reaches the same bucket when the lookup covers exactly those properties. Anything else - another
    // index of the same type, or a partial key on a composite partition - hashes a different value set and would
    // point at an unrelated bucket, silently missing the record (issue #5589). Decline and let the caller fan out.
    if (!coversPartitionProperties(lookupProperties, keyValues))
      return -1;

    // A COLLATE CI partition index folds two spellings into one key, but placement hashed the spelling the writer
    // used, so 'Hello' and 'hello' are one index entry living in two different buckets. Unlike the boxed-type case
    // below there is no lookup-side normalisation that repairs this - only placement itself could, and changing
    // placement would force every existing partitioned database through a repartition. Never prune instead.
    if (partitionKeyIsCaseInsensitive())
      return -1;

    int hash = 0;
    for (int i = 0; i < keyValues.length; i++) {
      final Object value = keyValues[i];
      if (value == null)
        continue;

      // Placement hashed the value AFTER the schema coerced it to the declared type; the caller's key has had no
      // such treatment (TypeIndex hands the raw keys over, and the index's own convertKeys runs much later). Since
      // hashCode is type-dependent - Long.hashCode(v) is (int) (v ^ (v >>> 32)) while Integer.hashCode(v) is v -
      // the numerically identical key boxed differently used to hash to a different bucket and miss the record
      // (issue #5595). Replay the write-path coercion here so both sides hash the same object.
      final Object storedForm = toStoredForm(lookupProperties.get(i), value);
      if (storedForm == UNKNOWN_STORED_FORM)
        return -1;

      hash += storedForm.hashCode();
    }
    return (hash & 0x7fffffff) % total;
  }

  /**
   * Whether the index backing this partition folds case on any of its properties, in which case the bucket a record
   * was placed in is not derivable from a lookup key at all.
   * <p>
   * Resolved on every call rather than remembered: an index can be dropped and recreated with a different collation
   * without the strategy being re-bound, so a cached answer could outlive the schema it was read from. The cost is a
   * map lookup keyed on the property-name list, whose hash the JDK caches per String, which is the same order of
   * magnitude as the per-key property lookup the hashing loop already does.
   * <p>
   * No index, or no metadata on it, means nothing declares a collation and therefore nothing folds case - answer
   * "case-sensitive" and let the normal hashing proceed. A partitioned type can legitimately outlive its index: the
   * unique index is mandated when the strategy is assigned but never re-checked afterwards.
   */
  private boolean partitionKeyIsCaseInsensitive() {
    final TypeIndex index = type.getPolymorphicIndexByProperties(propertyNames);
    final IndexMetadata metadata = index != null ? index.getMetadata() : null;
    return metadata != null && metadata.hasAnyCaseInsensitive();
  }

  /**
   * Returns {@code value} in the form {@link #getBucketIdByRecord} would have hashed it, or
   * {@link #UNKNOWN_STORED_FORM} when that form is not derivable.
   * <p>
   * The stored form is the one {@code MutableDocument.convertValueToSchemaType} produces, so the conversion target
   * comes from the SCHEMA property and not from the index key types: a case-insensitive index lowercases its keys
   * and a string index stores them as {@code byte[]}, neither of which placement ever applied.
   * <p>
   * An undeclared property has no conversion target - the record kept whatever Java type the writer used - so the
   * two sides cannot be reconciled and this declines. That costs a fan-out, which is correct, only slower.
   */
  private Object toStoredForm(final String propertyName, final Object value) {
    final Property property = type.getPolymorphicPropertyIfExists(propertyName);
    if (property == null)
      return UNKNOWN_STORED_FORM;

    final Database database = type.getSchema().getEmbedded().getDatabase();
    try {
      final Object converted = Type.convert(database, value, property.getType().getJavaImplementation(database), property);
      return converted != null ? converted : UNKNOWN_STORED_FORM;
    } catch (final Exception e) {
      // A key that cannot be coerced to the declared type cannot match any stored value either, but answering
      // "bucket N" on a guess would be wrong: let the caller fan out and have the index itself reject the key.
      return UNKNOWN_STORED_FORM;
    }
  }

  /**
   * Whether {@code lookupProperties} is exactly this strategy's partition property set, with one key value each.
   * <p>
   * Order is deliberately NOT required: the hash both sides compute is a SUM over the per-value hash codes, which
   * is commutative, so a permutation of the same properties reaches the same bucket.
   * <p>
   * The comparison is multiset equality, not "every lookup property is also a partition property". The weaker test
   * would accept a lookup on {@code [a, a]} against a partition of {@code [a, b]}, which sums a different pair of
   * values than placement did and would prune to the wrong bucket. No index declares a repeated property today, so
   * this is unreachable - but this method is the guard the whole fix rests on, so it enforces the invariant instead
   * of assuming callers uphold it. Counting with nested scans rather than a Set keeps it allocation-free: partition
   * keys hold one to three properties, and this runs on a per-query path.
   */
  private boolean coversPartitionProperties(final List<String> lookupProperties, final Object[] keyValues) {
    if (lookupProperties == null)
      // THE CALLER COULD NOT SAY WHICH PROPERTIES THE KEYS BELONG TO: UNVERIFIABLE, SO NOT A MATCH
      return false;

    final int size = propertyNames.size();
    if (lookupProperties.size() != size || keyValues.length != size)
      return false;

    for (int i = 0; i < size; i++) {
      final String partitionProperty = propertyNames.get(i);
      if (occurrencesOf(partitionProperty, lookupProperties) != occurrencesOf(partitionProperty, propertyNames))
        return false;
    }

    return true;
  }

  private static int occurrencesOf(final String property, final List<String> properties) {
    int occurrences = 0;
    for (int i = 0; i < properties.size(); i++)
      if (properties.get(i).equals(property))
        ++occurrences;
    return occurrences;
  }

  @Override
  public String getName() {
    return "partitioned";
  }

  public List<String> getProperties() {
    return propertyNames;
  }

  @Override
  public JSONObject toJSON() {
    return new JSONObject().put("name", getName()).put("properties", new JSONArray(propertyNames));
  }
}
