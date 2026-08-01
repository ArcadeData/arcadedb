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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

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
 * stored form. Placement itself is never altered, so no existing database needs a repartition.
 * <p>
 * <b>The bucket must be a function of the index key.</b> Pruning is only half of what the modulus buys: a UNIQUE index
 * on a partitioned type is a set of per-bucket sub-indexes, so the constraint is global only while every record
 * carrying one index key lands in one bucket. Where the hash disagrees with the way the index decides two keys are
 * equal, the same key lives in several buckets and each of them accepts its own copy - a UNIQUE index that silently
 * holds duplicates. Measured on {@code BINARY} (a {@code byte[]} hashes by identity), {@code DECIMAL} ({@code 1.1} and
 * {@code 1.10} are one index key but two hash codes) and a {@code DATE}/{@code DATETIME} key read back through a
 * zone-carrying implementation (the writer's zone is hashed, only the instant is stored). Those partitions are
 * therefore never pruned either, which restores the constraint by making the check fan out; see
 * {@link #checkSuitability()}, which the schema layer uses to refuse such a configuration outright (issue #5603).
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

  /**
   * Binds the strategy to its type. Binding only: it must never throw.
   * <p>
   * This runs on every rebind, not just when the user asks for the strategy - {@code addIndexInternal} calls it once
   * per bucket index, {@code addBucketInternal} on every bucket added, and the schema loader on every open - so a
   * validation failure here is a failure of whatever operation happened to touch the type. It used to demand the
   * unique automatic index on the partition properties, which made a database whose partition index had been dropped
   * unopenable: the loader binds the persisted strategy inside its own try/catch, so the throw aborted the rest of
   * the load - triggers, function libraries, extensions and the compaction file-migration map - and the schema was
   * reported as "reset" (issue #5637). That requirement now lives in {@link #checkSuitability()}, which the schema
   * layer refuses on at assignment time and only warns about when it reads an existing database back.
   */
  @Override
  public void setType(final LocalDocumentType type) {
    super.setType(type);
    this.type = type;
  }

  /**
   * A verdict on the partition configuration, split by what it costs.
   * <p>
   * A {@code blocker} is a state the strategy must not be assigned into, because the type pays the strategy's
   * constraints - a partition key that must not be updated, an uneven bucket fill - and gets nothing usable back:
   * either {@link #getBucketIdByKeys} can never answer anything but -1, or the unique automatic index those pruned
   * lookups would go through is not there at all. Assigning the strategy into such a state is refused; finding an
   * existing database in one is only warned about, so it still opens.
   * <p>
   * A {@code warning} is a configuration that prunes, just less than the wording suggests. Today that is a second
   * index on properties the partition does not cover: those lookups cannot be pruned (issue #5589) and fan out
   * across every bucket, which is correct but no faster than not partitioning at all.
   */
  public record Suitability(List<String> blockers, List<String> warnings) {
    public boolean isUsable() {
      return blockers.isEmpty();
    }
  }

  /**
   * Diagnoses the partition configuration for the schema layer. Cold path only - called when the strategy is assigned
   * and when a database is reopened, never per query, so it is free to allocate the messages it reports.
   * <p>
   * Most of the rules are the ones {@link #getBucketIdByKeys} enforces silently, said out loud: a configuration
   * reported below as a hashing blocker is exactly one in which that method returns -1 for every key it will ever be
   * handed. The first blocker is the odd one out - a missing partition index does not stop the strategy from
   * computing a bucket, it removes the index lookup that would have been pruned by it - and it is grouped here
   * because it draws the same reaction: refuse the assignment, warn about a database already in that state.
   */
  public Suitability checkSuitability() {
    final Database database = type.getSchema().getEmbedded().getDatabase();
    final List<String> blockers = new ArrayList<>();
    final List<String> warnings = new ArrayList<>();

    final TypeIndex partitionIndex = type.getPolymorphicIndexByProperties(propertyNames);
    if (partitionIndex == null || !partitionIndex.isAutomatic() || !partitionIndex.isUnique())
      blockers.add("cannot find a unique automatic index on the partition properties " + propertyNames
          + ", which is what a pruned lookup would go through");

    for (final String propertyName : propertyNames) {
      final Property property = type.getPolymorphicPropertyIfExists(propertyName);
      if (property == null)
        blockers.add("partition property '" + propertyName + "' is not declared in the schema, so a record keeps "
            + "whatever Java type its writer happened to use and no lookup key can be normalised to match it");
      else if (!hashAgreesWithIndexKeyIdentity(database, property.getType()))
        blockers.add("partition property '" + propertyName + "' is declared " + property.getType()
            + ", whose stored form does not hash the way the index compares keys, so one key would be spread over "
            + "several buckets and a UNIQUE index on it would admit duplicates");
    }

    if (partitionKeyIsCaseInsensitive())
      blockers.add("the index on " + propertyNames + " is declared COLLATE CI, and case folding is an index-level "
          + "normalisation that placement never applies, so the two spellings of one key sit in two buckets");

    for (final TypeIndex index : type.getAllIndexes(true))
      if (!coversPartitionProperties(index.getPropertyNames()))
        warnings.add("index '" + index.getName() + "' is on " + index.getPropertyNames() + " rather than the "
            + "partition properties " + propertyNames + ", so lookups through it cannot be pruned to one bucket and "
            + "fan out across all of them");

    return new Suitability(Collections.unmodifiableList(blockers), Collections.unmodifiableList(warnings));
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

    // RESOLVED ONCE: THE SAME DATABASE BACKS EVERY KEY OF THE LOOKUP
    final Database database = type.getSchema().getEmbedded().getDatabase();

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
      final Object storedForm = toStoredForm(database, lookupProperties.get(i), value);
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
  private Object toStoredForm(final Database database, final String propertyName, final Object value) {
    final Property property = type.getPolymorphicPropertyIfExists(propertyName);
    if (property == null)
      return UNKNOWN_STORED_FORM;

    // Reproducing the stored form is not enough on its own: the hash of that form has to agree with the way the
    // index decides two keys are equal, or one key spans several buckets. See hashAgreesWithIndexKeyIdentity.
    if (!hashAgreesWithIndexKeyIdentity(database, property.getType()))
      return UNKNOWN_STORED_FORM;

    try {
      final Object converted = Type.convert(database, value, property.getType().getJavaImplementation(database), property);
      return converted != null ? converted : UNKNOWN_STORED_FORM;
    } catch (final Exception e) {
      // A key that cannot be coerced to the declared type cannot match any stored value either, but answering
      // "bucket N" on a guess would be wrong: let the caller fan out and have the index itself reject the key.
      // The catch stays broad so no conversion failure can ever turn a lookup into a wrong answer, but it is logged
      // so an unrelated bug surfacing here degrades visibly instead of silently costing every query a fan-out.
      LogManager.instance().log(this, Level.FINE,
          "Cannot reproduce the stored form of the partition key '%s' on type '%s': searching every bucket", e,
          propertyName, type.getName());
      return UNKNOWN_STORED_FORM;
    }
  }

  /**
   * Whether {@code Object.hashCode()} on the stored form of a {@code propertyType} value is a faithful stand-in for
   * the way the index decides two keys are equal. Only then does one index key map to exactly one bucket, which is
   * what both the pruning and - more importantly - the global reach of a UNIQUE constraint rest on.
   * <p>
   * Deliberately an ALLOW-list. A type that is not named here declines to prune, so a {@code Type} added later is
   * merely slower until someone confirms it belongs, rather than silently placing one key in several buckets.
   * <p>
   * The exclusions, each measured against a round-robin control that rejects the duplicate every time:
   * <ul>
   *   <li>{@code BINARY} - the stored form is a {@code byte[]}, which inherits identity {@code hashCode}, so every
   *       single write of the same bytes draws a fresh bucket.</li>
   *   <li>{@code DECIMAL} - {@code BigDecimal.hashCode} folds in the scale, so {@code 1.1} and {@code 1.10} hash
   *       apart while the index compares them equal.</li>
   *   <li>{@code LIST}, {@code MAP}, {@code EMBEDDED} and the {@code ARRAY_OF_*} family - structurally the same
   *       identity-hash problem as {@code BINARY} (and not indexable today, so this is a guard, not a fix).</li>
   *   <li>{@code DATE}/{@code DATETIME*} read back as a {@link Calendar}, {@link ZonedDateTime} or
   *       {@link OffsetDateTime} - only the instant reaches disk, so the writer's zone is hashed at placement and
   *       lost on the way back. Two records for the same instant written from different zones are one index key in
   *       two buckets. The zone-free implementations ({@code java.util.Date}, {@code Instant}, {@code LocalDate},
   *       {@code LocalDateTime}) round-trip unchanged and stay prunable.</li>
   * </ul>
   */
  private static boolean hashAgreesWithIndexKeyIdentity(final Database database, final Type propertyType) {
    return switch (propertyType) {
      case BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, STRING, LINK -> true;
      case DATE, DATETIME, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> isZoneFree(readBackClass(database, propertyType));
      default -> false;
    };
  }

  /**
   * The class the binary deserializer will hand a temporal property back as, which is what decides whether the zone a
   * record was placed under survives a round trip.
   * <p>
   * Asked of the serializer directly rather than through {@link Type#getJavaImplementation}, which resolves the
   * configured implementation for {@code DATE} and {@code DATETIME} only and answers the static default
   * ({@code LocalDateTime}) for the three precision subtypes. {@code BinarySerializer.deserializeValue} passes the
   * configured {@code dateTimeImplementation} for all four datetime binary types, so reading the subtypes through that
   * helper would report every one of them zone-free and admit exactly the configuration this guard exists to catch -
   * measured: a {@code DATETIME_NANOS} partition key under {@code ZonedDateTime} let 3 of 6 writes of a single instant
   * into a UNIQUE index.
   * <p>
   * {@code getJavaImplementation} is left alone deliberately. Its other caller is the write path's
   * {@code convertValueToSchemaType}, where the same mismatch is inert - {@code Type.convert} returns an
   * already-temporal value untouched, so the conversion target is never consulted for one - and changing it would
   * alter the class a subtype property converts a String or a Long to, a wider behaviour change than this guard needs.
   */
  private static Class<?> readBackClass(final Database database, final Type propertyType) {
    if (!(database instanceof DatabaseInternal internal))
      return propertyType.getJavaImplementation(database);

    return propertyType == Type.DATE ?
        internal.getSerializer().getDateImplementation() :
        internal.getSerializer().getDateTimeImplementation();
  }

  /**
   * Whether a temporal implementation carries nothing beyond the instant that reaches disk, so that a value hashes
   * the same before and after a round trip.
   * <p>
   * An allow-list, like the {@code Type} switch above and for the same reason: a deny-list of the zone-carrying
   * classes would silently pass any implementation nobody thought to name - including one configured by a user -
   * which is exactly the failure this guard exists to prevent. These four are the zone-free half of what
   * {@code DateUtils.dateTime}/{@code DateUtils.date} can construct; the rest ({@link Calendar},
   * {@link ZonedDateTime}, {@link OffsetDateTime}) carry a zone that placement hashes and the deserializer cannot
   * give back.
   */
  private static boolean isZoneFree(final Class<?> implementation) {
    return implementation == Date.class || implementation == Instant.class || implementation == LocalDate.class
        || implementation == LocalDateTime.class;
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
    return keyValues.length == propertyNames.size() && coversPartitionProperties(lookupProperties);
  }

  /**
   * The property-set half of {@link #coversPartitionProperties(List, Object[])}, without the arity check on the key
   * values. Also used to tell a type's own partition index apart from its other indexes, which is the same question:
   * does this property set hash to the bucket placement chose?
   */
  private boolean coversPartitionProperties(final List<String> lookupProperties) {
    if (lookupProperties == null)
      // THE CALLER COULD NOT SAY WHICH PROPERTIES THE KEYS BELONG TO: UNVERIFIABLE, SO NOT A MATCH
      return false;

    final int size = propertyNames.size();
    if (lookupProperties.size() != size)
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
