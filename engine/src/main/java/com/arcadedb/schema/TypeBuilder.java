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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TypeAlreadyExistsException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.utility.FileUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

/**
 * Builder class for schema types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeBuilder<T> {
  /** What {@link #create()} holds when nothing it does waits for the async workers to drain. */
  private static final AsyncQuiesce NOTHING_TO_QUIESCE = () -> {
  };

  // EVERY field below that describes the type must be rendered by toSQL(), which a replica sends to the leader in place of creating
  // the type itself (issue #10067): a field left out would make the leader create a different type than the caller asked for, with no
  // compile-time signal. Issue10067TypeBuilderToSQLTest fails when a field is added here without being listed there, and
  // Issue10146TypeBuilderSetterRoundTripTest when a setter is added without a case proving its statement makes the same type.
  final DatabaseInternal database;
  final Class<T>         type;
  boolean                 ignoreIfExists    = false;
  String                  typeName;
  List<LocalDocumentType> superTypes;
  List<Bucket>            bucketInstances   = Collections.emptyList();
  int                     buckets;
  boolean                 bucketsChosen     = false;
  int                     pageSize;
  boolean                 pageSizeChosen    = false;
  boolean                 edgeBidirectional = true;
  boolean                 edgeLightweight   = false;
  boolean                 edgeUnique        = false;

  protected TypeBuilder(final DatabaseInternal database, final Class<T> type) {
    this.database = database;
    this.type = type;
    this.buckets = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    this.pageSize = database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE);
  }

  public T create() {
    if (typeName == null || typeName.isEmpty())
      throw new IllegalArgumentException("Missing type");

    final LocalSchema schema = database.getSchema().getEmbedded();

    // INVARIANT: creating a type is a check-then-act (look it up, then add the missing buckets and super
    // types) and must be atomic, otherwise two callers racing on the same name both observe the type as
    // incomplete and both try to associate the same bucket, the loser failing with "the bucket is already
    // associated to the type". The lookup runs under the shared lock so a type being built by another
    // thread - which publishes itself into the schema's type map BEFORE its buckets are added - is never observed
    // half-populated; anything that mutates re-checks under the exclusive lock. The fast path can also throw,
    // for the type-already-exists and wrong-type cases, exactly as the locked path does.
    //
    // UPDATE_SCHEMA is demanded by createInternal(), at the point the builder is about to change the schema, and
    // not here: a getOrCreate on a type that already exists as requested changes nothing, so a user holding only
    // record grants can run a Cypher CREATE (whose label resolution is a getOrCreateVertexType) on a declared type
    // exactly as they can run the equivalent SQL INSERT (issue #7368).
    final T alreadyComplete = completeTypeIfPresent(schema);
    if (alreadyComplete != null)
      return alreadyComplete;

    // A replica running a statement in a transaction its client opened cannot create the type itself, but the leader can do it
    // for the statement: asked BEFORE the schema write lock is taken, because the replica has to apply the leader's schema entry
    // before the statement goes on, and that apply needs the same lock (issue #10067). Only a type that is missing altogether:
    // completing a half-built one changes a type that exists, which has no such form.
    // The lookup is not under the read lock, deliberately: it is a pre-check on a ConcurrentHashMap (the schema's type map is one,
    // published or staged), and a type created by someone else in between only makes the leader's statement a no-op (it is
    // rendered IF NOT EXISTS), after which the type is read under the lock below.
    if (!schema.typeMap().containsKey(typeName)) {
      // IF NOT EXISTS whatever this builder says: a create() that found the type already there is an error for its caller, but here
      // the type was missing when the call began and a racing creation by another client (the same new label, through another
      // replica) must leave it with the type, as it does on a node that creates types itself and finds it complete afterwards
      // A supplier: the string is built only by a database that is going to send it, never on an embedded one
      if (database.getWrappedDatabaseInstance().createMissingTypeOnTheLeader(typeName, () -> toSQL(true))) {
        // Not the "already exists" refusal of a create() that finds the type: the type did not exist when this call began, and
        // the leader made it on this call's behalf. Its buckets are not counted against this builder's: a count the caller chose
        // was rendered into the statement, and one it did not is the leader's own default, which may differ from this node's
        // (counting it would send a type the leader just made back to a local schema change this node cannot make)
        final T createdByTheLeader = database.executeInReadLock(() -> {
          final LocalDocumentType created = schema.typeMap().get(typeName);
          return created != null && hasEverySuperType(created) ? (T) checkIsOfTheRequestedKind(created) : null;
        });
        if (createdByTheLeader != null)
          return createdByTheLeader;
      }
    }

    // ONE SCHEMA PUBLICATION FOR WHATEVER THE BUILDER DOES (issue #10537), as the CREATE ... TYPE statement is (issue #10147): the
    // frame is the one createInternal() opens around the type, its buckets, its super types and the index of a UNIQUE edge type.
    // Under Raft every outermost schema frame ships its own SCHEMA_ENTRY, and a follower that counts the entries a transaction has
    // to tolerate reads each extra one as somebody else's schema change.
    //
    // The async quiescence is taken OUTSIDE the write lock, for what waits for the workers to drain (linking a super type that has
    // indexes, building the index of the constraint): the workers need that lock to commit their batch, so asked from inside it the
    // quiescence stalls until its timeout. It is reentrant, so a builder reached from inside a frame of the caller (the statement,
    // Labels.ensureCompositeType) rides on the one that caller took.
    //
    // Asked whenever a super type is linked or a unique edge is made, even when no super type has an index to propagate: deciding that
    // would be a read of every index of the hierarchy to save a park of the workers that is free without an executor and brief with one
    final T created;
    try (final AsyncQuiesce asyncPaused = (superTypes != null && !superTypes.isEmpty()) || (edgeUnique && !edgeLightweight) ?
        database.quiesceAsync() : NOTHING_TO_QUIESCE) {
      created = database.executeInWriteLock(() -> createInternal(schema));
    }

    // A type that was there already and is unique with no index behind it is healed here, after the frame, and is the only case that
    // gets this far with something to do: a type made above has had its index built inside its own frame, which this finds and leaves
    if (created instanceof LocalEdgeType edgeType)
      LocalEdgeType.applyUniqueConstraint(schema, edgeType);

    return created;
  }

  private T completeTypeIfPresent(final LocalSchema schema) {
    return database.executeInReadLock(() -> {
      final LocalDocumentType existing = schema.typeMap().get(typeName);
      return existing != null && isComplete(existing) ? (T) checkExistingIsCompatible(existing) : null;
    });
  }

  /**
   * The {@code CREATE ... TYPE} statement that makes the type this builder describes, for a database that has to ask another
   * node to create it (issue #10067); {@code null} when the description has no SQL form, which is the case of a type built on
   * bucket instances the caller already holds. The bucket count and the page size are rendered when the caller CHOSE them
   * ({@link #withTotalBuckets}, {@link #withPageSize}), even if the value equals this node's default, so the node that runs the
   * statement creates exactly what was asked; one the caller did not choose is left to that node's own default, as it would be for
   * a statement sent to it.
   */
  public String toSQL() {
    return toSQL(ignoreIfExists);
  }

  /**
   * As {@link #toSQL()}, with {@code IF NOT EXISTS} rendered or not whatever {@link #withIgnoreIfExists} said.
   */
  public String toSQL(final boolean ifNotExists) {
    if (typeName == null || typeName.isEmpty() || !bucketInstances.isEmpty())
      return null;

    final StringBuilder sql = new StringBuilder("CREATE ");
    if (type.equals(VertexType.class))
      sql.append("VERTEX");
    else if (type.equals(EdgeType.class))
      sql.append("EDGE");
    else
      sql.append("DOCUMENT");
    sql.append(" TYPE ").append(Identifier.quote(typeName));
    if (ifNotExists)
      sql.append(" IF NOT EXISTS");

    if (superTypes != null && !superTypes.isEmpty()) {
      sql.append(" EXTENDS ");
      for (int i = 0; i < superTypes.size(); i++) {
        if (i > 0)
          sql.append(", ");
        sql.append(Identifier.quote(superTypes.get(i).getName()));
      }
    }

    if (type.equals(EdgeType.class)) {
      if (!edgeBidirectional)
        sql.append(" UNIDIRECTIONAL");
      if (edgeLightweight)
        sql.append(" LIGHTWEIGHT");
      if (edgeUnique)
        sql.append(" UNIQUE");
    }

    if (bucketsChosen)
      sql.append(" BUCKETS ").append(buckets);
    if (pageSizeChosen)
      sql.append(" PAGESIZE ").append(pageSize);
    return sql.toString();
  }

  /**
   * A type needs no further work from this builder when it already carries at least the requested number
   * of buckets and every requested super type.
   */
  private boolean isComplete(final LocalDocumentType t) {
    return t.buckets.size() >= buckets && hasEverySuperType(t);
  }

  private boolean hasEverySuperType(final LocalDocumentType t) {
    if (superTypes != null)
      for (final LocalDocumentType sup : superTypes)
        if (!t.getSuperTypes().contains(sup))
          return false;
    return true;
  }

  private LocalDocumentType checkExistingIsCompatible(final LocalDocumentType t) {
    if (!ignoreIfExists)
      throw new TypeAlreadyExistsException(typeName);

    return checkIsOfTheRequestedKind(t);
  }

  private LocalDocumentType checkIsOfTheRequestedKind(final LocalDocumentType t) {
    if (!type.isAssignableFrom(t.getClass())) {
      final String expectedType = type.isAssignableFrom(VertexType.class) ?
          "vertex" :
          type.isAssignableFrom(EdgeType.class) ? "edge" : "document";
      throw new SchemaException("Type '" + typeName + "' is not a " + expectedType + " type");
    }

    return t;
  }

  private T createInternal(final LocalSchema schema) {
    final LocalDocumentType t = schema.typeMap().get(typeName);
    if (t != null) {
      checkExistingIsCompatible(t);

      // Re-checked under the exclusive lock: another thread may have completed the type since the shared-lock
      // fast path looked, and a type that needs nothing added is not a schema change.
      if (isComplete(t))
        return (T) t;

      database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

      final List<LocalDocumentType> toLink = new ArrayList<>();
      if (superTypes != null)
        for (final LocalDocumentType sup : superTypes)
          if (!t.getSuperTypes().contains(sup) && !toLink.contains(sup))
            toLink.add(sup);

      // Every super type is asked before the first one is linked or the first bucket is made, so a refusal of the last leaves the
      // type as it was (issue #10394). create() runs this under the database write lock, which the checks need
      for (final LocalDocumentType sup : toLink)
        t.checkCanAddSuperType(sup, true);

      // A bucket another type owns is refused by addBucket, but only after the ones before it are attached: the refused growth
      // would keep them (issue #10317)
      if (t.buckets.size() < buckets)
        checkAutoNamedBucketsAreFree(schema, t.buckets.size());

      // ONE FRAME for the growth and the links, as for a type made from nothing (issue #10537): every bucket created and every super
      // type linked used to publish a schema entry of its own
      return schema.recordFileChanges(() -> {
        if (t.buckets.size() < buckets) {
          // CREATE MISSING BUCKETS
          for (int i = t.buckets.size(); i < buckets; ++i) {
            final String bucketName = FileUtils.encode(typeName, schema.getEncoding()) + "_" + i;
            if (schema.existsBucket(bucketName)) {
              LogManager.instance().log(this, Level.WARNING, "Reusing found bucket '%s' for type '%s'", null, bucketName, typeName);
              t.addBucket(schema.getBucketByName(bucketName));
            } else
              // CREATE A NEW ONE
              t.addBucket(schema.createBucket(bucketName, pageSize));
          }
        }

        final List<LocalDocumentType> linked = new ArrayList<>(toLink.size());
        try {
          for (final LocalDocumentType sup : toLink) {
            t.addSuperType(sup);
            linked.add(sup);
          }
        } catch (final RuntimeException e) {
          // Each link is undone by addSuperType itself when it refuses, the ones before it are not: they are given back here, so the
          // type is not left under some of the super types it asked for (issue #10394)
          for (final LocalDocumentType sup : linked.reversed())
            try {
              t.removeSuperType(sup);
            } catch (final RuntimeException unlinkFailure) {
              // Logged as well as suppressed, as in the take-back: a type left under a super type it was refused is easy to miss
              e.addSuppressed(unlinkFailure);
              LogManager.instance().log(this, Level.WARNING, "Cannot unlink the super type '%s' from the type '%s' after its growth was refused: %s",
                  unlinkFailure, sup.getName(), typeName, unlinkFailure.getMessage());
            }
          throw e;
        }

        // The frame saves schema.json when it ends: what the links changed is written then, with the buckets
        if (!linked.isEmpty())
          schema.updateSecurity();

        return t;
      });
    }

    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (buckets > 32)
      throw new IllegalArgumentException("Cannot create " + buckets + " buckets: maximum is 32");

    if (typeName.contains(","))
      throw new IllegalArgumentException("Type name '" + typeName + "' contains non valid characters");

    return schema.recordFileChanges(() -> {
      // BEFORE ANYTHING IS CREATED: the type is registered in the schema, because the buckets and the indexes that follow find it
      // there, and its super types are linked after that. Every refusal the super types can already give is asked first, or the
      // refused create would leave a half-built type behind (issues #9846, #10394)
      if (superTypes != null)
        for (final LocalDocumentType sup : superTypes)
          LocalDocumentType.checkSuperTypeOfNewType(typeName, sup, edgeLightweight);

      // The same for a bucket another type owns, or one listed twice: addBucket refuses it, but only after the type is registered,
      // which left the type behind with the buckets before it (issue #10317)
      if (bucketInstances.isEmpty())
        checkAutoNamedBucketsAreFree(schema, 0);
      else
        checkBucketInstancesAreFree(schema);

      final LocalDocumentType c;
      if (type.equals(VertexType.class))
        c = new LocalVertexType(schema, typeName);
      else if (type.equals(EdgeType.class))
        c = new LocalEdgeType(schema, typeName, edgeBidirectional, edgeLightweight, edgeUnique);
      else {
        c = new LocalDocumentType(schema, typeName);

        // CREATE ENTRY IN DICTIONARY IF NEEDED. THIS IS USED BY EMBEDDED DOCUMENT WHERE THE DICTIONARY ID IS SAVED
        schema.getDictionary().getIdByName(typeName, true);
      }

      schema.typeMap().put(typeName, c);

      // The buckets the type did not make: the ones it found under its own names and the ones it was given. They were nobody's
      // before it, and what the take-back below leaves alone
      final List<Bucket> notMadeByTheType = new ArrayList<>();
      try {
        if (bucketInstances.isEmpty()) {
          for (int i = 0; i < buckets; ++i) {
            final String bucketName = FileUtils.encode(typeName, schema.getEncoding()) + "_" + i;
            if (schema.existsBucket(bucketName)) {
              LogManager.instance().log(this, Level.WARNING, "Reusing found bucket '%s' for type '%s'", null, bucketName, typeName);
              final Bucket found = schema.getBucketByName(bucketName);
              notMadeByTheType.add(found);
              c.addBucket(found);
            } else
              // CREATE A NEW ONE
              c.addBucket(schema.createBucket(bucketName, pageSize));
          }
        } else {
          notMadeByTheType.addAll(bucketInstances);
          for (final Bucket bucket : bucketInstances)
            c.addBucket(bucket);
        }

        if (superTypes != null)
          for (final LocalDocumentType sup : superTypes)
            c.addSuperType(sup);

        // The index of a UNIQUE edge type, in the same frame as the type it constrains (issue #10537): the type is published into the
        // type map above, which is all the index needs of it, and a refusal takes the type back like a refused super type link
        if (c instanceof LocalEdgeType edgeType)
          LocalEdgeType.applyUniqueConstraint(schema, edgeType);

        schema.updateSecurity();
      } catch (final RuntimeException e) {
        takeBack(schema, c, notMadeByTheType, e);
        throw e;
      }

      return c;
    });
  }

  /**
   * Takes back a type {@link #createInternal} registered and could not finish, so that a refused create leaves the schema as it found
   * it (issue #10394). What can refuse once the type is registered is the linkage of its super types: a paired external-property
   * bucket another type owns, an index that cannot be propagated, a partition the subtype cannot take. Asking those beforehand
   * would be a copy of the code that decides them and would still miss an I/O failure, so this is the net under the checks that
   * are asked beforehand.
   * <p>
   * A compensating drop inside the same schema write lock, as {@code CreateTypeAbstractStatement} does for the statement that links
   * its super types after the create: the type leaves the type map, takes its super type links, its sub-indexes and the buckets it made
   * with it, and the buckets it found or was given are detached first so they stay where they were. A failure of the take-back itself
   * is added to the refusal as suppressed, which stays the exception the caller sees.
   */
  private void takeBack(final LocalSchema schema, final LocalDocumentType created, final List<Bucket> notMadeByTheType,
      final RuntimeException refusal) {
    try {
      for (final Bucket bucket : notMadeByTheType)
        if (created.hasBucket(bucket.getName()))
          created.removeBucket(bucket);
      schema.dropType(typeName);
    } catch (final RuntimeException takeBackFailure) {
      // Added to the refusal, which stays what the caller sees, and logged: a suppressed exception is easy to miss, and a type that
      // could not be taken back is still registered
      refusal.addSuppressed(takeBackFailure);
      LogManager.instance().log(this, Level.WARNING, "Cannot take back the type '%s' after its creation was refused (%s): %s",
          takeBackFailure, typeName, refusal.getMessage(), takeBackFailure.getMessage());
    }
  }

  /**
   * Refuses, before the schema is touched, a {@code <type>_<n>} bucket from {@code firstBucket} on that exists and belongs to a
   * type: the type would reuse it ("Reusing found bucket") and {@code addBucket} would refuse it.
   */
  private void checkAutoNamedBucketsAreFree(final LocalSchema schema, final int firstBucket) {
    for (int i = firstBucket; i < buckets; ++i) {
      final String bucketName = FileUtils.encode(typeName, schema.getEncoding()) + "_" + i;
      if (schema.existsBucket(bucketName))
        schema.checkBucketIsFree(typeName, schema.getBucketByName(bucketName));
    }
  }

  /**
   * Refuses, before the schema is touched, a bucket passed with {@link #withBuckets} that belongs to a type, or that is passed
   * twice: the second one is already the new type's by the time {@code addBucket} looks at it. The lists are a handful of
   * buckets, so the pairs are compared in place rather than collected in a set.
   */
  private void checkBucketInstancesAreFree(final LocalSchema schema) {
    for (int i = 0; i < bucketInstances.size(); ++i) {
      final Bucket bucket = bucketInstances.get(i);
      schema.checkBucketIsFree(typeName, bucket);
      for (int j = 0; j < i; ++j)
        if (bucketInstances.get(j).getFileId() == bucket.getFileId())
          throw new SchemaException(
              "Cannot add the bucket '" + bucket.getName() + "' to the type '" + typeName + "' twice: it is listed more than once");
    }
  }

  public TypeBuilder<T> withName(final String typeName) {
    this.typeName = typeName;
    return this;
  }

  public TypeBuilder<T> withSuperType(final String superType) {
    if (superTypes == null)
      superTypes = new ArrayList<>();
    superTypes.add((LocalDocumentType) database.getSchema().getType(superType));
    return this;
  }

  public TypeBuilder<T> withIgnoreIfExists(final boolean ignoreIfExists) {
    this.ignoreIfExists = ignoreIfExists;
    return this;
  }

  /**
   * Sets to zero to have a type that can only be embedded or abstract (no records of this direct type, but subtypes can)
   *
   * @param buckets Number of buckets to use. By default the {@link GlobalConfiguration:TYPE_DEFAULT_BUCKETS} configuration is used.
   */
  public TypeBuilder<T> withTotalBuckets(final int buckets) {
    this.buckets = buckets;
    this.bucketsChosen = true;
    return this;
  }

  public TypeBuilder<T> withBuckets(final List<Bucket> bucketInstances) {
    this.bucketInstances = bucketInstances;
    this.buckets = 0;
    // CHOSEN: the caller asked for no buckets of the builder's own, so an empty list renders BUCKETS 0 and the node that runs the
    // statement does not give the type its default count (issue #10146). A non-empty list has no SQL form at all
    this.bucketsChosen = true;
    return this;
  }

  public TypeBuilder<T> withBidirectional(final boolean edgeBidirectional) {
    if (!type.isAssignableFrom(EdgeType.class))
      throw new UnsupportedOperationException("withBidirectional() on non edge type");
    this.edgeBidirectional = edgeBidirectional;
    return this;
  }

  /**
   * Declares every edge of this type lightweight: no edge record, therefore no properties. See
   * {@link EdgeType#isLightweight()}.
   */
  public TypeBuilder<T> withLightweight(final boolean edgeLightweight) {
    if (!type.isAssignableFrom(EdgeType.class))
      throw new UnsupportedOperationException("withLightweight() on non edge type");
    this.edgeLightweight = edgeLightweight;
    return this;
  }

  /**
   * Declares that at most one edge of this type may connect a given ordered pair of vertices. See
   * {@link EdgeType#isUnique()} for the cost, which differs between lightweight and regular types.
   */
  public TypeBuilder<T> withUnique(final boolean edgeUnique) {
    if (!type.isAssignableFrom(EdgeType.class))
      throw new UnsupportedOperationException("withUnique() on non edge type");
    this.edgeUnique = edgeUnique;
    return this;
  }

  public TypeBuilder<T> withPageSize(final int pageSize) {
    this.pageSize = pageSize;
    this.pageSizeChosen = true;
    return this;
  }
}
