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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema regression tests: property definition rules.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class PropertyDefinitionRulesTest {
  /**
   * Regression for #7777: mutable singleton collection.
   *
   * Regression test for issue #7777: {@link Type#convert} wrapped a non-collection value bound for a {@code List}, {@code Set} or
   * {@code Collection} target in an immutable JDK singleton ({@code List.of}/{@code Set.of}). A scalar written to a property
   * declared {@code LIST} was therefore stored as an immutable list, so the second
   * {@link MutableDocument#newEmbeddedDocument(String, String)} on that property - which appends to the existing collection, as
   * its javadoc documents - threw {@link UnsupportedOperationException}.
   */
  @Nested
  class MutableSingletonCollectionTest extends TestHelper {

    @Test
    void convertScalarToListReturnsAMutableList() {
      final Object converted = Type.convert(database, "a", List.class);
      assertThat(converted).isInstanceOf(List.class);

      final List<Object> list = (List<Object>) converted;
      list.add("b");
      assertThat(list).containsExactly("a", "b");
    }

    @Test
    void convertScalarToSetReturnsAMutableSet() {
      final Object converted = Type.convert(database, "a", Set.class);
      assertThat(converted).isInstanceOf(Set.class);

      final Set<Object> set = (Set<Object>) converted;
      set.add("b");
      assertThat(set).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void convertScalarToCollectionReturnsAMutableCollection() {
      final Collection<Object> collection = (Collection<Object>) Type.convert(database, "a", Collection.class);
      collection.add("b");
      assertThat(collection).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void getListOfAScalarPropertyReturnsAMutableList() {
      database.getSchema().createDocumentType("Doc7777GetList");

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc7777GetList").set("items", "a");
        final List<Object> list = doc.getList("items");
        list.add("b");
        assertThat(list).containsExactly("a", "b");
      });
    }

    @Test
    void scalarSetOnDeclaredListIsStoredAsAMutableList() {
      database.getSchema().createDocumentType("Doc7777Set").createProperty("items", Type.LIST);

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc7777Set").set("items", "a");
        final List<Object> items = (List<Object>) doc.get("items");
        items.add("b");
        assertThat(items).containsExactly("a", "b");
      });
    }

    @Test
    void twoEmbeddedDocumentsOnADeclaredListProperty() {
      database.getSchema().createDocumentType("Item7777").createProperty("n", Type.INTEGER);
      database.getSchema().createDocumentType("Doc7777Emb").createProperty("items", Type.LIST);

      final RID[] rid = new RID[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc7777Emb");
        doc.newEmbeddedDocument("Item7777", "items").set("n", 1);
        doc.newEmbeddedDocument("Item7777", "items").set("n", 2);

        final List<EmbeddedDocument> items = doc.getList("items");
        assertThat(items).hasSize(2);
        doc.save();
        rid[0] = doc.getIdentity();
      });

      final Document reloaded = rid[0].asDocument();
      final List<EmbeddedDocument> items = reloaded.getList("items");
      assertThat(items).hasSize(2);
      assertThat(items.get(0).getInteger("n")).isEqualTo(1);
      assertThat(items.get(1).getInteger("n")).isEqualTo(2);
    }

    @Test
    void embeddedDocumentAppendedToACallerSuppliedImmutableList() {
      // A List value is assignable to a LIST property, so Type.convert() hands the caller's own immutable list through
      // unchanged; newEmbeddedDocument() must still be able to append to it.
      database.getSchema().createDocumentType("Item7777b").createProperty("n", Type.INTEGER);
      database.getSchema().createDocumentType("Doc7777Imm").createProperty("items", Type.LIST);

      final RID[] rid = new RID[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc7777Imm").set("items", List.of("first"));
        doc.newEmbeddedDocument("Item7777b", "items").set("n", 1);

        final List<Object> items = doc.getList("items");
        assertThat(items).hasSize(2);
        assertThat(items.get(0)).isEqualTo("first");
        assertThat(items.get(1)).isInstanceOf(EmbeddedDocument.class);
        doc.save();
        rid[0] = doc.getIdentity();
      });

      final List<Object> items = rid[0].asDocument().getList("items");
      assertThat(items).hasSize(2);
      assertThat(((EmbeddedDocument) items.get(1)).getInteger("n")).isEqualTo(1);
    }

    @Test
    void embeddedDocumentAppendedToACallerSuppliedImmutableSet() {
      // The defensive copy keeps a Set a Set, so the existing elements are neither duplicated nor reordered.
      database.getSchema().createDocumentType("Item7777c").createProperty("n", Type.INTEGER);
      database.getSchema().createDocumentType("Doc7777ImmSet");

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc7777ImmSet").set("items", Set.of("first"));
        final MutableEmbeddedDocument emb = doc.newEmbeddedDocument("Item7777c", "items");
        emb.set("n", 1);

        final Object items = doc.get("items");
        assertThat(items).isInstanceOf(Set.class);
        assertThat((Set<Object>) items).containsExactly("first", emb);
      });
    }
  }

  /**
   * Regression for #7918: create property validation under write lock.
   *
   * Regression for issue #7918, the mirror image of #7672: {@code createProperty} ran every one of its checks OUTSIDE
   * {@code recordFileChanges} - i.e. outside the database write lock - and only {@code properties.put} inside, while
   * the sibling {@code dropProperty} in the same class had just been fixed to do the opposite and documents why.
   * <p>
   * The check that matters is {@code getPolymorphicPropertyNames().contains(propertyName)}, which recurses over
   * {@code superTypes}: a plain {@code ArrayList} that {@code linkSuperType}/{@code unlinkSuperType} structurally
   * modify under that same write lock. So an unlocked walk can land in a {@code ConcurrentModificationException} out
   * of an unrelated {@code CREATE PROPERTY}, and a check that passes outside the lock can be invalidated by a
   * concurrent {@code CREATE TYPE ... EXTENDS} before the put applies - leaving the subtype shadowing a super type's
   * property, which is exactly what the upward walk exists to prevent.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CreatePropertyValidationUnderWriteLockTest extends TestHelper {

    private static final String PARENT = "Issue7918Parent";
    private static final String CHILD  = "Issue7918Child";

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(PARENT).createProperty("code", Type.STRING);
      database.getSchema().createDocumentType(CHILD).addSuperType(PARENT);
    }

    /**
     * The observable consequence of the fix, and the only one a test can pin deterministically: with the database
     * write lock held elsewhere, {@code createProperty} cannot reach its verdict at all. Before the fix it answered
     * immediately, which is precisely what "the checks do not take the lock the mutation takes" means.
     */
    @Test
    void theSuperTypeWalkWaitsForTheWriteLockTheMutationTakes() throws Exception {
      final LocalDatabase    embedded    = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final CountDownLatch   lockHeld    = new CountDownLatch(1);
      final CountDownLatch   releaseLock = new CountDownLatch(1);
      final ExecutorService  threads     = Executors.newFixedThreadPool(2);

      try {
        final Future<?> lockHolder = threads.submit(() -> embedded.executeInWriteLock(() -> {
          lockHeld.countDown();
          assertThat(releaseLock.await(30, TimeUnit.SECONDS)).isTrue();
          return null;
        }));

        assertThat(lockHeld.await(30, TimeUnit.SECONDS)).as("the helper thread took the database write lock").isTrue();

        final AtomicReference<Throwable> createOutcome = new AtomicReference<>();
        final Future<?> create = threads.submit(() -> {
          try {
            database.getSchema().getType(CHILD).createProperty("code", Type.STRING);
          } catch (final Throwable t) {
            createOutcome.set(t);
          }
        });

        // A short wait EXPECTED TO TIME OUT: no elapsed-time claim is being made, so a stalled JVM can only make this
        // more true. Before the fix the create returned its SchemaException here without ever asking for the lock.
        assertThatThrownBy(() -> create.get(2, TimeUnit.SECONDS))
            .as("createProperty must not reach a verdict while the write lock its mutation needs is held elsewhere")
            .isInstanceOf(TimeoutException.class);

        releaseLock.countDown();
        lockHolder.get(30, TimeUnit.SECONDS);
        create.get(30, TimeUnit.SECONDS);

        assertThat(createOutcome.get())
            .as("and once it does get the lock it still refuses, naming the super type's property")
            .isInstanceOf(SchemaException.class);
        assertThat(createOutcome.get().getMessage()).contains("code").contains("super type");
      } finally {
        releaseLock.countDown();
        threads.shutdownNow();
        assertThat(threads.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
      }

      // Nothing was created: the refusal is still a refusal, not a half-applied change.
      assertThat(database.getSchema().getType(CHILD).existsProperty("code")).isFalse();
    }

    /**
     * The behaviour the moved checks must keep, one per check that moved.
     */
    @Test
    void theMovedChecksStillRefuseAndStillAllow() {
      // 1. A name already declared by a super type.
      assertThatThrownBy(() -> database.getSchema().getType(CHILD).createProperty("code", Type.STRING))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("super type");

      // 2. A name this type already declares.
      assertThatThrownBy(() -> database.getSchema().getType(PARENT).createProperty("code", Type.STRING))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("already exists");

      // 3. A LIGHTWEIGHT edge type has no record to hold a value.
      final LocalEdgeType light = (LocalEdgeType) database.getSchema().createEdgeType("Issue7918Light");
      light.setLightweight(true);
      assertThatThrownBy(() -> light.createProperty("weight", Type.LONG))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("LIGHTWEIGHT");

      // And a free name still lands, with everything the non-moved part of the method used to set.
      final Property created = database.getSchema().getType(CHILD).createProperty("label", Type.LIST, "STRING");
      assertThat(created.getName()).isEqualTo("label");
      assertThat(created.getType()).isEqualTo(Type.LIST);
      assertThat(created.getOfType()).isEqualTo("STRING");
      assertThat(database.getSchema().getType(CHILD).existsProperty("label")).isTrue();
    }

    /**
     * {@code rename()}'s name bookkeeping, which the same issue touched (and which CodeRabbit's review of PR #7935
     * sharpened): the old name has to stay mapped to the type for the WHOLE rename, because the rename can still
     * roll back after the new name is reserved. Released early, a concurrent {@code CREATE TYPE} could legitimately
     * take it in that window and the rollback's restore would have evicted that type.
     * <p>
     * The window itself needs a failure injected mid-rename to observe directly; what is pinned here are the two
     * post-conditions that bracket it and that any regression in the bookkeeping breaks - a rename that succeeds
     * releases exactly the old name, and a rename that is refused changes nothing and evicts nobody.
     */
    @Test
    void renameReleasesTheOldNameOnlyOnSuccessAndEvictsNobodyOnRefusal() {
      final Schema schema = database.getSchema();
      schema.createDocumentType("Issue7918Renamed").createProperty("label", Type.STRING);

      // A rename that succeeds: the old name is gone, the new one resolves to the same type, and it kept its
      // properties (so the map entry really is the type, not a fresh one).
      schema.getType("Issue7918Renamed").rename("Issue7918RenamedNow");

      assertThat(schema.existsType("Issue7918Renamed")).isFalse();
      assertThat(schema.existsType("Issue7918RenamedNow")).isTrue();
      assertThat(schema.getType("Issue7918RenamedNow").existsProperty("label")).isTrue();

      // A rename onto a name somebody else holds is refused, and - the part that matters - the holder is still
      // there afterwards, under its own name, with its own properties.
      schema.createDocumentType("Issue7918Occupant").createProperty("occupied", Type.STRING);

      assertThatThrownBy(() -> schema.getType("Issue7918RenamedNow").rename("Issue7918Occupant"))
          .isInstanceOf(AlreadyExistsException.class);

      assertThat(schema.existsType("Issue7918Occupant")).isTrue();
      assertThat(schema.getType("Issue7918Occupant").existsProperty("occupied"))
          .as("the refused rename must not have evicted the type that legitimately holds the name").isTrue();
      assertThat(schema.existsType("Issue7918RenamedNow"))
          .as("and the type that tried to rename is still reachable under its own name").isTrue();
      assertThat(schema.getType("Issue7918RenamedNow").existsProperty("label")).isTrue();
    }

    /**
     * The property must survive a reopen, i.e. the mutation still goes through {@code recordFileChanges} and still
     * reaches {@code schema.json} - the move must not have turned the write into an in-memory-only one.
     */
    @Test
    void theCreatedPropertyIsPersisted() {
      database.getSchema().getType(CHILD).createProperty("persisted", Type.INTEGER);

      reopenDatabase();

      assertThat(database.getSchema().getType(CHILD).existsProperty("persisted")).isTrue();
      assertThat(database.getSchema().getType(CHILD).getProperty("persisted").getType()).isEqualTo(Type.INTEGER);
    }
  }

  /**
   * Regression for #9023: readonly array property.
   *
   * Issue #9023: a READONLY array-valued property (BINARY, ARRAY_OF_*) blocked every later update of its record, even of
   * other properties, because the stored value was compared with the current one by reference.
   */
  @Nested
  class ReadonlyArrayPropertyTest extends TestHelper {

    static Stream<Arguments> arrayTypes() {
      return Stream.of(//
          Arguments.of(Type.BINARY, (Supplier<Object>) () -> new byte[] { 1, 2, 3 }, (Supplier<Object>) () -> new byte[] { 1, 2, 4 }),//
          Arguments.of(Type.ARRAY_OF_SHORTS, (Supplier<Object>) () -> new short[] { 1, 2 }, (Supplier<Object>) () -> new short[] { 1, 3 }),//
          Arguments.of(Type.ARRAY_OF_INTEGERS, (Supplier<Object>) () -> new int[] { 1, 2 }, (Supplier<Object>) () -> new int[] { 1, 3 }),//
          Arguments.of(Type.ARRAY_OF_LONGS, (Supplier<Object>) () -> new long[] { 1L, 2L }, (Supplier<Object>) () -> new long[] { 1L, 3L }),//
          Arguments.of(Type.ARRAY_OF_FLOATS, (Supplier<Object>) () -> new float[] { 0.1f, 0.2f },
              (Supplier<Object>) () -> new float[] { 0.1f, 0.3f }),//
          Arguments.of(Type.ARRAY_OF_DOUBLES, (Supplier<Object>) () -> new double[] { 0.1d, 0.2d },
              (Supplier<Object>) () -> new double[] { 0.1d, 0.3d }));
    }

    @ParameterizedTest
    @MethodSource("arrayTypes")
    void updateOfOtherPropertyIsAcceptedThroughSql(final Type type, final Supplier<Object> value) {
      final RID rid = createRecord(type, value.get());

      database.transaction(() -> database.command("sql", "UPDATE " + rid + " SET note = 'b'").close());

      assertThat(database.lookupByRID(rid, true).asDocument().getString("note")).isEqualTo("b");
    }

    @ParameterizedTest
    @MethodSource("arrayTypes")
    void updateOfOtherPropertyIsAcceptedThroughApi(final Type type, final Supplier<Object> value) {
      final RID rid = createRecord(type, value.get());

      database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("note", "c").save());

      assertThat(database.lookupByRID(rid, true).asDocument().getString("note")).isEqualTo("c");
    }

    @ParameterizedTest
    @MethodSource("arrayTypes")
    void settingAContentEqualArrayIsAccepted(final Type type, final Supplier<Object> value) {
      final RID rid = createRecord(type, value.get());

      database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("p", value.get()).set("note", "d").save());

      assertThat(database.lookupByRID(rid, true).asDocument().getString("note")).isEqualTo("d");
    }

    @ParameterizedTest
    @MethodSource("arrayTypes")
    void changingTheArrayContentIsStillRefused(final Type type, final Supplier<Object> value, final Supplier<Object> other) {
      final RID rid = createRecord(type, value.get());

      assertThatThrownBy(
          () -> database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("p", other.get()).save()))//
          .isInstanceOf(ValidationException.class)//
          .hasMessageContaining("is immutable");

      assertThatThrownBy(
          () -> database.transaction(() -> {
            final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
            doc.remove("p");
            doc.save();
          }))//
          .isInstanceOf(ValidationException.class)//
          .hasMessageContaining("is immutable");
    }

    @Test
    void refusalMessagePrintsTheArrayContent() {
      final RID rid = createRecord(Type.ARRAY_OF_INTEGERS, new int[] { 1, 2 });

      assertThatThrownBy(() -> database.transaction(
          () -> database.lookupByRID(rid, true).asDocument().modify().set("p", new int[] { 7, 8 }).save()))//
          .isInstanceOf(ValidationException.class)//
          .hasMessageContaining("[7, 8]");
    }

    @Test
    void readonlyListHoldingAnArrayDoesNotBlockOtherUpdates() {
      final List<Object> list = new ArrayList<>();
      list.add(new byte[] { 1, 2 });
      list.add("x");
      final RID rid = createRecord(Type.LIST, list);

      database.transaction(() -> database.command("sql", "UPDATE " + rid + " SET note = 'b'").close());
      assertThat(database.lookupByRID(rid, true).asDocument().getString("note")).isEqualTo("b");

      final List<Object> changed = new ArrayList<>();
      changed.add(new byte[] { 1, 3 });
      changed.add("x");
      assertThatThrownBy(() -> database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("p", changed).save()))//
          .isInstanceOf(ValidationException.class);
    }

    @Test
    void readonlyMapHoldingAnArrayDoesNotBlockOtherUpdates() {
      final Map<String, Object> map = new HashMap<>();
      map.put("k", new float[] { 0.5f, 1.5f });
      final RID rid = createRecord(Type.MAP, map);

      database.transaction(() -> database.command("sql", "UPDATE " + rid + " SET note = 'b'").close());
      assertThat(database.lookupByRID(rid, true).asDocument().getString("note")).isEqualTo("b");

      final Map<String, Object> changed = new HashMap<>();
      changed.put("k", new float[] { 0.5f, 2.5f });
      assertThatThrownBy(() -> database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("p", changed).save()))//
          .isInstanceOf(ValidationException.class);
    }

    @Test
    void refusalMessagePrintsAnArrayNestedInAListOrMap() {
      final List<Object> list = new ArrayList<>();
      list.add(new byte[] { 1, 2 });
      final RID listRid = createRecord(Type.LIST, list);
      final List<Object> changedList = new ArrayList<>();
      changedList.add(new byte[] { 7, 8 });
      assertThatThrownBy(
          () -> database.transaction(() -> database.lookupByRID(listRid, true).asDocument().modify().set("p", changedList).save()))//
          .isInstanceOf(ValidationException.class)//
          .hasMessageContaining("[[7, 8]]");

      final Map<String, Object> map = new HashMap<>();
      map.put("k", new int[] { 1, 2 });
      final RID mapRid = createRecord(Type.MAP, map);
      final Map<String, Object> changedMap = new HashMap<>();
      changedMap.put("k", new int[] { 7, 8 });
      assertThatThrownBy(
          () -> database.transaction(() -> database.lookupByRID(mapRid, true).asDocument().modify().set("p", changedMap).save()))//
          .isInstanceOf(ValidationException.class)//
          .hasMessageContaining("{k=[7, 8]}");
    }

    @Test
    void readonlyEmbeddedHoldingAnArrayDoesNotBlockOtherUpdates() {
      database.getSchema().createDocumentType("Inner");
      final DocumentType type = database.getSchema().createDocumentType("T_EMBEDDED");
      type.createProperty("p", Type.EMBEDDED).setReadonly(true);

      final RID[] rid = new RID[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("T_EMBEDDED");
        doc.newEmbeddedDocument("Inner", "p").set("bytes", new byte[] { 9, 8, 7 });
        doc.set("note", "a");
        rid[0] = doc.save().getIdentity();
      });

      database.transaction(() -> database.command("sql", "UPDATE " + rid[0] + " SET note = 'b'").close());
      database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().modify().set("note", "c").save());

      final Document reloaded = database.lookupByRID(rid[0], true).asDocument();
      assertThat(reloaded.getString("note")).isEqualTo("c");

      // the same embedded type holding other bytes is a change
      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument doc = database.lookupByRID(rid[0], true).asDocument().modify();
        doc.newEmbeddedDocument("Inner", "p").set("bytes", new byte[] { 9, 8, 6 });
        doc.save();
      }))//
          .isInstanceOf(ValidationException.class)//
          .hasMessageContaining("is immutable");

      // and so is another embedded type holding the same bytes
      database.getSchema().createDocumentType("OtherInner");
      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument doc = database.lookupByRID(rid[0], true).asDocument().modify();
        doc.newEmbeddedDocument("OtherInner", "p").set("bytes", new byte[] { 9, 8, 7 });
        doc.save();
      }))//
          .isInstanceOf(ValidationException.class)//
          .hasMessageContaining("is immutable");

      // while an equal embedded document rebuilt from scratch is not
      database.transaction(() -> {
        final MutableDocument doc = database.lookupByRID(rid[0], true).asDocument().modify();
        doc.newEmbeddedDocument("Inner", "p").set("bytes", new byte[] { 9, 8, 7 });
        doc.set("note", "d");
        doc.save();
      });
      assertThat(database.lookupByRID(rid[0], true).asDocument().getString("note")).isEqualTo("d");
    }

    @Test
    void updateOfOtherPropertyIsAcceptedThroughCypherSet() {
      database.getSchema().createVertexType("V9023").createProperty("p", Type.ARRAY_OF_FLOATS).setReadonly(true);
      database.transaction(() -> database.newVertex("V9023").set("p", new float[] { 0.1f, 0.2f }).set("note", "a").save());

      database.transaction(() -> database.command("opencypher", "MATCH (n:V9023) SET n.note = 'b'").close());

      assertThat(database.query("sql", "SELECT note FROM V9023").next().<String>getProperty("note")).isEqualTo("b");
    }

    private RID createRecord(final Type type, final Object value) {
      final String typeName = "T_" + type.name();
      database.getSchema().createDocumentType(typeName).createProperty("p", type).setReadonly(true);
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument(typeName).set("p", value).set("note", "a").save().getIdentity());
      return rid[0];
    }
  }

  /**
   * Regression for #9172: invalid property definition.
   *
   * Regression test for #9172 (merged #9042 and #9026): a property definition the schema cannot honor is refused when it is
   * declared, instead of being accepted and then breaking every later schema save (an unknown type name) or every later
   * write of the property (a MIN/MAX bound the type cannot use or read).
   */
  @Nested
  class InvalidPropertyDefinitionTest extends TestHelper {

    // ---------------------------------------------------------------- #9042: unknown property type

    @Test
    void createPropertyWithUnknownTypeNameIsRefusedBeforeTouchingTheSchema() {
      final DocumentType person = database.getSchema().createDocumentType("Person");
      person.createProperty("a", "STRING");

      assertThatThrownBy(() -> person.createProperty("b", "NOSUCHTYPE")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("'b'").hasMessageContaining("Person").hasMessageContaining("NOSUCHTYPE");
      assertThat(person.getPropertyNames()).containsExactly("a");

      // every later schema save keeps working
      person.createProperty("c", "STRING");
      assertThatThrownBy(() -> database.getSchema().dropType("NoSuchType")).isInstanceOf(SchemaException.class);
      database.command("sql", "CREATE DOCUMENT TYPE Other");
      assertThat(database.getSchema().existsType("Other")).isTrue();

      reopenDatabase();
      assertThat(database.getSchema().getType("Person").getPropertyNames()).containsExactlyInAnyOrder("a", "c");
    }

    @Test
    void createPropertyWithNullTypeNameIsRefused() {
      final DocumentType person = database.getSchema().createDocumentType("Person");
      assertThatThrownBy(() -> person.createProperty("b", (String) null)).isInstanceOf(SchemaException.class)
          .hasMessageContaining("'b'");
      assertThat(person.getPropertyNames()).isEmpty();
    }

    @Test
    void createPropertyWithNullTypeIsRefused() {
      final DocumentType person = database.getSchema().createDocumentType("Person");
      assertThatThrownBy(() -> person.createProperty("b", (Type) null)).isInstanceOf(SchemaException.class)
          .hasMessageContaining("'b'");
      assertThatThrownBy(() -> person.createProperty("b", (Type) null, null)).isInstanceOf(SchemaException.class);
      assertThat(person.getPropertyNames()).isEmpty();
      person.createProperty("c", Type.STRING);
      assertThat(person.getPropertyNames()).containsExactly("c");
    }

    @Test
    void createPropertyWithUnmappedJavaClassIsRefused() {
      final DocumentType person = database.getSchema().createDocumentType("Person");
      assertThatThrownBy(() -> person.createProperty("b", Thread.class)).isInstanceOf(SchemaException.class)
          .hasMessageContaining("'b'").hasMessageContaining(Thread.class.getName());
      assertThat(person.getPropertyNames()).isEmpty();
      person.createProperty("c", String.class);
      assertThat(person.getPropertyNames()).containsExactly("c");
    }

    @Test
    void getOrCreatePropertyWithUnknownTypeNameDoesNotDropTheExistingProperty() {
      final DocumentType person = database.getSchema().createDocumentType("Person");
      person.createProperty("a", Type.STRING);

      assertThatThrownBy(() -> person.getOrCreateProperty("a", "NOSUCHTYPE")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("NOSUCHTYPE");
      assertThatThrownBy(() -> person.getOrCreateProperty("a", "NOSUCHTYPE", null)).isInstanceOf(SchemaException.class);
      assertThatThrownBy(() -> person.getOrCreateProperty("a", (Type) null)).isInstanceOf(SchemaException.class);
      assertThatThrownBy(() -> person.getOrCreateProperty("a", Thread.class)).isInstanceOf(SchemaException.class);
      assertThatThrownBy(() -> person.getOrCreateProperty("n", "NOSUCHTYPE")).isInstanceOf(SchemaException.class);

      assertThat(person.getPropertyNames()).containsExactly("a");
      assertThat(person.getProperty("a").getType()).isEqualTo(Type.STRING);
      person.createProperty("c", Type.STRING);
    }

    // ---------------------------------------------------------------- #9026: MIN/MAX the type cannot use or read

    @Test
    void minMaxOnArrayPropertyBoundsTheNumberOfElements() {
      database.command("sql", "CREATE DOCUMENT TYPE T1");
      database.command("sql", "CREATE PROPERTY T1.p ARRAY_OF_FLOATS (max 3)");
      database.command("sql", "CREATE DOCUMENT TYPE T2");
      database.command("sql", "CREATE PROPERTY T2.p ARRAY_OF_FLOATS (min 1)");

      database.transaction(() -> database.newDocument("T1").set("p", new float[] { 1f, 2f }).save());
      database.transaction(() -> database.newDocument("T2").set("p", new float[] { 1f, 2f }).save());

      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T1").set("p", new float[] { 1f, 2f, 3f, 4f }).save()))
          .isInstanceOf(ValidationException.class).hasMessageContaining("T1.p").hasMessageContaining("more items than 3");
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T2").set("p", new float[0]).save()))
          .isInstanceOf(ValidationException.class).hasMessageContaining("T2.p").hasMessageContaining("fewer items than 1");
    }

    @Test
    void arrayBoundOverAStoredNonArrayValueIsAValidationError() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.transaction(() -> database.newDocument("T").set("p", "abc").save());

      // the stored value is checked against the new bound, and a value that is no array is reported, not a crash
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.p ARRAY_OF_FLOATS (max 3)")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("not an array");
      assertThat(database.getSchema().getType("T").existsProperty("p")).isFalse();
    }

    @Test
    void minMaxOnEveryArrayTypeIsAccepted() {
      for (final Type type : List.of(Type.ARRAY_OF_SHORTS, Type.ARRAY_OF_INTEGERS, Type.ARRAY_OF_LONGS, Type.ARRAY_OF_FLOATS,
          Type.ARRAY_OF_DOUBLES)) {
        final DocumentType t = database.getSchema().createDocumentType("A_" + type.name());
        t.createProperty("p", type).setMin("1").setMax("2");
      }
      database.transaction(() -> {
        database.newDocument("A_ARRAY_OF_SHORTS").set("p", new short[] { 1 }).save();
        database.newDocument("A_ARRAY_OF_INTEGERS").set("p", new int[] { 1, 2 }).save();
        database.newDocument("A_ARRAY_OF_LONGS").set("p", new long[] { 1 }).save();
        database.newDocument("A_ARRAY_OF_DOUBLES").set("p", new double[] { 1, 2 }).save();
      });
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("A_ARRAY_OF_LONGS").set("p", new long[] { 1, 2, 3 }).save()))
          .isInstanceOf(ValidationException.class);
    }

    @Test
    void alterPropertyMaxOnArrayChecksStoredElementCount() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.p ARRAY_OF_INTEGERS");
      database.transaction(() -> database.newDocument("T").set("p", new int[] { 1, 2, 3 }).save());

      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.p MAX 2")).isInstanceOf(CommandExecutionException.class);
      assertThat(database.getSchema().getType("T").getProperty("p").getMax()).isNull();

      database.command("sql", "ALTER PROPERTY T.p MAX 3");
      assertThat(database.getSchema().getType("T").getProperty("p").getMax()).isEqualTo("3");
    }

    @Test
    void createPropertyWithUnreadableIntegerBoundIsRefused() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.p INTEGER (max 'abc')")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("abc");
      assertThat(database.getSchema().getType("T").existsProperty("p")).isFalse();

      // a bound the write path would parse as an int, not just one convert() could round
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.p INTEGER (min '3.5')")).isInstanceOf(
          CommandExecutionException.class);
      assertThat(database.getSchema().getType("T").existsProperty("p")).isFalse();

      database.command("sql", "CREATE PROPERTY T.p INTEGER (max 3)");
      database.transaction(() -> database.newDocument("T").set("p", 1).save());
    }

    @Test
    void createPropertyWithUnreadableDateBoundIsRefused() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.p DATETIME (min 'yesterday')")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("yesterday");
      assertThat(database.getSchema().getType("T").existsProperty("p")).isFalse();

      database.command("sql", "CREATE PROPERTY T.p DATETIME (min '2020-01-01 00:00:00')");
      database.transaction(() -> database.newDocument("T").set("p", "2026-10-03 10:00:00").save());
    }

    /**
     * The declaration check reads a date bound the way the write path does for every date subtype, so a readable bound is
     * accepted and enforced, and an unreadable one is refused, whichever subtype declares it.
     */
    @Test
    void dateBoundIsCheckedTheSameWayForEveryDateSubtype() {
      for (final Type type : List.of(Type.DATE, Type.DATETIME, Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS)) {
        final String typeName = "D_" + type.name();
        final DocumentType t = database.getSchema().createDocumentType(typeName);
        final Property p = t.createProperty("d", type);

        assertThatThrownBy(() -> p.setMin("yesterday")).as(type.name()).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("yesterday");
        assertThat(p.getMin()).as(type.name()).isNull();

        p.setMin("2020-01-01 00:00:00");
        database.transaction(() -> database.newDocument(typeName).set("d", "2026-10-03 10:00:00").save());
        assertThatThrownBy(() -> database.transaction(() -> database.newDocument(typeName).set("d", "2019-06-01 10:00:00").save()))
            .as(type.name()).isInstanceOf(ValidationException.class).hasMessageContaining("precedes");
      }
    }

    @Test
    void unreadableBoundIsRefusedThroughTheJavaApi() {
      final DocumentType t = database.getSchema().createDocumentType("T");
      final Property integer = t.createProperty("i", Type.INTEGER);
      assertThatThrownBy(() -> integer.setMax("abc")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("abc");
      assertThatThrownBy(() -> integer.setMin("abc")).isInstanceOf(IllegalArgumentException.class);
      assertThat(integer.getMax()).isNull();
      assertThat(integer.getMin()).isNull();

      for (final Type type : List.of(Type.LONG, Type.SHORT, Type.BYTE, Type.FLOAT, Type.DOUBLE, Type.DECIMAL, Type.DATE,
          Type.DATETIME_MICROS, Type.STRING, Type.BINARY, Type.LIST, Type.MAP, Type.ARRAY_OF_FLOATS)) {
        final Property p = t.createProperty("p_" + type.name(), type);
        assertThatThrownBy(() -> p.setMax("not-a-bound")).as(type.name()).isInstanceOf(IllegalArgumentException.class);
        assertThat(p.getMax()).as(type.name()).isNull();
      }

      for (final Type type : List.of(Type.FLOAT, Type.DOUBLE)) {
        final Property p = t.getProperty("p_" + type.name());
        assertThatThrownBy(() -> p.setMin("NaN")).as(type.name()).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("NaN");
        p.setMin("-1.5");
        assertThat(p.getMin()).isEqualTo("-1.5");
      }

      final Property string = t.getProperty("p_STRING");
      assertThatThrownBy(() -> string.setMax("-1")).isInstanceOf(IllegalArgumentException.class);
      string.setMax("10");
      assertThat(string.getMax()).isEqualTo("10");
      // clearing a bound is always allowed
      string.setMax(null);
      assertThat(string.getMax()).isNull();
    }

    @Test
    void minMaxOnUnorderedTypesIsRefused() {
      final DocumentType t = database.getSchema().createDocumentType("T");
      for (final Type type : List.of(Type.BOOLEAN, Type.LINK, Type.EMBEDDED, Type.OFFSET_TIME, Type.LOCAL_TIME,
          Type.ZONED_DATETIME, Type.DURATION)) {
        final Property p = t.createProperty("p_" + type.name(), type);
        assertThatThrownBy(() -> p.setMax("3")).as(type.name()).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not applicable");
        assertThatThrownBy(() -> p.setMin("3")).as(type.name()).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not applicable");
      }
    }

    @Test
    void alterPropertyNullClearsTheBound() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a INTEGER (min 1, max 10)");
      database.command("sql", "CREATE PROPERTY T.s STRING (regexp '[a-z]+')");

      database.command("sql", "ALTER PROPERTY T.a MIN null");
      database.command("sql", "ALTER PROPERTY T.a MAX null");
      database.command("sql", "ALTER PROPERTY T.s REGEXP null");

      final DocumentType t = database.getSchema().getType("T");
      assertThat(t.getProperty("a").getMin()).isNull();
      assertThat(t.getProperty("a").getMax()).isNull();
      assertThat(t.getProperty("s").getRegexp()).isNull();
      database.transaction(() -> database.newDocument("T").set("a", 50).set("s", "ABC").save());
    }

    @Test
    void legacySchemaWithUnreadableBoundStillOpensAndCanBeRepaired() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.p INTEGER (max 3)");
      database.command("sql", "CREATE PROPERTY T.d DATETIME (min '2020-01-01 00:00:00')");
      database.close();

      // a schema written before #9172 could carry a bound the type cannot read
      final File schemaJson = new File(database.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
      final JSONObject schema = new JSONObject(Files.readString(schemaJson.toPath()));
      final JSONObject properties = schema.getJSONObject("types").getJSONObject("T").getJSONObject("properties");
      properties.getJSONObject("p").put("max", "abc");
      properties.getJSONObject("d").put("min", "yesterday");
      Files.writeString(schemaJson.toPath(), schema.toString());

      database = factory.open();
      final Property p = database.getSchema().getType("T").getProperty("p");
      final Property d = database.getSchema().getType("T").getProperty("d");
      assertThat(p.getMax()).isEqualTo("abc");
      assertThat(d.getMin()).isEqualTo("yesterday");

      // the write-time check still reports such a bound as a validation error naming it
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T").set("d", "2026-10-03 10:00:00").save()))
          .isInstanceOf(ValidationException.class).hasMessageContaining("yesterday");

      database.command("sql", "ALTER PROPERTY T.p MAX null");
      database.command("sql", "ALTER PROPERTY T.d MIN null");
      assertThat(p.getMax()).isNull();
      assertThat(d.getMin()).isNull();
      database.transaction(() -> database.newDocument("T").set("p", 1).set("d", "2026-10-03 10:00:00").save());
    }
  }
}
