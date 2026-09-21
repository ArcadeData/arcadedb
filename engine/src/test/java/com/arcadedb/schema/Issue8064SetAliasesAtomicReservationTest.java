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
import com.arcadedb.exception.SchemaException;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8064
 * <p>
 * {@code LocalDocumentType.setAliases} mutated the schema's shared type map with a check-then-act sequence and no
 * write lock, which is the arrangement {@code rename()}, {@code createProperty} and {@code dropProperty} were just
 * converted away from. Two holes in the one method:
 * <ol>
 *   <li><b>Check-then-put.</b> It refused an alias already used by another type through
 *   {@code schema.existsType(alias)} and put the alias into {@code schema.typeMap()} several statements later, with
 *   nothing holding the map in between - so two concurrent {@code ALTER TYPE ... ALIASES} on different types could
 *   both pass the check and the second put silently won, leaving two types believing they owned one name.</li>
 *   <li><b>Unconditional deregistration.</b> EVERY previous alias was removed from the map before the new set was
 *   installed, so a reader resolving one of those names saw it disappear even when the new set still carried it.</li>
 * </ol>
 * The class's own comment on the {@code aliases} field already claimed the protection the method never took.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8064SetAliasesAtomicReservationTest extends TestHelper {

  private static final int CONCURRENT_ATTEMPTS = 32;

  @Test
  void twoConcurrentSetAliasesOnTheSameNameCannotBothSucceed() throws Exception {
    for (int attempt = 0; attempt < CONCURRENT_ATTEMPTS; attempt++) {
      final String contested = "Shared" + attempt;
      final LocalDocumentType first = (LocalDocumentType) database.getSchema().createDocumentType("A" + attempt);
      final LocalDocumentType second = (LocalDocumentType) database.getSchema().createDocumentType("B" + attempt);

      final CountDownLatch start = new CountDownLatch(1);
      final AtomicInteger succeeded = new AtomicInteger();
      final AtomicReference<Throwable> unexpected = new AtomicReference<>();

      final Thread[] threads = new Thread[2];
      for (int t = 0; t < 2; t++) {
        final LocalDocumentType claimant = t == 0 ? first : second;
        threads[t] = new Thread(() -> {
          try {
            start.await();
            claimant.setAliases(Set.of(contested));
            succeeded.incrementAndGet();
          } catch (final SchemaException e) {
            // THE EXPECTED LOSER'S OUTCOME
          } catch (final Throwable e) {
            unexpected.set(e);
          }
        });
        threads[t].start();
      }

      start.countDown();
      for (final Thread thread : threads) {
        thread.join(TimeUnit.SECONDS.toMillis(30));
        // ASSERTED, NOT ASSUMED: A join() THAT TIMED OUT LEAVES THE CLAIMANT STILL RUNNING, AND EVERY ASSERTION
        // BELOW WOULD THEN BE READING A HALF-FINISHED RESERVATION AND BLAMING IT ON THE FIX
        assertThat(thread.isAlive()).as("claimant thread did not finish on attempt " + attempt).isFalse();
      }

      assertThat(unexpected.get()).as("unexpected failure on attempt " + attempt).isNull();
      assertThat(succeeded.get()).as("exactly one claimant may take '" + contested + "'").isEqualTo(1);

      // AND THE MAP AND THE TWO TYPES AGREE ON WHO WON: EXACTLY ONE OF THEM CARRIES THE ALIAS IN ITS OWN SET, AND
      // THAT IS THE ONE THE NAME RESOLVES TO
      final DocumentType resolved = database.getSchema().getType(contested);
      assertThat(resolved).isNotNull();
      assertThat(first.getAliases().contains(contested) ^ second.getAliases().contains(contested)).isTrue();
      assertThat(resolved.getName()).isEqualTo(first.getAliases().contains(contested) ? first.getName() : second.getName());
    }
  }

  @Test
  void anAliasTheNewSetStillCarriesIsNeverDeregistered() {
    final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");
    order.setAliases(Set.of("PO", "PurchaseOrder"));

    // A SUPERSET: BOTH PREVIOUS NAMES SURVIVE, SO NEITHER MAY EVER LEAVE THE MAP
    order.setAliases(Set.of("PO", "PurchaseOrder", "Ordine"));

    assertThat(database.getSchema().getType("PO").getName()).isEqualTo("Order");
    assertThat(database.getSchema().getType("PurchaseOrder").getName()).isEqualTo("Order");
    assertThat(database.getSchema().getType("Ordine").getName()).isEqualTo("Order");
    assertThat(order.getAliases()).containsExactlyInAnyOrder("PO", "PurchaseOrder", "Ordine");
  }

  @Test
  void anAliasTheNewSetDropsStopsResolving() {
    final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");
    order.setAliases(Set.of("PO", "PurchaseOrder"));

    order.setAliases(Set.of("PO"));

    assertThat(database.getSchema().existsType("PurchaseOrder")).isFalse();
    assertThat(database.getSchema().getType("PO").getName()).isEqualTo("Order");
    assertThat(order.getAliases()).containsExactly("PO");

    order.setAliases(Set.of());
    assertThat(database.getSchema().existsType("PO")).isFalse();
    assertThat(order.getAliases()).isEmpty();
  }

  @Test
  void aRefusedCallLeavesNoHalfInstalledAliasBehind() {
    final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");
    database.getSchema().createDocumentType("Invoice");

    // ONE FREE NAME AND ONE ALREADY TAKEN. WHICHEVER ORDER THE SET ITERATES IN, THE FREE ONE MUST NOT SURVIVE A
    // CALL THAT WAS REFUSED - THE ALIASES ARE ALL-OR-NOTHING
    assertThatThrownBy(() -> order.setAliases(new HashSet<>(Set.of("PO", "Invoice")))).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Invoice");

    assertThat(order.getAliases()).isEmpty();
    assertThat(database.getSchema().existsType("PO")).isFalse();
    assertThat(database.getSchema().getType("Invoice").getName()).isEqualTo("Invoice");
  }

  @Test
  void anAliasEqualToTheTypesOwnNameIsRefusedWithoutBlamingItself() {
    // The type map holds the type's own name too, so putIfAbsent hands back THIS type - and the generic wording
    // would name it on both sides of the sentence, which reads as an engine bug rather than as a refusal.
    final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");

    assertThatThrownBy(() -> order.setAliases(Set.of("Order"))).isInstanceOf(SchemaException.class)
        .hasMessageContaining("is the name of the type itself")
        .hasMessageNotContaining("already used by type");

    assertThat(order.getAliases()).isEmpty();
    assertThat(database.getSchema().getType("Order").getName()).isEqualTo("Order");
  }

  @Test
  void aRefusedCallKeepsThePreviousAliasesIntact() {
    final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");
    order.setAliases(Set.of("PO"));
    database.getSchema().createDocumentType("Invoice");

    assertThatThrownBy(() -> order.setAliases(new HashSet<>(Set.of("PO", "Invoice")))).isInstanceOf(SchemaException.class);

    assertThat(order.getAliases()).containsExactly("PO");
    assertThat(database.getSchema().getType("PO").getName()).isEqualTo("Order");
  }

  @Test
  void aliasesSurviveAReopen() {
    final LocalDocumentType order = (LocalDocumentType) database.getSchema().createDocumentType("Order");
    order.setAliases(Set.of("PO", "PurchaseOrder"));

    // recordFileChanges() SAVES schema.json ITSELF, WHICH IS WHY THE EXPLICIT saveConfiguration() CALL WENT AWAY -
    // THIS IS WHAT PROVES THE SAVE STILL HAPPENS
    reopenDatabase();

    assertThat(database.getSchema().getType("PO").getName()).isEqualTo("Order");
    assertThat(database.getSchema().getType("PurchaseOrder").getName()).isEqualTo("Order");
  }
}
