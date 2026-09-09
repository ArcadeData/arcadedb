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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7299, and the end of a series: #6678 made two members of {@link LocalDocumentType} volatile, #7033 added
 * two more, #7119 a fifth, and each pass fixed only the member its own issue happened to name.
 * <p>
 * The members of this class are reassigned under the schema mutation lock and read LOCK-FREE by query planning -
 * {@code instanceOf} is called by openCypher label resolution with no database lock held - so every one of them
 * needs the publication edge a volatile write provides. The reflective test below is what closes the series: a
 * seventh field added without it fails here rather than in a sixth issue.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7299SchemaCopyOnWritePublicationTest {
  private Database database;

  @BeforeEach
  public void setUp() {
    database = new DatabaseFactory("./target/databases/test-issue-7299").create();
  }

  @AfterEach
  public void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * The sweep, run by the machine rather than by the next reader of the class.
   */
  @Test
  public void everyMutableReferenceMemberIsPublishedSafely() {
    final List<String> offenders = new ArrayList<>();

    for (final Field field : LocalDocumentType.class.getDeclaredFields()) {
      final int modifiers = field.getModifiers();
      if (Modifier.isStatic(modifiers) || Modifier.isFinal(modifiers) || Modifier.isVolatile(modifiers))
        continue;
      offenders.add(field.getType().getSimpleName() + " " + field.getName());
    }

    assertThat(offenders).as(
            "Members of LocalDocumentType that a schema mutation reassigns while a lock-free reader walks them must "
                + "be final (immutable or a concurrent collection) or volatile, so the reader gets a happens-before "
                + "edge against the write. Add the keyword rather than relaxing this assertion (issue #7299).")
        .isEmpty();
  }

  /**
   * The half of the issue that needs no concurrency at all.
   */
  @Test
  public void setAliasesDoesNotPublishTheCallersSet() {
    database.getSchema().createDocumentType("Invoice");
    final LocalDocumentType invoice = (LocalDocumentType) database.getSchema().getType("Invoice");

    final Set<String> callerOwned = new HashSet<>(Set.of("Bill"));
    invoice.setAliases(callerOwned);

    // The caller keeps its set and goes on using it. That must not reach live schema state.
    callerOwned.add("Smuggled");

    assertThat(invoice.getAliases()).containsExactly("Bill");
    assertThat(invoice.instanceOf("Smuggled")).isFalse();
    assertThat(invoice.instanceOf("Bill")).isTrue();

    // And what getAliases() hands out is not a back door into it either.
    assertThatThrownBy(() -> invoice.getAliases().add("Smuggled")).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void aliasesStillBehaveAsBefore() {
    database.getSchema().createDocumentType("Order");
    final DocumentType order = database.getSchema().getType("Order");

    order.setAliases(Set.of("PurchaseOrder", "PO"));
    assertThat(order.getAliases()).containsExactlyInAnyOrder("PurchaseOrder", "PO");
    assertThat(database.getSchema().getType("PO")).isSameAs(order);

    // Replacing the set deregisters the previous aliases, and the type stops answering to them.
    order.setAliases(Set.of("PO"));
    assertThat(order.getAliases()).containsExactly("PO");
    assertThat(order.instanceOf("PurchaseOrder")).isFalse();
    assertThat(database.getSchema().existsType("PurchaseOrder")).isFalse();

    order.setAliases(Set.of());
    assertThat(order.getAliases()).isEmpty();
    assertThat(database.getSchema().existsType("PO")).isFalse();
  }

  /**
   * The field #7119's closing argument rests on: the volatile write in {@code super.setType} happens BEFORE this
   * one, so it orders nothing here and cannot be borrowed as the publication edge.
   */
  @Test
  public void thePartitionStrategyTypeReferenceIsPublishedSafely() throws NoSuchFieldException {
    final Field type = PartitionedBucketSelectionStrategy.class.getDeclaredField("type");
    assertThat(Modifier.isVolatile(type.getModifiers())).isTrue();
  }
}
