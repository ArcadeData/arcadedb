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
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.net.URL;
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
 * <p>
 * Issue #7866: the sweep first reflected over {@code LocalDocumentType.class.getDeclaredFields()} alone, so by
 * construction it could not see a subclass. {@link LocalEdgeType} held two plain booleans, {@code lightweight} and
 * {@code unique}, that {@code ALTER TYPE ... WITH} reassigns while the edge-creation path reads them lock-free - and
 * for a LIGHTWEIGHT type {@code unique} is the only enforcement of the constraint. The sweep now covers the whole
 * family: every class of the schema package that extends {@link LocalDocumentType}, found by scanning the package
 * rather than by a hand-kept list, so a new subclass is swept the day it is added.
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
   * The sweep, run by the machine rather than by the next reader of the class, over every class of the family.
   */
  @Test
  public void everyMutableReferenceMemberIsPublishedSafely() throws Exception {
    final List<String> offenders = new ArrayList<>();

    for (final Class<?> type : localDocumentTypeFamily())
      for (final Field field : type.getDeclaredFields()) {
        final int modifiers = field.getModifiers();
        if (Modifier.isStatic(modifiers) || Modifier.isFinal(modifiers) || Modifier.isVolatile(modifiers))
          continue;
        offenders.add(type.getSimpleName() + "." + field.getName() + " (" + field.getType().getSimpleName() + ")");
      }

    assertThat(offenders).as(
            "Members of the LocalDocumentType family that a schema mutation reassigns while a lock-free reader walks "
                + "them must be final (immutable or a concurrent collection) or volatile, so the reader gets a "
                + "happens-before edge against the write. Add the keyword rather than relaxing this assertion "
                + "(issues #7299, #7866).")
        .isEmpty();
  }

  /**
   * Issue #7866: the discovery the sweep rests on must actually find the subclasses, or the sweep above degrades
   * back into the single-class check it replaced and passes for the wrong reason.
   */
  @Test
  public void theSweepReachesEverySubclassOfTheFamily() throws Exception {
    assertThat(localDocumentTypeFamily()).contains(LocalDocumentType.class, LocalVertexType.class, LocalEdgeType.class,
        LocalTimeSeriesType.class);
  }

  /**
   * Issue #7866, named explicitly: the two members the issue reported, which the first sweep could not see.
   */
  @Test
  public void edgeTypeFlagsSettableThroughAlterTypeAreVolatile() throws NoSuchFieldException {
    assertThat(Modifier.isVolatile(LocalEdgeType.class.getDeclaredField("lightweight").getModifiers())).isTrue();
    assertThat(Modifier.isVolatile(LocalEdgeType.class.getDeclaredField("unique").getModifiers())).isTrue();
  }

  /**
   * Every class of {@code com.arcadedb.schema} assignable to {@link LocalDocumentType}, read from the directory the
   * package was compiled into. Only the package of the class itself is scanned: the family has no member elsewhere,
   * and a class outside it cannot reach the package-private state the sweep is protecting.
   */
  private static List<Class<?>> localDocumentTypeFamily() throws URISyntaxException, ClassNotFoundException {
    final String packageName = LocalDocumentType.class.getPackageName();
    final URL url = LocalDocumentType.class.getResource(LocalDocumentType.class.getSimpleName() + ".class");
    assertThat(url).isNotNull();
    assertThat(url.getProtocol()).as("the sweep scans the compiled package directory, not a jar").isEqualTo("file");

    final File[] files = new File(url.toURI()).getParentFile().listFiles((dir, name) -> name.endsWith(".class"));
    assertThat(files).isNotNull();

    final List<Class<?>> family = new ArrayList<>();
    for (final File file : files) {
      final String className = packageName + "." + file.getName().substring(0, file.getName().length() - ".class".length());
      final Class<?> type = Class.forName(className, false, LocalDocumentType.class.getClassLoader());
      if (LocalDocumentType.class.isAssignableFrom(type))
        family.add(type);
    }
    return family;
  }

  /**
   * Issue #7866: restoring a TimeSeries type from JSON used to clear and refill the live downsampling tier list in
   * place, under the feet of the maintenance scheduler that iterates it with no lock held. A reader that already
   * holds the list must keep seeing the tiers it read; the restored ones arrive as a new, fully built list.
   */
  @Test
  public void restoringDownsamplingTiersDoesNotMutateTheListReadersHold() {
    database.command("sql", "CREATE TIMESERIES TYPE Sensor7866 TIMESTAMP ts FIELDS (v DOUBLE)");
    database.command("sql", "ALTER TIMESERIES TYPE Sensor7866 ADD DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS");
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Sensor7866");

    final List<DownsamplingTier> heldByReader = type.getDownsamplingTiers();
    assertThat(heldByReader).containsExactly(new DownsamplingTier(7L * 86_400_000L, 3_600_000L));

    final JSONObject json = type.toJSON();
    json.put("downsamplingTiers", new JSONArray());
    type.fromJSON(json);

    assertThat(type.getDownsamplingTiers()).isEmpty();
    assertThat(heldByReader).containsExactly(new DownsamplingTier(7L * 86_400_000L, 3_600_000L));

    // And what the getter hands out is not a back door into the published list.
    assertThatThrownBy(() -> heldByReader.add(new DownsamplingTier(1L, 1L))).isInstanceOf(UnsupportedOperationException.class);
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
