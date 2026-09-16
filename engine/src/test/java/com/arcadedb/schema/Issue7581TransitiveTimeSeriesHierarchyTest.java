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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for a gap in the #7581 fix found during PR #7627's review: {@code checkTimeSeriesHierarchy}
 * originally checked only {@code this} and the super type about to be linked, so it missed a TIMESERIES type
 * sitting deeper in {@code this}'s existing descendant subtree.
 * <p>
 * That subtree can only exist on a database written before #7581 was fixed - the direct case
 * ({@code TimeSeriesType.addSuperType(ordinaryType)}) is refused unconditionally at runtime, so it is simulated
 * here the same way a pre-fix export/reload would produce it: wiring the in-memory super/subtype lists directly,
 * bypassing {@code addSuperType} and its checks entirely, exactly as {@code LocalSchema.readConfiguration()} does
 * for a hierarchy it is loading rather than validating.
 * <p>
 * Once that legacy shape exists, linking a brand new, entirely ordinary super type onto the ordinary type in the
 * middle must still be refused: the new super type's properties flow down through the middle type to the
 * TIMESERIES type at the bottom, which is exactly the silent-drop {@link Issue7581TimeSeriesSupertypeTest} closes
 * for the direct case.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7581TransitiveTimeSeriesHierarchyTest extends TestHelper {

  @Test
  void aSuperTypeLinkIsRefusedWhenATimeSeriesTypeIsBelowInTheExistingSubtree() {
    database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (s STRING) FIELDS (v DOUBLE)");
    database.command("sql", "CREATE DOCUMENT TYPE Middle");
    database.command("sql", "CREATE DOCUMENT TYPE NewParent");

    // Wires "Reading extends Middle" directly, bypassing addSuperType()/checkTimeSeriesHierarchy() - the only way
    // to reach this shape, since the direct call is refused unconditionally outside a schema-file load.
    final LocalDocumentType reading = (LocalDocumentType) database.getSchema().getType("Reading");
    final LocalDocumentType middle = (LocalDocumentType) database.getSchema().getType("Middle");
    reading.superTypes.add(middle);
    middle.subTypes.add(reading);

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Middle SUPERTYPE +NewParent"))
        .hasMessageContaining("TIMESERIES")
        .hasMessageContaining("Reading");
  }
}
