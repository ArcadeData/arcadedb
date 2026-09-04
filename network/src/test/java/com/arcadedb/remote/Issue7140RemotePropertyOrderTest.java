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
package com.arcadedb.remote;

import com.arcadedb.database.RID;
import com.arcadedb.schema.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7140: {@code Document.getPropertyNames()} documents "in the order they were set", and both embedded
 * implementations were deliberately built around a {@code LinkedHashMap} to honour it. {@link RemoteImmutableDocument}
 * re-hashed the already-ordered map the server sends into a plain {@code HashMap}, so the same record read embedded
 * and remotely came back in two different orders - and every {@code toMap()} on the remote record classes did it
 * again on the way out.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7140RemotePropertyOrderTest {
  /** Insertion order deliberately differs from the hash order of these names - asserted below, not assumed. */
  private static final List<String> PROPERTIES = List.of("zeta", "alpha", "mango", "delta", "beta", "gamma", "omega");

  private RemoteDatabase          mockDatabase;
  private RemoteImmutableDocument document;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(RemoteDatabase.class);
    final RemoteSchema mockSchema = mock(RemoteSchema.class);
    final RemoteDocumentType mockType = mock(RemoteDocumentType.class);

    when(mockDatabase.getSchema()).thenReturn(mockSchema);
    when(mockSchema.getType("Ordered")).thenReturn(mockType);
    when(mockType.getName()).thenReturn("Ordered");
    when(mockType.getPolymorphicPropertyIfExists(ArgumentMatchers.anyString())).thenReturn(null);
    when(mockDatabase.newRID(ArgumentMatchers.anyString())).thenAnswer(inv -> new RID(inv.getArgument(0)));

    // A LinkedHashMap, like the one RemoteDatabase.json2Record() gets out of JSONObject.toMap()
    final Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put(Property.TYPE_PROPERTY, "Ordered");
    attributes.put(Property.CAT_PROPERTY, "d");
    for (int i = 0; i < PROPERTIES.size(); i++)
      attributes.put(PROPERTIES.get(i), i);
    attributes.put(Property.RID_PROPERTY, "#1:0");

    document = new RemoteImmutableDocument(mockDatabase, attributes);
  }

  /**
   * The premise. Without it every assertion below would pass on a {@code HashMap} too and the test would prove
   * nothing about the fix.
   */
  @Test
  void hashOrderReallyDiffersFromInsertionOrder() {
    final Map<String, Object> hashed = new HashMap<>();
    for (final String name : PROPERTIES)
      hashed.put(name, 0);
    assertThat(new ArrayList<>(hashed.keySet()))
        .as("these property names must be reordered by hashing, or this test cannot detect the bug")
        .isNotEqualTo(PROPERTIES);
  }

  @Test
  void getPropertyNamesKeepsInsertionOrder() {
    assertThat(document.getPropertyNames()).containsExactlyElementsOf(PROPERTIES);
  }

  @Test
  void toMapKeepsInsertionOrder() {
    assertThat(document.toMap(false).keySet()).containsExactlyElementsOf(PROPERTIES);
  }

  @Test
  void propertiesAsMapKeepsInsertionOrderAndIsNotEmpty() {
    // Also covers the inherited ImmutableDocument.propertiesAsMap(), which answered an empty map for a remote
    // record because it has no database and no buffer to deserialize from
    assertThat(document.propertiesAsMap().keySet()).containsExactlyElementsOf(PROPERTIES);
  }

  @Test
  void modifyCarriesTheOrderIntoTheMutableCopy() {
    final RemoteMutableDocument mutable = (RemoteMutableDocument) document.modify();
    assertThat(mutable.getPropertyNames()).containsExactlyElementsOf(PROPERTIES);
    assertThat(mutable.toMap(false).keySet()).containsExactlyElementsOf(PROPERTIES);
  }

  @Test
  void getPropertyNamesIsASnapshotNotAView() {
    // The contract on Document.getPropertyNames() asks for an unmodifiable snapshot; a keySet() view is neither
    assertThat(document.getPropertyNames()).isNotSameAs(document.getPropertyNames());
  }
}
