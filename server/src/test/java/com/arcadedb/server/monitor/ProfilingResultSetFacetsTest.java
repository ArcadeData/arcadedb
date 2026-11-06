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
package com.arcadedb.server.monitor;

import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.facet.FacetResultsSamples;
import com.arcadedb.query.sql.executor.InternalResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The profiler's wrapper hands the facets of the query it wraps through, or a recorded FACETS query would lose them. */
class ProfilingResultSetFacetsTest {

  @Test
  void facetsAreDelegated() {
    final FacetResults facets = FacetResultsSamples.allKinds();
    final InternalResultSet delegate = new InternalResultSet();
    delegate.setFacets(facets);
    final ProfilingResultSet wrapper = new ProfilingResultSet(delegate, null, "db", "sql", "SELECT FROM A FACETS b TERMS", System.nanoTime());
    assertThat(wrapper.getFacets()).isSameAs(facets);
    assertThat(new ProfilingResultSet(new InternalResultSet(), null, "db", "sql", "SELECT FROM A", System.nanoTime()).getFacets()).isNull();
  }
}
