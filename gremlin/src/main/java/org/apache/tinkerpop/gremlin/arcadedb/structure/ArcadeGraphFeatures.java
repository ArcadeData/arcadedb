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
package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class ArcadeGraphFeatures implements Graph.Features {

  protected GraphFeatures          graphFeatures          = new ArcadeGraphGraphFeatures();
  protected VertexFeatures         vertexFeatures         = new ArcadeVertexFeatures();
  protected EdgeFeatures           edgeFeatures           = new ArcadeEdgeFeatures();
  protected VertexPropertyFeatures vertexPropertyFeatures = new ArcadeVertexPropertyFeatures();
  protected EdgePropertyFeatures   edgePropertyFeatures   = new ArcadeEdgePropertyFeatures();

  @Override
  public GraphFeatures graph() {
    return graphFeatures;
  }

  @Override
  public VertexFeatures vertex() {
    return vertexFeatures;
  }

  @Override
  public EdgeFeatures edge() {
    return edgeFeatures;
  }

  @Override
  public String toString() {
    return StringFactory.featureString(this);
  }

  public static class ArcadeGraphGraphFeatures implements GraphFeatures {

    private final VariableFeatures variableFeatures = new ArcadeVariableFeatures();

    @Override
    public boolean supportsConcurrentAccess() {
      return true;
    }

    @Override
    public boolean supportsComputer() {
      return false;
    }

    @Override
    public boolean supportsThreadedTransactions() {
      return false;
    }

    @Override
    public VariableFeatures variables() {
      return variableFeatures;
    }

  }

  public static class ArcadeElementFeatures implements ElementFeatures {
    @Override
    public boolean supportsNullPropertyValues() {
      return false;
    }

    @Override
    public boolean supportsNumericIds() {
      return false;
    }

    @Override
    public boolean supportsCustomIds() {
      return false;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
      return false;
    }

    @Override
    public boolean supportsUuidIds() {
      return false;
    }

    @Override
    public boolean supportsAnyIds() {
      return false;
    }

    @Override
    public boolean willAllowId(Object id) {
      return false;
    }

    @Override
    public boolean supportsStringIds() {
      return true;
    }
  }

  public class ArcadeVertexFeatures extends ArcadeElementFeatures implements VertexFeatures {
    @Override
    public boolean supportsNullPropertyValues() {
      return false;
    }

    @Override
    public VertexPropertyFeatures properties() {
      return vertexPropertyFeatures;
    }

    @Override
    public VertexProperty.Cardinality getCardinality(String key) {
      return VertexProperty.Cardinality.single;
    }

    @Override
    public boolean supportsMetaProperties() {
      return false;
    }

    @Override
    public boolean supportsMultiProperties() {
      return false;
    }
  }

  public class ArcadeEdgeFeatures extends ArcadeElementFeatures implements EdgeFeatures {
    @Override
    public boolean supportsNullPropertyValues() {
      return false;
    }

    @Override
    public EdgePropertyFeatures properties() {
      return edgePropertyFeatures;
    }
  }

  public static class ArcadeVertexPropertyFeatures extends ArcadeDataTypeFeatures implements VertexPropertyFeatures {
    @Override
    public boolean supportsNullPropertyValues() {
      return false;
    }

    @Override
    public boolean supportsAnyIds() {
      return false;
    }

    @Override
    public boolean supportsCustomIds() {
      return false;
    }

    @Override
    public boolean supportsNumericIds() {
      return true;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
      return false;
    }

    @Override
    public boolean supportsUuidIds() {
      return false;
    }

    @Override
    public boolean willAllowId(Object id) {
      return false;
    }
  }

  public static class ArcadeEdgePropertyFeatures extends ArcadeDataTypeFeatures implements EdgePropertyFeatures {
  }

  public static class ArcadeDataTypeFeatures implements DataTypeFeatures {

    @Override
    public boolean supportsIntegerArrayValues() {
      return false;
    }

    @Override
    public boolean supportsFloatArrayValues() {
      return false;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
      return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
      return false;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
      return false;
    }

    @Override
    public boolean supportsLongArrayValues() {
      return false;
    }

    @Override
    public boolean supportsSerializableValues() {
      return false;
    }
  }

}
