/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;

public class ArcadeVariableFeatures implements Graph.Features.VariableFeatures {

  @Override
  public boolean supportsVariables() {
    return false;
  }

  @Override
  public boolean supportsBooleanArrayValues() {
    return false;
  }

  @Override
  public boolean supportsBooleanValues() {
    return false;
  }

  @Override
  public boolean supportsByteArrayValues() {
    return false;
  }

  @Override
  public boolean supportsByteValues() {
    return false;
  }

  @Override
  public boolean supportsDoubleArrayValues() {
    return false;
  }

  @Override
  public boolean supportsDoubleValues() {
    return false;
  }

  @Override
  public boolean supportsFloatArrayValues() {
    return false;
  }

  @Override
  public boolean supportsFloatValues() {
    return false;
  }

  @Override
  public boolean supportsIntegerArrayValues() {
    return false;
  }

  @Override
  public boolean supportsIntegerValues() {
    return false;
  }

  @Override
  public boolean supportsLongArrayValues() {
    return false;
  }

  @Override
  public boolean supportsLongValues() {
    return false;
  }

  @Override
  public boolean supportsMapValues() {
    return false;
  }

  @Override
  public boolean supportsMixedListValues() {
    return false;
  }

  @Override
  public boolean supportsSerializableValues() {
    return false;
  }

  @Override
  public boolean supportsStringArrayValues() {
    return false;
  }

  @Override
  public boolean supportsStringValues() {
    return false;
  }

  @Override
  public boolean supportsUniformListValues() {
    return false;
  }
}
