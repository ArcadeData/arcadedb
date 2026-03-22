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
package com.arcadedb.query.opencypher.ast;

/**
 * Path mode for variable-length traversals per GQL/Cypher 25 standard.
 * Controls how cycles are handled during path expansion.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum PathMode {
  /** No restrictions — vertices and edges can be revisited. Requires an explicit max hop bound. */
  WALK,
  /** Edges must be unique per path; vertices can repeat. Default for variable-length patterns. */
  TRAIL,
  /** No vertex repeated per path — strongest restriction. Implies edge uniqueness. */
  ACYCLIC
}
