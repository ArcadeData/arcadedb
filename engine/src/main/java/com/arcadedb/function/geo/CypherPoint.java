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
package com.arcadedb.function.geo;

import java.util.LinkedHashMap;

/**
 * The value {@link CypherPointFunction} constructs: a Neo4j Point is a primitive property type, but ArcadeDB has no
 * dedicated Geometry runtime type yet (issue #4870), so a map of coordinate keys is what stands in for it. Being its
 * own class - rather than a plain {@link LinkedHashMap} - lets a property-value validator recognise and allow it
 * where an ordinary user-authored map is refused (issue #7629), without changing anything for the many places that
 * read a point by its {@code Map} interface (coordinate lookups, distance/bbox functions, serialization).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherPoint extends LinkedHashMap<String, Object> {
}
