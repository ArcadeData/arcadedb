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
package com.arcadedb.gremlin.io.graphml;

/**
 * The GraphML element and attribute names, and TinkerPop's label keys, as TinkerPop's own (package-private) tokens spell them:
 * a file written by either side is read by the other.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class GraphMLConstants {
  static final String XML_SCHEMA_NAMESPACE_TAG       = "xsi";
  static final String DEFAULT_GRAPHML_SCHEMA_LOCATION = "http://graphml.graphdrawing.org/xmlns/1.1/graphml.xsd";
  static final String XML_SCHEMA_LOCATION_ATTRIBUTE  = "schemaLocation";
  static final String GRAPHML                        = "graphml";
  static final String GRAPHML_XMLNS                  = "http://graphml.graphdrawing.org/xmlns";
  static final String G                              = "G";
  static final String EDGEDEFAULT                    = "edgedefault";
  static final String DIRECTED                       = "directed";
  static final String KEY                            = "key";
  static final String FOR                            = "for";
  static final String ID                             = "id";
  static final String ATTR_NAME                      = "attr.name";
  static final String ATTR_TYPE                      = "attr.type";
  static final String GRAPH                          = "graph";
  static final String NODE                           = "node";
  static final String EDGE                           = "edge";
  static final String SOURCE                         = "source";
  static final String TARGET                         = "target";
  static final String DATA                           = "data";
  static final String LABEL_E                        = "labelE";
  static final String LABEL_V                        = "labelV";
  static final String STRING                         = "string";
  static final String FLOAT                          = "float";
  static final String DOUBLE                         = "double";
  static final String LONG                           = "long";
  static final String BOOLEAN                        = "boolean";
  static final String INT                            = "int";
  static final String VERTEX_SUFFIX                  = "V";
  static final String EDGE_SUFFIX                    = "E";

  private GraphMLConstants() {
  }
}
