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
package com.arcadedb.function.sql.geo;

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * SQL function geo.asGeoJson: returns the GeoJSON representation of a geometry.
 * Uses JTS for geometry parsing and manual serialization via JSONObject/JSONArray.
 *
 * <p>Usage: {@code geo.asGeoJson(<geometry>)}</p>
 * <p>Returns: GeoJSON string</p>
 */
public class SQLFunctionGeoAsGeoJson extends SQLFunctionAbstract {
  public static final String NAME = "geo.asGeoJson";

  public SQLFunctionGeoAsGeoJson() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {
    if (iParams == null || iParams.length < 1 || iParams[0] == null)
      return null;

    final Geometry geometry = GeoUtils.parseJtsGeometry(iParams[0]);
    if (geometry == null)
      return null;

    return toGeoJson(geometry).toString();
  }

  private JSONObject toGeoJson(final Geometry geometry) {
    final JSONObject result = new JSONObject();

    if (geometry instanceof Point point) {
      result.put("type", "Point");
      result.put("coordinates", coordToArray(point.getCoordinate()));
    } else if (geometry instanceof LineString lineString) {
      result.put("type", "LineString");
      result.put("coordinates", coordsToArray(lineString.getCoordinates()));
    } else if (geometry instanceof Polygon polygon) {
      result.put("type", "Polygon");
      result.put("coordinates", polygonToArray(polygon));
    } else if (geometry instanceof MultiPoint multiPoint) {
      result.put("type", "MultiPoint");
      final JSONArray coords = new JSONArray();
      for (int i = 0; i < multiPoint.getNumGeometries(); i++)
        coords.put(coordToArray(((Point) multiPoint.getGeometryN(i)).getCoordinate()));
      result.put("coordinates", coords);
    } else if (geometry instanceof MultiLineString multiLineString) {
      result.put("type", "MultiLineString");
      final JSONArray coords = new JSONArray();
      for (int i = 0; i < multiLineString.getNumGeometries(); i++)
        coords.put(coordsToArray(multiLineString.getGeometryN(i).getCoordinates()));
      result.put("coordinates", coords);
    } else if (geometry instanceof MultiPolygon multiPolygon) {
      result.put("type", "MultiPolygon");
      final JSONArray coords = new JSONArray();
      for (int i = 0; i < multiPolygon.getNumGeometries(); i++)
        coords.put(polygonToArray((Polygon) multiPolygon.getGeometryN(i)));
      result.put("coordinates", coords);
    } else {
      // GeometryCollection fallback
      result.put("type", "GeometryCollection");
      final JSONArray geometries = new JSONArray();
      for (int i = 0; i < geometry.getNumGeometries(); i++)
        geometries.put(toGeoJson(geometry.getGeometryN(i)));
      result.put("geometries", geometries);
    }

    return result;
  }

  private JSONArray coordToArray(final Coordinate coord) {
    final JSONArray arr = new JSONArray();
    arr.put(coord.x);
    arr.put(coord.y);
    return arr;
  }

  private JSONArray coordsToArray(final Coordinate[] coords) {
    final JSONArray arr = new JSONArray();
    for (final Coordinate coord : coords)
      arr.put(coordToArray(coord));
    return arr;
  }

  private JSONArray ringToArray(final LineString ring) {
    return coordsToArray(ring.getCoordinates());
  }

  private JSONArray polygonToArray(final Polygon polygon) {
    final JSONArray rings = new JSONArray();
    rings.put(ringToArray(polygon.getExteriorRing()));
    for (int i = 0; i < polygon.getNumInteriorRing(); i++)
      rings.put(ringToArray(polygon.getInteriorRingN(i)));
    return rings;
  }

  @Override
  public String getSyntax() {
    return "geo.asGeoJson(<geometry>)";
  }
}
