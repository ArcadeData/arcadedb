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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spatial4j builds its shape factory, binary codec and WKT/GeoJSON/Polyshape readers and writers with
 * {@code Class.getConstructors()} and {@code Constructor.newInstance()}. The GraalVM native image answers that only for the
 * constructors its reachability metadata registers, so without them {@code SpatialContext.GEO} cannot even be initialized and
 * creating a geospatial index failed there with "class ShapeFactoryImpl needs a constructor that takes: [SpatialContext,
 * SpatialContextFactory]" (#10423), while the JVM was fine.
 */
class Issue10423SpatialContextNativeMetadataTest {
  private static final Path METADATA = Path.of("..", "native", "src", "main", "resources", "META-INF", "native-image",
      "com.arcadedb", "arcadedb-native", "reachability-metadata.json");

  /** The plain context behind {@code SpatialContext.GEO}, which Lucene's spatial module and spatial4j itself reach for. */
  @Test
  void everyConstructorOfThePlainSpatialContextFactoryIsInTheNativeMetadata() throws IOException {
    final SpatialContextFactory factory = new SpatialContextFactory();
    assertThat(requiredConstructors(factory, new SpatialContext(factory))).contains(
        "org.locationtech.spatial4j.shape.impl.ShapeFactoryImpl(org.locationtech.spatial4j.context.SpatialContext,org.locationtech.spatial4j.context.SpatialContextFactory)");

    assertThat(missingFromMetadata(requiredConstructors(factory, new SpatialContext(factory))))
        .as("add a <init> entry for each of these to " + METADATA).isEmpty();
  }

  /** The JTS context {@link GeoUtils} builds, the one every geospatial index and function uses. */
  @Test
  void everyConstructorOfTheJtsSpatialContextFactoryIsInTheNativeMetadata() throws IOException {
    final Set<String> required = requiredConstructors(GeoUtils.getFactory(), GeoUtils.getSpatialContext());
    assertThat(required).anyMatch(c -> c.startsWith("org.locationtech.spatial4j.shape.jts.JtsShapeFactory("));

    assertThat(missingFromMetadata(required)).as("add a <init> entry for each of these to " + METADATA).isEmpty();
  }

  /** The (context, factory) constructor of every class the factory creates by reflection, as {@code Class(paramTypes)}. */
  private static Set<String> requiredConstructors(final SpatialContextFactory factory, final SpatialContext context) {
    factory.makeFormats(context);

    final List<Class<?>> classes = new ArrayList<>();
    classes.add(factory.shapeFactoryClass);
    classes.add(factory.binaryCodecClass);
    classes.addAll(factory.readers);
    classes.addAll(factory.writers);
    // a scan that finds nothing would pass for the wrong reason
    assertThat(classes).hasSizeGreaterThan(4);

    // the factory takes the (context, factory) constructor, or the no-arg one when a class has none
    final Set<String> required = new TreeSet<>();
    for (final Class<?> clazz : classes) {
      Constructor<?> chosen = null;
      for (final Constructor<?> constructor : clazz.getConstructors())
        if (constructor.getParameterCount() == 2 || (constructor.getParameterCount() == 0 && chosen == null))
          chosen = constructor;
      assertThat(chosen).as("a constructor spatial4j can call on " + clazz.getName()).isNotNull();
      required.add(clazz.getName() + "(" + String.join(",", Arrays.stream(chosen.getParameterTypes()).map(Class::getName).toList()) + ")");
    }
    return required;
  }

  private static Set<String> missingFromMetadata(final Set<String> required) throws IOException {
    assertThat(Files.isRegularFile(METADATA)).as(METADATA.toAbsolutePath()
        + " not found: run this test with the engine module as working directory, as Maven Surefire does").isTrue();
    final JSONArray reflection = new JSONObject(Files.readString(METADATA, StandardCharsets.UTF_8)).getJSONArray("reflection");

    final Set<String> registered = new HashSet<>();
    for (int i = 0; i < reflection.length(); i++) {
      final JSONObject entry = reflection.getJSONObject(i);
      if (!(entry.get("type", "") instanceof String type))
        continue;
      final JSONArray methods = entry.getJSONArray("methods", new JSONArray());
      for (int m = 0; m < methods.length(); m++) {
        final JSONObject method = methods.getJSONObject(m);
        if (!"<init>".equals(method.getString("name", "")))
          continue;
        final JSONArray parameterTypes = method.getJSONArray("parameterTypes", new JSONArray());
        final List<String> types = new ArrayList<>();
        for (int t = 0; t < parameterTypes.length(); t++)
          types.add(parameterTypes.getString(t));
        registered.add(type + "(" + String.join(",", types) + ")");
      }
    }

    final Set<String> missing = new TreeSet<>(required);
    missing.removeAll(registered);
    return missing;
  }
}
