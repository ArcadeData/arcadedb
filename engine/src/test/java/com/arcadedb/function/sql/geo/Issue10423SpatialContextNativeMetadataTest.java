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
    final Set<String> required = requiredConstructors(factory, new SpatialContext(factory));
    assertThat(required).contains(
        "org.locationtech.spatial4j.shape.impl.ShapeFactoryImpl(org.locationtech.spatial4j.context.SpatialContext,org.locationtech.spatial4j.context.SpatialContextFactory)");

    assertThat(missingFromMetadata(required)).as("add a <init> entry for each of these to " + METADATA).isEmpty();
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

    // SpatialContextFactory.makeClassInstance takes the first public (context, factory) constructor whose parameters accept
    // its arguments, else the public no-arg one. This mirrors it only while each class has exactly one candidate, so an
    // upgrade that adds an overload fails here instead of registering a constructor spatial4j might not pick
    final Set<String> required = new TreeSet<>();
    for (final Class<?> clazz : classes) {
      final List<Constructor<?>> twoArgs = Arrays.stream(clazz.getConstructors()).filter(c -> c.getParameterCount() == 2).toList();
      final List<Constructor<?>> noArgs = Arrays.stream(clazz.getConstructors()).filter(c -> c.getParameterCount() == 0).toList();
      final List<Constructor<?>> candidates = twoArgs.isEmpty() ? noArgs : twoArgs;
      assertThat(candidates).as("the constructor spatial4j calls on " + clazz.getName()).hasSize(1);
      required.add(clazz.getName() + "(" + String.join(",", Arrays.stream(candidates.getFirst().getParameterTypes()).map(Class::getName).toList()) + ")");
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
