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
package com.arcadedb.function.sql;

import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.util.AttributeImpl;
import org.tartarus.snowball.Among;
import org.tartarus.snowball.SnowballProgram;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A SQL function or method registered by CLASS is created with {@code getConstructor().newInstance()} on every call. The
 * GraalVM native image answers that only for the classes its reachability metadata registers, so a class registered here
 * and missing there works on the JVM and fails in the native binary only (found with #9495: {@code cchShortestPath()}).
 */
class NativeImageReflectionMetadataTest {
  private static final Path METADATA = Path.of("..", "native", "src", "main", "resources", "META-INF", "native-image",
      "com.arcadedb", "arcadedb-native", "reachability-metadata.json");

  @Test
  void everyClassRegisteredFunctionAndMethodIsInTheNativeMetadata() throws IOException {
    final Set<String> registered = new TreeSet<>();
    collectClasses(DefaultSQLFunctionFactory.getInstance().getFunctions(), registered);
    collectClasses(DefaultSQLMethodFactory.getInstance().getMethods(), registered);
    // a scan that finds nothing would pass for the wrong reason
    assertThat(registered).hasSizeGreaterThan(50);

    final Set<String> instantiable = noArgConstructorsInMetadata();
    final Set<String> missing = new TreeSet<>(registered);
    missing.removeAll(instantiable);

    assertThat(missing).as("add a no-arg <init> entry for each of these to " + METADATA).isEmpty();
  }

  /**
   * A full-text index creates its analyzer by the class name in its metadata, and Lucene creates every token attribute
   * through a reflective factory. Neither is covered by the GraalVM reachability-metadata repository for this Lucene
   * version, so without these entries no FULL_TEXT index could be created in the native image (#9495). A Lucene upgrade
   * that adds an analyzer or an attribute fails here instead of in the native binary only.
   */
  @Test
  void everyLuceneAnalyzerAndAttributeIsInTheNativeMetadata() throws Exception {
    final Set<String> required = new TreeSet<>();
    for (final Class<?> jarOf : new Class<?>[] { Analyzer.class, EnglishAnalyzer.class })
      for (final Class<?> clazz : classesInJarOf(jarOf))
        if ((Analyzer.class.isAssignableFrom(clazz) || AttributeImpl.class.isAssignableFrom(clazz)) && hasPublicNoArgConstructor(clazz))
          required.add(clazz.getName());
    // a scan that finds nothing would pass for the wrong reason
    assertThat(required).contains("org.apache.lucene.analysis.standard.StandardAnalyzer",
        "org.apache.lucene.analysis.tokenattributes.PackedTokenAttributeImpl").hasSizeGreaterThan(50);

    final Set<String> missing = new TreeSet<>(required);
    missing.removeAll(noArgConstructorsInMetadata());

    assertThat(missing).as("add a no-arg <init> entry for each of these to " + METADATA).isEmpty();
  }

  /**
   * A Snowball stemmer whose {@link Among} table names a method resolves it with {@code MethodHandles.findVirtual} in its
   * static initializer, so a stemmer missing from the metadata cannot even be loaded in the native image
   * ({@code FinnishAnalyzer} failed with "Could not initialize class FinnishStemmer", #9495).
   */
  @Test
  void everySnowballStemmerThatLooksUpMethodsIsInTheNativeMetadata() throws Exception {
    final Field methodField = Among.class.getDeclaredField("method");
    methodField.setAccessible(true);

    final Set<String> required = new TreeSet<>();
    for (final Class<?> clazz : classesInJarOf(SnowballProgram.class)) {
      if (!SnowballProgram.class.isAssignableFrom(clazz))
        continue;
      for (final Field field : clazz.getDeclaredFields())
        if (Modifier.isStatic(field.getModifiers()) && field.getType() == Among[].class) {
          field.setAccessible(true);
          for (final Among among : (Among[]) field.get(null))
            if (methodField.get(among) != null)
              required.add(clazz.getName());
        }
    }
    assertThat(required).contains("org.tartarus.snowball.ext.FinnishStemmer");

    final Set<String> missing = new TreeSet<>(required);
    missing.removeAll(typesWithMethodsInMetadata());

    assertThat(missing).as("register the methods its Among tables name, in " + METADATA).isEmpty();
  }

  private static List<Class<?>> classesInJarOf(final Class<?> anchor) throws IOException, URISyntaxException {
    final List<Class<?>> classes = new ArrayList<>();
    try (final JarFile jar = new JarFile(Path.of(anchor.getProtectionDomain().getCodeSource().getLocation().toURI()).toFile())) {
      for (final JarEntry entry : jar.stream().toList()) {
        final String name = entry.getName();
        if (!name.endsWith(".class") || name.contains("-") || name.startsWith("META-INF"))
          continue;
        try {
          classes.add(Class.forName(name.substring(0, name.length() - ".class".length()).replace('/', '.'), false,
              anchor.getClassLoader()));
        } catch (final ClassNotFoundException | LinkageError e) {
          // a class whose optional dependency is absent cannot be configured by name either
        }
      }
    }
    return classes;
  }

  private static boolean hasPublicNoArgConstructor(final Class<?> clazz) {
    if (!Modifier.isPublic(clazz.getModifiers()) || Modifier.isAbstract(clazz.getModifiers()))
      return false;
    try {
      clazz.getConstructor();
      return true;
    } catch (final NoSuchMethodException | LinkageError e) {
      return false;
    }
  }

  private static void collectClasses(final Map<String, Object> registry, final Set<String> classes) {
    for (final Object value : registry.values())
      if (value instanceof Class<?> clazz)
        classes.add(clazz.getName());
  }

  private static Set<String> typesWithMethodsInMetadata() throws IOException {
    final JSONArray reflection = new JSONObject(Files.readString(METADATA, StandardCharsets.UTF_8)).getJSONArray("reflection");
    final Set<String> result = new HashSet<>();
    for (int i = 0; i < reflection.length(); i++) {
      final JSONObject entry = reflection.getJSONObject(i);
      if (!entry.getJSONArray("methods", new JSONArray()).isEmpty())
        result.add(entry.getString("type", ""));
    }
    return result;
  }

  private static Set<String> noArgConstructorsInMetadata() throws IOException {
    final JSONArray reflection = new JSONObject(Files.readString(METADATA, StandardCharsets.UTF_8)).getJSONArray("reflection");
    final Set<String> result = new HashSet<>();
    for (int i = 0; i < reflection.length(); i++) {
      final JSONObject entry = reflection.getJSONObject(i);
      final JSONArray methods = entry.getJSONArray("methods", new JSONArray());
      for (int m = 0; m < methods.length(); m++) {
        final JSONObject method = methods.getJSONObject(m);
        if ("<init>".equals(method.getString("name", "")) && method.getJSONArray("parameterTypes", new JSONArray()).isEmpty())
          result.add(entry.getString("type", ""));
      }
    }
    return result;
  }
}
