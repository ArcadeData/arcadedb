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
package com.arcadedb.census;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The compiled classes every census of this module ranges over, and the call-site scan they share.
 * <p>
 * The range is the set of classes each module PRODUCES: every class file of every module with main sources, whatever its
 * package (#11273). A filter on a package prefix would let a call site out of range for where its file sits, which is the
 * shape the censuses exist to close. The modules are listed from the source tree, so a module with main sources the build
 * forgot to put on this module's classpath fails every census rather than escaping it.
 * <p>
 * A call site is keyed {@code class#method}, the class relative to {@code com/arcadedb} when it is in that package and its
 * full path otherwise. A lambda counts for the method it is written in and an anonymous or local class for the class it is
 * written in, so a call cannot leave its key by moving into either; a named nested class keeps its own name
 * ({@code Outer$Nested#method}), so a call in it cannot hide under a reviewed key of the class around it (#11270).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CompiledClasses {
  static final         Path                REACTOR_ROOT = Path.of(System.getProperty("basedir", "."), "..");
  private static final String              PREFIX       = "com/arcadedb/";
  /** An {@code arcadedb-server} dependency declared {@code provided}: the module plugs into a server it does not own. */
  private static final Pattern             PROVIDED_SERVER_DEPENDENCY = Pattern.compile(
      "<artifactId>arcadedb-server</artifactId>(?:(?!</dependency>).)*?<scope>provided</scope>", Pattern.DOTALL);
  /** Modules with main sources the default build does not compile, each with why the censuses do not read them. */
  static final         Map<String, String> NOT_BUILT    = Map.of(//
      "native", "built only under the 'native' profile: GraalVM image-build features, no route");

  private static volatile Map<String, Map<String, byte[]>> byModule;
  private static volatile Map<String, byte[]>              everyModule;

  /** What a call site is a call of: the owner, name and descriptor of the instruction or of the method reference. */
  @FunctionalInterface
  interface CallMatcher {
    boolean matches(String owner, String name, String descriptor);
  }

  private CompiledClasses() {
  }

  /** Every class file of every module with main sources, keyed by its path in the module's root. Read once per JVM. */
  static Map<String, byte[]> ofEveryModule() throws IOException, URISyntaxException {
    Map<String, byte[]> classes = everyModule;
    if (classes == null) {
      synchronized (CompiledClasses.class) {
        classes = everyModule;
        if (classes == null) {
          final Map<String, byte[]> all = new HashMap<>();
          for (final Map<String, byte[]> module : byModule().values())
            all.putAll(module);
          classes = Collections.unmodifiableMap(all);
          everyModule = classes;
        }
      }
    }
    return classes;
  }

  /** The classes of {@link #ofEveryModule()}, by the name of the module that produces them. Read once per JVM. */
  static Map<String, Map<String, byte[]>> byModule() throws IOException, URISyntaxException {
    Map<String, Map<String, byte[]>> modules = byModule;
    if (modules == null) {
      synchronized (CompiledClasses.class) {
        modules = byModule;
        if (modules == null) {
          final Map<String, Map<String, byte[]>> read = new TreeMap<>();
          for (final Map.Entry<String, Path> root : classRootsOfEveryModule().entrySet())
            read.put(root.getKey(), Collections.unmodifiableMap(read(Map.of(root.getKey(), root.getValue()))));
          modules = Collections.unmodifiableMap(read);
          byModule = modules;
        }
      }
    }
    return modules;
  }

  /**
   * The modules that plug into a server they do not own: their {@code pom.xml} declares {@code arcadedb-server} in
   * {@code provided} scope, which is what makes a module an extension of the server (the wire-protocol rule of the root
   * {@code CLAUDE.md}). Read from the poms, where the scope is defined.
   */
  static Set<String> modulesPluggedIntoTheServer() throws IOException {
    final Set<String> modules = new TreeSet<>();
    try (final Stream<Path> children = Files.list(REACTOR_ROOT)) {
      for (final Path dir : children.filter(Files::isDirectory).toList()) {
        final Path pom = dir.resolve("pom.xml");
        if (Files.isRegularFile(pom) && PROVIDED_SERVER_DEPENDENCY.matcher(Files.readString(pom, StandardCharsets.UTF_8)).find())
          modules.add(dir.getFileName().toString());
      }
    }
    return modules;
  }

  /**
   * The classes of this module's own tests, read the way {@link #ofEveryModule()} reads a module: the probes each census
   * control runs its scan against ({@link CensusProbe}, {@code ShapeProbes}, the out-of-package probe of #11273).
   */
  static Map<String, byte[]> ofTheProbes() throws IOException, URISyntaxException {
    final Map<String, byte[]> classes = new HashMap<>();
    read(Path.of(CompiledClasses.class.getProtectionDomain().getCodeSource().getLocation().toURI()), classes);
    return classes;
  }

  /**
   * The class root (a directory or a jar) of every module with main sources, found through one of its own source files,
   * so no naming convention decides which module a root belongs to.
   */
  static Map<String, Path> classRootsOfEveryModule() throws IOException, URISyntaxException {
    assertThat(REACTOR_ROOT.resolve("engine").resolve("pom.xml")).as("the census must start at the reactor root, or it checks "
        + "nothing").isRegularFile();

    final Map<String, Path> roots = new TreeMap<>();
    final List<String> missing = new ArrayList<>();
    try (final Stream<Path> modules = Files.list(REACTOR_ROOT)) {
      for (final Path module : modules.filter(Files::isDirectory).sorted().toList()) {
        final Path sources = module.resolve("src").resolve("main").resolve("java");
        final String name = module.getFileName().toString();
        if (!Files.isDirectory(sources) || NOT_BUILT.containsKey(name))
          continue;

        final String anchor = anchorClassOf(sources);
        final URL resource = CompiledClasses.class.getClassLoader().getResource(anchor);
        if (resource == null) {
          missing.add(name + " (" + anchor + ")");
          continue;
        }
        roots.put(name, rootOf(resource, anchor));
      }
    }
    assertThat(missing).as("modules with main sources this module's classpath does not carry: add each one to census/pom.xml")
        .isEmpty();
    assertThat(roots).as("the census must find the engine").containsKey("engine");
    return roots;
  }

  /** Every class file under each root, whatever its package, keyed by its path in the root. */
  static Map<String, byte[]> read(final Map<String, Path> roots) throws IOException {
    final Map<String, byte[]> classes = new HashMap<>();
    for (final Map.Entry<String, Path> root : roots.entrySet()) {
      final int before = classes.size();
      read(root.getValue(), classes);
      assertThat(classes.size()).as("no class read from module " + root.getKey() + " at " + root.getValue()).isGreaterThan(before);
    }
    return classes;
  }

  /** Every class file under {@code root}, a directory or a jar, whatever its package. */
  static void read(final Path root, final Map<String, byte[]> classes) throws IOException {
    if (Files.isDirectory(root))
      readTree(root, classes);
    else
      try (final FileSystem jar = FileSystems.newFileSystem(root)) {
        readTree(jar.getPath("/"), classes);
      }
  }

  private static void readTree(final Path root, final Map<String, byte[]> classes) throws IOException {
    try (final Stream<Path> files = Files.walk(root)) {
      files.filter(p -> p.toString().endsWith(".class") && !p.getFileName().toString().equals("module-info.class")).forEach(file -> {
        try {
          classes.put(root.relativize(file).toString().replace('\\', '/'), Files.readAllBytes(file));
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    }
  }

  /**
   * Every call site of a method {@code matcher} accepts, keyed {@code class#method}, with what was called there: an
   * instruction as {@code Owner.name}, a method reference as {@code Owner::name}.
   */
  static Map<String, Set<String>> callSitesOf(final Map<String, byte[]> classes, final CallMatcher matcher) {
    final Map<String, Set<String>> sites = new TreeMap<>();
    for (final Map.Entry<String, byte[]> entry : classes.entrySet()) {
      final String outerClass = keyOf(entry.getKey());
      new ClassReader(entry.getValue()).accept(new ClassVisitor(Opcodes.ASM9) {
        @Override
        public MethodVisitor visitMethod(final int access, final String methodName, final String descriptor, final String signature,
            final String[] exceptions) {
          final String key = outerClass + "#" + methodOf(methodName);
          return new MethodVisitor(Opcodes.ASM9) {
            @Override
            public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc,
                final boolean isInterface) {
              if (matcher.matches(owner, name, desc))
                sites.computeIfAbsent(key, k -> new TreeSet<>()).add(simpleNameOf(owner) + "." + name);
            }

            @Override
            public void visitInvokeDynamicInsn(final String name, final String desc, final Handle bootstrap,
                final Object... bootstrapArguments) {
              // A METHOD REFERENCE (owner::name) IS A HANDLE PASSED TO THE LAMBDA BOOTSTRAP, NOT A CALL
              for (final Object argument : bootstrapArguments)
                if (argument instanceof Handle handle && matcher.matches(handle.getOwner(), handle.getName(), handle.getDesc()))
                  sites.computeIfAbsent(key, k -> new TreeSet<>()).add(simpleNameOf(handle.getOwner()) + "::" + handle.getName());
            }
          };
        }
      }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }
    return sites;
  }

  /**
   * Every method of {@code classes} that calls a method {@code matcher} accepts, directly or through other methods of
   * {@code classes} that do, as {@code owner.name descriptor}. A call is resolved the way the JVM resolves it, up the super
   * types of the owner it names, so a call of an inherited helper ({@code this.registerRunningQuery(...)} declared by a
   * super class) reaches what the helper reaches. A lambda's body is its own method, reached through the method reference
   * of the method it is written in.
   */
  static Set<String> methodsReaching(final Map<String, byte[]> classes, final CallMatcher matcher) {
    final Map<String, List<String>> supertypes = new HashMap<>();
    final Set<String> declared = new HashSet<>();
    final Map<String, List<String[]>> calls = new HashMap<>();
    final Set<String> reaching = new HashSet<>();
    for (final byte[] bytes : classes.values()) {
      final ClassReader reader = new ClassReader(bytes);
      final String className = reader.getClassName();
      final List<String> parents = new ArrayList<>(List.of(reader.getInterfaces()));
      if (reader.getSuperName() != null)
        parents.add(reader.getSuperName());
      supertypes.put(className, parents);
      reader.accept(new ClassVisitor(Opcodes.ASM9) {
        @Override
        public MethodVisitor visitMethod(final int access, final String methodName, final String descriptor, final String signature,
            final String[] exceptions) {
          final String method = className + "." + methodName + " " + descriptor;
          declared.add(method);
          final List<String[]> made = calls.computeIfAbsent(method, k -> new ArrayList<>());
          return new MethodVisitor(Opcodes.ASM9) {
            @Override
            public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc,
                final boolean isInterface) {
              if (matcher.matches(owner, name, desc))
                reaching.add(method);
              else
                made.add(new String[] { owner, name, desc });
            }

            @Override
            public void visitInvokeDynamicInsn(final String name, final String desc, final Handle bootstrap,
                final Object... bootstrapArguments) {
              for (final Object argument : bootstrapArguments)
                if (argument instanceof Handle handle)
                  if (matcher.matches(handle.getOwner(), handle.getName(), handle.getDesc()))
                    reaching.add(method);
                  else
                    made.add(new String[] { handle.getOwner(), handle.getName(), handle.getDesc() });
            }
          };
        }
      }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }

    boolean grew = true;
    while (grew) {
      grew = false;
      for (final Map.Entry<String, List<String[]>> method : calls.entrySet())
        if (!reaching.contains(method.getKey()))
          for (final String[] call : method.getValue())
            if (resolvesInto(call[0], call[1] + " " + call[2], supertypes, declared, reaching)) {
              reaching.add(method.getKey());
              grew = true;
              break;
            }
    }
    return reaching;
  }

  /** Whether the method {@code nameAndDesc} named on {@code owner} resolves, up its super types, to one in {@code target}. */
  private static boolean resolvesInto(final String owner, final String nameAndDesc, final Map<String, List<String>> supertypes,
      final Set<String> declared, final Set<String> target) {
    final Deque<String> pending = new ArrayDeque<>(List.of(owner));
    final Set<String> seen = new HashSet<>();
    while (!pending.isEmpty()) {
      final String current = pending.pop();
      if (!seen.add(current))
        continue;
      final String method = current + "." + nameAndDesc;
      if (declared.contains(method))
        return target.contains(method);
      pending.addAll(supertypes.getOrDefault(current, List.of()));
    }
    return false;
  }

  /** {@code type} and every class or interface of {@code classes} whose super types reach it. */
  static Set<String> implementationsOf(final String type, final Map<String, byte[]> classes) {
    final Map<String, List<String>> supertypes = new HashMap<>();
    for (final byte[] bytes : classes.values()) {
      final ClassReader reader = new ClassReader(bytes);
      final List<String> parents = new ArrayList<>(List.of(reader.getInterfaces()));
      if (reader.getSuperName() != null)
        parents.add(reader.getSuperName());
      supertypes.put(reader.getClassName(), parents);
    }

    final Set<String> result = new HashSet<>();
    result.add(type);
    final Map<String, Boolean> memo = new HashMap<>();
    for (final String candidate : supertypes.keySet())
      if (reaches(candidate, type, supertypes, memo))
        result.add(candidate);
    return result;
  }

  /** The offenders of a census: call sites {@code reviewed} does not list, each with what was called there. */
  static List<String> unreviewed(final Map<String, Set<String>> callSites, final Map<String, String> reviewed) {
    final List<String> offenders = new ArrayList<>();
    for (final Map.Entry<String, Set<String>> site : callSites.entrySet())
      if (!reviewed.containsKey(site.getKey()))
        offenders.add(site.getKey() + " (" + String.join(", ", site.getValue()) + ")");
    return offenders;
  }

  /** The entries of {@code reviewed} that are no longer a call site, so the reviewed set cannot go stale. */
  static List<String> stale(final Map<String, Set<String>> callSites, final Map<String, String> reviewed) {
    final List<String> stale = new ArrayList<>();
    for (final String entry : new TreeSet<>(reviewed.keySet()))
      if (!callSites.containsKey(entry))
        stale.add(entry);
    return stale;
  }

  /** {@code com/arcadedb/a/C$1.class} -> {@code a/C}, {@code org/x/C.class} -> {@code org/x/C}. */
  static String keyOf(final String classFile) {
    final String outer = outerClassOf(classFile);
    return outer.startsWith(PREFIX) ? outer.substring(PREFIX.length()) : outer;
  }

  /** The class file of the first top-level class of a module's sources, as a resource name. */
  private static String anchorClassOf(final Path sources) throws IOException {
    try (final Stream<Path> files = Files.walk(sources)) {
      final Path first = files.filter(p -> p.toString().endsWith(".java"))
          .filter(p -> !p.getFileName().toString().equals("package-info.java") && !p.getFileName().toString().equals("module-info.java"))
          .sorted().findFirst().orElseThrow(() -> new AssertionError("no source file under " + sources));
      final String relative = sources.relativize(first).toString().replace('\\', '/');
      return relative.substring(0, relative.length() - ".java".length()) + ".class";
    }
  }

  private static Path rootOf(final URL resource, final String anchor) throws URISyntaxException {
    if ("jar".equals(resource.getProtocol())) {
      final String spec = resource.getPath();
      return Path.of(URI.create(spec.substring(0, spec.indexOf("!/"))));
    }
    Path root = Path.of(resource.toURI());
    for (int i = anchor.split("/").length; i > 0; i--)
      root = root.getParent();
    return root;
  }

  private static boolean reaches(final String from, final String type, final Map<String, List<String>> supertypes,
      final Map<String, Boolean> memo) {
    final Deque<String> pending = new ArrayDeque<>(List.of(from));
    final Set<String> seen = new HashSet<>();
    while (!pending.isEmpty()) {
      final String current = pending.pop();
      if (current.equals(type) || Boolean.TRUE.equals(memo.get(current))) {
        memo.put(from, true);
        return true;
      }
      if (seen.add(current))
        pending.addAll(supertypes.getOrDefault(current, List.of()));
    }
    memo.put(from, false);
    return false;
  }

  /**
   * {@code a/b/C$1.class} and {@code a/b/C$1Local.class} -> {@code a/b/C}, {@code a/b/C$Named$2.class} -> {@code a/b/C$Named}:
   * an anonymous or local class counts for the class it is written in, whose numbering moves with every edit, while a named
   * nested class keeps its own name, so its methods cannot take a reviewed key of the class around it.
   */
  static String outerClassOf(final String relative) {
    final String withoutExtension = relative.endsWith(".class") ? relative.substring(0, relative.length() - ".class".length()) : relative;
    int end = withoutExtension.indexOf('$');
    while (end >= 0 && end + 1 < withoutExtension.length() && !Character.isDigit(withoutExtension.charAt(end + 1)))
      end = withoutExtension.indexOf('$', end + 1);
    return end < 0 ? withoutExtension : withoutExtension.substring(0, end);
  }

  /** {@code lambda$execute$3} -> {@code execute}: a lambda counts for the method it is written in, whatever its number. */
  static String methodOf(final String methodName) {
    if (methodName.startsWith("lambda$")) {
      final int end = methodName.lastIndexOf('$');
      return end > "lambda$".length() ? methodName.substring("lambda$".length(), end) : methodName;
    }
    return methodName;
  }

  private static String simpleNameOf(final String owner) {
    return owner.substring(owner.lastIndexOf('/') + 1);
  }
}
