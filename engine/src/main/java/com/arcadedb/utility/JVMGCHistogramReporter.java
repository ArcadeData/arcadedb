package com.arcadedb.utility;

import com.arcadedb.serializer.json.JSONObject;
import jdk.jfr.consumer.RecordedClass;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordingStream;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
import java.util.stream.*;

public class JVMGCHistogramReporter {
  private              RecordingStream                     rs;
  private              Map<RecordedClass, AllocationStats> stats;
  private final static JVMGCHistogramReporter              INSTANCE = new JVMGCHistogramReporter();

  public record ClassHistogramEntry(String className, long instances, long bytes, String source) {
  }

  private static class AllocationStats {
    long        count;
    long        bytes;
    Set<String> allocationSites = ConcurrentHashMap.newKeySet();
  }

  private JVMGCHistogramReporter() {
  }

  public static JVMGCHistogramReporter getInstance() {
    return INSTANCE;
  }

  public void startRecording() {
    if (rs != null) {
      rs.close();
      rs = null;
    }

    rs = new RecordingStream();
    rs.enable("jdk.ObjectAllocationInNewTLAB");
    rs.enable("jdk.ObjectAllocationOutsideTLAB");

    stats = new HashMap<>();

    rs.onEvent(event -> {
      RecordedClass klass = event.getClass("objectClass");

      // Get allocation size based on event type
      final long allocationSize = event.hasField("tlabSize")
          ? event.getLong("tlabSize")
          : event.getLong("allocationSize");

      // Get top stack trace frame
      String site;
      RecordedStackTrace stackTrace = event.getStackTrace();
      if (stackTrace != null && !stackTrace.getFrames().isEmpty()) {
        RecordedFrame frame = stackTrace.getFrames().get(0);
        site = frame.getMethod().getType().getName() + "." +
            frame.getMethod().getName() + ":" +
            frame.getLineNumber();
      } else {
        site = "unknown";
      }

      // Update stats atomically
      stats.compute(klass, (k, v) -> {
        if (v == null)
          v = new AllocationStats();
        v.count++;
        v.bytes += allocationSize;
        v.allocationSites.add(site);
        return v;
      });
    });

    rs.startAsync();
  }

  public List<ClassHistogramEntry> stopRecording(final int topN) {
    if (rs != null) {
      rs.close();
      rs = null;
    }

    return stats.entrySet().stream()
        .sorted(Comparator.comparingLong(e -> -e.getValue().count))
        .limit(topN)
        .map(entry -> new ClassHistogramEntry(
            entry.getKey().getName(),
            entry.getValue().count,
            entry.getValue().bytes,
            String.join("; ", entry.getValue().allocationSites)
        ))
        .toList();
  }

  public List<ClassHistogramEntry> getTopAllocatedObjects(final int topN) {
    try {
      final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      final ObjectName diagnosticCmdMBean = new ObjectName("com.sun.management:type=DiagnosticCommand");

      // Invoke diagnostic command to get class histogram
      // Invoke diagnostic command and split the result into lines
      String histogramOutput = (String) mBeanServer.invoke(
          diagnosticCmdMBean,
          "gcClassHistogram",
          new Object[] { new String[] { "-all" } },
          new String[] { String[].class.getName() }
      );

      final List<ClassHistogramEntry> entries = parseHistogram(histogramOutput);

      entries.sort((a, b) -> Long.compare(b.bytes(), a.bytes())); // Sort descending

      return entries.stream().limit(topN).collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve GC histogram", e);
    }
  }

  private List<ClassHistogramEntry> parseHistogram(final String histogramOutput) {
    final List<ClassHistogramEntry> entries = new ArrayList<>();
    final Pattern linePattern = Pattern.compile("^\\s*\\d+:\\s+(\\d+)\\s+(\\d+)\\s+(.+)$");
    final String[] lines = histogramOutput.split("\\R"); // Split by any newline format

    for (String line : lines) {
      if (line.startsWith(" num") || line.startsWith("----") || line.isBlank())
        continue;

      final Matcher matcher = linePattern.matcher(line);
      if (matcher.find()) {
        final long instances = Long.parseLong(matcher.group(1));
        final long bytes = Long.parseLong(matcher.group(2));
        final String className = matcher.group(3).replaceAll("\\s+\\(.*", ""); // Remove module/classloader info
        entries.add(new ClassHistogramEntry(className, instances, bytes, null));
      }
    }
    return entries;
  }

  public JSONObject generateJson(final List<ClassHistogramEntry> entries) {
    final JSONObject json = new JSONObject();
    for (ClassHistogramEntry entry : entries) {
      JSONObject entryJson = new JSONObject();
      entryJson.put("instances", entry.instances());
      entryJson.put("bytes", FileUtils.getSizeAsString(entry.bytes()));
      entryJson.put("source", entry.source());
      json.put(entry.className(), entryJson);
    }
    return json;
  }
}
