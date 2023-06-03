package com.arcadedb.index.vector;

import com.arcadedb.log.LogManager;
import com.github.jelmerk.knn.DistanceFunctions;
import com.github.jelmerk.knn.SearchResult;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;
import java.util.logging.*;
import java.util.stream.*;
import java.util.zip.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Example application that downloads the english fast-text word vectors, inserts them into an hnsw index and lets
 * you query them.
 */
public class FastTextRam {

  private static final String WORDS_FILE_URL = "https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.en.300.vec.gz";

  private static final Path TMP_PATH = Paths.get(System.getProperty("java.io.tmpdir"));

  public static void main(String[] args) throws Exception {
    HnswVectorIndexRAM<String, float[], Word, Float> hnswIndex;
    final long start = System.currentTimeMillis();

    Path indexPath = TMP_PATH.resolve("index.bin");
    if (Files.exists(indexPath)) {
      LogManager.instance().log(null, Level.SEVERE, "Loading index from file %s...", indexPath);
      hnswIndex = HnswVectorIndexRAM.load(indexPath);
      LogManager.instance().log(null, Level.SEVERE, "Index loaded in %ss", System.currentTimeMillis() - start);
    } else {
      final Path file = TMP_PATH.resolve("cc.en.300.vec.gz");
      if (!Files.exists(file)) {
        downloadFile(WORDS_FILE_URL, file);
      } else {
        LogManager.instance().log(null, Level.SEVERE, "Input file already downloaded. Using %s", file);
      }

      List<Word> words = loadWordVectors(file);

      LogManager.instance().log(null, Level.SEVERE, "Constructing index.");

      hnswIndex = HnswVectorIndexRAM.newBuilder(300, DistanceFunctions.FLOAT_INNER_PRODUCT, words.size()).withM(16).withEf(200).withEfConstruction(200).build();

      hnswIndex.addAll(words, (workDone, max) -> LogManager.instance()
          .log(null, Level.SEVERE, "Added %d out of %d words to the index (elapsed %ds).", workDone, max, (System.currentTimeMillis() - start) / 1000));

      long end = System.currentTimeMillis();
      long duration = end - start;

      LogManager.instance().log(null, Level.SEVERE, "Creating index with %d words took %d millis which is %d minutes.", hnswIndex.size(), duration,
          MILLISECONDS.toMinutes(duration));

      LogManager.instance().log(null, Level.SEVERE, "Saving index to disk (%s)...", indexPath);
      hnswIndex.save(indexPath);
      LogManager.instance().log(null, Level.SEVERE, "Index saved in %ds.", System.currentTimeMillis() - end);
    }

    //Index<String, float[], Word, Float> groundTruthIndex = hnswIndex.asExactIndex();

    //Console console = System.console();

    int k = 10;

    while (true) {
      System.out.println("Enter an english word : ");

      String input = "dog";  // console.readLine();

      final long startWord = System.currentTimeMillis();

      List<SearchResult<Word, Float>> approximateResults = hnswIndex.findNeighbors(input, k);

      LogManager.instance().log(null, Level.SEVERE, "Found similar words for '%s' in %dms", input, System.currentTimeMillis() - startWord);

//      final long startBruteForce = System.currentTimeMillis();
//
//      List<SearchResult<Word, Float>> groundTruthResults = groundTruthIndex.findNeighbors(input, k);
//
//      LogManager.instance().log(null, Level.SEVERE,"Found similar words using brute force for '%s' in %dms", input, System.currentTimeMillis() - startBruteForce);
//
      LogManager.instance().log(null, Level.SEVERE, "Most similar words found using HNSW index : ");
      for (SearchResult<Word, Float> result : approximateResults) {
        LogManager.instance().log(null, Level.SEVERE, "%s %.4f", result.item().id(), result.distance());
      }
//
//      LogManager.instance().log(null, Level.SEVERE,"Most similar words found using exact index: ");
//      for (SearchResult<Word, Float> result : groundTruthResults) {
//        LogManager.instance().log(null, Level.SEVERE,"%s %.4f", result.item().id(), result.distance());
//      }
//      int correct = groundTruthResults.stream().mapToInt(r -> approximateResults.contains(r) ? 1 : 0).sum();
//      LogManager.instance().log(null, Level.SEVERE,"Accuracy : %.4f", correct / (double) groundTruthResults.size());

      Thread.sleep(10000);
    }
  }

  private static void downloadFile(String url, Path path) throws IOException {
    LogManager.instance().log(null, Level.SEVERE, "Downloading %s to %s. This may take a while.", url, path);

    try (InputStream in = new URL(url).openStream()) {
      Files.copy(in, path);
    }
  }

  private static List<Word> loadWordVectors(Path path) throws IOException {
    LogManager.instance().log(null, Level.SEVERE, "Loading words from %s", path);

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path)), StandardCharsets.UTF_8))) {
      return reader.lines().skip(1).map(line -> {
        String[] tokens = line.split(" ");

        String word = tokens[0];

        float[] vector = new float[tokens.length - 1];
        for (int i = 1; i < tokens.length - 1; i++) {
          vector[i] = Float.parseFloat(tokens[i]);
        }

        return new Word(word, VectorUtils.normalize(vector)); // normalize the vector so we can do inner product search
      }).collect(Collectors.toList());
    }
  }

}
