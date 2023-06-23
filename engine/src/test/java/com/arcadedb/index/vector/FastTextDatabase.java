package com.arcadedb.index.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
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
public class FastTextDatabase {

  private static final String WORDS_FILE_URL = "https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.en.300.vec.gz";

  private static final Path TMP_PATH = Paths.get(System.getProperty("java.io.tmpdir"));

  public static void main(String[] args) throws Exception {
    new FastTextDatabase();
  }

  public FastTextDatabase() throws IOException, InterruptedException, ReflectiveOperationException {
    final long start = System.currentTimeMillis();

    final Database database;
    final HnswVectorIndex persistentIndex;

    final DatabaseFactory factory = new DatabaseFactory("textdb");

    // TODO: REMOVE THIS
//    if (factory.exists())
//      factory.open().drop();

    if (factory.exists()) {
      database = factory.open();
      //LogManager.instance().log(this, Level.SEVERE, "Found existent database with %d words", database.countType("Word", false));

      persistentIndex = (HnswVectorIndex) database.getSchema().getIndexByName("Word[name,vector]");

    } else {
      database = factory.create();
      LogManager.instance().log(this, Level.SEVERE, "Creating new database");
      final VertexType vType = database.getSchema().createVertexType("Word");
      vType.getOrCreateProperty("name", Type.STRING);

      final Path file = TMP_PATH.resolve("cc.en.300.vec.gz");
      if (!Files.exists(file)) {
        downloadFile(WORDS_FILE_URL, file);
      } else {
        LogManager.instance().log(this, Level.SEVERE, "Input file already downloaded. Using %s", file);
      }

      List<Word> words = loadWordVectors(file);

      final HnswVectorIndexRAM<String, float[], Word, Float> hnswIndex = HnswVectorIndexRAM.newBuilder(300, DistanceFunctions.FLOAT_INNER_PRODUCT, words.size())
          .withM(16).withEf(200).withEfConstruction(200).build();

      hnswIndex.addAll(words, (workDone, max) -> LogManager.instance()
          .log(null, Level.SEVERE, "Added %d out of %d words to the index (elapsed %ds).", workDone, max, (System.currentTimeMillis() - start) / 1000));

      long end = System.currentTimeMillis();
      long duration = end - start;

      LogManager.instance().log(null, Level.SEVERE, "Creating index with %d words took %d millis which is %d minutes.", hnswIndex.size(), duration,
          MILLISECONDS.toMinutes(duration));

      LogManager.instance().log(null, Level.SEVERE, "Saving index into the database...");

      persistentIndex = hnswIndex.createPersistentIndex(database)//
          .withVertexType("Word").withEdgeType("Proximity").withVectorPropertyName("vector").withIdProperty("name").create();

      persistentIndex.save();

      LogManager.instance().log(null, Level.SEVERE, "Index saved in %ds.", System.currentTimeMillis() - end);
    }

    try {
      long end = System.currentTimeMillis();
      long duration = end - start;

      LogManager.instance().log(this, Level.SEVERE, "Creating index with took %d millis which is %d minutes.%n", duration, MILLISECONDS.toMinutes(duration));

      int k = 10;

      final Random random = new Random();

      for (int cycle = 0; ; ++cycle) {
        LogManager.instance().log(this, Level.SEVERE, "%d Selecting a random word from the database...", cycle);

        String input = "";
        final int wordNum = random.nextInt(2_000_000 - 1);
        final Iterator<Record> it = database.iterateType("Word", false);
        for (int i = 0; it.hasNext(); ++i) {
          final Record w = it.next();
          if (i == wordNum) {
            input = w.asVertex().getString("name");
            break;
          }
        }

        LogManager.instance().log(this, Level.SEVERE, "Searching for words similar to '%s'...", input);

        final long startWord = System.currentTimeMillis();

        List<SearchResult<Vertex, Float>> approximateResults = persistentIndex.findNeighbors(input, k);

        LogManager.instance().log(this, Level.SEVERE, "Found similar words for '%s' in %dms", input, System.currentTimeMillis() - startWord);

        for (SearchResult<Vertex, Float> result : approximateResults) {
          LogManager.instance().log(this, Level.SEVERE, "- %s %.4f", result.item().getString("name"), result.distance());
        }

        //Thread.sleep(1000);
      }
    } finally {
      database.close();
    }
  }

  private void downloadFile(String url, Path path) throws IOException {
    LogManager.instance().log(this, Level.SEVERE, "Downloading %s to %s. This may take a while", url, path);
    try (InputStream in = new URL(url).openStream()) {
      Files.copy(in, path);
    }
    LogManager.instance().log(this, Level.SEVERE, "Downloaded %s", FileUtils.getSizeAsString(path.toFile().length()));
  }

  private static List<Word> loadWordVectors(Path path) throws IOException {
    LogManager.instance().log(null, Level.SEVERE, "Loading words from %s", path);

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path)), StandardCharsets.UTF_8))) {
      return reader.lines().skip(1)//
          //.limit(100_000)//
          .map(line -> {
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
