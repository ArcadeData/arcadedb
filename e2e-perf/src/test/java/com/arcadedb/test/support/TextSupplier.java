package com.arcadedb.test.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class TextSupplier implements Supplier<String> {
  private static final Logger           logger = LoggerFactory.getLogger(TextSupplier.class.getName());
  private final        List<String>     words;
  private final        int              nomOfWords;
  private              Iterator<String> wordsIterator;

  public TextSupplier(int nomOfWords) {
    this.nomOfWords = nomOfWords;
    URL res = getClass().getClassLoader().getResource("words_en.txt");
    try {
      words = Files.readAllLines(Path.of(res.getPath()), Charset.defaultCharset());
      logger.info("total loaded english words: {}", words.size());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    wordsIterator = words.iterator();
  }

  /**
   * @return
   */
  @Override
  public String get() {
    StringBuilder text = new StringBuilder();
    IntStream.range(0, nomOfWords).forEach(i -> {
      if (!wordsIterator.hasNext()) {
        wordsIterator = words.iterator();
      }
      text.append(wordsIterator.next());
    });
    return text.toString();
  }

  /**
   * Just a main method  to test it
   */
  public static void main(String[] args) {
    TextSupplier textSupplier = new TextSupplier(10);
    System.out.println(textSupplier.get());
    System.out.println(textSupplier.get());
    System.out.println(textSupplier.get());
    System.out.println(textSupplier.get());
    System.out.println(textSupplier.get());
    System.out.println(textSupplier.get());
    System.out.println(textSupplier.get());
    System.out.println(textSupplier.get());
  }
}
