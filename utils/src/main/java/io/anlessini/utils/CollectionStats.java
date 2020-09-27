package io.anlessini.utils;

import com.amazonaws.services.dynamodbv2.document.Item;
import io.anserini.collection.DocumentCollection;
import io.anserini.collection.FileSegment;
import io.anserini.collection.SourceDocument;
import io.anserini.index.IndexArgs;
import io.anserini.index.generator.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.kohsuke.args4j.*;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class CollectionStats {
  private static final Logger LOG = LogManager.getLogger(CollectionStats.class);

  public static class StatsArgs {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "Location of input collection.")
    public String input;

    @Option(name = "-threads", metaVar = "[num]", usage = "Number of threads.")
    public int threads = 8;

    @Option(name = "-collection", metaVar = "[class]", required = true,
        usage = "Collection class in package 'io.anserini.collection'.")
    public String collectionClass;

    @Option(name = "-generator", metaVar = "[class]",
        usage = "Document generator class in package 'io.anserini.index.generator'.")
    public String generatorClass = "DefaultLuceneDocumentGenerator";
  }

  private final StatsArgs args;
  private final Path collectionPath;
  private final Class collectionClass;
  private final Class generatorClass;
  private final Counters counters;
  private final DocumentCollection collection;
  private final Collection<Map<String, Stats>> allFieldStats;
  private final Set<String> fieldNames;

  public static final class Stats {
    public final BigInteger count;
    public final long max;
    public final long min;
    public final BigInteger avg;

    public Stats() {
      this(BigInteger.ZERO, Long.MIN_VALUE, Long.MAX_VALUE, BigInteger.ZERO);
    }

    public Stats(BigInteger count, long max, long min, BigInteger avg) {
      this.count = count;
      this.max = max;
      this.min = min;
      this.avg = avg;
    }

    public Stats addPoint(long point) {
      long newMax = Math.max(point, max);
      long newMin = Math.min(point, min);
      BigInteger newCount = count.add(BigInteger.ONE);
      BigInteger newAvg = ((avg.multiply(count)).add(BigInteger.valueOf(point))).divide(newCount);

      return new Stats(newCount, newMax, newMin, newAvg);
    }

    public static Stats combine(Stats first, Stats second) {
      long newMax = Math.max(first.max, second.max);
      long newMin = Math.min(first.min, second.min);
      BigInteger newCount = first.count.add(second.count);
      BigInteger newAvg = newCount.equals(BigInteger.ZERO) ? BigInteger.ZERO
          : ((first.avg.multiply(first.count)).add(second.avg.multiply(second.count))).divide(newCount);

      return new Stats(newCount, newMax, newMin, newAvg);
    }
  }

  public final class Counters {
    /**
     * Counter for successfully imported documents
     */
    public AtomicLong imported = new AtomicLong();

    /**
     * Counter for empty documents that are not imported
     */
    public AtomicLong empty = new AtomicLong();

    /**
     * Counter for unindexable documents
     */
    public AtomicLong unindexable = new AtomicLong();

    /**
     * Counter for skipped documents. These are cases documents are skipped as part of normal
     * processing logic, e.g., using a whitelist, not indexing retweets or deleted tweets.
     */
    public AtomicLong skipped = new AtomicLong();

    /**
     * Counter for unexpected errors
     */
    public AtomicLong errors = new AtomicLong();
  }

  public CollectionStats(StatsArgs args) throws Exception {
    this.args = args;

    collectionPath = Paths.get(args.input);
    if (!Files.exists(collectionPath) || !Files.isReadable(collectionPath) || !Files.isDirectory(collectionPath)) {
      throw new RuntimeException("Document directory " + collectionPath.toString() + " does not exist or is not readable, please check the path");
    }

    generatorClass = Class.forName("io.anserini.index.generator." + args.generatorClass);
    collectionClass = Class.forName("io.anserini.collection." + args.collectionClass);

    collection = (DocumentCollection) collectionClass.getConstructor(Path.class).newInstance(collectionPath);

    counters = new Counters();
    allFieldStats = new ConcurrentLinkedQueue<>();
    fieldNames = Collections.newSetFromMap(new ConcurrentHashMap<>());
  }

  public void run() {
    Configurator.setRootLevel(Level.INFO);
    LOG.info("=================== Collection Stats ==================");

    final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(args.threads);
    LOG.info("Thread pool with " + args.threads + " threads initialized.");

    LOG.info("Initializing collection in " + collectionPath.toString());
    final List segmentPaths = collection.getSegmentPaths();
    final int segmentCnt = segmentPaths.size();
    LOG.info(String.format("%,d %s found", segmentCnt, (segmentCnt == 1 ? "file" : "files" )));

    LOG.info("Computing stats...");
    for (int i = 0; i < segmentCnt; i++) {
      executor.execute(new ComputingThread((Path) segmentPaths.get(i), collection));
    }

    executor.shutdown();

    try {
      // Wait for existing tasks to terminate
      while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        if (segmentCnt == 1) {
          LOG.info(String.format("%,d documents imported", counters.imported.get()));
        } else {
          LOG.info(String.format("%.2f%% of files completed, %,d documents imported",
              (double) executor.getCompletedTaskCount() / segmentCnt * 100.0d, counters.imported.get()));
        }
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }

    LOG.info("=================== Collection Stats ==================");
    LOG.info(String.format("%-20s %10s %10s %10s %10s %10s %10s", "Field", "Max(bytes)", "Min(bytes)", "Avg(bytes)", "Max(KB)", "Min(KB)", "Avg(KB)"));
    for (String fieldName: fieldNames) {
      Stats stats = allFieldStats.stream().map(m -> m.getOrDefault(fieldName, new Stats())).reduce(Stats::combine).get();
      LOG.info(String.format("%-20s %,10d %,10d %,10d %,10d %,10d %,10d",
          fieldName, stats.max, stats.min, stats.avg.longValueExact(),
          stats.max / 1024, stats.min / 1024, stats.avg.longValueExact() / 1024));
    }
    Stats totalItemStats = allFieldStats.stream().map(m -> m.getOrDefault("TOTAL_ITEM_SIZE", new Stats())).reduce(Stats::combine).get();
    LOG.info(String.format("%-20s %,10d %,10d %,10d %,10d %,10d %,10d",
        "total", totalItemStats.max, totalItemStats.min, totalItemStats.avg.longValueExact(),
        totalItemStats.max / 1024, totalItemStats.min / 1024, totalItemStats.avg.longValueExact() / 1024));
  }

  private final class ComputingThread extends Thread {
    private final Path input;
    private final DocumentCollection collection;
    private FileSegment<SourceDocument> fileSegment;

    private ComputingThread(Path input, DocumentCollection collection) {
      this.input = input;
      this.collection = collection;
      setName(input.getFileName().toString());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      try {
        LuceneDocumentGenerator generator =
            (LuceneDocumentGenerator) generatorClass.getDeclaredConstructor(IndexArgs.class).newInstance(new IndexArgs());

        long cnt = 0;

        Map<String, Stats> fieldStats = new HashMap<>();

        // in order to call close() and clean up resources in case of exception
        fileSegment = collection.createFileSegment(input);
        for (SourceDocument d: fileSegment) {
          if (!d.indexable()) {
            counters.unindexable.incrementAndGet();
            continue;
          }

          Document doc;
          try {
            doc = generator.createDocument(d);
          } catch (GeneratorException e) {
            if (e instanceof EmptyDocumentException) {
              counters.empty.incrementAndGet();
            } else if (e instanceof SkippedDocumentException) {
              counters.skipped.incrementAndGet();
            } else if (e instanceof InvalidDocumentException) {
              counters.errors.incrementAndGet();
            } else {
              LOG.error("Unhandled exception in document generation", e);
            }
            continue;
          }

          long totalLength = 0;
          Item item = toDynamoDBItem(doc);
          for (Map.Entry<String, Object> entry: item.attributes()) {
            String fieldName = entry.getKey();
            fieldNames.add(fieldName);
            long fieldSize = 0;
            fieldSize += fieldName.getBytes(StandardCharsets.UTF_8).length;
            if (entry.getValue() instanceof String) {
              fieldSize += ((String) entry.getValue()).getBytes(StandardCharsets.UTF_8).length;
            } else {
              List<String> values = (List<String>) entry.getValue();
              for (String value: values) {
                fieldSize += value.getBytes(StandardCharsets.UTF_8).length;
              }
            }
            Stats fieldStat = fieldStats.getOrDefault(fieldName, new Stats());
            fieldStats.put(fieldName, fieldStat.addPoint(fieldSize));
            totalLength += fieldSize;
          }
          Stats fieldStat = fieldStats.getOrDefault("TOTAL_ITEM_SIZE", new Stats());
          fieldStats.put("TOTAL_ITEM_SIZE",fieldStat.addPoint(totalLength));
        }

        allFieldStats.add(fieldStats);

        int skipped = fileSegment.getSkippedCount();
        if (skipped > 0) {
          counters.skipped.addAndGet(skipped);
          LOG.warn(input.getParent().getFileName().toString() + File.separator +
              input.getFileName().toString() + ": " + skipped + " docs skipped.");
        }

        if (fileSegment.getErrorStatus()) {
          counters.errors.incrementAndGet();
          LOG.error(input.getParent().getFileName().toString() + File.separator +
              input.getFileName().toString() + ": error iterating through segment.");
        }

        LOG.debug(input.getParent().getFileName().toString() + File.separator +
            input.getFileName().toString() + ": " + cnt + " docs added.");
      } catch (Exception e) {
        LOG.error(Thread.currentThread().getName() + ": Unexpected exception:", e);
      } finally {
        if (fileSegment != null) {
          fileSegment.close();
        }
      }
    }

    public Item toDynamoDBItem(Document doc) {
      Item ret = new Item();
      Map<String, List<IndexableField>> documentFields = new HashMap<>();
      for (IndexableField field: doc.getFields()) {
        List<IndexableField> fields = documentFields.getOrDefault(field.name(), new LinkedList<>());
        fields.add(field);
        documentFields.put(field.name(), fields);
      }

      for (Map.Entry<String, List<IndexableField>> entry: documentFields.entrySet()) {
        String fieldName = entry.getKey();
        List<String> fieldValues = entry.getValue().stream()
            .map(IndexableField::stringValue)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        ret.with(fieldName, fieldValues);
      }

      // override single-value fields
      ret.with(IndexArgs.ID, doc.getField(IndexArgs.ID).stringValue());
      ret.with(IndexArgs.CONTENTS, doc.getField(IndexArgs.CONTENTS).stringValue());
      if (doc.getField(IndexArgs.RAW) != null) {
        ret.with(IndexArgs.RAW, doc.getField(IndexArgs.RAW).stringValue());
      }

      return ret;
    }
  }

  public static void main(String[] args) throws Exception {
    StatsArgs statsArgs = new StatsArgs();
    CmdLineParser parser = new CmdLineParser(statsArgs, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println("Example: " + StatsArgs.class.getSimpleName() +
          parser.printExample(OptionHandlerFilter.REQUIRED));
      System.exit(1);
    }

    new CollectionStats(statsArgs).run();
  }
}
