package experiments.baselines;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import help.Utilities;
import lucene.Index;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

/**
 * Helper class for BlancoEntityBaseline.
 * Finds the number of passages in the TREC-CAR paragraphCorpus which contains an entity.
 * Stores the results in a HashMap and serializes the Map to disk for future usage.
 * Source of entities: Entity pool created from all entities in a passage ranking.
 *
 * @author Shubham Chatterjee
 * @version 6/30/2020
 */

public class EntityStat {
    private IndexSearcher searcher;
    private Set<String> entityPool = new HashSet<>();
    private HashMap<String, ArrayList<String>> paraRankings;
    private final AtomicInteger count = new AtomicInteger();

    public EntityStat(String indexDir, String idField, String entityField, String entityPoolFile, String outputFile, String cborFile, String passageRunFile, boolean parallel) {
        setUp(indexDir, passageRunFile, entityPoolFile, parallel);
        findStat(cborFile, outputFile, idField, entityField, parallel);
    }

    public EntityStat(String indexDir, String idField, String entityField, String entityPoolFile, String outputFile, String passageRunFile, boolean parallel) {
        setUp(indexDir, passageRunFile, entityPoolFile, parallel);
        findStat(outputFile, idField, entityField, parallel);
    }

    private void setUp(String indexDir, String passageRunFile, String entityPoolFile, boolean parallel) {

        System.out.print("Setting up index for use...");
        searcher = new Index.Setup(indexDir).getSearcher();
        System.out.println("[Done].");

        System.out.print("Reading passage rankings...");
        paraRankings = Utilities.getRankings(passageRunFile);
        System.out.println("[Done].");

        System.out.print("Making entity pool....");
        entityPool = readEntityPoolFile(entityPoolFile);
        //System.out.println(entityPool.size());
        System.out.println("[Done].");

        if (parallel) {
            System.out.println("Using Parallel Streams.");
            int parallelism = ForkJoinPool.commonPool().getParallelism();
            int numOfCores = Runtime.getRuntime().availableProcessors();
            System.out.println("Number of available processors = " + numOfCores);
            System.out.println("Number of threads generated = " + parallelism);

            if (parallelism == numOfCores - 1) {
                System.err.println("WARNING: USING ALL AVAILABLE PROCESSORS");
                System.err.println("USE: \"-Djava.util.concurrent.ForkJoinPool.common.parallelism=N\" " +
                        "to set the number of threads used");
            }
        }
    }

    @NotNull
    private Set<String> readEntityPoolFile(String entityPoolFile) {
        Set<String> entityPool = new HashSet<>();

        BufferedReader br = null;
        String line;

        try {
            br = new BufferedReader(new FileReader(entityPoolFile));
            while((line = br.readLine()) != null) {
               entityPool.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(br != null) {
                    br.close();
                } else {
                    System.out.println("Buffer has not been initialized!");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return entityPool;
    }

    private void findStat(String outputFile, String idField, String entityField, boolean parallel) {
        Map<String, Map<String, Integer>> stats = new ConcurrentHashMap<>();

        Set<String> querySet = paraRankings.keySet();
        StreamSupport.stream(querySet.spliterator(), parallel)
                .forEach(query -> {
                    ArrayList<String> topKQueryParas = new ArrayList<>(paraRankings.get(query).subList(0, Math.min(1000, paraRankings.get(query).size())));
                    Map<String, Integer> statsInner = new ConcurrentHashMap<>();
                    for (String paraId : topKQueryParas) {
                        Set<String> entitySet = new HashSet<>(entityPool);
                        Document doc;
                        try {
                            doc = Index.Search.searchIndex(idField, paraId, searcher);
                            assert doc != null;
                            //Set<String> docEntities = new HashSet<>(Arrays.asList(doc.get(entityField).split("\n")));
                            Set<String> docEntities = preProcess(String.join("\n", getDocEntities(doc, entityField)));
                            entitySet.retainAll(docEntities);
                            for (String e : entitySet) {
                                statsInner.compute(e, (t, oldV) -> (oldV == null) ? 1 : oldV + 1);
                            }
                        } catch (IOException | ParseException | NullPointerException e) {
                            e.printStackTrace();
                        }
                    }
                    stats.put(query, statsInner);
                    count.getAndIncrement();
                    System.out.println("Progress: " + count.get() + " of " + querySet.size());
                });

        System.out.print("Writing to file.....");
        try {
            Utilities.writeMap(stats, outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");
        System.out.println("File written at: " + outputFile);
    }


    private  void findStat(String cborFile, String outputFile,  String idField, String entityField, boolean parallel) {
        Map<String, Integer> stats = new ConcurrentHashMap<>();


        BufferedInputStream bis = null;
        try {
            bis = new BufferedInputStream(new FileInputStream(new File(cborFile)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        final Iterable<Data.Paragraph> paragraphIterable = DeserializeData.iterableParagraphs(bis);
        StreamSupport.stream(paragraphIterable.spliterator(), parallel)
                .forEach(paragraph -> {
                    String paraId = paragraph.getParaId();
                    Set<String> entitySet = new HashSet<>(entityPool);
                    Document doc;
                    try {
                        doc = Index.Search.searchIndex(idField, paraId, searcher);
                        if (doc != null) {
                            //Set<String> docEntities = new HashSet<>(Arrays.asList(doc.get(entityField).split("\n")));
                            Set<String> docEntities = preProcess(String.join("\n", getDocEntities(doc, entityField)));
                            entitySet.retainAll(docEntities);
                            for (String e : entitySet) {
                                stats.compute(e, (t, oldV) -> (oldV == null) ? 1 : oldV + 1);
                            }
                            count.getAndIncrement();
                            if (count.get() % 1000 == 0) {
                                System.out.println("Progress: " + count.get() + " of 29,794,697.");
                            }
                        }

                    } catch (IOException | ParseException | NullPointerException e) {
                        e.printStackTrace();
                    }
                });

        System.out.print("Writing to file.....");
        try {
            Utilities.writeMap(stats, outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");
        System.out.println("File written at: " + outputFile);


    }

    @NotNull
    private Set<String> getDocEntities(@NotNull Document document, String entityField) {
        Set<String> entities = new HashSet<>();
        String[] docEntities = document.get(entityField).split("\n");

        for (String e : docEntities) {
            if (!e.equals("")) {
                String s = e.split(":")[0];
                entities.add(s);
            }
        }
        return entities;
    }

    @NotNull
    private Set<String> preProcess(String text) {

        // Remove all special characters such as - + ^ . : , ( )
        text = text.replaceAll("[\\-+.^*:,;=(){}\\[\\]\"]","");

        // Catch single character non-whitespace, delimited by a whitespace on either side
        text = text.replaceAll("\\s([^\\s])\\s","");

        // Get all words
        Set<String> words = new HashSet<>(Arrays.asList(text.split("\n")));
        words.removeAll(Collections.singleton(null));
        words.removeAll(Collections.singleton(""));

        return words;
    }

    public static void main(@NotNull String[] args) {

        String cbor, passageRunFile;
        boolean parallel;

        String indexDir = args[0];
        String idField = args[1];
        String entityField = args[2];
        String entityPoolFile = args[3];
        String outputFile = args[4];
        String type = args[5];
        if (type.equalsIgnoreCase("corpus")) {
            cbor = args[6];
            passageRunFile = args[7];
            parallel = args[8].equalsIgnoreCase("true");
            new EntityStat(indexDir, idField, entityField, entityPoolFile, outputFile, cbor, passageRunFile, parallel);
        } else if (type.equalsIgnoreCase("run")) {
            passageRunFile = args[6];
            parallel = args[7].equalsIgnoreCase("true");
            new EntityStat(indexDir, idField, entityField, entityPoolFile, outputFile, passageRunFile, parallel);
        }

    }

}
