package experiments.baselines;

import help.PseudoDocument;
import help.Utilities;
import lucene.Index;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

public class BlancoEntityBaselines {
    private final IndexSearcher searcher;
    private final IndexSearcher stanfordIndexSearcher;
    private final HashMap<String, ArrayList<String>> paraRankings;
    private final HashMap<String,ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> entityQrels;
    private Map<String, Integer> corpusStats = new ConcurrentHashMap<>();
    private Map<String, Map<String, Integer>> runStats = new ConcurrentHashMap<>();
    private final ArrayList<String> runStrings;
    private final Set<String> entityPool;
    private final boolean parallel;
    private final DecimalFormat df;
    private final String rankingMethod, entityStat;


    public BlancoEntityBaselines(String indexDir,
                                 String stanfordIndexDir,
                                 String mainDir,
                                 String dataDir,
                                 String outputDir,
                                 String paraRunFile,
                                 String entityRunFile,
                                 String entityQrelFile,
                                 String corpusStatFile,
                                 String runStatFile,
                                 String entityPoolFile,
                                 String outFile,
                                 String rankingMethod,
                                 String entityStat,
                                 boolean parallel) {

        String entityRunFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String passageRunFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String entityQrelFilePath = mainDir + "/" + dataDir + "/" + entityQrelFile;
        String corpusStatFilePath = mainDir + "/" + dataDir + "/" + corpusStatFile;
        String runStatFilePath = mainDir + "/" + dataDir + "/" + runStatFile;
        String entityPoolFilePath = mainDir + "/" + dataDir + "/" + entityPoolFile;
        String outFilePath = mainDir + "/" + outputDir + "/" + outFile;

        this.runStrings = new ArrayList<>();
        this.parallel = parallel;
        this.rankingMethod = rankingMethod;
        this.entityStat = entityStat;

        System.out.println("Entity Statistic: " + entityStat);
        System.out.println("Ranking Method: " + rankingMethod);

        df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);

        System.out.print("Reading entity rankings...");
        entityRankings = Utilities.getRankings(entityRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading passage rankings...");
        paraRankings = Utilities.getRankings(passageRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelFilePath);
        System.out.println("[Done].");

        System.out.print("Reading corpus stat file...");
        try {
            corpusStats = Utilities.readMap(corpusStatFilePath);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");

        System.out.print("Reading run stat file...");
        try {
            runStats = Utilities.readMap(runStatFilePath);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");

        System.out.print("Setting up index for use...");
        searcher = new Index.Setup(indexDir).getSearcher();
        System.out.println("[Done].");

        System.out.print("Setting up NER index for use...");
        stanfordIndexSearcher = new Index.Setup(stanfordIndexDir).getSearcher();
        System.out.println("[Done].");

        System.out.print("Reading entity pool file....");
        entityPool = readEntityPoolFile(entityPoolFilePath);
        System.out.print("Number of entities read = " + entityPool.size());
        System.out.println("[Done].");


        feature(outFilePath);

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

    /**
     * Method to calculate the feature.
     * Works in parallel using Java 8 parallelStreams.
     * DEFAULT THREAD POOL SIE = NUMBER OF PROCESSORS
     * USE : System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "N") to set the thread pool size
     * @param outputFilePath String Path to the output file.
     */

    private  void feature(String outputFilePath) {
        //Get the set of queries
        Set<String> querySet = entityRankings.keySet();
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
            // Do in parallel
            querySet.parallelStream().forEach(this::doTask);
        } else {
            System.out.println("Using Sequential Streams.");

            // Do in serial
            ProgressBar pb = new ProgressBar("Progress", querySet.size());
            for (String q : querySet) {
                doTask(q);
                pb.step();
            }
            pb.close();
        }


        // Create the run file
        System.out.print("Writing to run file.....");
        Utilities.writeFile(runStrings, outputFilePath);
        System.out.println("[Done].");
        System.out.println("Run file written at: " + outputFilePath);
    }
    private void doTask(String queryId) {

        Map<String, Double> distribution = new HashMap<>();

        if (entityRankings.containsKey(queryId) && entityQrels.containsKey(queryId)) {

            // Get the set of entities retrieved for the query
            Set<String> retEntitySet = new HashSet<>(entityRankings.get(queryId));

            // Get the set of entities relevant for the query
            Set<String> relEntitySet = new HashSet<>(entityQrels.get(queryId));

            // Get the number of retrieved entities which are also relevant
            // Finding support passage for non-relevant entities makes no sense!!

            retEntitySet.retainAll(relEntitySet);

            Map<String, Integer> entityStatsForQuery = runStats.get(queryId);

            // Get the list of passages retrieved for the query
            ArrayList<String> allQueryParas = paraRankings.get(queryId);

            // Get the list of top-K passages retrieved for the query
            // We use K = 1000 here
            ArrayList<String> topKQueryParas = new ArrayList<>(allQueryParas.subList(0, Math.min(1000, allQueryParas.size())));

            // For every entity in this list of relevant entities do
            for (String entityId : retEntitySet) {



                // Find all passages among the top-K passages, mentioning the entity
                PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                        " ", topKQueryParas, searcher);

                if (d != null) {

                    if (rankingMethod.equalsIgnoreCase("freq")) {
                        distribution = EntityRanking.rankByFrequency(entityStatsForQuery, entityPool);
                    } else if (rankingMethod.equalsIgnoreCase("rarity")) {
                        distribution = EntityRanking.rankByRarity(corpusStats, entityPool);
                    } else if (rankingMethod.equalsIgnoreCase("comb")) {
                        distribution = EntityRanking.rankByComb(entityStatsForQuery, corpusStats, entityPool);
                    } else if (rankingMethod.equalsIgnoreCase("kld")) {
                        distribution = EntityRanking.rankByKLD(entityStatsForQuery, corpusStats, entityPool,
                                topKQueryParas.size());
                    }

                    // Score the passages in the pseudo-document for this entity using the frequency distribution of
                    // co-occurring entities
                    scoreDoc(queryId, d, distribution);
                }
            }
            if (parallel) {
                System.out.println("Done query: " + queryId);
            }
        }
    }


    private void scoreDoc(String queryId, @NotNull PseudoDocument d, Map<String, Double> freqMap) {
        // Get the entity corresponding to the pseudo-document
        String entityId = d.getEntity();
        HashMap<String, Double> scoreMap = new HashMap<>();

        // Get the list of documents in the pseudo-document corresponding to the entity
        ArrayList<Document> documents = d.getDocumentList();

        // For every document do
        for (Document doc : documents) {

            // Get the paragraph id of the document
            String paraId = doc.getField("id").stringValue();

            // Get the score of the document
            double score = getParaScore(paraId, freqMap);

            // Store the paragraph id and score in a HashMap
            scoreMap.put(paraId, score);
        }

        makeRunStrings(queryId, entityId, scoreMap);

    }

    /**
     * Method to find the score of a paragraph.
     * This method looks at all the entities in the paragraph and calculates the score from them.
     * For every entity in the paragraph, if the entity has a score from the entity context pseudo-document,
     * then sum over the entity scores and store the score in a HashMap.
     *
     * @param freqMap HashMap where Key = entity id and Value = score
     * @return Integer
     */

    private double getParaScore(@NotNull String paraId, Map<String, Double> freqMap) {

        double paraScore = 0.0d;
        Document doc = null;
        // Get the entities in the paragraph
        // Search the paraId in the NER index
        try {
            doc = Index.Search.searchIndex("Id", paraId, stanfordIndexSearcher);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        assert doc != null;
        ArrayList<String> docEntities = preProcess(String.join("\n", getDocEntities(doc)));

        if (docEntities.isEmpty()) {
            return 0;
        }


        // Make an ArrayList from the String array
        //ArrayList<String> pEntList = new ArrayList<>(Arrays.asList(Utilities.clean(doc.getField(entityField).stringValue().split(delimiter))));
        /* For every entity in the paragraph do */

        if (entityStat.equalsIgnoreCase("sum")) {
            paraScore = scoreDocBySum(docEntities, freqMap);
        } else if (entityStat.equalsIgnoreCase("mean")) {
            paraScore = scoreDocByMean(docEntities, freqMap);
        } else if (entityStat.equalsIgnoreCase("max")) {
            paraScore = scoreDocByMax(freqMap);
        } else if (entityStat.equalsIgnoreCase("min")) {
            paraScore = scoreDocByMin(freqMap);
        }
        return paraScore;
    }

    @NotNull
    private List<String> getDocEntities(@NotNull Document document) {
        List<String> entities = new ArrayList<>();
        String[] docEntities = document.get("Entity").split("\n");

        for (String e : docEntities) {
            if (!e.equals("")) {
                String s = e.split(":")[0];
                entities.add(s);
            }
        }
        return entities;
    }

    @NotNull
    private ArrayList<String> preProcess(String text) {

        // Remove all special characters such as - + ^ . : , ( )
        text = text.replaceAll("[\\-+.^*:,;=(){}\\[\\]\"]","");

        // Catch single character non-whitespace, delimited by a whitespace on either side
        text = text.replaceAll("\\s([^\\s])\\s","");

        // Get all words
        ArrayList<String> words = new ArrayList<>(Arrays.asList(text.split("\n")));
        words.removeAll(Collections.singleton(null));
        words.removeAll(Collections.singleton(""));

        return words;
    }

    private double scoreDocByMin(@NotNull Map<String, Double> freqMap) {
        Map.Entry<String, Double> minEntry = Collections.min(freqMap.entrySet(), Map.Entry.comparingByValue());
        return minEntry.getValue();
    }

    private double scoreDocByMax(@NotNull Map<String, Double> freqMap) {
        Map.Entry<String, Double> maxEntry = Collections.max(freqMap.entrySet(), Map.Entry.comparingByValue());
        return maxEntry.getValue();
    }

    private double scoreDocByMean(ArrayList<String> pEntList, Map<String, Double> freqMap) {
        return scoreDocBySum(pEntList, freqMap) / pEntList.size();
    }

    private double scoreDocBySum(@NotNull ArrayList<String> pEntList, Map<String, Double> freqMap) {
        double entityScore, paraScore = 0;
        for (String e : pEntList) {
            // Lookup this entity in the HashMap of frequencies for the entities
            // Sum over the scores of the entities to get the score for the passage
            // Store the passage score in the HashMap
            if (freqMap.containsKey(e)) {
                entityScore = freqMap.get(e);
                paraScore += entityScore;
            }
        }
        return paraScore;
    }

    /**
     * Method to make the run file strings.
     *
     * @param queryId  Query ID
     * @param scoreMap HashMap of the scores for each paragraph
     */

    private void makeRunStrings(String queryId, String entityId, HashMap<String, Double> scoreMap) {
        LinkedHashMap<String, Double> paraScore = Utilities.sortByValueDescending(scoreMap);
        String runFileString;
        int rank = 1;

        for (String paraId : paraScore.keySet()) {
            if (Double.isFinite(paraScore.get(paraId))) {
                double score = Double.parseDouble(df.format(paraScore.get(paraId)));
                if (score != 0) {
                    runFileString = queryId + "+" + entityId + " Q0 " + paraId + " " + rank + " " + score + " " + "BlancoEntityBaselines";
                    runStrings.add(runFileString);
                    rank++;
                }
            }
        }
    }


    /**
     * Main method.
     * @param args Command Line arguments
     */

    public static void main(@NotNull String[] args) {
        String indexDir = args[0];
        String stanfordIndexDir = args[1];
        String mainDir = args[2];
        String dataDir = args[3];
        String outputDir = args[4];
        String paraRunFile = args[5];
        String entityRunFile = args[6];
        String entityQrelFile = args[7];
        String corpusStatFile = args[8];
        String runStatFile = args[9];
        String entityPoolFile = args[10];
        String rankingMethod = args[11];
        String entityStat = args[12];
        boolean parallel = args[13].equalsIgnoreCase("true");

        if (!rankingMethod.equalsIgnoreCase("freq") &&
                !rankingMethod.equalsIgnoreCase("rarity") &&
                !rankingMethod.equalsIgnoreCase("comb") &&
                !rankingMethod.equalsIgnoreCase("kld")) {
            System.err.println("ERROR: Wrong ranking method. May be (freq|rarity|comb|kld). Try again.");
            System.exit(-1);
        }

        if (!entityStat.equalsIgnoreCase("sum") &&
                !entityStat.equalsIgnoreCase("mean") &&
                !entityStat.equalsIgnoreCase("max") &&
                !entityStat.equalsIgnoreCase("min")) {
            System.err.println("ERROR: Wrong entity statistic. May be (sum|mean|max|min). Try again.");
            System.exit(-1);
        }

        String outFile = "BlancoEntityBaselines-" + rankingMethod + "-" + entityStat + ".run";

        new BlancoEntityBaselines(indexDir, stanfordIndexDir, mainDir, dataDir, outputDir, paraRunFile, entityRunFile, entityQrelFile,
                corpusStatFile, runStatFile, entityPoolFile, outFile, rankingMethod, entityStat, parallel);
    }

}
