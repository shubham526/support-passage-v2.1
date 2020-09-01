package experiments.ecd;

import help.PseudoDocument;
import help.Utilities;
import lucene.Index;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

/**
 * This class scores support passages for a query-entity pair using other frequently co-occurring entities.
 * For every entity, we first build a pseudo-document (of passages mentioning the entity).
 * Then we find the most frequently co-occurring entities with the given entity.
 * We score a passage in the pseudo-document about an entity by summing over the frequency of the co-occurring entities.
 * For more details, refer to the paper and the online appendix.
 * @author Shubham Chatterjee
 * @version 02/25/2019
 */

public class ECNFreq {
    private final IndexSearcher searcher;
    //HashMap where Key = queryID and Value = list of paragraphs relevant for the queryID
    private final HashMap<String, LinkedHashMap<String, Double>>  paraRankings = new HashMap<>();
    //HashMap where Key = queryID and Value = list of entities relevant for the queryID
    private final HashMap<String,ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> entityQrels;
    // ArrayList of run strings
    private final ArrayList<String> runStrings;
    private final boolean parallel;
    private final DecimalFormat df;

    /**
     * Constructor.
     * @param indexDir String Path to the index directory.
     * @param mainDir String Path to the TREC-CAR directory.
     * @param outputDir String Path to output directory within TREC-CAR directory.
     * @param dataDir String Path to data directory within TREC-CAR directory.
     * @param passageRunFile String Name of the passage run file within data directory.
     * @param entityRunFile String Name of the entity run file within data directory.
     * @param outFile String Name of the output run file. This will be stored in the output directory mentioned above.
     * @param entityQrelFile String Path to the entity ground truth file.
     * @param parallel Boolean Whether to run code in parallel or not.
     */

    public ECNFreq(String indexDir,
                   String mainDir,
                   String outputDir,
                   String dataDir,
                   String passageRunFile,
                   String entityRunFile,
                   String outFile,
                   String entityQrelFile,
                   boolean parallel) {


        String entityRunFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String passageRunFilePath = mainDir + "/" + dataDir + "/" + passageRunFile;
        String entityQrelFilePath = mainDir + "/" + dataDir + "/" + entityQrelFile;
        String outFilePath = mainDir + "/" + outputDir + "/" + outFile;
        this.runStrings = new ArrayList<>();
        this.parallel = parallel;

        df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);

        System.out.print("Reading entity rankings...");
        entityRankings = Utilities.getRankings(entityRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading passage rankings...");
        Utilities.getRankings(passageRunFilePath, paraRankings);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelFilePath);
        System.out.println("[Done].");

        System.out.print("Setting up index for use...");
        searcher = new Index.Setup(indexDir).getSearcher();
        System.out.println("[Done].");

        feature(outFilePath);

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

    /**
     * Helper method.
     * For every query, look at all the entities relevant for the query.
     * For every such entity, create a pseudo-document consisting of passages which contain this entity.
     * For every co-occurring entity in the pseudo-document, if the entity is also relevant for the query,
     * then find the frequency of this entity in the pseudo-document and score the passages using this frequency information.
     *
     * @param queryId String
     */

    private void doTask(String queryId) {
        ArrayList<String> pseudoDocEntityList;
        Map<String, Double> freqDist;

        if (entityRankings.containsKey(queryId) && entityQrels.containsKey(queryId)) {

            // Get the set of entities retrieved for the query
            Set<String> retEntitySet = new HashSet<>(entityRankings.get(queryId));

            // Get the set of entities relevant for the query
            Set<String> relEntitySet = new HashSet<>(entityQrels.get(queryId));

            // Get the number of retrieved entities which are also relevant
            // Finding support passage for non-relevant entities makes no sense!!

            retEntitySet.retainAll(relEntitySet);

            // Get the list of passages retrieved for the query
            ArrayList<String> paraList = new ArrayList<>(paraRankings.get(queryId).keySet());
            // For every entity in this list of relevant entities do
            for (String entityId : retEntitySet) {


                // Create a pseudo-document for the entity
                PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                        " ", paraList, searcher);

                if (d != null) {

                    // Get the list of entities that co-occur with this entity in the pseudo-document
                    pseudoDocEntityList = d.getEntityList();

                    // Find the frequency distribution over the co-occurring entities
                    freqDist = getDistribution(pseudoDocEntityList, retEntitySet);

                    // Score the passages in the pseudo-document for this entity using the frequency distribution of
                    // co-occurring entities
                    scoreDoc(queryId, d, freqDist);
                }
            }
            if (parallel) {
                System.out.println("Done query: " + queryId);
            }
        }
    }

    @NotNull
    private Map<String, Double> getDistribution(@NotNull ArrayList<String> pseudoDocEntityList,
                                                Set<String> retEntitySet) {

        HashMap<String, Integer> freqMap = new HashMap<>();
        Set<String> processedRetEntitySet = new HashSet<>(Utilities.process(new ArrayList<>(retEntitySet)));


        // For every co-occurring entity do
        for (String e : pseudoDocEntityList) {
            // If the entity also occurs in the list of entities relevant for the query then
            if ( processedRetEntitySet.contains(e)) {

                // Find the frequency of this entity in the pseudo-document and store it
                //freqMap.put(e, Utilities.frequency(e, pseudoDocEntityList));
                freqMap.compute(e, (t, oldV) -> (oldV == null) ? 1 : oldV + 1);
            }
        }
        return  toDistribution(freqMap);
    }

    @NotNull
    private Map<String, Double> toDistribution (@NotNull Map<String, Integer> freqMap) {
        Map<String, Double> dist = new HashMap<>();

        // Calculate the normalizer
        int norm = 0;
        for (int val : freqMap.values()) {
            norm += val;
        }

        // Normalize the map
        for (String word: freqMap.keySet()) {
            int freq = freqMap.get(word);
            double normFreq = (double) freq / norm;
            normFreq = Double.parseDouble(df.format(normFreq));
            if (! (normFreq < 0.0d) ) {
                dist.put(word, normFreq);
            }
        }
        return dist;
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
            double score = getParaScore(doc, freqMap);

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
     * @param doc  Document
     * @param freqMap HashMap where Key = entity id and Value = score
     * @return Integer
     */

    @Contract("null, _ -> fail")
    private double getParaScore(Document doc, Map<String, Double> freqMap) {

        double entityScore, paraScore = 0;
        // Get the entities in the paragraph
        // Make an ArrayList from the String array
        assert doc != null;
        ArrayList<String> pEntList = Utilities.getEntities(doc);
        /* For every entity in the paragraph do */
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
            double score = paraScore.get(paraId);
            if (score > 0) {
                runFileString = queryId + "+" +entityId + " Q0 " + paraId + " " + rank + " " + score + " " + "ECN";
                runStrings.add(runFileString);
                rank++;
            }

        }
    }

    /**
     * Main method.
     * @param args Command Line arguments
     */

    public static void main(@NotNull String[] args) {
        String indexDir = args[0];
        String mainDir = args[1];
        String outputDir = args[2];
        String dataDir = args[3];
        String paraRunFile = args[4];
        String entityRunFile = args[5];
        String entityQrel = args[6];
        boolean parallel = args[7].equalsIgnoreCase("true");

        String outFile = "ECN.run";

        new ECNFreq(indexDir, mainDir, outputDir, dataDir, paraRunFile, entityRunFile,
                outFile, entityQrel, parallel);
    }
}

