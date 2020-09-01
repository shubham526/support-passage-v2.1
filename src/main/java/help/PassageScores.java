package help;

import experiments.ecd.ECNFreq;
import lucene.Index;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.NotNull;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

/**
 * Score of a candidate support passage = score of passage from passage ranking for query
 *
 * @author Shubham Chatterjee
 * @version 7/30/2020
 */

public class PassageScores {
    private final IndexSearcher searcher;
    private final HashMap<String, LinkedHashMap<String, Double>>  paraRankings = new HashMap<>();
    private final HashMap<String, ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> entityQrels;
    private final ArrayList<String> runStrings;
    private final boolean parallel;
    private final DecimalFormat df;

    public PassageScores(String indexDir,
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

    private void doTask(String queryId) {
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
                    scoreDoc(queryId, d);
                }

            }
        }
    }
    private void scoreDoc(String queryId, @NotNull PseudoDocument d) {
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
            double score = paraRankings.get(queryId).get(paraId);

            // Store the paragraph id and score in a HashMap
            scoreMap.put(paraId, score);
        }
        makeRunStrings(queryId, entityId, scoreMap);

    }
    private void makeRunStrings(String queryId, String entityId, HashMap<String, Double> scoreMap) {
        LinkedHashMap<String, Double> paraScore = Utilities.sortByValueDescending(scoreMap);
        String runFileString;
        int rank = 1;

        for (String paraId : paraScore.keySet()) {
            float score = Float.parseFloat((df.format(paraScore.get(paraId))));
            if (score > 0) {
                runFileString = queryId + "+" +entityId + " Q0 " + paraId + " " + rank + " " + score + " " + "PassageScores";
                runStrings.add(runFileString);
                rank++;
            }

        }
    }
    public static void main(@NotNull String[] args) {
        String indexDir = args[0];
        String mainDir = args[1];
        String outputDir = args[2];
        String dataDir = args[3];
        String paraRunFile = args[4];
        String entityRunFile = args[5];
        String entityQrel = args[6];
        boolean parallel = args[7].equalsIgnoreCase("true");

        String outFile = "PassageScores.run";

        new PassageScores(indexDir, mainDir, outputDir, dataDir, paraRunFile, entityRunFile,
                outFile, entityQrel, parallel);
    }
}
