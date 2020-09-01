package experiments.salience;

import help.PseudoDocument;
import help.Utilities;
import lucene.Index;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

/**
 * ============================================Sal-ECD-Ent-Scores=======================================================
 * This class class scores passages in a pseudo-document about an entity using the salience score of the entity.
 * Method: Score(p | q, e)  = Score(e | q)  * Salience(p | e)
 * where   Score(e | q)     = normalized retrieval score of entity for query (obtained from the entity ranking)
 *         Salience (p | e) = normalized salience score of entity 'e' for passage 'p' (obtained from SWAT).
 * @author Shubham Chatterjee
 * @version 07/27/2020
 */

public class SalECDEntScores {
    private final IndexSearcher searcher;
    //HashMap where Key = queryID and Value = list of paragraphs relevant for the queryID
    private final HashMap<String, LinkedHashMap<String, Double>> paraRankings;
    //HashMap where Key = queryID and Value = list of entities relevant for the queryID
    private final HashMap<String,LinkedHashMap<String, Double>> entityRankings;
    // ArrayList of run strings
    private final ArrayList<String> runStrings;
    private final HashMap<String, ArrayList<String>> entityQrels;
    private Map<String, Map<String, Double>> salientEntityMap;
    private final Map<String, String> salienceStats = new HashMap<>();
    private final boolean parallel;
    private final DecimalFormat df;

    /**
     * Constructor.
     * @param indexDir  String Path to the index on disk.
     * @param mainDir String Path to the support passage directory.
     * @param outputDir String Path to the output directory within the support passage directory.
     * @param dataDir String Path to the data directory within the support passage directory.
     * @param passageRunFile String Name of the passage run file (obtained from ECN) withn the data directory.
     * @param entityRunFile String Name of the entity run file.
     * @param outputRunFile String Name of the output file.
     * @param entityQrelFile String Name of the entity ground truth file.
     * @param swatFile String Path to the swat annotation file.
     */

    public SalECDEntScores(String indexDir,
                           String mainDir,
                           String outputDir,
                           String dataDir,
                           String passageRunFile,
                           String entityRunFile,
                           String outputRunFile,
                           String statsFile,
                           String entityQrelFile,
                           String swatFile,
                           boolean parallel) {

        this.runStrings = new ArrayList<>();
        this.entityRankings = new HashMap<>();
        this.paraRankings = new HashMap<>();
        this.parallel = parallel;

        df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);

        String entityRunFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String passageRunFilePath = mainDir + "/" + dataDir + "/" + passageRunFile;
        String outputRunFilePath = mainDir + "/" + outputDir + "/" + outputRunFile;
        String entityQrelFilePath = mainDir + "/" + dataDir + "/" + entityQrelFile;
        String swatFilePath = mainDir + "/" + dataDir + "/" + swatFile;
        String statsFilePath = mainDir + "/" + outputDir + "/" + statsFile;

        System.out.print("Setting up index for use...");
        searcher = new Index.Setup(indexDir).getSearcher();
        System.out.println("[Done].");

        System.out.print("Reading entity rankings...");
        Utilities.getRankings(entityRunFilePath, entityRankings);
        System.out.println("[Done].");

        System.out.print("Reading paragraph rankings...");
        Utilities.getRankings(passageRunFilePath, paraRankings);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelFilePath);
        System.out.println("[Done].");

        System.out.print("Reading the SWAT annotations...");
        try {
            this.salientEntityMap = Utilities.readMap(swatFilePath);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");

        feature(outputRunFilePath);

        writeStatsToFile(statsFilePath);

    }

    private void writeStatsToFile(String statsFilePath) {
        List<String> stats = new ArrayList<>();

        for (String qid : salienceStats.keySet()) {
            String c = salienceStats.get(qid);
            String s = qid + " " + c;
            stats.add(s);
        }
        Utilities.writeFile(stats, statsFilePath);
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
     * For every relevant entity retrieved for the query, find the passages mentioning the entity.
     * For every such passage, the score of the passage is equal to the salience score of the entity if the entity is
     * salient in the passage and zero otherwise.
     * @param queryId String
     */


    private void doTask(String queryId)  {
        int count = 0;
        if (entityRankings.containsKey(queryId) && entityQrels.containsKey(queryId)) {

            // Get the list of paragraphs retrieved for the query
            ArrayList<String> paraList = new ArrayList<>(paraRankings.get(queryId).keySet());

            // Get the set of entities retrieved for the query
            Set<String> retEntitySet = new HashSet<>(entityRankings.get(queryId).keySet());

            // Get the set of entities relevant for the query
            Set<String> relEntitySet = new HashSet<>(entityQrels.get(queryId));

            // Get the number of retrieved entities which are also relevant
            // Finding support passage for non-relevant entities makes no sense!!

            retEntitySet.retainAll(relEntitySet);

            ArrayList<PseudoDocument> pseudoDocuments = new ArrayList<>();
            HashMap<String, HashMap<String, Double>> entityParaMap = new HashMap<>();

            // For every entity in this list of relevant entities do
            for (String entityId : retEntitySet) {

                // This map is will store the score of the entity for each passage mentioning it.
                HashMap<String, Double> paraMap = new HashMap<>();

                // For every passage mentioning the entity, get the score of the entity given the passage, i.e., P(e|p).
                getEntityToParaMap(entityId, paraList, pseudoDocuments, paraMap);

                if (!paraMap.isEmpty()) {


                    // When we reach here, it means that we have a HashMap of paraIDs with their score for an entity
                    // So now put that hashMap in the HashMap for the entity
                    entityParaMap.put(entityId, paraMap);
                } else {
                    count++;
                }
            }

            // Now score the passages in the pseudo-documents
            scorePassage(queryId, pseudoDocuments, entityParaMap);
            salienceStats.put(queryId, count + "/" + retEntitySet.size());

            if (parallel) {
                System.out.println("Done: " + queryId);
            }
        }
    }

    /**
     * Helper method.
     * Creates a pseudo-document for the given entity. For passage in the pseudo-document, scores the passage.
     * @param entityID String entityID
     * @param paraList List List of passages retrieved for the query.
     * @param pseudoDocuments List List of pseudo-documents for the entity.
     * @param paraMap Map Map of (paraID, sore) where score = Salience(e|p).
     */

    private void getEntityToParaMap(String entityID,
                                    ArrayList<String> paraList,
                                    ArrayList<PseudoDocument> pseudoDocuments,
                                    HashMap<String, Double> paraMap) {
        // Create a pseudo-document for the entity
        PseudoDocument d = Utilities.createPseudoDocument(entityID, "id", "entity",
                " ", paraList, searcher);

        String processedEntityId = Utilities.process(entityID);

        if (d != null) {

            // Get the list of lucene documents that make up this pseudo-document
            ArrayList<Document> documents = d.getDocumentList();

            // For every document in the pseudo-document for the entity
            for (Document document : documents) {

                //Get the id of the document
                String paraID = document.get("id");

                if (salientEntityMap.containsKey(paraID)) {

                    // Get the salient entities in the document
                    Map<String, Double> saliencyMap = salientEntityMap.get(paraID);
                    Set<String> salientEntitySet = saliencyMap.keySet();

                    for (String salientEntity : salientEntitySet) {
                        if (salientEntity.equalsIgnoreCase(processedEntityId)) {
                            double score = saliencyMap.get(salientEntity);
                            paraMap.put(paraID, score);
                            // Add it to the list of pseudo-documents for this entity
                            pseudoDocuments.add(d);
                            break;
                        }
                    }
                }

                /*

                // Get the salient entities in the document
                Map<String, Double> saliencyMap = salientEntityMap.get(paraID);

                // If there are no salient entities, then continue
                if (saliencyMap == null) {
                    continue;
                }

                // Otherwise check if the entity is salient to the document
                // If it is, then the score of the document is the salience score of the entity
                // Otherwise it is zero
                paraMap.put(paraID, saliencyMap.getOrDefault(Utilities.process(entityID), 0.0d));

                 */
            }


        }

    }

    /**
     * Score th passages in the pseudo-document.
     * @param queryId String QueryID
     * @param pseudoDocuments List List of pseudo-documents
     * @param entityParaMap Map
     */
    private void scorePassage(String queryId,
                              @NotNull ArrayList<PseudoDocument> pseudoDocuments,
                              HashMap<String, HashMap<String, Double>> entityParaMap ) {


        // Normalize the document scores to get a distribution
        LinkedHashMap<String, Double> normalizedEntityRankings = normalize(entityRankings.get(queryId));
        double score;

        // For every pseudo-document do
        for (PseudoDocument pseudoDocument : pseudoDocuments) {

            // Get the entity corresponding to the pseudo-document
            String entityId = pseudoDocument.getEntity();

            HashMap<String, Double> scoreMap = new HashMap<>();

            // Normalize the saliency scores for that entity

            LinkedHashMap<String, Double> normalizedSaliencyScores = normalize(entityParaMap.get(entityId));

            // Get the documents which make up the pseudo-document for the entity
            ArrayList<Document> documents = pseudoDocument.getDocumentList();

            // For every such document in the list do
            for (Document document : documents) {

                // Get the paragraph id of the document
                String paraId = document.getField("id").stringValue();

                // Get the score of the document
                // P(p|e,q) = P(e|q) * P(p|e)
                // P(e|q) = Score(e|q) / Sum(Score(e|q))
                // P(p|e) = P(e|p) = Saliency(e|p) / Sum(Saliency(e|p))


                if (normalizedEntityRankings.containsKey(entityId) && normalizedSaliencyScores.containsKey(paraId)) {
                    score = normalizedEntityRankings.get(entityId) * normalizedSaliencyScores.get(paraId);
                } else {
                    score = 0.0d;
                }




                // Store the paragraph id and score in a HashMap
                scoreMap.put(paraId, score);
            }
            // Make the run file strings for query-entity and document
            makeRunStrings(queryId, entityId, scoreMap);
        }
    }

    /**
     * Normalize a map.
     * @param rankings Map
     * @return Map
     */
    @NotNull
    private LinkedHashMap<String, Double> normalize(@NotNull Map<String, Double> rankings) {
        LinkedHashMap<String, Double> normRankings = new LinkedHashMap<>();
        double sum = 0.0d;
        for (double score : rankings.values()) {
            sum += score;
        }

        for (String s : rankings.keySet()) {
            double normScore = rankings.get(s) / sum;
            normRankings.put(s,normScore);
        }

        return normRankings;
    }

    /**
     * Make run file strings.
     * @param queryId String
     * @param entityId String
     * @param scoreMap Map
     */
    private void makeRunStrings(String queryId, String entityId, HashMap<String, Double> scoreMap) {
        LinkedHashMap<String, Double> paraScore = Utilities.sortByValueDescending(scoreMap);
        String runFileString;
        int rank = 1;

        for (String paraId : paraScore.keySet()) {
            float score = Float.parseFloat((df.format(paraScore.get(paraId))));
            if (score > 0) {
                runFileString = queryId + "+" +entityId + " Q0 " + paraId + " " + rank
                        + " " + score + " " + "SalECDEntScores";
                runStrings.add(runFileString);
                rank++;
            }
        }
    }

    /**
     * Main method.
     * @param args Command line arguments.
     */
    public static void main(@NotNull String[] args) {
        String indexDir = args[0];
        String mainDir = args[1];
        String outputDir = args[2];
        String dataDir = args[3];
        String passageRunFile = args[4];
        String entityRunFile = args[5];
        String entityQrelFile = args[6];
        String swatFile = args[7];
        String statsFile = args[8];
        boolean parallel = args[9].equalsIgnoreCase("true");

        String outputRunFile = "SalECDEntScores.run";

        new SalECDEntScores(indexDir, mainDir, outputDir, dataDir, passageRunFile, entityRunFile, outputRunFile, statsFile,
                entityQrelFile, swatFile, parallel);

    }
}
