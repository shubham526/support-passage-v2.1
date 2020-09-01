package experiments.salience;

import api.SWATApi;
import help.Utilities;
import lucene.Index;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

/**
 * ===========================================================Sal-SP-Psg-Scores=========================================
 * This class re-ranks the support passages obtained using some method using entity salience scores obtained using SWAT.
 * Method: Score(p | e, q) = Score(p | q) * Score(e | p)
 * where   Score(p | q)    = normalized retrieval score of passage 'p' for the query 'q'
 *                          (obtained from the candidate passage ranking.
 *         Score(e | p)    = normalized salience score of entity 'e' for passage 'p' (obtained from SWAT).
 * @author Shubham Chatterjee
 * @version 09/18/2019
 */

public class SalSPPsgScores {
    private final IndexSearcher searcher;
    private Map<String, Map<String, Map<String, Double>>> supportPsgRunFileMap;
    private final HashMap<String, LinkedHashMap<String, Double>> paraRankings;
    private final HashMap<String, Map<String, Double>> salientEntityMap;
    private Map<String, Map<String, Double>> swatMap;
    private final List<String> runStrings;
    private final DecimalFormat df;

    /**
     * Constructor.
     * @param indexDir String Path to the index directory.
     * @param mainDir String Path to the support passage directory.
     * @param outputDir String Path to the output directory within the support passage directory.
     * @param dataDir String Path to the data directory within the support passage directory.
     * @param supportPsgRunFile String Name of the support passage run file within the data directory.
     * @param paraRunFile String Name of the passage run file.
     * @param outFile String Name of the output file.
     * @param swatFile String Path to the swat annotation file.
     */

    public SalSPPsgScores(String indexDir,
                          String mainDir,
                          String outputDir,
                          String dataDir,
                          String supportPsgRunFile,
                          String paraRunFile,
                          String outFile,
                          String swatFile) {

        this.runStrings = new ArrayList<>();
        this.supportPsgRunFileMap = new LinkedHashMap<>();
        this.paraRankings = new LinkedHashMap<>();
        this.supportPsgRunFileMap = new HashMap<>();
        this.swatMap = new HashMap<>();
        this.salientEntityMap = new HashMap<>();

        String supportPsgRunFilePath = mainDir + "/" + dataDir + "/" + supportPsgRunFile;
        String paraRunFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String outFilePath = mainDir + "/" + outputDir + "/" + outFile;
        String swatFilePath = mainDir + "/" + dataDir + "/" + swatFile;

        df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);

        System.out.print("Setting up index for use...");
        searcher = new Index.Setup(indexDir).getSearcher();
        System.out.println("[Done].");

        System.out.print("Reading support passage run file...");
        getRunFileMap(supportPsgRunFilePath, supportPsgRunFileMap);
        System.out.println("[Done].");

        System.out.print("Reading passage rankings...");
        Utilities.getRankings(paraRunFilePath, paraRankings);
        System.out.println("[Done].");

        System.out.print("Reading the SWAT annotations...");
        try {
            this.swatMap = Utilities.readMap(swatFilePath);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");

        experiment(outFilePath);

    }

    /**
     * Works in parallel using Java 8 parallelStreams.
     * DEFAULT THREAD POOL SIE = NUMBER OF PROCESSORS
     * USE : System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "N") to set the thread pool size
     */
    private void experiment(String outFilePath) {
        //Get the set of queries
        Set<String> querySet = paraRankings.keySet();
        System.out.println(querySet.size());

        // Do in serial
        ProgressBar pb = new ProgressBar("Progress", querySet.size());
        for (String q : querySet) {
            doTask(q);
            pb.step();
        }
        pb.close();



        // Create the run file
        System.out.print("Writing to run file....");
        Utilities.writeFile(new ArrayList<>(runStrings), outFilePath);
        System.out.println("[Done].");
        System.out.println("Run file written at: " + outFilePath);
    }

    /**
     * Do the actual work.
     * @param queryID String query
     */
    private void doTask(String queryID) {
        if (supportPsgRunFileMap.containsKey(queryID) && paraRankings.containsKey(queryID)) {

            // Get the list of entities for the query
            Map<String, Map<String, Double>> entityToParaMap = supportPsgRunFileMap.get(queryID);
            Set<String> entitySet = entityToParaMap.keySet();

            // For every entity for the query do
            for (String entityID : entitySet) {

                // Get the map of support passage scores for the query-entity pair
                Map<String, Double> paraToScoreMap = entityToParaMap.get(entityID);
                Set<String> paraSet = paraToScoreMap.keySet(); // Set of passages

                // This is a Map of entities and their scores (P(e|p))
                Map<String, Double> entitySalScoreMap = new HashMap<>();

                // This is a Map where Key = Query and Value = (paragraph, score)
                Map<String, Map<String, Double>> scoreMap = new HashMap<>();

                String processedEntityID = Utilities.process(entityID); // Remove enwiki: from the entityID

                // Get the scores for the entity for the paragraph from the salience scores, that is, P(e|p)
                // Note that here "entitySalScoreMap" is actually a map from paragraphs to score but for the given entity
                getParaScores(paraSet, entitySalScoreMap, processedEntityID);

                if (entitySalScoreMap.size() != 0) {
                    // If you get back anything then
                    // Convert this entitySalScoreMap to a distribution
                    Map<String, Double> normalizedMap = normalize(entitySalScoreMap);
                    // Score the paragraphs
                    Map<String, Double> scores = new HashMap<>(); // Stores the paragraph scores
                    scoreParas(queryID, entityID, normalizedMap, scores);
                    scoreMap.put(queryID + "+" + entityID, scores);
                    makeRunStrings(scoreMap);
                }
            }
            //System.out.println("Done: " + queryID);
        }
    }

    /**
     * Get the P(e|p).
     * @param paraSet Set Set of paragraphs retrieved for the query-entity pair
     * @param entitySalScoreMap Map Map of paragraph and their scores.
     * @param processedEntityID String EntityID after removing enwiki:
     */

    private void getParaScores(@NotNull Set<String> paraSet,
                               Map<String, Double> entitySalScoreMap,
                               String processedEntityID) {


        Map<String, Double> saliencyMap;
        String paraText;
        Document document = null;
        for (String paraID : paraSet) {

            if (swatMap.containsKey(paraID)) {
                // If you find the swat annotations for the passage in the swat file then good
                saliencyMap = swatMap.get(paraID);
            } else if (salientEntityMap.containsKey(paraID)) {
                // If not, then look in the in-memory cache
                saliencyMap = salientEntityMap.get(paraID);
            } else {
                // If the swat annotations are not found in the in-memory cache too,
                // then we need to query the SWAT API :-(
                // To do this, we need the text of the paragraph for which we need to query the Lucene index
                try {
                    document = Index.Search.searchIndex("id", paraID, searcher);
                } catch (IOException | ParseException e) {
                    e.printStackTrace();
                }
                assert document != null;
                paraText = document.get("text"); // Get the paragraph text
                saliencyMap = SWATApi.getEntities(paraText, "all"); // Query the SWAT API
                salientEntityMap.put(paraID, saliencyMap); // Store the annotations received from SWAT in cache.
            }
            if (saliencyMap == null) {
                // If no annotations for the paragraph were found anywhere then skip this passage :-(
                continue;
            }
            //entitySalScoreMap.put(paraID, saliencyMap.getOrDefault(processedEntityID, 0.0d));
            Set<String> saliencyMapKeySet = saliencyMap.keySet();
            for (String e : saliencyMapKeySet) {
                if (e.equalsIgnoreCase(processedEntityID)) {
                    double score = saliencyMap.get(e);
                    entitySalScoreMap.put(paraID, score);
                    break;
                }
            }
        }
    }

    /**
     * Load the run file in memory.
     * This loads the run file in the form of a Map of a Map of a Map.
     * @param runFile String Run file to load.
     * @param queryMap Map Map to load the run file into.
     */

    private void getRunFileMap(String runFile,
                               Map<String, Map<String, Map<String, Double>>> queryMap) {
        BufferedReader in = null;
        String line;
        Map<String, Map<String, Double>> entityMap;
        Map<String, Double> paraMap;
        try {
            in = new BufferedReader(new FileReader(runFile));
            while((line = in.readLine()) != null) {
                String[] fields = line.split(" ");
                String queryID = fields[0].split("\\+")[0];
                String entityID = fields[0].split("\\+")[1];
                String paraID = fields[2];
                double paraScore = Double.parseDouble(fields[4]);
                if (queryMap.containsKey(queryID)) {
                    entityMap = queryMap.get(queryID);
                } else {
                    entityMap = new HashMap<>();
                }
                if (entityMap.containsKey(entityID)) {
                    paraMap = entityMap.get(entityID);
                } else {
                    paraMap = new HashMap<>();
                }
                paraMap.put(paraID,paraScore);
                entityMap.put(entityID,paraMap);
                queryMap.put(queryID,entityMap);

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(in != null) {
                    in.close();
                } else {
                    System.out.println("Input Buffer has not been initialized!");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * Score the passages.
     * Method: Score(p | q, e)  = Score(p | q)  * Salience(e | p)
     * @param queryID String QueryID
     * @param entityID String EntityID
     * @param normalizedSalMap Map
     * @param scores Map
     */

    private void scoreParas(String queryID,
                            String entityID,
                            @NotNull Map<String, Double> normalizedSalMap,
                            Map<String, Double> scores) {
        double score;

        // Get the passages retrieved for the query
        // Normalize the scores to get a distribution
        Map<String, Double> normalizedParaRankings = normalize(paraRankings.get(queryID));

        // Get the set of paragraphs
        Set<String> paraSet = normalizedSalMap.keySet();

        for (String paraID : paraSet) {
            if (normalizedParaRankings.containsKey(paraID) && normalizedSalMap.containsKey(paraID)) {
                try {
                    double prob_para_given_query = normalizedParaRankings.get(paraID);
                    double prob_entity_given_para = normalizedSalMap.get(paraID);

                    if (prob_entity_given_para == 0.0d) {
                        score = -9999;
                    } else {
                        score = Math.log(prob_para_given_query) + Math.log(prob_entity_given_para);
                    }
                    scores.put(paraID, score);
                } catch (NullPointerException e) {
                    System.out.printf("NullPointerException for query %s and entity %s\n", queryID, entityID);
                }
            }
        }
    }

    @NotNull
    private Map<String, Double> normalize(@NotNull Map<String, Double> rankings) {
        Map<String, Double> normRankings = new HashMap<>();
        double sum = 0.0d;
        for (double score : rankings.values()) {
            sum += score;
        }

        if (sum == 0.0d) {
            return rankings;
        }

        for (String s : rankings.keySet()) {
            double normScore = rankings.get(s) / sum;
            normRankings.put(s, normScore);
        }

        return normRankings;
    }

    /**
     * Make the run file strings.
     * @param scoreMap Map
     */

    private void makeRunStrings(@NotNull Map<String, Map<String, Double>> scoreMap) {
        String runFileString;
        for (String query : scoreMap.keySet()) {
            int rank = 1;
            LinkedHashMap<String, Double> sortedScoreMap = Utilities.sortByValueDescending(scoreMap.get(query));
            for (String paraId : sortedScoreMap.keySet()) {
                //double score = sortedScoreMap.get(paraId);
                float score = Float.parseFloat((df.format(sortedScoreMap.get(paraId))));
                runFileString = query + " Q0 " + paraId + " " + rank + " " + score + " " + "Sal-SP-Psg-Scores";
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
        String supportPsgRunFile = args[4];
        String passageRunFile = args[5];
        String swatFile= args[6];

        String outputRunFile = "Sal-SP-Psg-Scores.run";

        new SalSPPsgScores(indexDir, mainDir, outputDir, dataDir, supportPsgRunFile, passageRunFile,
                outputRunFile, swatFile);

    }

}
