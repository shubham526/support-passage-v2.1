package experiments.relatedness;

import api.WATApi;
import help.PseudoDocument;
import help.Utilities;
import lucene.Index;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

/**
 * ==============================================WikiEntities==========================================================
 * (1) Get a relatedness distribution over the entities from the Wikipedia article of the target entity.
 * (2) Create a PseudoDocument for the entity. The Documents in the PseudoDocument are the candidate support passages.
 * (3) Score a passage by accumulating the relatedness(e,target), obtained from the distribution in (1).
 * ====================================================================================================================
 *
 * @author Shubham Chatterjee
 * @version 6/19/2020
 */

public class WikiEntities {
    private final IndexSearcher paraIndexSearcher;
    private final IndexSearcher pageIndexSearcher;

    //HashMap where Key = queryID and Value = list of entities relevant for the queryID
    private final HashMap<String, ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> paraRankings;

    private final HashMap<String, ArrayList<String>> entityQrels;
    private Map<String, Integer> entityIDMap = new ConcurrentHashMap<>();
    private Map<String, Map<String, Double>> entRelMap = new ConcurrentHashMap<>();

    // ArrayList of run strings
    private final ArrayList<String> runStrings = new ArrayList<>();
    private String relType;
    private final boolean parallel;
    private final DecimalFormat df;

    /**
     * Constructor.
     * @param paraIndexDir String Path to the paragraph.entity.lucene index directory.
     * @param pageIndexDir String Path to the page.lucene index directory.
     * @param mainDir String Path to the main directory.
     * @param outputDir String Path to the output directory within the main directory.
     * @param dataDir String Path to the data directory within the main directory.
     * @param entityRunFile String Name of the entity run file within the data directory.
     * @param entityQrel String Name of the entity ground truth file.
     * @param outFile String Name of the output file.
     * @param similarity Similarity Type of similarity to use.
     * @param analyzer Analyzer Type of analyzer to use.
     */

    public   WikiEntities(String paraIndexDir,
                          String pageIndexDir,
                          String mainDir,
                          String outputDir,
                          String dataDir,
                          String relFile,
                          String paraRunFile,
                          String entityRunFile,
                          String outFile,
                          String entityQrel,
                          @NotNull String relType,
                          boolean parallel,
                          Analyzer analyzer,
                          Similarity similarity) {

        String entityFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String paraFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String relFilePath = mainDir + "/" + dataDir + "/" + relFile;
        String entityQrelPath = mainDir + "/" + dataDir + "/" + entityQrel;
        String outputFilePath = mainDir + "/" + outputDir + "/" + outFile;
        this.parallel = parallel;

        df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);

        if (relType.equalsIgnoreCase("mw")) {
            System.out.println("Entity Similarity Measure: Milne-Witten");
            this.relType = "mw";
        } else if (relType.equalsIgnoreCase("jaccard")) {
            System.out.println("Entity Similarity Measure: Jaccard");
            this.relType = "jaccard";
        } else if (relType.equalsIgnoreCase("lm")) {
            System.out.println("Entity Similarity Measure: Language Models");
            this.relType = "lm";
        } else if (relType.equalsIgnoreCase("w2v")) {
            System.out.println("Entity Similarity Measure: Word2Vec");
            this.relType = "w2v";
        } else if (relType.equalsIgnoreCase("cp")) {
            System.out.println("Entity Similarity Measure: Conditional Probability");
            this.relType = "conditionalprobability";
        } else if (relType.equalsIgnoreCase("ba")) {
            System.out.println("Entity Similarity Measure: Barabasi-Albert on the Wikipedia Graph");
            this.relType = "barabasialbert";
        } else if (relType.equalsIgnoreCase("pmi")) {
            System.out.println("Entity Similarity Measure: Pointwise Mutual Information");
            this.relType = "pmi";
        }

        System.out.print("Reading entity rankings...");
        entityRankings = Utilities.getRankings(entityFilePath);
        System.out.println("[Done].");

        System.out.print("Reading paragraph rankings...");
        paraRankings = Utilities.getRankings(paraFilePath);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelPath);
        System.out.println("[Done].");

        System.out.print("Reading relatedness file...");
        try {
            entRelMap = Utilities.readMap(relFilePath);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");

        System.out.print("Setting up paragraph index for use...");
        paraIndexSearcher = new Index.Setup(paraIndexDir, "text", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        System.out.print("Setting up page index for use...");
        pageIndexSearcher = new Index.Setup(pageIndexDir, "OutlinkIds", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        feature(outputFilePath);
    }
    /**
     * Runs the code.
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
     * For every such entity, find all entities on the Wikipedia page of the entity along with their relatedness measures.
     * Use the top-k of these entities to expand the query and retrieve support passages.
     * @param queryId String
     */

    private void doTask(String queryId) {

        Map<String, Double> pageEntityDist;
        ArrayList<String> paraList = paraRankings.get(queryId);

        if (entityQrels.containsKey(queryId) && entityQrels.containsKey(queryId)) {

            // Get the set of entities retrieved for the query
            ArrayList<String> entityList = entityRankings.get(queryId);
            Set<String> retEntitySet = new HashSet<>(entityList);

            // Get the set of entities relevant for the query
            Set<String> relEntitySet = new HashSet<>(entityQrels.get(queryId));

            // Get the retrieved entities which are also relevant
            // Finding support passage for non-relevant entities makes no sense!!

            retEntitySet.retainAll(relEntitySet);

            // For every entity in this set of relevant retrieved  entities do
            for (String entityId : retEntitySet) {

                // Get the list of all entities on the Wikipedia page of this entity.
                pageEntityDist = getPageEntityDist(entityId);

                // Create a pseudo-document for the entity
                PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                        " ", paraList, paraIndexSearcher);

                // Score the passages in the pseudo-document for this entity using the frequency distribution of
                // co-occurring entities
                if (d != null) {
                    scoreDoc(queryId, d, pageEntityDist);
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
        //freqMap = entFreqMap.get(entityId);
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
     * Helper method.
     * Returns the entities on the Wikipedia page of the given entity along with its relatedness measure.
     * @param entityID String Given entity.
     */

    @NotNull
    private Map<String, Double> getPageEntityDist(String entityID) {

        Map<String, Double> pageEntityMap = new HashMap<>();

        try {
            // Get the document corresponding to the entityID from the page.lucene index
            Document doc = Index.Search.searchIndex("Id", entityID, pageIndexSearcher);

            // Get the list of entities in the document
            String entityString = Objects.requireNonNull(doc).getField("OutlinkIds").stringValue();

            // Make a list from this string
            Set<String> entitySet = new HashSet<>(Arrays.asList(entityString.split("\n")));

            // Get relatedness
            pageEntityMap = getRelatedness(entityID, entitySet);

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        return pageEntityMap;

    }

    @NotNull
    private Map<String, Double>  getRelatedness(String entityID,
                                                @NotNull Set<String> contextEntitySet) {

        Map<String, Double> relMap = new HashMap<>();
        String processedEntity1 = processString(entityID);
        double relatedness;


        if (processedEntity1 != null) {
            for (String e : contextEntitySet) {
                String processedEntity2 = processString(e);
                if (processedEntity2 == null) {
                    continue;
                }
                if (processedEntity1.equalsIgnoreCase(processedEntity2)) {
                    relatedness = 1.0;
                } else if (entRelMap.get(entityID).containsKey(e)) {
                    relatedness = entRelMap.get(entityID).get(e);
                } else {
                    int id1 = WATApi.TitleResolver.getId(processedEntity1);
                    int id2 = WATApi.TitleResolver.getId(processedEntity2);
                    relatedness = getRelatedness(id1, id2);
                }
                relMap.put(e, relatedness);
            }
        }
        return relMap;
    }
    @NotNull
    private Double getRelatedness(int id1, int id2) {
        if (id1 < 0 || id2 < 0) {
            return 0.0d;
        }

        List<WATApi.EntityRelatedness.Pair> pair = WATApi.EntityRelatedness.getRelatedness(relType,id1, id2);
        if (!pair.isEmpty()) {
            return pair.get(0).getRelatedness();
        } else {
            return 0.0d;
        }
    }
    @Nullable
    private String processString(@NotNull String e) {
        e = e.substring(e.indexOf(":") + 1).replaceAll("%20", "_");
        try {
            String[] parts = e.split("_");
            parts[0] = parts[0].substring(0, 1).toUpperCase() + parts[0].substring(1);
            for (int i = 1; i < parts.length; i++) {
                parts[i] = parts[i].substring(0, 1).toLowerCase() + parts[i].substring(1);
            }
            return String.join("_", parts);
        } catch (StringIndexOutOfBoundsException ex) {
            System.err.println("ERROR in preprocessString(): " + ex.getMessage());
            return null;
        }
    }

    /**
     * Method to find the score of a paragraph.
     * This method looks at all the entities in the paragraph and calculates the score from them.
     * For every entity in the paragraph, if the entity has a score from the entity context pseudo-document,
     * then sum over the entity scores and store the score in a HashMap.
     * @param doc  Document
     * @param relMap HashMap where Key = entity id and Value = score
     * @return Integer
     */

    @Contract("null, _ -> fail")
    private double getParaScore(Document doc, Map<String, Double> relMap) {

        double entityScore, paraScore = 0;
        // Get the entities in the paragraph
        // Make an ArrayList from the String array
        assert doc != null;
        ArrayList<String> pEntList = Utilities.getEntities(doc);
        /* For every entity in the paragraph do */
        for (String e : pEntList) {
            // Lookup this entity in the HashMap of scores for the entities
            // Sum over the scores of the entities to get the score for the passage
            // Store the passage score in the HashMap
            if (relMap.containsKey(e)) {
                entityScore = relMap.get(e);
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

    private void makeRunStrings(String queryId, String entityId, Map<String, Double> scoreMap) {
        LinkedHashMap<String, Double> paraScore = Utilities.sortByValueDescending(scoreMap);
        String runFileString;
        int rank = 1;

        for (String paraId : paraScore.keySet()) {
            double score = Double.parseDouble(df.format(paraScore.get(paraId)));
            if (score > 0) {
                runFileString = queryId + "+" +entityId + " Q0 " + paraId + " " + rank++ + " " + score + " " + "WikiEntities";
                runStrings.add(runFileString);
            }

        }
    }
    /**
     * Main method to run the code.
     * @param args Command line parameters.
     */

    public static void main(@NotNull String[] args) {

        Similarity similarity = null;
        Analyzer analyzer = null;

        String paraIndexDir = args[0];
        String pageIndexDir = args[1];
        String mainDir = args[2];
        String outputDir = args[3];
        String dataDir = args[4];
        String relFile = args[5];
        String paraRunFile = args[6];
        String entityRunFile = args[7];
        String entityQrel = args[8];
        String relType = args[9];
        boolean parallel = args[10].equalsIgnoreCase("true");
        String a = args[11];
        String sim = args[12];




        switch (a) {
            case "std" :
                System.out.println("Analyzer: Standard");
                analyzer = new StandardAnalyzer();
                break;
            case "eng":
                System.out.println("Analyzer: English");
                analyzer = new EnglishAnalyzer();

                break;
            default:
                System.out.println("Wrong choice of analyzer! Exiting.");
                System.exit(1);
        }
        switch (sim) {
            case "BM25" :
            case "bm25":
                similarity = new BM25Similarity();
                System.out.println("Similarity: BM25");
                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                float lambda;
                try {
                    lambda = Float.parseFloat(args[13]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Missing lambda value for similarity LM-JM");
                    System.exit(1);
                }
                break;
            case "LMDS":
            case "lmds":
                System.out.println("Similarity: LMDS");
                similarity = new LMDirichletSimilarity();
                break;

            default:
                System.out.println("Wrong choice of similarity! Exiting.");
                System.exit(1);
        }
        String outFile = "WikiEntities.run";

        new WikiEntities(paraIndexDir, pageIndexDir, mainDir, outputDir, dataDir, relFile, paraRunFile, entityRunFile,
                outFile, entityQrel, relType, parallel, analyzer, similarity);

    }
}
