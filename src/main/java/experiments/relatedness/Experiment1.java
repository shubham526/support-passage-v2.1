package experiments.relatedness;

import api.WATApi;
import help.EntityRMExpand;
import help.Utilities;
import lucene.Index;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ======================================Experiment-1=====================================
 * Expand the query using related entities on the Wikipedia page of the entity
 * and retrieve document using expanded model.
 * =======================================================================================
 *
 * @author Shubham Chatterjee
 * @version 03/18/2020
 */

public class Experiment1 {
    private final IndexSearcher paraIndexSearcher;
    private final IndexSearcher pageIndexSearcher;

    //HashMap where Key = queryID and Value = list of entities relevant for the queryID
    private final HashMap<String,ArrayList<String>> entityRankings;

    private final HashMap<String, ArrayList<String>> entityQrels;
    private Map<String, Integer> entityIDMap = new ConcurrentHashMap<>();

    // ArrayList of run strings
    private final ArrayList<String> runStrings = new ArrayList<>();
    private final int takeKEntities; // Number of query expansion terms
    private final boolean omitQueryTerms; // Omit query terms or not when calculating expansion terms
    private final Analyzer analyzer; // Analyzer to use
    private String relType;

    /**
     * Constructor.
     * @param paraIndexDir String Path to the paragraph.entity.lucene index directory.
     * @param pageIndexDir String Path to the page.lucene index directory.
     * @param mainDir String Path to the main directory.
     * @param outputDir String Path to the output directory within the main directory.
     * @param dataDir String Path to the data directory within the main directory.
     * @param entityRunFile String Name of the entity run file within the data directory.
     * @param entityQrelPath String Path to the entity ground truth file.
     * @param outFile String Name of the output file.
     * @param takeKEntities Integer Top K entities for query expansion.
     * @param similarity Similarity Type of similarity to use.
     * @param analyzer Analyzer Type of analyzer to use.
     * @param omitQueryTerms Boolean Whether or not to omit query terms during expansion.
     */

    public Experiment1(String paraIndexDir,
                       String pageIndexDir,
                       String mainDir,
                       String outputDir,
                       String dataDir,
                       String idFile,
                       String entityRunFile,
                       String outFile,
                       String entityQrelPath,
                       @NotNull String relType,
                       int takeKEntities,
                       boolean omitQueryTerms,
                       Analyzer analyzer,
                       Similarity similarity) {


        this.takeKEntities = takeKEntities;
        this.analyzer = analyzer;
        this.omitQueryTerms = omitQueryTerms;

        String entityFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String idFilePath = mainDir + "/" + dataDir + "/" + idFile;
        String outputFilePath = mainDir + "/" + outputDir + "/" + outFile;

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

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelPath);
        System.out.println("[Done].");

        System.out.print("Reading id file...");
        try {
            entityIDMap = Utilities.readMap(idFilePath);
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
     * Method to calculate the first feature.
     * Works in parallel using Java 8 parallelStreams.
     * DEFAULT THREAD POOL SIE = NUMBER OF PROCESSORS
     * USE : System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "N") to set the thread pool size
     */

    private  void feature(String outputFilePath) {
        //Get the set of queries
        Set<String> querySet = entityRankings.keySet();

        // Do in parallel
        querySet.parallelStream().forEach(queryId -> {
            try {
                doTask(queryId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });


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

    private void doTask(String queryId) throws IOException {

        List<Map.Entry<String, Double>> expansionEntities;
        List<Map.Entry<String, Double>> pageEntityList = new ArrayList<>();

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
                getPageEntities(entityId, pageEntityList);

                // Use the top K entities for expansion
                expansionEntities = pageEntityList.subList(0, Math.min(takeKEntities, pageEntityList.size()));

                if (expansionEntities.size() == 0) {
                    continue;
                }
                // Process the query
                String queryStr = queryId
                        .substring(queryId.indexOf(":") + 1)          // remove enwiki: from query
                        .replaceAll("%20", " ")     // replace %20 with whitespace
                        .toLowerCase();                            //  convert query to lowercase

                // Convert the query to an expanded BooleanQuery
                BooleanQuery booleanQuery = EntityRMExpand.toEntityRmQuery(queryStr, expansionEntities, omitQueryTerms ,
                        "text", analyzer);

                // Search the index
                TopDocs tops = Index.Search.searchIndex(booleanQuery, 100, paraIndexSearcher);
                makeRunStrings(queryId, entityId, tops);

            }
            System.out.println("Done query: " + queryId);
        }
    }

    /**
     * Helper method.
     * Returns the entities on the Wikipedia page of the given entity along with its relatedness measure.
     * @param entityID String Given entity.
     * @param pageEntityList List List of (Entity, Relatedness) pairs.
     */

    private void getPageEntities(String entityID, @NotNull List<Map.Entry<String, Double>> pageEntityList) {
        pageEntityList.clear();
        Map<String, Double> pageEntityMap = new HashMap<>();

        try {
            // Get the document corresponding to the entityID from the page.lucene index
            Document doc = Index.Search.searchIndex("Id", entityID, pageIndexSearcher);

            // Get the list of entities in the document
            String entityString = Objects.requireNonNull(doc).getField("OutlinkIds").stringValue();

            // Make a list from this string
            String[] entityArray = entityString.split("\n");
           for (String eid : entityArray) {
               double rel = getRelatedness(entityID, eid);
               pageEntityMap.put(Utilities.process(eid), rel);
           }
           pageEntityList.addAll(Utilities.sortByValueDescending(pageEntityMap).entrySet());

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

    }

    /**
     * Helper method.
     * Returns the relatedness between between two entities.
     * @param e1 String First Entity.
     * @param e2 String Second Entity
     * @return Double Relatedness
     */

    private double getRelatedness(@NotNull String e1, String e2) {

        int id1, id2;

        if (e1.equalsIgnoreCase(e2)) {
            return 1.0d;
        }


        id1 = entityIDMap.containsKey(e1)
                ? entityIDMap.get(e1)
                : WATApi.TitleResolver.getId(e1.substring(e1.indexOf(":") + 1).replaceAll("%20", "_"));

        id2 = entityIDMap.containsKey(e2)
                ? entityIDMap.get(e2)
                : WATApi.TitleResolver.getId(e2.substring(e2.indexOf(":") + 1).replaceAll("%20", "_"));


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

    /**
     * Make run file strings.
     * @param queryId String
     * @param entityId String
     * @param topDocs TopDocs
     * @throws IOException Exception
     */
    private void makeRunStrings(String queryId,
                                String entityId,
                                @NotNull TopDocs topDocs) throws IOException {
        String query = queryId + "+" + entityId;
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        Document d;
        String runFileString;

        for (int i = 0; i < scoreDocs.length; i++) {
            d = paraIndexSearcher.doc(scoreDocs[i].doc);
            String pID = d.getField("id").stringValue();
            runFileString = query + " Q0 " + pID + " " + (i + 1) + " " + topDocs.scoreDocs[i].score + " " + "exp1";
            //System.out.println(runFileString);
            runStrings.add(runFileString);
        }
    }
    /**
     * Main method to run the code.
     * @param args Command line parameters.
     */

    public static void main(@NotNull String[] args) {

        Similarity similarity = null;
        Analyzer analyzer = null;
        boolean omit;
        String s1 = null, s2;

        String paraIndexDir = args[0];
        String pageIndexDir = args[1];
        String mainDir = args[2];
        String outputDir = args[3];
        String dataDir = args[4];
        String idFile = args[5];
        String entityRunFile = args[6];
        String entityQrel = args[7];
        String relType = args[8];
        int takeKEntities = Integer.parseInt(args[9]);
        String o = args[10];
        omit = o.equalsIgnoreCase("y") || o.equalsIgnoreCase("yes");
        String a = args[11];
        String sim = args[12];

        System.out.printf("Using %d entities for query expansion\n", takeKEntities);

        if (omit) {
            System.out.println("Using RM1");
            s2 = "rm1";
        } else {
            System.out.println("Using RM3");
            s2 = "rm3";
        }


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
                s1 = "bm25";
                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                float lambda;
                try {
                    lambda = Float.parseFloat(args[13]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                    s1 = "lmjm";
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Missing lambda value for similarity LM-JM");
                    System.exit(1);
                }
                break;
            case "LMDS":
            case "lmds":
                System.out.println("Similarity: LMDS");
                similarity = new LMDirichletSimilarity();
                s1 = "lmds";
                break;

            default:
                System.out.println("Wrong choice of similarity! Exiting.");
                System.exit(1);
        }
        String outFile = "qe-rel-ent-page" + "-" + s1 + "-" + s2 + "-" + relType + ".run";

        new Experiment1(paraIndexDir, pageIndexDir, mainDir, outputDir, dataDir, idFile, entityRunFile,
                outFile, entityQrel, relType, takeKEntities, omit, analyzer, similarity);

    }

}

