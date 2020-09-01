package experiments.relatedness;

import api.WATApi;
import help.EntityRMExpand;
import help.PseudoDocument;
import help.Utilities;
import lucene.Index;
import lucene.RAMIndex;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ===============================QE-Rel-ECD-Entities==========================
 * Expand the query using related entities in the ECD of the entity
 * and retrieve document using expanded model.
 * ============================================================================
 *
 * @author Shubham Chatterjee
 * @version 07/30/2020
 */

public class QERelECDEntities {

    private final IndexSearcher searcher;
    private final HashMap<String, ArrayList<String>> paraRankings;
    private final HashMap<String,ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> entityQrels;
    private Map<String, Map<String, Double>> entRelMap = new ConcurrentHashMap<>();

    // ArrayList of run strings
    private final ArrayList<String> runStrings = new ArrayList<>();
    private final int takeKEntities, takeKDocs; // Number of query expansion terms
    private final boolean omitQueryTerms, parallel, useEcd; // Omit query terms or not when calculating expansion terms
    private final Analyzer analyzer; // Analyzer to use
    private final Similarity similarity;
    private String relType;
    private final DecimalFormat df;
    AtomicInteger count = new AtomicInteger(0);
    private int N;

    /**
     * Constructor.
     * @param indexDir String Path to the paragraph.entity.lucene index directory.
     * @param mainDir String Path to the main directory.
     * @param outputDir String Path to the output directory within the main directory.
     * @param dataDir String Path to the data directory within the main directory.
     * @param paraRunFile String Name of the passage run file within the data directory.
     * @param entityRunFile String Name of the entity run file within the data directory.
     * @param entityQrelFile String Path to the entity ground truth file.
     * @param outFile String Name of the output file.
     * @param takeKEntities Integer Top K entities for query expansion.
     * @param similarity Similarity Type of similarity to use.
     * @param analyzer Analyzer Type of analyzer to use.
     * @param omitQueryTerms Boolean Whether or not to omit query terms during expansion.
     */

    public QERelECDEntities(String indexDir,
                            String mainDir,
                            String outputDir,
                            String dataDir,
                            String relFile,
                            String paraRunFile,
                            String entityRunFile,
                            String outFile,
                            String entityQrelFile,
                            int takeKEntities,
                            int takeKDocs,
                            boolean omitQueryTerms,
                            boolean parallel,
                            boolean useEcd,
                            @NotNull String relType,
                            Analyzer analyzer,
                            Similarity similarity) {


        this.takeKEntities = takeKEntities;
        this.takeKDocs = takeKDocs;
        this.analyzer = analyzer;
        this.similarity = similarity;
        this.omitQueryTerms = omitQueryTerms;
        this.parallel = parallel;
        this.useEcd = useEcd;

        df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);

        String entityFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String entityQrelFilePath = mainDir + "/" + dataDir + "/" + entityQrelFile;
        String paraFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String relFilePath = mainDir + "/" + dataDir + "/" + relFile;
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

        System.out.print("Reading paragraph rankings...");
        paraRankings = Utilities.getRankings(paraFilePath);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelFilePath);
        System.out.println("[Done].");

        System.out.print("Reading relatedness file...");
        try {
            entRelMap = Utilities.readMap(relFilePath);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");

        System.out.print("Setting up paragraph index for use...");
        searcher = new Index.Setup(indexDir, "text", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        feature(outputFilePath);
    }
    /**
     * Method to calculate the first feature.
     * Works in parallel using Java 8 parallelStreams.
     * DEFAULT THREAD POOL SIE = NUMBER OF PROCESSORS
     * USE : System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "N") to set the thread pool size
     */

    private void feature(String outputFilePath) {
        //Get the set of queries
        Set<String> querySet = entityRankings.keySet();
        N = querySet.size();
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

    private void doTask(@NotNull String queryId) {
        List<Map.Entry<String, Double>> expansionEntities;
        List<Map.Entry<String, Double>> contextEntityList;
        Map<String, Float> results;
        // Process the query
        String queryStr = queryId
                .substring(queryId.indexOf(":") + 1)          // remove enwiki: from query
                .replaceAll("%20", " ")     // replace %20 with whitespace
                .toLowerCase();                            //  convert query to lowercase

        if (entityQrels.containsKey(queryId) && entityRankings.containsKey(queryId)) {

            // Get the set of entities retrieved for the query
            ArrayList<String> entityList = entityRankings.get(queryId);
            Set<String> retEntitySet = new HashSet<>(entityList);

            // Get the set of entities relevant for the query
            Set<String> relEntitySet = new HashSet<>(entityQrels.get(queryId));

            // Get the retrieved entities which are also relevant
            // Finding support passage for non-relevant entities makes no sense!!

            retEntitySet.retainAll(relEntitySet);

            // Get the list of passages retrieved for the query
            ArrayList<String> paraList = paraRankings.get(queryId);

            // For every entity in this set of relevant retrieved  entities do
            for (String entityId : retEntitySet) {

                PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                        " ", paraList, searcher);

                if (d != null) {

                    // Get the list of all entities which co-occur with this entity in a given context
                    // Context here is the same as a PseudoDocument for the entity
                    // So we are actually looking at all entities that occur in the PseudoDocument
                    // sorted in descending order of frequency
                    // Here we are using all entities retrieved for the query to get the expansion terms
                    contextEntityList = getContextEntities(d);
                    // Use the top K entities for expansion
                    expansionEntities = contextEntityList.subList(0, Math.min(takeKEntities, contextEntityList.size()));

                    if (expansionEntities.size() == 0) {
                        continue;
                    }

                    // Convert the query to an expanded BooleanQuery
                    BooleanQuery booleanQuery = null;
                    try {
                        booleanQuery = EntityRMExpand.toEntityRmQuery(queryStr, expansionEntities, omitQueryTerms,
                                "text", analyzer);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    if (useEcd) {
                        ////////////////////////////////////////////////////////////////////
                        /////////////////////Searching the Index of ECD passages////////////
                        ////////////////////////////////////////////////////////////////////

                        // Get the candidate passages
                        List<Document> candidatePassages = d.getDocumentList();

                        results = searchIndex(booleanQuery, candidatePassages);

                    } else {
                        ////////////////////////////////////////////////////////////////////
                        /////////////////////Searching the Paragraph Index//////////////////
                        ////////////////////////////////////////////////////////////////////

                        results = searchIndex(booleanQuery);
                    }

                    // Make the run file strings for the query-entity pair
                    if (!results.isEmpty()) {
                        makeRunStrings(queryId, entityId, results);
                    }

                }
            }
            if (parallel) {
                count.getAndIncrement();
                System.out.println("Progress: " + count + " of " + N);
            }
        }
    }

    @NotNull
    private List<Map.Entry<String, Double>> getContextEntities(PseudoDocument d) {
        Map<String, Double> relMap = new HashMap<>();
        Set<String> pseudoDocEntitySet;

        // Get the list of entities that co-occur with this entity in the pseudo-document
        if (d != null) {
            String entityId = d.getEntity();
            // Get the list of co-occurring entities
            pseudoDocEntitySet = new HashSet<>(d.getEntityList());
            // Get the relatedness with the target entity
            relMap = getRelatedness(entityId, pseudoDocEntitySet);
        }


        // Sort the entities in decreasing order of relatedness
        // Add all the entities to the list
        // Return the list
        return new ArrayList<>(Utilities.sortByValueDescending(relMap).entrySet());
    }

    @NotNull
    private Map<String, Float> searchIndex(BooleanQuery booleanQuery, List<Document> documents) {

        Map<String, Float> results;
        // Get the top documents for this query-entity pair
        // This is obtained after expanding the query with contextual words
        // And retrieving with the expanded query from the index
        // Searching the ECD-Index

        // First create the IndexWriter
        IndexWriter iw = RAMIndex.createWriter(analyzer);

        // Now create the index
        try {
            RAMIndex.createIndex(documents, iw);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create the IndexSearcher
        IndexSearcher is = null;
        try {
            is = RAMIndex.createSearcher(similarity, iw);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Search the index
        assert is != null;
        results = RAMIndex.searchIndex(booleanQuery, takeKDocs, is);

        return results;
    }

    @NotNull
    private Map<String, Float> searchIndex(BooleanQuery booleanQuery) {

        Map<String, Float> results = new HashMap<>();

        TopDocs topDocs = null;
        try {
            topDocs = Index.Search.searchIndex(booleanQuery, takeKDocs, searcher);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert topDocs != null;
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (int i = 0; i < scoreDocs.length; i++) {
            Document d = null;
            try {
                d = searcher.doc(scoreDocs[i].doc);
            } catch (IOException e) {
                e.printStackTrace();
            }
            assert d != null;
            String pID = d.getField("id").stringValue();
            float score = topDocs.scoreDocs[i].score;
            results.put(pID, score);
        }
        return results;
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

//    @NotNull
//    private Map<String, Double>  getRelatedness(String entityID,
//                                                @NotNull Set<String> contextEntitySet) {
//
//        Map<String, Double> relMap = new HashMap<>();
//
//        for (String e : contextEntitySet) {
//            relMap.put(e, getRelatedness(entityID, e));
//        }
//        return relMap;
//    }



//    private double getRelatedness(@NotNull String e1, String e2) {
//
//        Map<String, Double> erelMap = entRelMap.get(e1);
//        if (erelMap.containsKey(e2)) {
//            return erelMap.get(e2);
//        } else {
//            //System.out.println("Querying WAT");
//
//
//            int id1, id2;
//
//            if (e1.equalsIgnoreCase(e2)) {
//                return 1.0d;
//            }
//
//
//            id1 = entityIDMap.containsKey(e1)
//                    ? entityIDMap.get(e1)
//                    : WATApi.TitleResolver.getId(e1.substring(e1.indexOf(":") + 1).replaceAll("%20", "_"));
//
//            id2 = entityIDMap.containsKey(e2)
//                    ? entityIDMap.get(e2)
//                    : WATApi.TitleResolver.getId(e2.substring(e2.indexOf(":") + 1).replaceAll("%20", "_"));
//
//
//            if (id1 < 0 || id2 < 0) {
//                return 0.0d;
//            }
//
//            List<WATApi.EntityRelatedness.Pair> pair = WATApi.EntityRelatedness.getRelatedness(relType, id1, id2);
//            if (!pair.isEmpty()) {
//                return pair.get(0).getRelatedness();
//            } else {
//                return 0.0d;
//            }
//        }
//    }
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
     * Makes the run file strings.
     * @param queryId String
     * @param entityId String
     * @param scoreMap Map of (ParaID, Score)
     */

    private void makeRunStrings(String queryId, String entityId, Map<String, Float> scoreMap) {
        LinkedHashMap<String, Float> paraScore = Utilities.sortByValueDescending(scoreMap);
        String runFileString;
        int rank = 1;

        for (String paraId : paraScore.keySet()) {
            float score = paraScore.get(paraId);
            score = Float.parseFloat((df.format(score)));
            if (score > 0) {
                runFileString = queryId + "+" + entityId + " Q0 " + paraId + " " + rank++
                        + " " + score + " " + "QERelECDEntities";
                runStrings.add(runFileString);
            }

        }
    }

    /**
     * Main method to run the code.
     * @param args Command line parameters.
     */

    public static void main(@NotNull String[] args){

        Similarity similarity = null;
        Analyzer analyzer = null;
        String s1 = null, s2, s3;

        String indexDir = args[0];
        String mainDir = args[1];
        String outputDir = args[2];
        String dataDir = args[3];
        String relFile = args[4];
        String paraRunFile = args[5];
        String entityRunFile = args[6];
        String entityQrel = args[7];
        int takeKEntities = Integer.parseInt(args[8]);
        int takeKDocs = Integer.parseInt(args[9]);
        boolean omit = args[10].equalsIgnoreCase("yes");
        boolean parallel = args[11].equalsIgnoreCase("true");
        boolean useEcd = args[12].equalsIgnoreCase("true");
        String relType = args[13];
        String a = args[14];
        String sim = args[15];

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
                    lambda = Float.parseFloat(args[16]);
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

        if (useEcd) {
            System.out.println("Using ECD Index");
            s3 = "ecd-index";
        } else {
            System.out.println("Using Paragraph Index");
            s3 = "para-index";
        }
        String outFile = "QERelECDEntities" + "-" + s1 + "-" + s2 + "-" + relType + "-" + s3 +".run";


        new QERelECDEntities(indexDir, mainDir, outputDir, dataDir, relFile, paraRunFile, entityRunFile, outFile,
                entityQrel, takeKEntities, takeKDocs, omit, parallel, useEcd, relType, analyzer, similarity);

    }
}
