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
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ======================================QEWikiEntities=====================================
 * Expand the query using related entities on the Wikipedia page of the entity
 * and retrieve document using expanded model.
 * =======================================================================================
 *
 * @author Shubham Chatterjee
 * @version 03/18/2020
 */

public class QEWikiEntities {
    private final IndexSearcher paraIndexSearcher;
    private final IndexSearcher pageIndexSearcher;

    //HashMap where Key = queryID and Value = list of entities relevant for the queryID
    private final HashMap<String, ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> paraRankings;

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

    /**
     * Constructor.
     *
     * @param paraIndexDir   String Path to the paragraph.entity.lucene index directory.
     * @param pageIndexDir   String Path to the page.lucene index directory.
     * @param mainDir        String Path to the main directory.
     * @param outputDir      String Path to the output directory within the main directory.
     * @param dataDir        String Path to the data directory within the main directory.
     * @param entityRunFile  String Name of the entity run file within the data directory.
     * @param entityQrel     String Name of the entity ground truth file.
     * @param outFile        String Name of the output file.
     * @param takeKEntities  Integer Top K entities for query expansion.
     * @param similarity     Similarity Type of similarity to use.
     * @param analyzer       Analyzer Type of analyzer to use.
     * @param omitQueryTerms Boolean Whether or not to omit query terms during expansion.
     */

    public QEWikiEntities(String paraIndexDir,
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
                          int takeKEntities,
                          int takeKDocs,
                          boolean omitQueryTerms,
                          boolean parallel,
                          boolean useEcd,
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
        String paraFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String relFilePath = mainDir + "/" + dataDir + "/" + relFile;
        String entityQrelPath = mainDir + "/" + dataDir + "/" + entityQrel;
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
     * Method to calculate the first feature.
     * Works in parallel using Java 8 parallelStreams.
     * DEFAULT THREAD POOL SIE = NUMBER OF PROCESSORS
     * USE : System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "N") to set the thread pool size
     */

    private void feature(String outputFilePath) {
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
            querySet.parallelStream().forEach(queryId -> {
                try {
                    doTask(queryId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } else {
            System.out.println("Using Sequential Streams.");

            // Do in serial
            ProgressBar pb = new ProgressBar("Progress", querySet.size());
            for (String q : querySet) {
                try {
                    doTask(q);
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
     *
     * @param queryId String
     */

    private void doTask(@NotNull String queryId) throws IOException {
        Map<String, Float> results;

        // Process the query
        String queryStr = queryId
                .substring(queryId.indexOf(":") + 1)          // remove enwiki: from query
                .replaceAll("%20", " ")     // replace %20 with whitespace
                .toLowerCase();                            //  convert query to lowercase



        List<Map.Entry<String, Double>> expansionEntities;
        List<Map.Entry<String, Double>> pageEntityList;

        if (entityQrels.containsKey(queryId) && entityQrels.containsKey(queryId)) {

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

                // Get the list of all entities on the Wikipedia page of this entity.
                pageEntityList = getPageEntities(entityId);

                // Use the top K entities for expansion
                expansionEntities = pageEntityList.subList(0, Math.min(takeKEntities, pageEntityList.size()));

                if (expansionEntities.size() == 0) {
                    continue;
                }

                // Convert the query to an expanded BooleanQuery
                BooleanQuery booleanQuery = EntityRMExpand.toEntityRmQuery(queryStr, expansionEntities, omitQueryTerms,
                        "text", analyzer);

                if (useEcd) {
                    ////////////////////////////////////////////////////////////////////
                    /////////////////////Searching the Index of ECD passages////////////
                    ////////////////////////////////////////////////////////////////////

                    PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                            " ", paraList, paraIndexSearcher);
                    List<Document> candidatePassages = new ArrayList<>();
                    if (d != null) {
                        // Get the candidate passages
                        candidatePassages = d.getDocumentList();
                    }

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
            if (parallel) {
                count.getAndIncrement();
                System.out.println("Progress: " + count + " of 117.");
            }
        }
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
            topDocs = Index.Search.searchIndex(booleanQuery, takeKDocs, paraIndexSearcher);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert topDocs != null;
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (int i = 0; i < scoreDocs.length; i++) {
            Document d = null;
            try {
                d = paraIndexSearcher.doc(scoreDocs[i].doc);
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


    /**
     * Helper method.
     * Returns the entities on the Wikipedia page of the given entity along with its relatedness measure.
     * @param entityID String Given entity.
     */

    @NotNull
    @Contract("_ -> new")
    private List<Map.Entry<String, Double>> getPageEntities(String entityID) {

        Map<String, Double> pageEntityMap = new HashMap<>();

        try {
            // Get the document corresponding to the entityID from the page.lucene index
            Document doc = Index.Search.searchIndex("Id", entityID, pageIndexSearcher);

            // Get the list of entities in the document
            String entityString = Objects.requireNonNull(doc).getField("OutlinkIds").stringValue();

            // Make a set from this string
            Set<String> entitySet = new HashSet<>(Arrays.asList(entityString.split("\n")));

            // Get relatedness
            pageEntityMap = getRelatedness(entityID, entitySet);

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        return new ArrayList<>(Utilities.sortByValueDescending(pageEntityMap).entrySet());
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
                        + " " + score + " " + "QEWikiEntities";
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

        String s1 = null, s2, s3;

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
        int takeKEntities = Integer.parseInt(args[10]);
        int takeKDocs = Integer.parseInt(args[11]);
        boolean omit = args[12].equalsIgnoreCase("yes");
        boolean parallel = args[13].equalsIgnoreCase("true");
        boolean useEcdIndex = args[14].equalsIgnoreCase("true");
        String a = args[15];
        String sim = args[16];

        System.out.printf("Using %d entities for query expansion\n", takeKEntities);

        if (useEcdIndex) {
            System.out.println("Using ECD Index");
            s3 = "ecd-index";
        } else {
            System.out.println("Using Paragraph Index");
            s3 = "para-index";
        }

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
                    lambda = Float.parseFloat(args[17]);
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
        String outFile = "QEWikiEntities" + "-" + s1 + "-" + s2 + "-" + relType + "-" + s3 +".run";

        new QEWikiEntities(paraIndexDir, pageIndexDir, mainDir, outputDir, dataDir, relFile, paraRunFile, entityRunFile,
                outFile, entityQrel, relType, takeKEntities, takeKDocs, omit, parallel, useEcdIndex,
                analyzer, similarity);

    }

}

