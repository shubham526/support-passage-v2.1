package experiments.ecd;

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

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

public class QEECDEntities {
    private final IndexSearcher searcher;

    //HashMap where Key = queryID and Value = list of paragraphs relevant for the queryID
    private final HashMap<String, ArrayList<String>> paraRankings;

    //HashMap where Key = queryID and Value = list of entities relevant for the queryID
    private final HashMap<String,ArrayList<String>> entityRankings;

    private final HashMap<String, ArrayList<String>> entityQrels;

    // ArrayList of run strings
    private final ArrayList<String> runStrings = new ArrayList<>();
    private final int takeKEntities, takeKDocs; // Number of query expansion terms
    private final boolean omitQueryTerms; // Omit query terms or not when calculating expansion terms
    private final Analyzer analyzer; // Analyzer to use
    private final Similarity similarity;
    private final boolean parallel, useEcd;
    private final DecimalFormat df;

    /**
     * Constructor.
     * @param indexDir String Path to the index directory.
     * @param mainDir String Path to the TREC-CAR directory.
     * @param outputDir String Path to the output directory within the TREC-CAR directory.
     * @param dataDir String Path to the data directory within the TREC-CAR directory.
     * @param paraRunFile String Name of the passage run file within the data directory.
     * @param entityRunFile String Name of the entity run file within the data directory.
     * @param entityQrel String Name of the entity ground truth file.
     * @param outFile String Name of the output file.
     * @param takeKEntities Integer Top K entities for query expansion.
     * @param similarity Similarity Type of similarity to use.
     * @param analyzer Analyzer Type of analyzer to use.
     * @param omitQueryTerms Boolean Whether or not to omit query terms during expansion.
     * @param parallel Boolean Whether to run code in parallel or not.
     */

    public QEECDEntities(String indexDir,
                         String mainDir,
                         String outputDir,
                         String dataDir,
                         String paraRunFile,
                         String entityRunFile,
                         String outFile,
                         String entityQrel,
                         int takeKEntities,
                         int takeKDocs,
                         boolean omitQueryTerms,
                         Analyzer analyzer,
                         Similarity similarity,
                         boolean parallel,
                         boolean useEcd) {


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
        String entityQrelPath = mainDir + "/" + dataDir + "/" + entityQrel;
        String outputFilePath = mainDir + "/" + outputDir + "/" + outFile;

        System.out.print("Reading entity rankings...");
        entityRankings = Utilities.getRankings(entityFilePath);
        System.out.println("[Done].");

        System.out.print("Reading paragraph rankings...");
        paraRankings = Utilities.getRankings(paraFilePath);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelPath);
        System.out.println("[Done].");

        System.out.print("Setting up index for use...");
        searcher = new Index.Setup(indexDir, "text", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        feature(outputFilePath);
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
     * For every such entity, create a pseudo-document consisting of passages which contain this entity.
     * For every co-occurring entity in the pseudo-document, if the entity is also relevant for the query,
     * then find the frequency of this entity in the pseudo-document and score the passages using this frequency information.
     *
     * @param queryId String
     */

    private void doTask(@NotNull String queryId) throws IOException {
        List<Map.Entry<String, Double>> contextEntityList;
        List<Map.Entry<String, Double>> expansionEntities;
        Map<String, Float> results;
        // Process the query
        String queryStr = queryId
                .substring(queryId.indexOf(":") + 1)          // remove enwiki: from query
                .replaceAll("%20", " ")     // replace %20 with whitespace
                .toLowerCase();                            //  convert query to lowercase

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

                PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                        " ", paraList, searcher);

                if (d != null) {

                    // Get the candidate passages
                    List<Document> candidatePassages = d.getDocumentList();

                    // Get the list of all entities which co-occur with this entity in a given context
                    // Context here is the same as a PseudoDocument for the entity
                    // So we are actually looking at all entities that occur in the PseudoDocument
                    // sorted in descending order of frequency
                    // Here we are using all entities retrieved for the query to get the expansion terms
                    contextEntityList = getContextEntities(entityList, d);

                    // Use the top K entities for expansion
                    expansionEntities = contextEntityList.subList(0, Math.min(takeKEntities, contextEntityList.size()));

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
                System.out.println("Done query: " + queryId);
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
    private List<Map.Entry<String, Double>> getContextEntities(List<String> entityList,
                                                                PseudoDocument d) {
        HashMap<String, Integer> freqMap = new HashMap<>();
        ArrayList<String> processedEntityList = Utilities.process(entityList);
        ArrayList<String> pseudoDocEntityList;

        // Get the list of entities that co-occur with this entity in the pseudo-document
        if (d != null) {
            // Get the list of co-occurring entities
            pseudoDocEntityList = d.getEntityList();
            // For every co-occurring entity do
            for (String e : pseudoDocEntityList) {
                // If the entity also occurs in the list of entities relevant for the query then
                if (processedEntityList.contains(e)) {
                    // Find the frequency of this entity in the pseudo-document and store it
                    freqMap.compute(e, (t, oldV) -> (oldV == null) ? 1 : oldV + 1);
                }
            }
        }


        // Sort the entities in decreasing order of frequency
        // Add all the entities to the list
        // Return the list
        return new ArrayList<>(Utilities.sortByValueDescending(normalize(freqMap)).entrySet());
    }

    /**
     * Normalize a Map.
     * @param freqMap Map
     * @return Normalized map
     */

    @NotNull
    private Map<String, Double> normalize(@NotNull Map<String, Integer> freqMap) {


        Map<String, Double> dist = new HashMap<>();

        // Calculate the normalizer
        int norm = 0;
        for (int val : freqMap.values()) {
            norm += val;
        }

        // Normalize the map
        for (String word: freqMap.keySet()) {
            int freq = freqMap.get(word);
            double normFreq = (float) freq / norm;
            normFreq = Double.parseDouble(df.format(normFreq));
            if (! (normFreq < 0.0d) ) {
                dist.put(word, normFreq);
            }
        }
        return dist;
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
                        + " " + score + " " + "QEECDEntities";
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

        String indexDir = args[0];
        String mainDir = args[1];
        String outputDir = args[2];
        String dataDir = args[3];
        String paraRunFile = args[4];
        String entityRunFile = args[5];
        String entityQrel = args[6];
        int takeKEntities = Integer.parseInt(args[7]);
        int takeKDocs = Integer.parseInt(args[8]);
        boolean omit = args[9].equalsIgnoreCase("yes");
        boolean parallel = args[10].equalsIgnoreCase("true");
        boolean useEcdIndex = args[11].equalsIgnoreCase("true");
        String a = args[12];
        String sim = args[13];

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
                    lambda = Float.parseFloat(args[14]);
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

        if (useEcdIndex) {
            System.out.println("Using ECD Index");
            s3 = "ecd-index";
        } else {
            System.out.println("Using Paragraph Index");
            s3 = "para-index";
        }
        String outFile = "QEECDEntities" + "-" + s1 + "-" + s2 + "-" + s3 + ".run";

        new QEECDEntities(indexDir, mainDir, outputDir, dataDir, paraRunFile, entityRunFile, outFile, entityQrel,
                takeKEntities, takeKDocs, omit, analyzer, similarity, parallel, useEcdIndex);

    }

}
