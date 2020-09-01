package experiments.wiki;



import help.PseudoDocument;
import help.RM3Expand;
import help.Utilities;
import lucene.Index;
import lucene.RAMIndex;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

/**
 * ====================================================QEWikiTerms========================================================
 * (1) Get a frequency distribution over the terms from the Wikipedia article of an entity.
 * (2) Expand the query with terms from the Wikipedia article.
 * (3) Retrieve passages using the expanded query from two types of indexes:
 *    a) Paragraph Index (consisting of paragraphs from the paragraph corpus).
 *    b) Index of paragraphs in the ECD about an entity.
 * =====================================================================================================================
 *
 * @author Shubham Chatterjee
 * @version 6/9/2020
 */

public class QEWikiTerms {

    private final IndexSearcher pageSearcher;
    private final IndexSearcher paraSearcher;
    private final HashMap<String, ArrayList<String>> paraRankings;
    private final HashMap<String,ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> entityQrels;
    private final ArrayList<String> runStrings;
    private final List<String> stopWords;
    private final boolean parallel, useECD;
    private final DecimalFormat df;
    private final int takeKTerms; // Number of query expansion terms
    private final int takeKDocs; // Number of documents for query expansion
    private final boolean omitQueryTerms; // Omit query terms or not when calculating expansion terms
    private  final Analyzer analyzer; // Analyzer to use
    private final Similarity similarity;

    /**
     * Constructor.
     * @param pageIndexDir String Path to the page index (knowledge graph).
     * @param paraIndexDir String Path to the paragraph index.
     * @param mainDir String Path to the main directory.
     * @param dataDir String Path to data directory within main directory.
     * @param outputDir String Path to output directory within main directory.
     * @param entityRunFile String Name of entity run file within the data directory.
     * @param entityQrelFile String Name of entity qrel file within the data directory.
     * @param outFile String Name of output run file (to be stored in directory specified with outputDir).
     * @param stopWordsFilePath String Path to the stop words list.
     * @param parallel Boolean Whether to run code in parallel or not.
     * @param useECD Boolean Whether to use the ECD-index to retrieve passages.
     * @param takeKTerms Integer Top K terms for query expansion.
     * @param takeKDocs Integer Top K documents for feedback set.
     * @param omitQueryTerms Boolean Whether or not to omit query terms during expansion.
     * @param analyzer Analyzer Type of lucene analyzer. Maybe English(eng) or Standard (std).
     * @param similarity Similarity Type of similarity. Maybe BM25, LMDS or LMJM.
     */

    public QEWikiTerms(String pageIndexDir,
                       String paraIndexDir,
                       String mainDir,
                       String dataDir,
                       String outputDir,
                       String paraRunFile,
                       String entityRunFile,
                       String entityQrelFile,
                       String outFile,
                       String stopWordsFilePath,
                       boolean parallel,
                       boolean useECD,
                       int takeKTerms,
                       int takeKDocs,
                       boolean omitQueryTerms,
                       Analyzer analyzer,
                       Similarity similarity) {


        String entityRunFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String paraFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String entityQrelFilePath = mainDir + "/" + dataDir + "/" + entityQrelFile;
        String outFilePath = mainDir + "/" + outputDir + "/" + outFile;
        this.runStrings = new ArrayList<>();
        this.stopWords = new ArrayList<>();
        this.parallel = parallel;
        this.takeKTerms = takeKTerms;
        this.takeKDocs = takeKDocs;
        this.omitQueryTerms = omitQueryTerms;
        this.analyzer = analyzer;
        this.similarity = similarity;
        this.useECD = useECD;

        df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);


        System.out.print("Reading paragraph rankings...");
        paraRankings = Utilities.getRankings(paraFilePath);
        System.out.println("[Done].");


        System.out.print("Reading entity rankings...");
        entityRankings = Utilities.getRankings(entityRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelFilePath);
        System.out.println("[Done].");

        System.out.print("Reading stop words list...");
        getStopWords(stopWordsFilePath);
        System.out.println("[Done].");

        System.out.print("Setting up page index for use...");
        pageSearcher = new Index.Setup(pageIndexDir, "Text", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        System.out.print("Setting up paragraph index for use...");
        paraSearcher = new Index.Setup(paraIndexDir, "text", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        feature(outFilePath);

    }

    /**
     * Reads the stop words file.
     * @param stopWordsFilePath String Path to the stop words file.
     */

    private void getStopWords(String stopWordsFilePath) {
        BufferedReader br = null;
        String line;

        try {
            br = new BufferedReader(new FileReader(stopWordsFilePath));
            while((line = br.readLine()) != null) {
                stopWords.add(line);
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
     * Does the actual work.
     * @param queryId String The query.
     */

    private void doTask(@NotNull String queryId) {

        String queryStr = queryId
                .substring(queryId.indexOf(":")+1)          // remove enwiki: from query
                .replaceAll("%20", " ")     // replace %20 with whitespace
                .toLowerCase();                            //  convert query to lowercase

        Map<String, Float> results = new HashMap<>();

        if (entityRankings.containsKey(queryId) && entityQrels.containsKey(queryId)) {

            // Get the set of entities retrieved for the query
            Set<String> retEntitySet = new HashSet<>(entityRankings.get(queryId));

            // Get the set of entities relevant for the query
            Set<String> relEntitySet = new HashSet<>(entityQrels.get(queryId));

            // Get the number of retrieved entities which are also relevant
            // Finding support passage for non-relevant entities makes no sense!!

            retEntitySet.retainAll(relEntitySet);

            // For every entity in this set of relevant (retrieved) entities do
            for (String entityId : retEntitySet) {

                // Get distribution over the terms from the Wikipedia article
                Map<String, Float> freqDist = getWikiTermDistribution(entityId);

                // Convert the query to an expanded BooleanQuery
                BooleanQuery booleanQuery = toBooleanQuery(queryStr, freqDist);

                // Search the index with this expanded query

                if (useECD) {
                    ////////////////////////////////////////////////////////////////////
                    /////////////////////Searching the Index of ECD passages////////////
                    ////////////////////////////////////////////////////////////////////

                    // Get the list of passages retrieved for the query
                    ArrayList<String> paraList = paraRankings.get(queryId);

                    // Create a pseudo-document for the entity
                    PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                            " ", paraList, paraSearcher);

                    // If there exists a pseudo-document about the entity
                    if (d != null) {

                        // Get the list of lucene documents in the pseudo-document
                        ArrayList<Document> documents = d.getDocumentList();

                        // Search the Index of ECD passages
                        results = searchIndex(booleanQuery, documents);
                    }
                } else {
                    ////////////////////////////////////////////////////////////////////
                    /////////////////////Searching the Paragraph Index//////////////////
                    ////////////////////////////////////////////////////////////////////
                    results = searchIndex(booleanQuery);
                }

                // Make the run file strings for the query-entity pair
                if (! results.isEmpty()) {
                    makeRunStrings(queryId, entityId, results);
                }
            }
        }
        if (parallel) {
            System.out.println("Done: " + queryId);
        }
    }

    private BooleanQuery toBooleanQuery(String queryStr, @NotNull Map<String, Float> freqDist) {
        // Convert the query to an expanded BooleanQuery
        BooleanQuery booleanQuery = null;
        List<Map.Entry<String, Float>> allWordFreqList = new ArrayList<>(freqDist.entrySet());
        List<Map.Entry<String, Float>> expansionTerms = allWordFreqList.subList(0,
                Math.min(takeKTerms, allWordFreqList.size()));
        try {
            booleanQuery = RM3Expand.toRm3Query(queryStr, expansionTerms, omitQueryTerms, "text", analyzer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return booleanQuery;
    }

    @NotNull
    private Map<String, Float> searchIndex(BooleanQuery booleanQuery, ArrayList<Document> documents) {

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
            topDocs = Index.Search.searchIndex(booleanQuery, takeKDocs, paraSearcher);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert topDocs != null;
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (int i = 0; i < scoreDocs.length; i++) {
            Document d = null;
            try {
                d = paraSearcher.doc(scoreDocs[i].doc);
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
            double score = paraScore.get(paraId);
            if (score > 0) {
                runFileString = queryId + "+" + entityId + " Q0 " + paraId + " " + rank++
                        + " " + score + " " + "QEWikiTerms";
                runStrings.add(runFileString);
            }

        }
    }

    /**
     * Calls a helper method to find the distribution over the terms from the Wikipedia article.
     * @param entity String
     * @return Map of (Term, Frequency).
     */

    private Map<String, Float> getWikiTermDistribution(String entity) {
        Document d;
        Map<String, Float> freqDist = new HashMap<>();
        try {
            d = Index.Search.searchIndex("Id", entity, pageSearcher);
            assert d != null;
            String text = d.getField("Content").stringValue();
            freqDist = frequencyDistribution(text);
        } catch (ParseException | IOException | NullPointerException e) {
            e.printStackTrace();
        }
        return freqDist;
    }

    /**
     * Helper method.
     * Finds the distribution over the terms from the Wikipedia article.
     * @param text String
     * @return Map
     */


//    @NotNull
//    private Map<String, Float> frequencyDistribution(String text) {
//        Map<String, Integer> freqMap = new HashMap<>();
//
//        List<String> words = preProcess(text);
//
//        for (String w : words) {
//            Integer n = freqMap.get(w);
//            n = (n == null) ? 1 : ++n;
//            freqMap.put(w, n);
//        }
//
//        Map<String, Float> distribution = normalize(freqMap);
//        return Utilities.sortByValueDescending(distribution);
//    }

    @NotNull
    private Map<String, Float> frequencyDistribution(String text) {
        Map<String, Integer> freqMap = new HashMap<>();

        List<String> words = preProcess(text);
        String processedText = String.join(" ", words);

        try {
            addTokens(processedText, freqMap, analyzer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<String, Float> distribution = normalize(freqMap);
        return Utilities.sortByValueDescending(distribution);
    }
    private  void addTokens(String content,
                            Map<String, Integer> wordFreq,
                            @NotNull Analyzer analyzer) throws IOException {
        TokenStream tokenStream = analyzer.tokenStream("text", new StringReader(content));
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
            final String token = tokenStream.getAttribute(CharTermAttribute.class).toString();
            wordFreq.compute(token, (t, oldV) -> (oldV == null) ? 1 : oldV + 1);
        }
        tokenStream.end();
        tokenStream.close();
    }

    /**
     * Normalize a Map.
     * @param freqMap Map
     * @return Normalized map
     */

    @NotNull
    private Map<String, Float> normalize(@NotNull Map<String, Integer> freqMap) {


        Map<String, Float> dist = new HashMap<>();

        // Calculate the normalizer
        int norm = 0;
        for (int val : freqMap.values()) {
            norm += val;
        }

        // Normalize the map
        for (String word: freqMap.keySet()) {
            int freq = freqMap.get(word);
            float normFreq = (float) freq / norm;
            normFreq = Float.parseFloat((df.format(normFreq)));
            if (! (normFreq < 0.0d) ) {
                dist.put(word, normFreq);
            }
        }
        return dist;
    }

    /**
     * Pre-process the text.
     * (1) Lowercase words.
     * (2) Remove all spaces.
     * (3) Remove special characters.
     * (4) Remove stop words.
     * @param text String Text to pre-process
     * @return List of words from the text after pre-processing.
     */

    @NotNull
    private List<String> preProcess(String text) {

        // Convert all words to lowercase
        text = text.toLowerCase();

        // Remove all spaces
        text = text.replace("\n", " ").replace("\r", " ");

        // Remove all special characters such as - + ^ . : , ( )
        text = text.replaceAll("[\\-+.^*:,;=(){}\\[\\]\"]","");

        // Get all words
        List<String> words = new ArrayList<>(Arrays.asList(text.split(" ")));
        words.removeAll(Collections.singleton(null));
        words.removeAll(Collections.singleton(""));

        // Remove all stop words
        words.removeIf(stopWords::contains);

        return words;
    }

    /**
     * Main method.
     * @param args Command line arguments.
     */


    public static void main(@NotNull String[] args) {
        Similarity similarity = null;
        Analyzer analyzer = null;

        String pageIndexDir = args[0];
        String paraIndexDir = args[1];
        String mainDir = args[2];
        String dataDir = args[3];
        String outputDir = args[4];
        String paraRunFile = args[5];
        String entityRunFile = args[6];
        String entityQrelFile = args[7];
        String stopWordsFilePath = args[8];
        int takeKTerms = Integer.parseInt(args[9]);
        int takeKDocs = Integer.parseInt(args[10]);
        String o = args[11];
        String p = args[12];
        String u = args[13];
        String a = args[14];
        String sim = args[15];

        boolean parallel = false;
        boolean omit = o.equalsIgnoreCase("true") || o.equalsIgnoreCase("yes");
        boolean useEcd = u.equalsIgnoreCase("true") || u.equalsIgnoreCase("yes");
        String s1 = null, s2, s3;

        if (useEcd) {
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
                analyzer = new StandardAnalyzer();
                System.out.println("Analyzer: Standard");
                break;
            case "eng":
                analyzer = new EnglishAnalyzer();
                System.out.println("Analyzer: English");
                break;
            default:
                System.out.println("Wrong choice of analyzer! Exiting.");
                System.exit(1);
        }
        switch (sim) {
            case "BM25" :
            case "bm25":
                System.out.println("Similarity: BM25");
                similarity = new BM25Similarity();
                s1 = "bm25";
                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                try {
                    float lambda = Float.parseFloat(args[16]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                    s1 = "lmjm";
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("No lambda value for similarity LM-JM.");
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
        String outFile = "QEWikiTerms" + "-" + s1 + "-" + s2 + "-" + s3 + ".run";

        if (p.equalsIgnoreCase("true")) {
            parallel = true;
        }

        new QEWikiTerms(pageIndexDir,paraIndexDir, mainDir, dataDir, outputDir, paraRunFile, entityRunFile,
                entityQrelFile, outFile, stopWordsFilePath,  parallel, useEcd, takeKTerms, takeKDocs, omit,
                analyzer, similarity);
    }


}
