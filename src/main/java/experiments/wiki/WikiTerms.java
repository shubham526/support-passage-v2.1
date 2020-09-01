package experiments.wiki;


import help.PseudoDocument;
import help.Utilities;
import lucene.Index;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.*;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ====================================================WikiTerms========================================================
 * (1) Get a frequency distribution over the terms from the Wikipedia article of an entity.
 * (2) Create a PseudoDocument for the entity. The Documents in the PseudoDocument are the candidate support passages.
 * (3) Score a passage by accumulating the frequency of each term, obtained from the distribution in (1).
 * =====================================================================================================================
 *
 * @author Shubham Chatterjee
 * @version 6/2/2020
 */

public class WikiTerms {

    private final IndexSearcher KBSearcher;
    private final IndexSearcher paraSearcher;
    private IndexReader indexReader = null;
    private final HashMap<String,ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> paraRankings;
    private final HashMap<String, ArrayList<String>> entityQrels;
    private final ArrayList<String> runStrings;
    private final List<String> stopWords;
    private final boolean parallel, useProb;
    private final DecimalFormat df;
    private double MU;
    private final AtomicInteger count = new AtomicInteger();

    /**
     * Constructor.
     * @param KBIndexDir String Path to the page index (knowledge graph).
     * @param paraIndexDir String Path to the paragraph index.
     * @param mainDir String Path to the main directory.
     * @param dataDir String Path to data directory within main directory.
     * @param outputDir String Path to output directory within main directory.
     * @param paraRunFile String Name of passage run file within the data directory.
     * @param entityRunFile String Name of entity run file within the data directory.
     * @param entityQrelFile String Name of entity qrel file within the data directory.
     * @param outFile String Name of output run file (to be stored in directory specified with outputDir).
     * @param stopWordsFilePath String Path to the stop words list.
     * @param parallel Boolean Whether to run code in parallel or not.
     * @param analyzer Analyzer Type of lucene analyzer. Maybe English(eng) or Standard (std).
     * @param similarity Similarity Type of similarity. Maybe BM25, LMDS or LMJM.
     */

    public WikiTerms(String KBIndexDir,
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
                     boolean useProb,
                     Analyzer analyzer,
                     Similarity similarity) {


        String entityRunFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String paraFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String entityQrelFilePath = mainDir + "/" + dataDir + "/" + entityQrelFile;
        String outFilePath = mainDir + "/" + outputDir + "/" + outFile;
        this.runStrings = new ArrayList<>();
        this.stopWords = new ArrayList<>();
        this.parallel = parallel;
        this.useProb = useProb;

        df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);

        System.out.print("Reading entity rankings...");
        entityRankings = Utilities.getRankings(entityRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading paragraph rankings...");
        paraRankings = Utilities.getRankings(paraFilePath);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelFilePath);
        System.out.println("[Done].");

        System.out.print("Reading stop words list...");
        getStopWords(stopWordsFilePath);
        System.out.println("[Done].");

        System.out.print("Setting up page index for search...");
        KBSearcher = new Index.Setup(KBIndexDir, "Text", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        System.out.print("Setting up paragraph index for search...");
        paraSearcher = new Index.Setup(paraIndexDir, "text", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        if (useProb) {

            System.out.print("Setting up paragraph index for reading...");
            try {
                indexReader = DirectoryReader.open(FSDirectory.open(new File(paraIndexDir).toPath()));
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                long m = indexReader.getSumTotalTermFreq("text");
                long n = indexReader.numDocs();
                MU = 1.0 * m / n;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

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
            querySet.parallelStream().forEach(query -> doTask(query, querySet.size()));
        } else {
            System.out.println("Using Sequential Streams.");

            // Do in serial
            ProgressBar pb = new ProgressBar("Progress", querySet.size());
            for (String q : querySet) {
                doTask(q, querySet.size());
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

    private void doTask(String queryId, int n) {

        if (entityRankings.containsKey(queryId) && entityQrels.containsKey(queryId)) {

            // Get the list of passages retrieved for the query
            ArrayList<String> paraList = paraRankings.get(queryId);

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
                Map<String, Double> freqDist = getWikiTermDistribution(entityId);

                // Create a pseudo-document for the entity
                PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                        " ", paraList, paraSearcher);

                if (d != null) {
                    // Get the candidate passages
                    List<Document> candidatePassages = d.getDocumentList();

                    // Score the candidate passages
                    Map<String, Double> paraScores = score(candidatePassages, freqDist);

                    // Create run file strings
                    makeRunStrings(queryId, entityId, paraScores);
                }
            }
        }
        if (parallel) {
            count.getAndIncrement();
            System.err.println("Progress: " + count.get() + " of " + n);
            //System.out.println("Done: " + queryId);
        }
    }

    /**
     * Makes the run file strings.
     * @param queryId String
     * @param entityId String
     * @param scoreMap Map of (ParaID, Score)
     */

    private void makeRunStrings(String queryId, String entityId, Map<String, Double> scoreMap) {
        LinkedHashMap<String, Double> paraScore = Utilities.sortByValueDescending(scoreMap);
        String runFileString;
        int rank = 1;

        for (String paraId : paraScore.keySet()) {
            double score = paraScore.get(paraId);
            runFileString = queryId + "+" + entityId + " Q0 " + paraId + " " + rank++ + " " + score + " " + "WikiTerms";
            runStrings.add(runFileString);

        }
    }

    /**
     * Scores a paragraph.
     * @param candidatePassages List
     * @param freqDist Map
     * @return Map
     */

    @NotNull
    private Map<String, Double> score(@NotNull List<Document> candidatePassages, Map<String, Double> freqDist) {
        Map<String, Double> paraScores = new HashMap<>();

        for (Document d : candidatePassages) {
            String text = d.get("text");
            String id = d.get("id");
            List<String> textWords = preProcess(text);
            double score = scorePara(textWords, freqDist);
            paraScores.put(id, score);
        }
        return paraScores;
    }

    /**
     * Scores a single passage.
     * @param textWords List of words in the paragraph.
     * @param freqDist Frequency distribution over terms from the Wiki article.
     * @return Double Paragrapgh score.
     */

    private double scorePara(@NotNull List<String> textWords, Map<String, Double> freqDist) {
        double score = 0.0d;

        for (String word : textWords) {
            if (freqDist.containsKey(word)) {
                if (useProb) {
                    score += Math.log(freqDist.get(word));
                } else {
                    score += freqDist.get(word);
                }

            }
        }
        return Double.parseDouble(df.format(score));
    }

    /**
     * Calls a helper method to find the distribution over the terms from the Wikipedia article.
     * @param entity String
     * @return Map of (Term, Frequency).
     */

    public Map<String, Double> getWikiTermDistribution(String entity) {
        Document d;
        Map<String, Integer> freqDist = new HashMap<>();
        Map<String, Double> probDist = new HashMap<>();
        String text = null;
        try {
            d = Index.Search.searchIndex("Id", entity, KBSearcher);
            assert d != null;
            text = d.getField("Content").stringValue();
        } catch (ParseException | IOException | NullPointerException e) {
            e.printStackTrace();
        }
        if (useProb) {
            probDist = probabilityDistribution(text);
            return probDist;
        } else {
            freqDist = frequencyDistribution(text);
            return Utilities.sortByValueDescending(normalize(freqDist));
        }
    }

    /**
     * Helper method.
     * Finds the distribution over the terms from the Wikipedia article.
     * @param text String
     * @return Map
     */


    @NotNull
    private Map<String, Integer> frequencyDistribution(String text) {
        Map<String, Integer> freqMap = new HashMap<>();

        List<String> words = preProcess(text);

//        for (String w : words) {
//            Integer n = freqMap.get(w);
//            n = (n == null) ? 1 : ++n;
//            freqMap.put(w, n);
//        }

        for (String w : words) {
            freqMap.compute(w, (t, oldV) -> (oldV == null) ? 1 : oldV + 1);
        }

        return freqMap;
    }

    @NotNull
    private Map<String, Double> probabilityDistribution(String text) {

        Map<String, Integer> freqMap = frequencyDistribution(text);
        Map<String, Double> probDist = new HashMap<>();

        for (String w : freqMap.keySet()) {
            int tf = freqMap.get(w);
            double dirichletPrior  = dirichletPrior(w);
            int docLength = preProcess(text).size();
            double posterior = (tf + MU * dirichletPrior) / (docLength + MU);
            probDist.put(w, posterior);
        }
        return probDist;
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
            double normFreq = (double) freq / norm;
            normFreq = Double.parseDouble(df.format(normFreq));
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

    private double dirichletPrior(String word) {
        long corpusTF = 0; // get the total frequency of the term in the "text" field
        long corpusLength = 0;  // get the total length of the "text" field
        try {
            corpusTF = indexReader.totalTermFreq( new Term( "text", word ) );
            corpusLength = indexReader.getSumTotalTermFreq( "text" );
        } catch (IOException e) {
            e.printStackTrace();
        }

        return 1.0 * corpusTF / corpusLength;
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
        boolean parallel = args[9].equalsIgnoreCase("true");
        boolean useProb = args[10].equalsIgnoreCase("true");
        String a = args[11];
        String sim = args[12];

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
                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                try {
                    float lambda = Float.parseFloat(args[13]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("No lambda value for similarity LM-JM.");
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
        String outFile = "WikiTerms.run";


        new WikiTerms(pageIndexDir,paraIndexDir, mainDir, dataDir, outputDir, paraRunFile, entityRunFile,
                entityQrelFile, outFile, stopWordsFilePath, parallel, useProb, analyzer, similarity);
    }


}
