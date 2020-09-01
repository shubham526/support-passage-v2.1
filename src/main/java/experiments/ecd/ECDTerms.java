package experiments.ecd;

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
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;


/**
 * ====================================================ECDTerms========================================================
 * (1) Create a PseudoDocument for the entity.
 * (2) Get a frequency distribution over the terms from the PseudoDocument of an entity.
 * (3) The Documents in the PseudoDocument are the candidate support passages.
 * (4) Score a passage by accumulating the frequency of each term, obtained from the distribution in (2).
 * =====================================================================================================================
 *
 * @author Shubham Chatterjee
 * @version 6/11/2020
 */

public class ECDTerms {

    private final IndexSearcher paraSearcher;
    private final HashMap<String, ArrayList<String>> entityRankings;
    private final HashMap<String, LinkedHashMap<String, Double>> paraRankings = new HashMap<>();
    private final HashMap<String, ArrayList<String>> entityQrels;
    private final ArrayList<String> runStrings;
    private final List<String> stopWords;
    private final boolean parallel;
    private final DecimalFormat df;

    /**
     * Constructor.
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

    public ECDTerms(String paraIndexDir,
                     String mainDir,
                     String dataDir,
                     String outputDir,
                     String paraRunFile,
                     String entityRunFile,
                     String entityQrelFile,
                     String outFile,
                     String stopWordsFilePath,
                     boolean parallel,
                     Analyzer analyzer,
                     Similarity similarity) {


        String entityRunFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String paraFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String entityQrelFilePath = mainDir + "/" + dataDir + "/" + entityQrelFile;
        String outFilePath = mainDir + "/" + outputDir + "/" + outFile;
        this.runStrings = new ArrayList<>();
        this.stopWords = new ArrayList<>();
        this.parallel = parallel;

        df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);

        System.out.print("Reading entity rankings...");
        entityRankings = Utilities.getRankings(entityRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading paragraph rankings...");
        Utilities.getRankings(paraFilePath, paraRankings);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelFilePath);
        System.out.println("[Done].");

        System.out.print("Reading stop words list...");
        getStopWords(stopWordsFilePath);
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

    private void doTask(String queryId) {

        if (entityRankings.containsKey(queryId) && entityQrels.containsKey(queryId)) {

            // Get the list of passages retrieved for the query
            Map<String, Double> paraScoreMap = paraRankings.get(queryId);
            ArrayList<String> paraList = new ArrayList<>(paraScoreMap.keySet());

            // Get the set of entities retrieved for the query
            Set<String> retEntitySet = new HashSet<>(entityRankings.get(queryId));

            // Get the set of entities relevant for the query
            Set<String> relEntitySet = new HashSet<>(entityQrels.get(queryId));

            // Get the number of retrieved entities which are also relevant
            // Finding support passage for non-relevant entities makes no sense!!

            retEntitySet.retainAll(relEntitySet);

            // For every entity in this set of relevant (retrieved) entities do
            for (String entityId : retEntitySet) {

                // Create a pseudo-document for the entity
                PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                        " ", paraList, paraSearcher);

                if (d != null) {
                    // Get the candidate passages
                    List<Document> candidatePassages = d.getDocumentList();

                    // Get the scores of the candidate passages
                    Map<String, Double> candidatePsgScoreMap = getCandidatePsgScores(paraScoreMap, candidatePassages);

                    // Get distribution over the terms from the Wikipedia article
                    Map<String, Double> freqDist = getEcdTermDistribution(candidatePsgScoreMap);

                    // Score the candidate passages
                    Map<String, Double> paraScores = score(candidatePassages, freqDist);

                    // Create run file strings
                    makeRunStrings(queryId, entityId, paraScores);
                }
            }
        }
        if (parallel) {
            System.out.println("Done: " + queryId);
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
            score = Double.parseDouble((df.format(score)));
            if (score > 0) {
                runFileString = queryId + "+" + entityId + " Q0 " + paraId + " " + rank++
                        + " " + score + " " + "ECDTerms";
                runStrings.add(runFileString);
            }

        }
    }

    /**
     * Scores a paragraph.
     * @param candidatePassages List
     * @param freqDist Map
     * @return Map
     */

    @NotNull
    private Map<String, Double> score(@NotNull List<Document> candidatePassages,
                                      Map<String, Double> freqDist) {
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
                score += freqDist.get(word);
            }
        }
        return Double.parseDouble(df.format(score));
    }

    /**
     * Helper method.
     * Finds the distribution over terms in the ECD.
     * Incorporates the retrieval score of the passage.
     * Frequency of term = Number of times term appears in text * retrieval score of passage.
     * @param candidatePsgScoreMap Map of (ParaID, Score).
     * @return A distribution over terms.
     */


    @NotNull
    private Map<String, Double> getEcdTermDistribution(@NotNull Map<String, Double> candidatePsgScoreMap) {
        Map<String, Double> freqDist = new HashMap<>();

        // compute score normalizer
        double normalizer = 0.0;
        for (String pid : candidatePsgScoreMap.keySet()) {
            normalizer += candidatePsgScoreMap.get(pid);
        }


        for (String pid : candidatePsgScoreMap.keySet()) {

            double weight = candidatePsgScoreMap.get(pid) / normalizer;
            List<String> words = getPsgWords(pid);

            for (String w : words) {
                freqDist.compute(w, (t, oldV) -> (oldV == null) ? weight : oldV + weight);
            }
        }
        return Utilities.sortByValueDescending(freqDist);
    }

    /**
     * Helper method.
     * Takes a ParaID and returns the list of words in the paragraph after preprocessing.
     * @param pid String ParaID
     * @return List of words in the paragraph.
     */

    @NotNull
    private List<String> getPsgWords(String pid) {
        // Get the document corresponding to the paragraph from the lucene index
        String docContents = "";
        try {
            Document doc = Index.Search.searchIndex("id", pid, paraSearcher);
            assert doc != null;
            docContents = doc.get("text");
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        return preProcess(docContents);
    }

    /**
     * Helper method.
     * Finds the passage scores corresponding to the list of Lucene Documents passed in.
     * @param paraScoreMap Map of (ParaID, Score) from the paragraph run file.
     * @param candidatePassages List of Lucene Documents.
     * @return Map of (ParaID, Score) corresponding to the list of Lucene Documents.
     */

    @NotNull
    private Map<String, Double> getCandidatePsgScores(Map<String, Double> paraScoreMap,
                                                      @NotNull List<Document> candidatePassages) {

        Map<String, Double> scoreMap = new HashMap<>();

        for (Document d : candidatePassages) {
            String pID = d.getField("id").stringValue();
            double score = paraScoreMap.getOrDefault(pID, 0.0d);
            scoreMap.put(pID, score);
        }

        return scoreMap;

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

        String paraIndexDir = args[0];
        String mainDir = args[1];
        String dataDir = args[2];
        String outputDir = args[3];
        String paraRunFile = args[4];
        String entityRunFile = args[5];
        String entityQrelFile = args[6];
        String stopWordsFilePath = args[7];
        String p = args[8];
        String a = args[9];
        String sim = args[10];

        boolean parallel = false;

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
                    float lambda = Float.parseFloat(args[11]);
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
        String outFile = "ECDTerms.run";

        if (p.equalsIgnoreCase("true")) {
            parallel = true;
        }

        new ECDTerms(paraIndexDir, mainDir, dataDir, outputDir, paraRunFile, entityRunFile,
                entityQrelFile, outFile, stopWordsFilePath, parallel, analyzer, similarity);
    }


}
