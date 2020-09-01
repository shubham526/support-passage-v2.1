package experiments.wiki;

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

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

/**
 * =================================================WikiNames================================================
 * Probability distribution over different surface forms being used as anchor text within links to an entity.
 * ==========================================================================================================
 *
 * @author Shubham Chatterjee
 * @version 6/3/2020
 */

public class WikiNames {
    private final IndexSearcher KBSearcher;
    private final IndexSearcher paraSearcher;
    private final HashMap<String, ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> paraRankings;
    private final HashMap<String, ArrayList<String>> entityQrels;
    private final ArrayList<String> runStrings;
    private final boolean parallel;
    private final DecimalFormat df;

    /**
     * Constructor.
     * @param KBIndexDir String Path to the knowledge base index.
     * @param paraIndexDir String Path to the paragraph index.
     * @param mainDir String Path to the main directory.
     * @param dataDir String Path to data directory within main directory.
     * @param outputDir String Path to output directory within main directory.
     * @param paraRunFile String Name of passage run file within the data directory.
     * @param entityRunFile String Name of entity run file within the data directory.
     * @param entityQrelFile String Name of entity qrel file within the data directory.
     * @param outFile String Name of output run file (to be stored in directory specified with outputDir).
     * @param parallel Boolean Whether to run code in parallel or not.
     * @param analyzer Analyzer Type of lucene analyzer. Maybe English(eng) or Standard (std).
     * @param similarity Similarity Type of similarity. Maybe BM25, LMDS or LMJM.
     */

    public WikiNames(String KBIndexDir,
                     String paraIndexDir,
                     String mainDir,
                     String dataDir,
                     String outputDir,
                     String paraRunFile,
                     String entityRunFile,
                     String entityQrelFile,
                     String outFile,
                     boolean parallel,
                     Analyzer analyzer,
                     Similarity similarity) {


        String entityRunFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String paraFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String entityQrelFilePath = mainDir + "/" + dataDir + "/" + entityQrelFile;
        String outFilePath = mainDir + "/" + outputDir + "/" + outFile;
        this.runStrings = new ArrayList<>();
        this.parallel = parallel;

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

        System.out.print("Setting up page index for use...");
        KBSearcher = new Index.Setup(KBIndexDir, "Text", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        System.out.print("Setting up paragraph index for use...");
        paraSearcher = new Index.Setup(paraIndexDir, "text", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        feature(outFilePath);

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

                // Get probability distribution over different surface forms being used as anchor text
                // within links to an entity
                Map<String, Double> freqDist = getWikiNameDistribution(entityId);

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
            System.out.println("Done: " + queryId);
        }
    }

    @NotNull
    private Map<String, Double> score(@NotNull List<Document> candidatePassages, Map<String, Double> freqDist) {
        Map<String, Double> paraScores = new HashMap<>();

        for (Document d : candidatePassages) {
            String text = d.get("text");
            String id = d.get("id");
            double score = scorePara(text, freqDist);
            paraScores.put(id, score);
        }
        return paraScores;
    }

    private double scorePara(String text, @NotNull Map<String, Double> freqDist) {

        double score = 0.0d;

        for (String anchor : freqDist.keySet()) {
            if (! (anchor.length() == 0) ) {
                int freqInText = countFreq(anchor, text);
                double anchorProb = freqDist.get(anchor);
                for (int i = 1; i <= freqInText; i++) {
                    score += anchorProb;
                }
            }
        }
        return score;
    }

    private int countFreq(String anchor, @NotNull String text) {
        String nstr = text.replace(anchor, "");
        return ((text.length() - nstr.length()) / anchor.length());
    }

    @NotNull
    public Map<String, Double> getWikiNameDistribution(String entity) {
        Document d;
        Map<String, Double> freqDist = new HashMap<>();
        try {
            d = Index.Search.searchIndex("Id", entity, KBSearcher);
            assert d != null;
            Map<String, Integer> anchorFreqMap = getAnchorFreq(d);
            freqDist = normalize(anchorFreqMap);
        } catch (ParseException | IOException | NullPointerException e) {
            e.printStackTrace();
        }
        return Utilities.sortByValueDescending(freqDist);
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

    @NotNull
    private Map<String, Integer> getAnchorFreq(@NotNull Document d) {
        Map<String, Integer> anchorFreqMap = new HashMap<>();
        String[] inLinkAnchors = d.getField("InlinkAnchors").stringValue().split("\n");
        for (String anchorFreq : inLinkAnchors) {
            try {
                String anchor = anchorFreq.split("_")[0];
                int freq = Integer.parseInt(anchorFreq.split("_")[1]);
                anchorFreqMap.put(anchor, freq);
            } catch (NumberFormatException e) {
                System.err.println("ERROR: NumberFormatException: " + anchorFreq);
            }
        }
        return anchorFreqMap;
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
            if (score > 0) {
                runFileString = queryId + "+" + entityId + " Q0 " + paraId + " " + rank++
                        + " " + score + " " + "WikiNames";
                runStrings.add(runFileString);
            }

        }
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
        String outFile = "WikiNames.run";

        if (p.equalsIgnoreCase("true")) {
            parallel = true;
        }

        new WikiNames(pageIndexDir,paraIndexDir, mainDir, dataDir, outputDir, paraRunFile, entityRunFile,
                entityQrelFile, outFile, parallel, analyzer, similarity);
    }

}
