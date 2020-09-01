package experiments.ecd;

import help.PseudoDocument;
import help.Utilities;
import lucene.Index;
import lucene.RAMIndex;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

/**
 * This class scores a support passage for a query-entity pair by summing over the retrieval scores
 * of the pseudo-document it appears in.
 * @author Shubham Chatterjee
 * @version 02/25/2019
 */
public class ECDRetScore {
    private final IndexSearcher searcher;
    //HashMap where Key = queryID and Value = list of paragraphs relevant for the queryID
    private final HashMap<String, ArrayList<String>> paraRankings;
    //HashMap where Key = queryID and Value = list of entities relevant for the queryID
    private final HashMap<String,ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> entityQrels;
    // ArrayList of run strings
    private final ArrayList<String> runStrings;
    private final boolean parallel;
    private final DecimalFormat df;

    /**
     * Constructor.
     * @param indexDir String Path to the index directory.
     * @param mainDir String Path to the TREC-CAR directory.
     * @param outputDir String Path to output directory within TREC-CAR directory.
     * @param dataDir String Path to data directory within TREC-CAR directory.
     * @param passageRunFile String Name of the passage run file within data directory.
     * @param entityRunFile String Name of the entity run file within data directory.
     * @param outFile String Name of the output run file. This will be stored in the output directory mentioned above.
     * @param entityQrelFile String Name of the entity ground truth file.
     * @param parallel Boolean Whether to run code in parallel or not.
     */

    public ECDRetScore(String indexDir,
                       String mainDir,
                       String outputDir,
                       String dataDir,
                       String passageRunFile,
                       String entityRunFile,
                       String outFile,
                       String entityQrelFile,
                       boolean parallel)  {

        String entityRunFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String passageRunFilePath = mainDir + "/" + dataDir + "/" + passageRunFile;
        String outFilePath = mainDir + "/" + outputDir + "/" + outFile;
        String entityQrelFilePath = mainDir + "/" + dataDir + "/" + entityQrelFile;
        this.runStrings = new ArrayList<>();
        this.parallel = parallel;

        df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);


        System.out.print("Reading entity rankings...");
        entityRankings = Utilities.getRankings(entityRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading passage rankings...");
        paraRankings = Utilities.getRankings(passageRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelFilePath);
        System.out.println("[Done].");

        System.out.print("Setting up index for use...");
        searcher = new Index.Setup(indexDir).getSearcher();
        System.out.println("[Done].");

        feature(outFilePath);

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
     * @param queryId String
     * @throws IOException Exception
     */
    private void doTask(String queryId) throws IOException {

        if (entityRankings.containsKey(queryId) && entityQrels.containsKey(queryId)) {

            // Get the set of entities retrieved for the query
            Set<String> retEntitySet = new HashSet<>(entityRankings.get(queryId));

            // Get the set of entities relevant for the query
            Set<String> relEntitySet = new HashSet<>(entityQrels.get(queryId));

            // Get the number of retrieved entities which are also relevant
            // Finding support passage for non-relevant entities makes no sense!!

            retEntitySet.retainAll(relEntitySet);

            // Get the list of passages retrieved for the query
            ArrayList<String> paraList = paraRankings.get(queryId);
            ArrayList<Document> queryDocs = new ArrayList<>();
            HashMap<String, PseudoDocument> entityToPseudoDocMap = new HashMap<>();
            Map<String, Float> documentScore = new HashMap<>();

            // Get the list of pseudo-documents and the map of entity to pseudo-documents for the query
            getPseudoDocList(retEntitySet, queryDocs, paraList, entityToPseudoDocMap);

            // Build the index
            // First create the IndexWriter
            IndexWriter iw = RAMIndex.createWriter(new EnglishAnalyzer());
            // Now create the index
            RAMIndex.createIndex(queryDocs, iw);
            // Create the IndexSearcher and QueryParser
            IndexSearcher is = RAMIndex.createSearcher(new BM25Similarity(), iw);
            QueryParser qp = RAMIndex.createParser("text", new EnglishAnalyzer());
            // Search the index for the query
            // But first process the query
            String query = queryId
                    .substring(queryId.indexOf(":") + 1)          // remove enwiki: from query
                    .replaceAll("%20", " ")     // replace %20 with whitespace
                    .toLowerCase();                            //  convert query to lowercase
            // Now search the query
            LinkedHashMap<Document, Float> results = Utilities.sortByValueDescending(searchIndex(query, 100, is, qp));
            if (!results.isEmpty()) {
                documentScore = Utilities.sortByValueDescending(scoreParas(results, documentScore, entityToPseudoDocMap));
                makeRunStrings(queryId, documentScore, entityToPseudoDocMap);
            } else {
                System.out.printf("No results found for query %s. Cannot score documents.", queryId);
            }
            if (parallel) {
                System.out.println("Done query: " + queryId);
            }
            RAMIndex.close(iw);
        }
    }
    @NotNull
    private Map<Document, Float> searchIndex(String query, int n, IndexSearcher is, @NotNull QueryParser qp) {
        HashMap<Document,Float> results = new HashMap<>();
        // Parse the query
        Query q = null;
        try {
            q = qp.parse(query);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        // Search the query
        TopDocs tds = null;
        try {
            tds = is.search(q,n);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Retrieve the results
        ScoreDoc[] retDocs;
        if (tds != null) {
            retDocs = tds.scoreDocs;
        } else {
            return results;
        }
        for (int i = 0; i < retDocs.length; i++) {
            try {
                Document doc = is.doc(retDocs[i].doc);
                float score = tds.scoreDocs[i].score;
                results.put(doc, score);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return results;
    }

    /**
     * Method to find the set of pseudo-documents for the query and the map from entity to pseudo-document.
     * @param retEntitySet Set Set of entities retrieved for the quert which are also relevant
     *                     (according to entity ground truth data)
     * @param queryDocs List List of pseudo-documents for the query.
     * @param paraList List List of passages retrieved for the query in the candidate pool.
     * @param entityToPseudoDocMap Map Map where Key = entityID and Value = PseudoDocument for the entity.
     */

    private void getPseudoDocList(@NotNull Set<String> retEntitySet,
                                  ArrayList<Document> queryDocs,
                                  ArrayList<String> paraList,
                                  HashMap<String, PseudoDocument>  entityToPseudoDocMap) {
        // For every entity in this list of relevant entities do
        for (String entityId : retEntitySet) {
            //System.out.println(entityId);

            // Create a pseudo-document for the entity
            PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                    " ", paraList, searcher);
            if (d != null) {
                // Add to HashMap where Key = entityID and Value = Pseudo-document
                entityToPseudoDocMap.put(entityId, d);

                // Convert Pseudo-document to lucene document
                Document doc = Utilities.pseudoDocToDoc(d);

                // Add it to list of documents for query
                queryDocs.add(doc);
            }
        }
    }

    /**
     * Make the run file strings.
     * @param queryId String
     *
     * @param entityToPseudoDocMap HashMap where Key = entity and Value = Pseudo-document for this entity
     */

    private void makeRunStrings(String queryId,
                                Map<String, Float> scores,
                                @NotNull Map<String, PseudoDocument> entityToPseudoDocMap) {


        // For every entity do
        for (String entityId : entityToPseudoDocMap.keySet()) {
            // Get the pseudo-document for the entity
            PseudoDocument doc = entityToPseudoDocMap.get(entityId);
            // Get the documents in the pseudo-document
            ArrayList<Document> docList = doc.getDocumentList();
            Map<String, Float> docScores = new LinkedHashMap<>();
            getPseudoDocScores(docList, docScores, scores);
            docScores = Utilities.sortByValueDescending(docScores);
            makeRunStrings(queryId, entityId, docScores);
        }
    }

    private void makeRunStrings(String queryID,
                                String entityID,
                                @NotNull Map<String, Float> docScores) {

        String query = queryID + "+" + entityID;
        String runFileString;
        Set<String> paraSet = docScores.keySet();
        int rank = 1;
        float score;
        for (String paraID : paraSet) {
            score = Float.parseFloat((df.format(docScores.get(paraID))));
            runFileString = query + " Q0 " + paraID + " " + rank++ + " " + score + " " + "ECDRetScore";
            runStrings.add(runFileString);
            //System.out.println(runFileString);
        }
    }

    private void getPseudoDocScores(@NotNull ArrayList<Document> docList,
                                    Map<String, Float> docScores,
                                    Map<String, Float> scores) {
        String paraId;
        float score;
        for (Document d : docList) {
            paraId = d.getField("id").stringValue();
            if (scores.containsKey(paraId)) {
                score = scores.get(paraId);
                docScores.put(paraId, score);
            }
        }
    }

    /**
     * Score the paragraphs in a Pseudo-document.
     * NOTE: One of the parameters of this method is a Map of (Document, Float)
     * and this method returns another Map of (Document, Float).
     * However, the Document in the parameter comes from the pseudo-documents meaning that for every pseudo-document,
     * we convert it to a Lucene Document by indexing it's components as Fields. See {@link Utilities#pseudoDocToDoc(PseudoDocument)} method.
     * The Document in the Map returned by this method is a Document from the original index.
     * @param results HashMap where Key = Document and Value = Score
     * @param entityToPseudoDocMap HashMap where Key = entity and Value = Pseudo-document for this entity
     * @return HashMap where Key = Document and Value = Score
     */


    @Contract("_, _, _ -> param2")
    @NotNull
    private Map<String, Float> scoreParas(@NotNull Map<Document, Float> results,
                                          Map<String, Float> documentScore,
                                          HashMap<String, PseudoDocument>  entityToPseudoDocMap) {

        // For every document retrieved do
        // Each Document is actually a PseudoDocument
        for (Document doc : results.keySet()) {
            String entity = doc.getField("entity").stringValue();
            // Get the pseudo-document corresponding to this entity

            PseudoDocument d = entityToPseudoDocMap.get(entity);
            if (d != null) {
                // Get the score of this document
                float score = results.get(doc);
                // Get the list of documents contained in the pseudo-document
                // This list is the list of actual Documents
                ArrayList<Document> documentList = d.getDocumentList();
                // For every document in this list of documents do
                for (Document document : documentList) {
                    float s = 0;
                    String id = document.get("id");
                    // If the document is already has a score get that score and add it to the new score
                    // Else add it to the score map
//                    if (documentScore.containsKey(id)) {
//                        s = documentScore.get(id);
//                    }
//                    s += score;
//                    documentScore.put(id, s);
                    documentScore.compute(id, (t, oldV) -> (oldV == null) ? score : oldV + score);
                }
            }
        }
        return documentScore;
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
        String paraRunFile = args[4];
        String entityRunFile = args[5];
        String entityQrel = args[6];
        boolean parallel = args[7].equalsIgnoreCase("true");

        String outFile = "ECDRetScore.run";

        new ECDRetScore(indexDir, mainDir, outputDir, dataDir, paraRunFile, entityRunFile,
                outFile, entityQrel, parallel);
    }
}
