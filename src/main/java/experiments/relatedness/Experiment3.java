package experiments.relatedness;

import api.WATApi;
import help.EntityRMExpand;
import help.PseudoDocument;
import help.Utilities;
import lucene.Index;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
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
 * ===============================Experiment-3==========================
 * Query expansion with relevant entities in context.
 * Context is the PseudoDocument of the entity.
 * =====================================================================
 *
 * @author Shubham Chatterjee
 * @version 03/22/2020
 */

public class Experiment3 {
    private final IndexSearcher searcher;

    //HashMap where Key = queryID and Value = list of paragraphs relevant for the queryID
    private final HashMap<String, ArrayList<String>> paraRankings;

    //HashMap where Key = queryID and Value = list of entities relevant for the queryID
    private final HashMap<String,ArrayList<String>> entityRankings;

    private final HashMap<String, ArrayList<String>> entityQrels;
    private Map<String, Integer> entityIDMap = new ConcurrentHashMap<>();
    private Map<String, Map<String, Double>> entRelMap = new ConcurrentHashMap<>();

    // ArrayList of run strings
    private final ArrayList<String> runStrings = new ArrayList<>();
    private final int takeKEntities; // Number of query expansion terms
    private final boolean omitQueryTerms; // Omit query terms or not when calculating expansion terms
    private final Analyzer analyzer; // Analyzer to use
    private String relType;

    /**
     * Constructor.
     * @param paraIndexDir String Path to the paragraph.entity.lucene index directory.
     * @param mainDir String Path to the main directory.
     * @param outputDir String Path to the output directory within the main directory.
     * @param dataDir String Path to the data directory within the main directory.
     * @param idFile String Name of id file within data directory.
     * @param paraRunFile String Name of the passage run file within the data directory.
     * @param entityRunFile String Name of the entity run file within the data directory.
     * @param entityQrelPath String Path to the entity ground truth file.
     * @param outFile String Name of the output file.
     * @param takeKEntities Integer Top K entities for query expansion.
     * @param similarity Similarity Type of similarity to use.
     * @param analyzer Analyzer Type of analyzer to use.
     * @param omitQueryTerms Boolean Whether or not to omit query terms during expansion.
     */

    public Experiment3(String paraIndexDir,
                       String mainDir,
                       String outputDir,
                       String dataDir,
                       String idFile,
                       String relFile,
                       String paraRunFile,
                       String entityRunFile,
                       String outFile,
                       String entityQrelPath,
                       int takeKEntities,
                       boolean omitQueryTerms,
                       boolean useFrequency,
                       @NotNull String relType,
                       Analyzer analyzer,
                       Similarity similarity) {


        this.takeKEntities = takeKEntities;
        this.analyzer = analyzer;
        this.omitQueryTerms = omitQueryTerms;

        String entityFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String paraFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String idFilePath = mainDir + "/" + dataDir + "/" + idFile;
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
        entityQrels = Utilities.getRankings(entityQrelPath);
        System.out.println("[Done].");

        System.out.print("Reading relatedness file...");
        try {
            entRelMap = Utilities.readMap(relFilePath);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");

        System.out.print("Reading id file...");
        try {
            entityIDMap = Utilities.readMap(idFilePath);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");

        System.out.print("Setting up paragraph index for use...");
        searcher = new Index.Setup(paraIndexDir, "text", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        feature(outputFilePath, useFrequency);
    }
    /**
     * Method to calculate the first feature.
     * Works in parallel using Java 8 parallelStreams.
     * DEFAULT THREAD POOL SIE = NUMBER OF PROCESSORS
     * USE : System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "N") to set the thread pool size
     */

    private  void feature(String outputFilePath, boolean useFrequency) {
        //Get the set of queries
        Set<String> querySet = entityRankings.keySet();

        // Do in parallel
        querySet.parallelStream().forEach(queryID -> doTask(queryID, useFrequency));


        //Do in serial
        //querySet.forEach(queryID -> doTask(queryID, useFrequency));

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

    private void doTask(String queryId, boolean useFrequency) {
        List<Map.Entry<String, Double>> expansionEntities = new ArrayList<>();

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

                // Get the list of all entities which co-occur with this entity in a given context
                // Context here is the same as a PseudoDocument for the entity
                // So we are actually looking at all entities that occur in the PseudoDocument
                // sorted in descending order of frequency
                // Here we are using all entities retrieved for the query to get the expansion terms
                getExpansionContextEntities(entityId, entityList, paraList, expansionEntities, useFrequency);

                if (expansionEntities.size() == 0) {
                    continue;
                }

                // Process the query
                String queryStr = queryId
                        .substring(queryId.indexOf(":") + 1)          // remove enwiki: from query
                        .replaceAll("%20", " ")     // replace %20 with whitespace
                        .toLowerCase();                            //  convert query to lowercase

                // Convert the query to an expanded BooleanQuery and search index
                BooleanQuery booleanQuery;
                TopDocs tops;
                try {
                    booleanQuery = EntityRMExpand.toEntityRmQuery(queryStr, expansionEntities, omitQueryTerms,
                            "text", analyzer);
                    tops = Index.Search.searchIndex(booleanQuery, 100, searcher);
                    makeRunStrings(queryId, entityId, tops);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Done query: " + queryId);
        }
    }

    private void getExpansionContextEntities(String entityId,
                                             List<String> entityList,
                                             ArrayList<String> paraList,
                                             @NotNull List<Map.Entry<String, Double>> expansionEntities,
                                             boolean useFrequency) {

        HashMap<String, Integer> freqMap = new HashMap<>();
        Map<String, Double> relMap = new LinkedHashMap<>();
        ArrayList<String> processedEntityList = Utilities.process(entityList);

        // Create a pseudo-document for the entity
        PseudoDocument d = Utilities.createPseudoDocument(entityId, paraList, searcher);
        if (d != null) {

            if (useFrequency) {
                ////////////////////////////////////////////////////////////////////////////////////////////////////////
                // If useFrequency is true then:
                // 1. Get a distribution over co-occurring entities using frequency.
                // 2. Sort the entities by frequency.
                // 3. Find relatedness of target entity to top-K entities in (2).
                ///////////////////////////////////////////////////////////////////////////////////////////////////////

                // Get the Map of (Entity, Frequency)
                getFreqMap(d, processedEntityList, freqMap);

                // Sort the entities in decreasing order of frequency
                Map<String, Integer> sortedFreqMap = Utilities.sortByValueDescending(freqMap);

                // Find the relatedness measure of top-K entities
                getRelatedness(entityId, sortedFreqMap, relMap);

                // Add all the entities to the list
                expansionEntities.addAll(Utilities.sortByValueDescending(relMap).entrySet());
            } else {
                //////////////////////////////////////////////////////////////////////////////////////////////////
                // If useFrequency is false, then find a distribution over co-occurring entities using relatedness.
                //////////////////////////////////////////////////////////////////////////////////////////////////
                // Get the list of co-occurring entities
                List<String> contextEntityList = d.getEntityList();

                // Get the relatedness with the target entity
                getRelatedness(entityId, contextEntityList, relMap);

                // Add all the entities to the list
                List<Map.Entry<String, Double>> contextEntities = new ArrayList<>(Utilities.sortByValueDescending(relMap).entrySet());

                // Use the top K entities for expansion
                expansionEntities.addAll(contextEntities.subList(0, Math.min(takeKEntities, contextEntities.size())));

            }
        }
    }

    private void  getRelatedness(String entityID,
                                 @NotNull Map<String, Integer> sortedFreqMap,
                                 Map<String, Double> relMap) {
        int i = 0;
        for (String e : sortedFreqMap.keySet()) {
            String e1 = Utilities.unprocess(e);
            relMap.put(e1, getRelatedness(entityID, e1));
            i ++;
            if (i > takeKEntities) {
                break;
            }
        }
    }

    private void  getRelatedness(String entityID,
                                 @NotNull List<String> contextEntityList,
                                 Map<String, Double> relMap) {

        for (String e : contextEntityList) {
            String e1 = Utilities.unprocess(e);
            relMap.put(e1, getRelatedness(entityID, e1));

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

        Map<String, Double> erelMap = entRelMap.get(e1);
        if (erelMap.containsKey(e2)) {
            return erelMap.get(e2);
        }

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

    private void getFreqMap(PseudoDocument d,
                            ArrayList<String> processedEntityList,
                            HashMap<String, Integer> freqMap) {

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
                    freqMap.put(e, Utilities.frequency(e, pseudoDocEntityList));
                }
            }
        }
    }

    @NotNull
    private Map<String, Double> toDistribution (@NotNull Map<String, Double> rankings) {
        Map<String, Double> normRankings = new HashMap<>();
        double sum = 0.0d;
        for (double score : rankings.values()) {
            sum += score;
        }

        for (String s : rankings.keySet()) {
            double normScore = rankings.get(s) / sum;
            normRankings.put(s,normScore);
        }

        return normRankings;
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
        String info = "7-QERelEntContext-" + relType;

        for (int i = 0; i < scoreDocs.length; i++) {
            d = searcher.doc(scoreDocs[i].doc);
            String pID = d.getField("id").stringValue();
            runFileString = query + " Q0 " + pID + " " + (i + 1) + " " + topDocs.scoreDocs[i].score + " " + info;
            runStrings.add(runFileString);
        }
    }

    /**
     * Main method to run the code.
     * @param args Command line parameters.
     */

    public static void main(@NotNull String[] args){

        Similarity similarity = null;
        Analyzer analyzer = null;
        boolean omit, useFrequency;
        String s1 = null, s2;

        String indexDir = args[0];
        String mainDir = args[1];
        String outputDir = args[2];
        String dataDir = args[3];
        String idFile = args[4];
        String relFile = args[5];
        String paraRunFile = args[6];
        String entityRunFile = args[7];
        String entityQrel = args[8];
        int takeKEntities = Integer.parseInt(args[9]);
        String o = args[10];
        String uf = args[11];
        String relType = args[12];
        String a = args[13];
        String sim = args[14];

        System.out.printf("Using %d entities for query expansion\n", takeKEntities);
        omit = o.equalsIgnoreCase("y") || o.equalsIgnoreCase("yes");
        useFrequency = uf.equalsIgnoreCase("true");

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
                    lambda = Float.parseFloat(args[15]);
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
        String outFile = "qe-rel-ent-context" + "-" + s1 + "-" + s2 + "-" + relType;
        if (useFrequency) {
            System.out.println("Using frequency of co-occurring entities: Yes");
            outFile += "-" + "freq-true";
        } else {
            System.out.println("Using frequency of co-occurring entities: No");
            outFile += "-" + "freq-false";
        }

        outFile += ".run";

        new Experiment3(indexDir, mainDir, outputDir, dataDir, idFile, relFile, paraRunFile, entityRunFile, outFile,
                entityQrel, takeKEntities, omit, useFrequency, relType, analyzer, similarity);

    }
}
