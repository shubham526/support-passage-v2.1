package random;

import api.WATApi;
import help.PseudoDocument;
import help.Utilities;
import lucene.Index;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static help.Utilities.process;

public class GetEntities {
    private final Map<String, Map<String, String>> contextEntityMap = new ConcurrentHashMap<>();
    private Map<String, Integer> entityIDMap = new ConcurrentHashMap<>();
    private final HashMap<String, ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> paraRankings;
    private final HashMap<String, ArrayList<String>> entityQrels;
    private final IndexSearcher pageIndexSearcher;
    private final IndexSearcher paraIndexSearcher;
    private String relType;

    public GetEntities(String pageIndexDir,
                       String paraIndexDir,
                       String mainDir,
                       String dataDir,
                       String outputDir,
                       String paraRunFile,
                       String entityRunFile,
                       String idFile,
                       String entityQrelFilePath,
                       String contextEntityFile,
                       @NotNull String relType,
                       String mode,
                       Analyzer analyzer,
                       Similarity similarity) {

        String paraRunFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String entityRunFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String idFilePath = mainDir + "/" + dataDir + "/" + idFile;
        String contextEntityFilePath = mainDir + "/" + outputDir + "/" + contextEntityFile;

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
        entityRankings = Utilities.getRankings(entityRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading para rankings...");
        paraRankings = Utilities.getRankings(paraRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelFilePath);
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

        getEntities(contextEntityFilePath, mode);

    }

    private void getEntities(String contextEntityFilePath, @NotNull String mode) {
        //Get the set of queries
        Set<String> querySet = entityRankings.keySet();


        if (mode.equalsIgnoreCase("page")) {
            System.out.println("Getting page entities");
            // Do in parallel
            querySet.parallelStream().forEach(this::getPageEntities);

            // Do in series
            //querySet.forEach(this::getPageEntities);
        } else {
            System.out.println("Getting ECN entities");
            // Do in parallel
            querySet.parallelStream().forEach(this::getECNEntities);
            // Do in series
            //querySet.forEach(this::getECNEntities);
        }

        System.out.print("Writing to file....");
        try {
            Utilities.writeMap(contextEntityMap, contextEntityFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");
    }


    private void getECNEntities(String queryId) {
        Map<String, String> ecnEntityMap;
        // Get the list of passages retrieved for the query
        ArrayList<String> paraList = paraRankings.get(queryId);
        if (entityQrels.containsKey(queryId) && entityQrels.containsKey(queryId)) {

            // Get the set of entities retrieved for the query
            ArrayList<String> entityList = entityRankings.get(queryId);
            Set<String> retEntitySet = new HashSet<>(entityList);

            // Get the set of entities relevant for the query
            Set<String> relEntitySet = new HashSet<>(entityQrels.get(queryId));

            // Get the retrieved entities which are also relevant
            retEntitySet.retainAll(relEntitySet);

            // For every entity in this set of relevant retrieved  entities do
            for (String entityId : retEntitySet) {

                // Get the list of all entities on the Wikipedia page of this entity.
                ecnEntityMap = getECNEntityMap(entityId, paraList);

                // Store
                contextEntityMap.put(entityId, ecnEntityMap);
            }
        }

    }

    @NotNull
    private Map<String, String> getECNEntityMap(String entityId,
                                                ArrayList<String> paraList) {
        // Create a pseudo-document for the entity
        PseudoDocument d = Utilities.createPseudoDocument(entityId, paraList, paraIndexSearcher);
        List<String> pseudoDocEntityList;
        Map<String, String> relMap = new HashMap<>();

        // Get the list of entities that co-occur with this entity in the pseudo-document
        if (d != null) {

            // Get the list of co-occurring entities
            pseudoDocEntityList = d.getEntityList();

            // For every co-occurring entity do
            for (String e : pseudoDocEntityList) {

                // If the entity also occurs in the list of entities relevant for the query then
                // And the relMap does not already contain this entity


                ////////////////////////////////////////////////////////////////////////////////////////////////
                // The condition is important because pseudoDocEntityList contains multiple occurrences
                // of the same entity (the original method depended on the frequency of the co-occurring entities).
                // However, for the purposes of this experiment, we are using the relatedness score between two
                // entities and hence we don't need multiple occurrences of the same entity. Finding relatedness
                // of same entity multiple times is going to increase run-time.
                ////////////////////////////////////////////////////////////////////////////////////////////////

                if (!relMap.containsKey(e)) {

                    // Find the relation score of this entity with the given entity and store it
                    String s = getRelatedness(entityId, Utilities.unprocess(e));
                    relMap.put(e, s);
                }
            }
        }
        return relMap;
    }

    private void getPageEntities(String queryId) {
        Map<String, String> pageEntityMap;
        if (entityQrels.containsKey(queryId) && entityQrels.containsKey(queryId)) {

            // Get the set of entities retrieved for the query
            ArrayList<String> entityList = entityRankings.get(queryId);
            Set<String> retEntitySet = new HashSet<>(entityList);

            // Get the set of entities relevant for the query
            Set<String> relEntitySet = new HashSet<>(entityQrels.get(queryId));

            // Get the retrieved entities which are also relevant
            retEntitySet.retainAll(relEntitySet);

            // For every entity in this set of relevant retrieved  entities do
            for (String entityId : retEntitySet) {

                // Get the list of all entities on the Wikipedia page of this entity.
                pageEntityMap = getPageEntityMap(entityId);

                // Store
                contextEntityMap.put(entityId, pageEntityMap);
            }
        }
    }
    @NotNull
    private Map<String, String> getPageEntityMap(String entityID) {
        Map<String, String> pageEntityMap = new HashMap<>();

        try {
            // Get the document corresponding to the entityID from the page.lucene index
            Document doc = Index.Search.searchIndex("Id", entityID, pageIndexSearcher);

            // Get the list of entities in the document
            String entityString = Objects.requireNonNull(doc).getField("OutlinkIds").stringValue();

            // Make a list from this string
            String[] entityArray = entityString.split("\n");
            for (String eid : entityArray) {
                //double rel = getRelatedness(entityID, eid);
                String s = getRelatedness(entityID, eid);
                pageEntityMap.put(Utilities.process(eid), s);
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        return pageEntityMap;
    }
    @NotNull
    private String getRelatedness(@NotNull String e1, @NotNull String e2) {

        int id1, id2;

        String s1 = toTitleCase(e1.substring(e1.indexOf(":") + 1).replaceAll("%20", "_"));
        String s2 = toTitleCase(e2.substring(e1.indexOf(":") + 1).replaceAll("%20", "_"));

        id1 = entityIDMap.containsKey(e1)
                ? entityIDMap.get(e1)
                : WATApi.TitleResolver.getId(s1);

        id2 = entityIDMap.containsKey(e2)
                ? entityIDMap.get(e2)
                : WATApi.TitleResolver.getId(s2);

        if (s1.equalsIgnoreCase(s2)) {
            return id2 + ":" + 1;
        }


        if (id1 < 0 || id2 < 0) {
            return id2 + ":" + 0;
        }



        List<WATApi.EntityRelatedness.Pair> pair = WATApi.EntityRelatedness.getRelatedness(relType,id1, id2);
        if (!pair.isEmpty()) {
            return id2 + ":" + pair.get(0).getRelatedness();
        } else {
            return id2 + ":" + 0;
        }
    }
    @NotNull
    private String toTitleCase(@NotNull String givenString) {
        String[] arr = givenString.split("_");
        StringBuilder sb = new StringBuilder();

        for (String s : arr) {
            sb.append(Character.toUpperCase(s.charAt(0)))
                    .append(s.substring(1)).append(" ");
        }
        return sb.toString().trim().replaceAll(" ","_");
    }

    public static void main(@NotNull String[] args) {
        String pageIndexDir = args[0];
        String paraIndexDir = args[1];
        String mainDir = args[2];
        String dataDir = args[3];
        String outputDir = args[4];
        String paraRunFile = args[5];
        String entityRunFile = args[6];
        String idFile = args[7];
        String entityQrelFilePath = args[8];
        String mode = args[9];
        String a = args[10];
        String sim = args[11];

        Analyzer analyzer = null;
        Similarity similarity = null;

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

                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                float lambda;
                try {
                    lambda = Float.parseFloat(args[12]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Missing lambda value for similarity LM-JM");
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

        Scanner sc = new Scanner(System.in);
        System.out.println("Enter the entity relation type to use. Your choices are:");
        System.out.println("mw (Milne-Witten)");
        System.out.println("jaccard (Jaccard measure of pages outlinks)");
        System.out.println("lm (language model)");
        System.out.println("w2v (Word2Vect)");
        System.out.println("cp (Conditional Probability)");
        System.out.println("ba (Barabasi-Albert on the Wikipedia Graph)");
        System.out.println("pmi (Pointwise Mutual Information)");
        System.out.println("Enter you choice:");
        String relType = sc.nextLine();

        String outputFile = "benchmarkY1-train-" + mode + "-entities-rel-" + relType;

        new GetEntities(pageIndexDir,paraIndexDir, mainDir, dataDir,outputDir, paraRunFile, entityRunFile, idFile,
                entityQrelFilePath, outputFile, relType, mode, analyzer, similarity);

    }

}
