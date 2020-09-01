package random;

import api.WATApi;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

public class GetEntityId {
    private final Map<String, Integer> entityIDMap = new ConcurrentHashMap<>();
    private final HashMap<String, ArrayList<String>> entityRankings;
    private final HashMap<String, ArrayList<String>> paraRankings;
    private final HashMap<String, ArrayList<String>> entityQrels;
    private final IndexSearcher pageIndexSearcher;
    private final IndexSearcher paraIndexSearcher;
    private final boolean parallel;

    public GetEntityId(String pageIndexDir,
                       String paraIndexDir,
                       String mainDir,
                       String dataDir,
                       String outputDir,
                       String paraRunFile,
                       String entityRunFile,
                       String entityQrelFile,
                       String outputFile,
                       String mode,
                       boolean parallel,
                       Analyzer analyzer,
                       Similarity similarity) {

        String paraRunFilePath = mainDir + "/" + dataDir + "/" + paraRunFile;
        String entityRunFilePath = mainDir + "/" + dataDir + "/" + entityRunFile;
        String outputFilePath = mainDir + "/" + outputDir + "/" + outputFile;
        String entityQrelFilePath = mainDir + "/" + dataDir + "/" + entityQrelFile;
        this.parallel = parallel;

        System.out.print("Reading entity rankings...");
        entityRankings = Utilities.getRankings(entityRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading para rankings...");
        paraRankings = Utilities.getRankings(paraRunFilePath);
        System.out.println("[Done].");

        System.out.print("Reading entity ground truth...");
        entityQrels = Utilities.getRankings(entityQrelFilePath);
        System.out.println("[Done].");

        System.out.print("Setting up paragraph index for use...");
        paraIndexSearcher = new Index.Setup(paraIndexDir, "text", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        System.out.print("Setting up page index for use...");
        pageIndexSearcher = new Index.Setup(pageIndexDir, "OutlinkIds", analyzer, similarity).getSearcher();
        System.out.println("[Done].");

        getEntities(outputFilePath, mode);

    }

    private void getEntities(String contextEntityFilePath, @NotNull String mode) {
        //Get the set of queries
        Set<String> querySet = entityRankings.keySet();


        if (mode.equalsIgnoreCase("page")) {
            System.out.println("Getting page entities");
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
                querySet.parallelStream().forEach(this::getPageEntities);
            } else {
                System.out.println("Using Sequential Streams.");

                // Do in serial
                ProgressBar pb = new ProgressBar("Progress", querySet.size());
                for (String q : querySet) {
                    getPageEntities(q);
                    pb.step();
                }
                pb.close();
            }
        } else {
            System.out.println("Getting ECN entities");
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
                querySet.parallelStream().forEach(this::getECNEntities);
            } else {
                System.out.println("Using Sequential Streams.");

                // Do in serial
                ProgressBar pb = new ProgressBar("Progress", querySet.size());
                for (String q : querySet) {
                    getECNEntities(q);
                    pb.step();
                }
                pb.close();
            }
        }

        System.out.print("Writing to file....");
        try {
            Utilities.writeMap(entityIDMap, contextEntityFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");
    }


    private void getECNEntities(String queryId) {

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
                getECNEntityMap(entityId, paraList);

            }
        }
        if (parallel) {
            System.out.println("Done: " + queryId);
        }

    }

    private void getECNEntityMap(String entityId, ArrayList<String> paraList) {
        int id;
        id = getID(entityId);
        entityIDMap.put(entityId, id);

        // Create a pseudo-document for the entity
        PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                " ", paraList, paraIndexSearcher);
        Set<String> pseudoDocEntitySet;

        // Get the list of entities that co-occur with this entity in the pseudo-document
        if (d != null) {

            // Get the list of co-occurring entities
            pseudoDocEntitySet = new HashSet<>(d.getEntityList());

            // For every co-occurring entity do
            for (String eid : pseudoDocEntitySet) {
                // Check to see of it already exists
                if (!entityIDMap.containsKey(eid)) {
                    id = getID(toTitleCase(eid));
                    entityIDMap.put(eid, id);
                }

            }
        }
    }

    private void getPageEntities(String queryId) {

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
                 getPageEntityMap(entityId);
            }
        }
        if (parallel) {
            System.out.println("Done: " + queryId);
        }
    }

    private void getPageEntityMap(String entityID) {
        int id;
        id = getID(entityID);
        entityIDMap.put(entityID, id);

        try {
            // Get the document corresponding to the entityID from the page.lucene index
            Document doc = Index.Search.searchIndex("Id", entityID, pageIndexSearcher);

            // Get the list of entities in the document
            String entityString = Objects.requireNonNull(doc).getField("OutlinkIds").stringValue();

            // Make a list from this string
            String[] entityArray = entityString.split("\n");
            for (String eid : entityArray) {
                // Check to see of it already exists
                if (!entityIDMap.containsKey(eid)) {
                    id = getID(toTitleCase(eid));
                    entityIDMap.put(eid, id);
                }
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }
    @NotNull
    private Integer getID(String entity) {
        entity = entity.substring(entity.indexOf(":") + 1)                // remove enwiki: from query
                .replaceAll("%20", "_");     // replace %20 with whitespace

        return WATApi.TitleResolver.getId(entity);
    }
    @NotNull
    private String toTitleCase(@NotNull String givenString) {
        String[] arr = givenString.split("_");
        StringBuilder sb = new StringBuilder();

        try {

            for (String s : arr) {
                sb.append(Character.toUpperCase(s.charAt(0)))
                        .append(s.substring(1)).append(" ");
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println("ERROR in toTitleCase(): " + e.getMessage());
        }
        return sb.toString().trim().replaceAll(" ", "_");
    }


    public static void main(@NotNull String[] args) {
        String pageIndexDir = args[0];
        String paraIndexDir = args[1];
        String mainDir = args[2];
        String dataDir = args[3];
        String outputDir = args[4];
        String paraRunFile = args[5];
        String entityRunFile = args[6];
        String entityQrelFile = args[7];
        String outputFile = args[8];
        String mode = args[9];
        boolean parallel = args[10].equalsIgnoreCase("true");
        String a = args[11];
        String sim = args[12];

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
                    lambda = Float.parseFloat(args[13]);
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

        new GetEntityId(pageIndexDir,paraIndexDir, mainDir, dataDir,outputDir, paraRunFile, entityRunFile,
                entityQrelFile, outputFile, mode, parallel, analyzer, similarity);

    }

}
