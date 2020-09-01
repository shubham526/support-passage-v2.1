package help;

import api.WATApi;
import lucene.Index;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Qualitative analysis to show a support passage for the helps-hurts analysis.
 * @author Shubham Chatterjee
 * @version 8/14/2020
 */

public class ShowSupportPassage {
    private final IndexSearcher searcher;
    private final HashMap<String, ArrayList<String>> paraRankings;
    private final Map<String, Map<String, Map<String, Double>>> supportPsgRunFileMap;
    private Map<String, Map<String, Double>> entRelMap = new ConcurrentHashMap<>();
    private final String type;

    public ShowSupportPassage(String query, String run, String index, String paraFilePath,
                              @NotNull String type, String relFilePath) {

        this.type = type;

        System.out.print("Setting up paragraph index for use...");
        searcher = new Index.Setup(index, "text", new EnglishAnalyzer(), new BM25Similarity()).getSearcher();
        System.out.println("[Done].");

        System.out.print("Reading paragraph rankings...");
        paraRankings = Utilities.getRankings(paraFilePath);
        System.out.println("[Done].");

        System.out.print("Reading support passage run file...");
        supportPsgRunFileMap = getRunFileMap(run);
        System.out.println("[Done].");

        if (type.equalsIgnoreCase("rel")) {

            System.out.print("Reading relatedness file...");
            try {
                entRelMap = Utilities.readMap(relFilePath);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            System.out.println("[Done].");
        }

        show(query);
    }
    private void show(@NotNull String query) {


        Map<String, Integer> freqEntityMap;
        Map<String, Double> relEntityMap;
        String q = query.split("\\+")[0];
        String e = query.split("\\+")[1];
        System.out.println("Query: " + q);
        System.out.println("Entity: " + e);
        System.out.println("-----------------------------------------------------------");

        if (check(q, e)) {

            ArrayList<String> paraList = paraRankings.get(q);

            if (type.equalsIgnoreCase("freq")) {
                System.out.println("Getting frequency distribution over co-occurring entities...");
                freqEntityMap = getFreqEntities(e, paraList);
                showFreqInfo(q, e, freqEntityMap);
            } else {
                System.out.println("Getting relatedness distribution over co-occurring entities...");
                relEntityMap = Utilities.sortByValueDescending(getRelEntities(e, paraList));
                showRelInfo(q, e, relEntityMap);
            }
        } else {
            System.exit(-1);
        }


    }

    private boolean check(String q, String e) {
        if (supportPsgRunFileMap.containsKey(q)) {
            Map<String, Map<String, Double>> inner = supportPsgRunFileMap.get(q);
            if (inner.containsKey(e)) {
               return true;
            } else {
                System.err.println(e + " is not present in the ranking for " + q);
                return false;
            }
        } else {
            System.err.println(q + " is not present in the run file.");
           return false;
        }
    }

    private void showRelInfo(String q, String e, @NotNull Map<String, Double> relEntityMap) {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Set<String> relEntSet = relEntityMap.keySet();
        Map<String, Double> supportPassages;
        System.out.println("Top 10 (or less) most related co-occurring entities with " + e + " -->");
        System.out.printf("%-30.30s  %-30.30s%n", "Entity", "Relatedness");
        int count = 1;
        for (Map.Entry<String, Double> entry : relEntityMap.entrySet()) {
            String entity = entry.getKey();
            double rel = entry.getValue();
            System.out.printf("%-30.30s  %-30.30s%n", entity, rel);
            count ++;
            if (count == 10) {
                break;
            }

        }
        supportPassages = Utilities.sortByValueDescending(supportPsgRunFileMap.get(q).get(e));
        System.out.println();

        for (String paraId : supportPassages.keySet()) {
            Document d = paraIdToLuceneDoc(paraId);
            System.out.println("Support Passage-->");
            String paraText = d.get("text");
            double paraScore = supportPassages.get(paraId);
            System.out.println(paraText);
            System.out.println("Score: " + paraScore);
            System.out.println("Related co-occurring entities with " + e + " in support passage-->");
            System.out.printf("%-30.30s  %-30.30s%n", "Entity", "Relatedness");
            Set<String> paraEntitySet = new HashSet<>(Utilities.getEntities(d));
            paraEntitySet.retainAll(relEntSet);
            for (String paraEntity : paraEntitySet) {

                System.out.printf("%-30.30s  %-30.30s%n", paraEntity, relEntityMap.get(paraEntity));
            }
            System.out.println("=====================================================================");
            System.out.println("Press any key --->");
            try {
                br.readLine();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }

        }

    }

    private void showFreqInfo(String q, String e, @NotNull Map<String, Integer> freqEntityMap) {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Set<String> freqEntSet = freqEntityMap.keySet();
        System.out.println("Top 10 most frequently co-occurring entities with " + e + " -->");
        System.out.printf("%-30.30s  %-30.30s%n", "Entity", "Frequency");
        int count = 1;
        for (Map.Entry<String, Integer> entry : freqEntityMap.entrySet()) {
            String entity = entry.getKey();
            int freq = entry.getValue();
            System.out.printf("%-30.30s  %-30.30s%n", entity, freq);
            count ++;
            if (count == 10) {
                break;
            }

        }
        System.out.println();

        Map<String, Double> supportPassages = Utilities.sortByValueDescending(supportPsgRunFileMap.get(q).get(e));

        for (String paraId : supportPassages.keySet()) {
            Document d = paraIdToLuceneDoc(paraId);
            System.out.println("Support Passage-->");
            String paraText = d.get("text");
            System.out.println(paraText);
            System.out.println("Frequently co-occurring entities with " + e + " in support passage-->");
            System.out.printf("%-30.30s  %-30.30s%n", "Entity", "Frequency");
            Set<String> paraEntitySet = new HashSet<>(Utilities.getEntities(d));
            paraEntitySet.retainAll(freqEntSet);
            for (String paraEntity : paraEntitySet) {

                System.out.printf("%-30.30s  %-30.30s%n", paraEntity, freqEntityMap.get(paraEntity));
            }
            System.out.println("=====================================================================");
            System.out.println("Press any key --->");
            try {
                br.readLine();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }

        }

    }

    private Map<String, Double> getRelEntities(String entity, ArrayList<String> paraList) {
        Map<String, Double> relDist = new HashMap<>();
        PseudoDocument d = Utilities.createPseudoDocument(entity, "id", "entity",
                " ", paraList, searcher);

        // Get the list of entities that co-occur with this entity in the pseudo-document
        if (d != null) {

            // Get the list of co-occurring entities
            Set<String> pseudoDocEntitySet = new HashSet<>(d.getEntityList());

            // Get the relatedness distribution over the co-occurring entities
            relDist = getRelatedness(entity, pseudoDocEntitySet);
        }


        return relDist;
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

        List<WATApi.EntityRelatedness.Pair> pair = WATApi.EntityRelatedness.getRelatedness("mw",id1, id2);
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


    @NotNull
    private Map<String, Integer> getFreqEntities(String entity, ArrayList<String> paraList) {
        Map<String, Integer> freqDist = new HashMap<>();
        PseudoDocument d = Utilities.createPseudoDocument(entity, "id", "entity",
                " ", paraList, searcher);
        if (d != null) {

            // Get the list of entities that co-occur with this entity in the pseudo-document
            List<String> pseudoDocEntityList = d.getEntityList();

            // Find the frequency distribution over the co-occurring entities
            freqDist = getDistribution(pseudoDocEntityList);
        }
        return freqDist;
    }
    @NotNull
    private Map<String, Integer> getDistribution(@NotNull List<String> pseudoDocEntityList) {

        HashMap<String, Integer> freqMap = new HashMap<>();

        // For every co-occurring entity do
        for (String e : pseudoDocEntityList) {
            freqMap.compute(e, (t, oldV) -> (oldV == null) ? 1 : oldV + 1);
        }
        return  Utilities.sortByValueDescending(freqMap);
    }

    private Document paraIdToLuceneDoc(String paraId) {
        Document d = null;
        try {
            d = Index.Search.searchIndex("id", paraId, searcher);
        } catch (IOException | ParseException ioException) {
            ioException.printStackTrace();
        }
        assert d != null;
        return d;
    }
    @NotNull
    private Map<String, Map<String, Map<String, Double>>> getRunFileMap(String runFile) {
        Map<String, Map<String, Map<String, Double>>> queryMap = new HashMap<>();
        BufferedReader in = null;
        String line;
        Map<String, Map<String, Double>> entityMap;
        Map<String, Double> paraMap;
        try {
            in = new BufferedReader(new FileReader(runFile));
            while((line = in.readLine()) != null) {
                String[] fields = line.split(" ");
                String queryID = fields[0].split("\\+")[0];
                String entityID = fields[0].split("\\+")[1];
                String paraID = fields[2];
                double paraScore = Double.parseDouble(fields[4]);
                if (queryMap.containsKey(queryID)) {
                    entityMap = queryMap.get(queryID);
                } else {
                    entityMap = new HashMap<>();
                }
                if (entityMap.containsKey(entityID)) {
                    paraMap = entityMap.get(entityID);
                } else {
                    paraMap = new HashMap<>();
                }
                paraMap.put(paraID,paraScore);
                entityMap.put(entityID,paraMap);
                queryMap.put(queryID,entityMap);

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(in != null) {
                    in.close();
                } else {
                    System.out.println("Input Buffer has not been initialized!");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return queryMap;
    }

    public static void main(@NotNull String[] args) {
        String query = args[0];
        String run = args[1];
        String index = args[2];
        String paraRunFilePath = args[3];
        String type = args[4];
        String relFilePath = args[5];
        if (type.equalsIgnoreCase("freq") || type.equalsIgnoreCase("rel")) {
            new ShowSupportPassage(query, run, index, paraRunFilePath, type, relFilePath);
        } else {
            System.err.println("ERROR! Wrong type! May be either (freq | rel).");
            System.exit(-1);
        }
    }
}
