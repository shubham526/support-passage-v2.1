package experiments.salience;

import api.SWATApi;
import help.PseudoDocument;
import help.Utilities;
import lucene.Index;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;

/**
 * Analyze why salience does not work.
 * This class reads in the input entity run and the candidate passage run.
 * It makes to HashMaps, one for entities with salient passages and other for entities with no salient passages.
 * It then writes the to HashMaps to disk.
 * @author Shubham Chatterjee
 * @version 7/16/2019
 */

public class Experiment5 {
    private final IndexSearcher searcher;
    // HashMap where Key = query and Value = List of entities retrieved for the query
    private final Map<String, ArrayList<String>> entityRankings;

    // HashMap where Key = query and Value = List of passages retrieved for the query
    private final Map<String, ArrayList<String>> passageRankings;

    // HashMap where Key = paraID and Value = Map of (entity, salience_score)
    private Map<String, Map<String, Double>> salientEntityMap;

    private final Map<String, Set<String>> entWithSalPsgMap = new HashMap<>();
    private final Map<String, Set<String>> entWithNoSalPsgMap = new HashMap<>();


    /**
     * Constructor.
     * @param entityFile String Path to the entity run file.
     * @param passageFile String Path to the passage run file.
     * @param swatFile String Path to the SWAT serialized file.
     */
    public Experiment5(String entityFile,
                       String passageFile,
                       String swatFile,
                       String indexDir,
                       String outDir) {

        System.out.print("Setting up index for use...");
        searcher = new Index.Setup(indexDir).getSearcher();
        System.out.println("[Done].");

        System.out.print("Reading entity rankings...");
        this.entityRankings = Utilities.getRankings(entityFile);
        System.out.println("[Done].");

        System.out.print("Reading passage rankings...");
        this.passageRankings = Utilities.getRankings(passageFile);
        System.out.println("[Done]");

        System.out.print("Reading the SWAT annotations...");
        try {
            this.salientEntityMap = Utilities.readMap(swatFile);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");

        analyze(outDir);
    }

    /**
     * Method to make the analysis.
     */
    private void analyze(String outDir) {
        //Get the set of queries
        Set<String> querySet = entityRankings.keySet();

        // Do in parallel
        //querySet.parallelStream().forEach(this::doTask);
        // Do in serial
        ProgressBar pb = new ProgressBar("Progress", querySet.size());
        for (String q : querySet) {
            doTask(q);
            pb.step();
        }
        pb.close();

        System.out.print("Writing to disk....");
        try {
            Utilities.writeMap(entWithSalPsgMap, outDir + "/entity-with-sal-psg-map.ser");
            Utilities.writeMap(entWithNoSalPsgMap, outDir + "/entity-with-no-sal-psg-map.ser");
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("[Done].");
        System.out.println("Files written to directory: " + outDir);



    }

    /**
     * Method to do the actual work.
     * @param queryID String The query ID
     */

    private void doTask(String queryID) {

        Set<String> entWithSalPsg = new HashSet<>();
        Set<String> entWithNoSalPsg = new HashSet<>();
        // Get the list of entities retrieved for the query
        ArrayList<String> entityList = entityRankings.get(queryID);
        //ArrayList<String> processedEntityList = Utilities.process(entityList);

        // Get the list of paragraphs retrieved for the query
        ArrayList<String> paraList = passageRankings.get(queryID);

        String paraText;
        Map<String, Double> saliencyMap;

        // For every entity in this list of retrieved entities do
        for (String entityId : entityList) {

            // Create a pseudo-document for the entity
            PseudoDocument d = Utilities.createPseudoDocument(entityId, "id", "entity",
                    " ", paraList, searcher);

            if (d != null) {
                // If the PseudoDocument is not null (that is, contains at least one document) then
                // Get the list of documents in the pseudo-document about the entity
                ArrayList<Document> documentList = d.getDocumentList();
                for (Document document : documentList) {
                    // For every such pseudo-document
                    // Get the ID of the paragraph
                    String paraID = document.getField("id").stringValue();
                    // If a  SWAT annotation exits for this paragraph in the swat file then we are good
                    // No need to query the SWAT API
                    if (salientEntityMap.containsKey(paraID)) {
                        // Get the set of entities salient in the paragraph
                        if (salientEntityMap.get(paraID) != null) {
                            Set<String> salEnt = salientEntityMap.get(paraID).keySet();
                            if (isPresent(salEnt, entityId)) {
                                // If the set of salient entities contains the entity then
                                // It means the entity has a passage in the candidate set
                                // And the entity is salient in the passage
                                // Add the entity to the list of entities with a passage in the candidate set
                                // and salient in the passage
                                entWithSalPsg.add(entityId);
                                // If we already found a passage for the entity in which it is salient, then we may stop
                                // so break out of this loop
                                break;
                            }
                        }
                    } else {
                        // Otherwise no SWAT annotation was found for the passage in the swat file
                        // Now we have to query the SWAT API :-(
                        // First we need the text of the passage for which we need to query the Lucene index
                        try {
                            document = Index.Search.searchIndex("id", paraID, searcher);
                        } catch (IOException | ParseException e) {
                            e.printStackTrace();
                        }
                        assert document != null;
                        paraText = document.get("text");
                        saliencyMap = SWATApi.getEntities(paraText, "sal"); // Now query SWAT
                        // If SWAT returned something then store it in the in-memory map and check if it contains
                        // the entity
                        salientEntityMap.put(paraID, saliencyMap);
                        if (isPresent(saliencyMap.keySet(), entityId)) {
                            // If the swat map contains the entityID then we found a passage in which the entity is salient
                            // So add it to the set of entities with salient passages
                            entWithSalPsg.add(entityId);
                            // If we already found a passage for the entity in which it is salient, then we may stop
                            // so break out of this loop
                            break;
                        }

                    }
                }
                // If we reach here (outside the for loop) and the "entityID" is not present in entWithSalPsg
                // Then it means that we iterated over all the documents mentioning the entity
                // But did not find a document where the entity was salient
                // So add this entity to the list entWithNoSalPsg
                if (! entWithSalPsg.contains(entityId)) {
                    entWithNoSalPsg.add(entityId);
                }
            } else {
                // Otherwise if the pseudo-document is null (that is, no document about the entity exists) then
                // This means this entity has no passage in the candidate set
                // Add it to the list of entities with no passages in the candidate set
                entWithNoSalPsg.add(entityId);
            }

        }
        entWithSalPsgMap.put(queryID, entWithSalPsg);
        entWithNoSalPsgMap.put(queryID, entWithNoSalPsg);
        //System.out.println("Done: " + queryID);
    }

    private boolean isPresent(@NotNull Set<String> salEnt, String entityId) {
        String eid = Utilities.process(entityId);

        for (String e : salEnt) {
            if (e.equalsIgnoreCase(eid)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Main method.
     * @param args Command line arguments.
     */

    public static void main(@NotNull String[] args) {
        String indexDir = args[0];
        String outDir = args[1];
        String entityFile = args[2];
        String passageFile = args[3];
        String swatFile = args[4];


        new Experiment5(entityFile, passageFile, swatFile, indexDir, outDir);
    }
}
