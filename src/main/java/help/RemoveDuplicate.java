package help;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Removes duplicate strings from run file.
 * @author Shubham Chatterjee
 * @version 04/03/2020
 */

public class RemoveDuplicate {
    HashMap<String, LinkedHashMap<String, Double>> runFileMap = new HashMap<>();
    HashMap<String, LinkedHashMap<String, Double>> newRunFileMap = new HashMap<>();

    RemoveDuplicate(String runFile, String newRunFile) {

        System.out.print("Reading run file rankings...");
        Utilities.getRankings(runFile, runFileMap);
        System.out.println("[Done].");

        System.out.print("Removing duplicates...");
        removeDuplicate(newRunFile);
        System.out.println("[Done].");

        ArrayList<String> runFileStrings = makeRunFileStrings();

        System.out.print("Writing new run file...");
        Utilities.writeFile(runFileStrings, newRunFile);
        System.out.println("[Done].");
    }

    @NotNull
    private ArrayList<String> makeRunFileStrings() {
        ArrayList<String> runStrings = new ArrayList<>();
        Set<String> querySet = newRunFileMap.keySet();
        for (String queryId : querySet) {
            LinkedHashMap<String, Double> paraScore = newRunFileMap.get(queryId);
            String runFileString;
            int rank = 1;
            for (String paraId : paraScore.keySet()) {
                double score = paraScore.get(paraId);
                if (score > 0) {
                    runFileString = queryId + " Q0 " + paraId + " " + rank++
                            + " " + score + " " + "exp4";
                    runStrings.add(runFileString);
                }
            }
        }
        return runStrings;
    }


    private void removeDuplicate(String newRunFile) {
        Set<String> querySet = runFileMap.keySet();
        System.out.println("Number of queries: " + querySet.size());

        for (String queryID : querySet) {
            LinkedHashMap<String, Double> rankings = runFileMap.get(queryID);
            LinkedHashMap<String, Double> newRankings = new LinkedHashMap<>();
            Set<String> paraSet = rankings.keySet();
            for (String paraID : paraSet) {
                if (!newRankings.containsKey(paraID)) {
                    newRankings.put(paraID, rankings.get(paraID));
                } else {
                    System.err.println("Already seen: " + paraID);
                }
            }
            newRunFileMap.put(queryID, newRankings);
        }
    }

    public static void main(@NotNull String[] args) {
        String runFile = args[0];
        String newRunFile = args[1];
        new RemoveDuplicate(runFile, newRunFile);

    }
}
