package help;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Class to modify the manual qrels.
 * The benchmarkY2-test manual qrels have annotations in the range -1 to 3.
 * According to TREC-CAR overview report, grades 3, 2 and 1 correspond to MUST, SHOULD and CAN
 * be mentioned respectively whereas the grades 0,1,-1 are non-relevant.
 * We do not want entities marked 0,1,-1 in our ground truth data, so we filter out all entities
 * and passages which are marked 0,1, or -1 and create a modified ground truth file.
 * This modified ground truth file would be used to create the support passage ground truth.
 * @author Shubham Chatterjee
 * @version 8/13/2019
 */

public class ModifyManualGroundTruth {

    /**
     * Constructor.
     * @param qrelFile String Qrel file to modify.
     * @param modifiedQrelFile String New qrel file.
     */
    public ModifyManualGroundTruth(String qrelFile,
                                   String modifiedQrelFile) {

        ArrayList<String> modifiedLines = new ArrayList<>();
        System.out.println("File: " + qrelFile);

        System.out.print("Modifying...");
        modify(qrelFile, modifiedLines);
        System.out.println("[Done].");

        System.out.print("Writing to new file...");
        Utilities.writeFile(modifiedLines, modifiedQrelFile);
        System.out.println("[Done].");

        System.out.println("Modified qrel file written to: " + modifiedQrelFile);
    }

    /**
     * Helper method.
     * Modifies the qrel file.
     * @param qrelFile String
     * @param modifiedLines List
     */

    private void modify(String qrelFile,
                        ArrayList<String> modifiedLines) {

        BufferedReader br = null;
        String line, modifiedLine;
        int grade;
        try {
            br = new BufferedReader(new FileReader(qrelFile));
            while((line = br.readLine()) != null) {
                String[] fields = line.split(" ");
                grade = Integer.parseInt(fields[3]);
                if (grade == 1 || grade == 2 || grade == 3) {
                    modifiedLine = fields[0] + " " + fields[1] + " " + fields[2] + " "
                            + "1";
                    modifiedLines.add(modifiedLine);
                }
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
     * Main method.
     * @param args Command line arguments.
     */

    public static void main(@NotNull String[] args) {
        String qrelFile = args[0];
        String modifiedQrelFile = args[1];
        new ModifyManualGroundTruth(qrelFile, modifiedQrelFile);
    }
}
