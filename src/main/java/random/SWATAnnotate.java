package random;

import api.SWATApi;
import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import help.Utilities;
import lucene.Index;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

public class SWATAnnotate {
    private final Map<String, Map<String, Double>> salientEntityMap = new HashMap<>();
    private AtomicInteger atomicInteger = new AtomicInteger();

    public SWATAnnotate(String inFile, String outFile, @NotNull String type) {
        System.out.println("Input File: " + inFile);
        System.out.println("Output File: " + outFile);

        if (type.equalsIgnoreCase("sal")) {
            System.out.println("Annotating salient entities");
        } else if (type.equalsIgnoreCase("all")) {
            System.out.println("Annotating all entities");
        } else {
            System.err.println("ERROR: Annotation type can be either \"sal\" for \"salient\" entities or \"all\" " +
                    "for both salient and non-salient entities.");
            System.exit(-1);
        }

        annotate(inFile, outFile, type);
    }

    public SWATAnnotate(String inFile, String outFile, @NotNull String type, IndexSearcher searcher) {
        System.out.println("Input File: " + inFile);
        System.out.println("Output File: " + outFile);

        if (type.equalsIgnoreCase("sal")) {
            System.out.println("Annotating salient entities");
        } else if (type.equalsIgnoreCase("all")) {
            System.out.println("Annotating all entities");
        } else {
            System.err.println("ERROR: Annotation type can be either \"sal\" for \"salient\" entities or \"all\" " +
                    "for both salient and non-salient entities.");
            System.exit(-1);
        }

        annotate(inFile, outFile, type, searcher);
    }

    public void annotate(String cbor, String file, String type) {
        BufferedInputStream bis = null;
        try {
            bis = new BufferedInputStream(new FileInputStream(new File(cbor)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Iterable<Data.Paragraph> ip = DeserializeData.iterableParagraphs(bis);

        StreamSupport.stream(ip.spliterator(), true)
                .forEach(paragraph ->
                {
                    String text = paragraph.getTextOnly();
                    Map<String, Double> salMap = SWATApi.getEntities(text, type);
                    salientEntityMap.put(paragraph.getParaId(), salMap);
                    //System.out.println(paragraph.getParaId());
                    atomicInteger.getAndIncrement();
                    if (atomicInteger.get() % 1000 == 0) {
                        System.out.println("Progress: " + atomicInteger.get());
                    }
                });
        System.out.println("Writing data to file: " + file);
        try {
            Utilities.writeMap(salientEntityMap,file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Done");
    }

    private void annotate(String inFile, String outFile, String type, IndexSearcher searcher) {

        Set<String> paraSet = getParaIds(inFile);
        int size = paraSet.size();
        paraSet.parallelStream().forEach(paraId -> doTask(paraId, type, searcher, size));

        System.out.println("Writing data to file: " + outFile);
        try {
            Utilities.writeMap(salientEntityMap,outFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Done");
    }
    private void doTask(String paraId, String type, IndexSearcher searcher, int size) {
        Document doc = null;
        String text = null;
        try {
            doc = Index.Search.searchIndex("id", paraId, searcher);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        if (doc != null) {
            text = doc.get("text");
        }
        Map<String, Double> salMap = SWATApi.getEntities(text, type);
        salientEntityMap.put(paraId, salMap);
        atomicInteger.getAndIncrement();
        if (atomicInteger.get() % 1000 == 0) {
            System.out.println("Progress: " + atomicInteger.get() + " of " + size);
        }
        //System.out.println(paraId);
    }

    @NotNull
    private Set<String> getParaIds(String inFile) {
        Set<String> paraIds = new HashSet<>();
        BufferedReader br = null;
        String line;
        try {
            br = new BufferedReader(new FileReader(inFile));
            while((line = br.readLine()) != null) {
                paraIds.add(line.split(" ")[2]);
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
        return paraIds;
    }

    public static void main(@NotNull String[] args) {
        boolean cbor = args[0].equalsIgnoreCase("cbor");
        String inFile, outFile, type, indexDir;
        IndexSearcher searcher;
        if (cbor) {
            inFile = args[1];
            outFile = args[2];
            type = args[3];
            new SWATAnnotate(inFile, outFile, type);
        } else {
            indexDir = args[1];
            searcher = new Index.Setup(indexDir).getSearcher();
            inFile = args[2];
            outFile = args[3];
            type = args[4];
            new SWATAnnotate(inFile, outFile, type, searcher);
        }
    }
}
