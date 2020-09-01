package random;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import help.Utilities;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

/**
 * Make a Map of (PageId, (ParaId, ParaText)) and store to disk.
 *
 * @author Shubham Chatterjee
 * @version 7/17/2020
 */

public class MakePageMap {
    private final static AtomicInteger count = new AtomicInteger(0);
    public static void makeMap(String pageCborFile, String outputFile, boolean parallel) throws IOException {
        Map<String, Map<String, String>> pageMap = new ConcurrentHashMap<>();

        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(pageCborFile)));

        final Iterable<Data.Page> pageIterable = DeserializeData.iterableAnnotations(bis);

        StreamSupport.stream(pageIterable.spliterator(), parallel)
                .forEach(page ->
                {
                    String pageId = page.getPageId();
                    Map<String, String> pageParaMap = getPageParas(page);
                    pageMap.put(pageId, pageParaMap);
                    count.getAndIncrement();
                    if (count.get() % 1000 == 0) {
                        System.out.println("Progress: " + count.get());
                    }
                });
        System.out.print("Writing to disk...");
        Utilities.writeMap(pageMap, outputFile);
        System.out.println("[Done].");
    }

    @NotNull
    private static Map<String, String> getPageParas(@NotNull Data.Page page) {
        Map<String , String> pageParaMap = new HashMap<>();

        for(Data.PageSkeleton pageSkeleton: page.getSkeleton()){
            if(pageSkeleton instanceof Data.Section) {
                sectionContent((Data.Section) pageSkeleton, pageParaMap);
            } else if(pageSkeleton instanceof Data.Para) {
                paragraphContent((Data.Para) pageSkeleton, pageParaMap);
            }
        }

        return pageParaMap;
    }
    private static void sectionContent(@NotNull Data.Section section, Map<String , String> pageParaMap){
        for (Data.PageSkeleton pageSkeleton: section.getChildren()) {
            if (pageSkeleton instanceof Data.Section) {
                sectionContent((Data.Section) pageSkeleton, pageParaMap);
            } else if (pageSkeleton instanceof Data.Para) {
                paragraphContent((Data.Para) pageSkeleton, pageParaMap);
            }
        }
    }
    private static void paragraphContent(@NotNull Data.Para paragraph, @NotNull Map<String , String> pageParaMap){
        String paraId = paragraph.getParagraph().getParaId();
        String paraText = paragraph.getParagraph().getTextOnly();
        pageParaMap.put(paraId, paraText);
    }

    public static void main(@NotNull String[] args) throws IOException {
        String pageCborFile = args[0];
        String outputFile = args[1];
        boolean parallel = args[2].equalsIgnoreCase("true");

        makeMap(pageCborFile, outputFile, parallel);

    }
}
