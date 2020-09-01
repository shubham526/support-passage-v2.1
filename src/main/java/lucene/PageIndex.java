package lucene;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

public class PageIndex {

    public static void createIndex(String cborFile, String indexDir, Analyzer analyzer) throws IOException {
        IndexWriter writer = createWriter(analyzer, indexDir);
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(cborFile)));

        final Iterable<Data.Page> pageIterable = DeserializeData.iterableAnnotations(bis);
        System.out.println("Creating index at location: " + indexDir);


        StreamSupport.stream(pageIterable.spliterator(), true)
                .forEach(page ->
                {
                    try {
                        writer.addDocument(createDocument(page));
                        System.out.println(page.getPageId());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        writer.commit();
        writer.close();

    }

    @NotNull
    private static Document createDocument(@NotNull Data.Page page) {
        String pageID = page.getPageId();
        String pageName = page.getPageName();
        String pageContent = getPageContent(page);
        Data.PageMetadata pageMetadata = page.getPageMetadata();
        ArrayList<String> inLinkAnchors = getInLinkAnchors(page);
        ArrayList<String> inLinkIds = pageMetadata.getInlinkIds();
        ArrayList<String> outLinkIds = getPageEntities(page);
        ArrayList<String> categoryNames = pageMetadata.getCategoryNames();
        ArrayList<String> categoryIds = pageMetadata.getCategoryIds();

        return pageToLuceneDoc(pageID, pageName, pageContent, inLinkAnchors, inLinkIds, outLinkIds,
                categoryIds, categoryNames);
    }

    @NotNull
    private static Document pageToLuceneDoc(String pageID, String pageName, String pageContent,
                                            ArrayList<String> inLinkAnchors, ArrayList<String> inLinkIds,
                                            ArrayList<String> outLinkIds, ArrayList<String> categoryIds,
                                            ArrayList<String> categoryNames) {
        Document doc = new Document();
        doc.add(new StringField("Id", pageID, Field.Store.YES));  // don't tokenize this!
        doc.add(new StringField("Name", pageName, Field.Store.YES));  // don't tokenize this!
        doc.add(new TextField("Content",pageContent,Field.Store.YES));
        doc.add(new TextField("InlinkAnchors", String.join("\n", inLinkAnchors), Field.Store.YES));
        doc.add(new TextField("InlinkIds", String.join("\n", inLinkIds), Field.Store.YES));
        doc.add(new TextField("OutlinkIds", String.join("\n", outLinkIds), Field.Store.YES));
        doc.add(new TextField("CategoryIds", String.join("\n", categoryIds), Field.Store.YES));
        doc.add(new TextField("CategoryNames", String.join("\n", categoryNames), Field.Store.YES));
        return doc;

    }

    @NotNull
    private static ArrayList<String> getInLinkAnchors(@NotNull Data.Page page) {
        Data.PageMetadata pageMetadata = page.getPageMetadata();
        ArrayList<String> inLinkAnchors = new ArrayList<>();
        ArrayList<Data.ItemWithFrequency<String>> anchors = pageMetadata.getInlinkAnchors();

        for (Data.ItemWithFrequency<String> anchor : anchors) {
            String item = anchor.getItem();
            int freq = anchor.getFrequency();
            String s = item + "_" + freq;
            inLinkAnchors.add(s);
        }
        return inLinkAnchors;
    }

    @NotNull
    private static IndexWriter createWriter(Analyzer analyzer, String indexDir)throws IOException {
        Directory dir = FSDirectory.open((new File(indexDir)).toPath());
        IndexWriterConfig conf = new IndexWriterConfig(analyzer);
        conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        return new IndexWriter(dir, conf);
    }

    private static void sectionContent(@NotNull Data.Section section, @NotNull StringBuilder content){
        content.append(section.getHeading()).append('\n');
        for (Data.PageSkeleton skel: section.getChildren()) {
            if (skel instanceof Data.Section) {
                sectionContent((Data.Section) skel, content);
            } else if (skel instanceof Data.Para) {
                paragraphContent((Data.Para) skel, content);
            }
        }
    }
    private static void paragraphContent(@NotNull Data.Para paragraph, @NotNull StringBuilder content){
        content.append(paragraph.getParagraph().getTextOnly()).append('\n');
    }

    @NotNull
    private static String getPageContent(@NotNull Data.Page page){
        StringBuilder content = new StringBuilder();
        content.append(page.getPageName()).append('\n');

        for(Data.PageSkeleton skel: page.getSkeleton()){
            if(skel instanceof Data.Section) {
                sectionContent((Data.Section) skel, content);
            } else if(skel instanceof Data.Para) {
                paragraphContent((Data.Para) skel, content);
            }
        }
        return content.toString();
    }

    @NotNull
    private static List<String> getEntityIdsOnly(@NotNull Data.Paragraph p) {
        List<String> result = new ArrayList<>();
        for(Data.ParaBody body: p.getBodies()){
            if(body instanceof Data.ParaLink){
                result.add(((Data.ParaLink) body).getPageId());
            }
        }
        return result;
    }


    private static void sectionEntities(@NotNull Data.Section section, ArrayList<String> entities){
        for (Data.PageSkeleton skel: section.getChildren()) {
            if (skel instanceof Data.Section) {
                sectionEntities((Data.Section) skel, entities);
            } else if (skel instanceof Data.Para) {
                paragraphEntities((Data.Para) skel, entities);
            }

        }
    }
    private static void paragraphEntities(@NotNull Data.Para paragraph, @NotNull ArrayList<String> entities){
        entities.addAll(getEntityIdsOnly(paragraph.getParagraph()));
    }

    @NotNull
    private static ArrayList<String> getPageEntities(@NotNull Data.Page page){
        ArrayList<String> entities = new ArrayList<>();
        for(Data.PageSkeleton skel: page.getSkeleton()) {
            if(skel instanceof Data.Section) {
                sectionEntities((Data.Section) skel, entities);
            } else if(skel instanceof Data.Para) {
                paragraphEntities((Data.Para) skel, entities);
            }
        }
        return entities;
    }


    public static void main(@NotNull String[] args) {
        String cborFile = args[0];
        String indexDir = args[1];
        String a = args[2];

        Analyzer analyzer = null;
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
        try {
            createIndex(cborFile, indexDir, analyzer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
