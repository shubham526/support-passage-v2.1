package lucene;

import api.WATApi;
import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

public class ParagraphIndex {
    private final static AtomicInteger count = new AtomicInteger(0);
    public static void createIndex(String cborFile, String indexDir, Analyzer analyzer) throws IOException {
        IndexWriter writer = createWriter(analyzer, indexDir);
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(cborFile)));

        final Iterable<Data.Paragraph> paragraphIterable = DeserializeData.iterableParagraphs(bis);
        System.out.println("Creating index at location: " + indexDir);


        StreamSupport.stream(paragraphIterable.spliterator(), true)
                .forEach(paragraph ->
                {
                    try {
                        writer.addDocument(createDocument(paragraph));
                        System.out.println("Done: " + paragraph.getParaId());
                        count.getAndIncrement();
                        if (count.get() % 10000 == 0) {
                            writer.commit();
                        }
//                        if (! isPresentInIndex(paragraph, indexDir)) {
//                            writer.addDocument(createDocument(paragraph));
//                            System.out.println("Done: " + paragraph.getParaId());
//                        } else {
//                            System.out.println("Skipping:" + paragraph.getParaId());
//                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        writer.commit();
        writer.close();
        System.out.println("Finished.");

    }

    @NotNull
    private static IndexSearcher createSearcher(String indexDir, Similarity sim) throws IOException {
        Directory dir = FSDirectory.open((new File(indexDir).toPath()));
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(sim);
        return searcher;
    }

    @Nullable
    private static Document searchIndex(String field, String query, @NotNull IndexSearcher is) throws IOException {
        Term term = new Term(field,query);
        Query q = new TermQuery(term);
        TopDocs tds = is.search(q,1);
        ScoreDoc[] retDocs = tds.scoreDocs;

        if(retDocs.length != 0) {
            return  is.doc(retDocs[0].doc);
        }
        return null;
    }

    private static boolean isPresentInIndex(@NotNull Data.Paragraph paragraph, String indexDir) throws IOException {
        IndexSearcher indexSearcher = createSearcher(indexDir, new BM25Similarity());
        Document d = searchIndex("Id", paragraph.getParaId(), indexSearcher);
        return d != null;
    }

    @NotNull
    private static Document createDocument(@NotNull Data.Paragraph paragraph) {

        String paraID = paragraph.getParaId();
        String paraText = paragraph.getTextOnly();
        List<String> entityLinks = getEntityLinks(paragraph);
        List<String> outlinkIds = getOutlinkIds(paragraph);
        //Document d = paraToLuceneDoc(paraID, paraText, entityLinks, outlinkIds);
        return paraToLuceneDoc(paraID, paraText, entityLinks, outlinkIds);

    }

    @NotNull
    private static Document paraToLuceneDoc(String paraID, String paraText,
                                            List<String> entityLinks, List<String> outlinkIds) {
        Document doc = new Document();
        doc.add(new StringField("Id", paraID, Field.Store.YES));
        doc.add(new TextField("Text", paraText, Field.Store.YES));
        doc.add(new TextField("EntityLinks", String.join("\n", entityLinks), Field.Store.YES));
        doc.add(new TextField("OutlinkIds", String.join("\n", outlinkIds), Field.Store.YES));

        return doc;
    }

    @NotNull
    private static List<String> getOutlinkIds(Data.Paragraph paragraph) {
        List<String> outlinkIds = new ArrayList<>();

        // Use entity links in the provided with the data
        outlinkIds.addAll(getParaEntityIds(paragraph));

        // Also use entity links from WAT
        outlinkIds.addAll(getWatEntityIds(paragraph));

        return outlinkIds;
    }

    @NotNull
    private static List<String> getEntityLinks(Data.Paragraph paragraph) {
        List<String> entityLinks = new ArrayList<>();

        // Use entity links in the provided with the data
        entityLinks.addAll(getParaEntities(paragraph));

        // Also use entity links from WAT
        entityLinks.addAll(getWatEntities(paragraph));

        return entityLinks;
    }

    @NotNull
    private static List<String> getWatEntities(@NotNull Data.Paragraph paragraph) {
        List<String> entities = new ArrayList<>();
        String text = paragraph.getTextOnly();
        List<WATApi.Annotation> annotations = WATApi.EntityLinker.getAnnotations(text,0.1);
        for (WATApi.Annotation annotation : annotations) {
            String wikiTitle = annotation.getWikiTitle().replaceAll("_", " ");
            String spot = annotation.getSpot();
            entities.add(wikiTitle + "_" + spot);
        }
        return entities;
    }

    @NotNull
    private static List<String> getWatEntityIds(@NotNull Data.Paragraph paragraph) {
        List<String> entities = new ArrayList<>();
        String text = paragraph.getTextOnly();
        List<WATApi.Annotation> annotations = WATApi.EntityLinker.getAnnotations(text,0.1);
        for (WATApi.Annotation annotation : annotations) {
            String wikiTitle = annotation.getWikiTitle().replaceAll("_", " ");
            String id = "enwiki:" + wikiTitle.replaceAll(" ","%20");
            String spot = annotation.getSpot();
            entities.add(id + "_" + spot);
        }
        return entities;
    }

    @NotNull
    private static IndexWriter createWriter(Analyzer analyzer, String indexDir)throws IOException {
        Directory dir = FSDirectory.open((new File(indexDir)).toPath());
        IndexWriterConfig conf = new IndexWriterConfig(analyzer);
        conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        return new IndexWriter(dir, conf);
    }

    @NotNull
    private static List<String> getParaEntities(@NotNull Data.Paragraph p) {
        List<String> result = new ArrayList<>();
        for(Data.ParaBody body: p.getBodies()) {
            if(body instanceof Data.ParaLink) {
                String pageName = ((Data.ParaLink) body).getPage();
                String anchorText = ((Data.ParaLink) body).getAnchorText();
                result.add(pageName + "_" + anchorText);
            }
        }
        return result;
    }

    @NotNull
    private static List<String> getParaEntityIds(@NotNull Data.Paragraph p) {
        List<String> result = new ArrayList<>();
        for(Data.ParaBody body: p.getBodies()){
            if(body instanceof Data.ParaLink){
                String pageID = ((Data.ParaLink) body).getPageId();
                String anchorText = ((Data.ParaLink) body).getAnchorText();
                result.add(pageID + "_" + anchorText);
            }
        }
        return result;
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
