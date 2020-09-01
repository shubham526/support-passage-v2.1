package lucene;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Run Stanford NER on paragraph corpus.
 *
 * @author Shubham Chatterjee
 * @version 7/6/2020
 */

public class StanfordNERIndex {

    private final static AtomicInteger count = new AtomicInteger(0);

    public static void createIndex(String cborFile, String indexDir, String stanfordFile, Analyzer analyzer, boolean parallel) throws IOException {
        AbstractSequenceClassifier<CoreLabel> classifier = null;
        try {
            classifier = CRFClassifier.getClassifier(stanfordFile);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        IndexWriter writer = createWriter(analyzer, indexDir);
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(cborFile)));

        final Iterable<Data.Paragraph> paragraphIterable = DeserializeData.iterableParagraphs(bis);
        System.out.println("Creating index at location: " + indexDir);


        AbstractSequenceClassifier<CoreLabel> finalClassifier = classifier;
        StreamSupport.stream(paragraphIterable.spliterator(), parallel)
                .forEach(paragraph ->
                {
                    Document d = null;
                    try {
                        d = createDocument(paragraph, finalClassifier);
                        writer.addDocument(d);
                        //System.out.println("Done: " + paragraph.getParaId());
                        count.getAndIncrement();
                        if (count.get() % 1000 == 0) {
                            System.out.println("Progress: " + count.get() + " of 29,794,697.");
                            writer.commit();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (IllegalArgumentException e) {
                        System.err.println("ERROR: " + e.getMessage());
                    }
                });
        writer.commit();
        writer.close();
        System.out.println("Finished.");

    }

    @NotNull
    private static IndexWriter createWriter(Analyzer analyzer, String indexDir)throws IOException {
        Directory dir = FSDirectory.open((new File(indexDir)).toPath());
        IndexWriterConfig conf = new IndexWriterConfig(analyzer);
        conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        return new IndexWriter(dir, conf);
    }

    @NotNull
    private static Document createDocument(@NotNull Data.Paragraph paragraph,
                                           AbstractSequenceClassifier<CoreLabel> classifier) {

        String paraID = paragraph.getParaId();
        String paraText = paragraph.getTextOnly();
        List<String> entityList = getEntities(paragraph,classifier);
        //String entityList = getEntities(paragraph,classifier);
        return paraToLuceneDoc(paraID, paraText, entityList);

    }

    @NotNull
    private static List<String> getEntities(@NotNull Data.Paragraph paragraph,
                                            @NotNull AbstractSequenceClassifier<CoreLabel> classifier) {
        JSONArray jsonArray = new JSONArray();
        List<String> entities = new ArrayList<>();
        String text = paragraph.getTextOnly();
        List<String> str =  new ArrayList<>(Arrays.asList(classifier.classifyToString(text, "tsv", false).split("\n")));
        str.removeAll(Collections.singleton(""));
       for (String s : str) {
           String entity = s.split("\t")[0];
           String tag = s.split("\t")[1];
           if (Stream.of("person", "organization", "location").anyMatch(tag::equalsIgnoreCase)) {
               entities.add(entity + ":" + tag);
//               JSONObject ob = new JSONObject();
//               try {
//                   ob.put("entity", entity);
//                   ob.put("tag", tag);
//               } catch (JSONException e) {
//                   e.printStackTrace();
//               }
//               jsonArray.put(ob);
           }
       }
       //return jsonArray.toString();
        return entities;
    }

    @NotNull
    private static Document paraToLuceneDoc(String paraID, String paraText, List<String> entityList) {
        Document doc = new Document();
        try {
            doc.add(new StringField("Id", paraID, Field.Store.YES));
            //doc.add(new StringField("Entity", entityList, Field.Store.YES));
            doc.add(new TextField("Text", paraText, Field.Store.YES));
            doc.add(new StringField("Entity", String.join("\n", entityList), Field.Store.YES));
        } catch (IllegalArgumentException e) {
            System.out.println(entityList);
        }

        return doc;
    }

    public static void main(@NotNull String[] args) {
        String cborFile = args[0];
        String indexDir = args[1];
        String stanfordFile = args[2];
        String a = args[3];
        boolean parallel = args[4].equalsIgnoreCase("true");

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
            createIndex(cborFile, indexDir, stanfordFile, analyzer, parallel);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
