package api;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class uses the WAT Entity Linking System to annotate text with entities.
 * @author Shubham Chatterjee
 * @version 3/8/2020
 */

public class WATApi {

    private final static String TOKEN = "xxxxxxx"; // INSERT YOUR TOKEN HERE

    /**
     * Inner class t represent an annotation.
     */

    public static class Annotation {

        String wikiTitle;
        int wikiId, start, end;
        double rho;

        /**
         * Constructor.
         * @param wikiId Integer Wikipedia ID of the page the entity links to.
         * @param wikiTitle String Wikipedia page title of the page the entity links to.
         * @param start Integer Character offset (included)
         * @param end Integer Character offset (not included)
         * @param rho Double Annotation accuracy
         */

        private Annotation(int wikiId, String wikiTitle, int start, int end, double rho) {
            this.wikiId = wikiId;
            this.wikiTitle = wikiTitle;
            this.start = start;
            this.end = end;
            this.rho = rho;
        }

        //////////////////////// GETTERS FOR THE VARIOUS FIELDS///////////////////

        public int getWikiId() {
            return wikiId;
        }

        public String getWikiTitle() {
            return wikiTitle;
        }

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }

        public double getRho() {
            return rho;
        }

        ///////////////////////////////////////////////////////////////////////////

    }

    /**
     * Inner class to represent the entity linking system.
     */


    public static class EntityLinker {
        private final static String URL = "https://wat.d4science.org/wat/tag/tag ";

        /**
         * Method to return the annotations in the text.
         * @param data String The text to annotate.
         * @return List List of annotations.
         */

        @NotNull
        private static ArrayList<Annotation> getAnnotations(String data) {
            ArrayList<Annotation> annotations = new ArrayList<>();
            Document doc;

            try {

                doc = getDocument(data);

                if (doc.text() != null) {
                    JSONObject json = new JSONObject(doc.text());
                    if (json.has("annotations")) {
                        JSONArray jsonArray = json.getJSONArray("annotations");
                        for (int i = 0; i < jsonArray.length(); i++) {

                            JSONObject jsonObject = jsonArray.getJSONObject(i);

                            String wikiTitle = jsonObject.getString("title");
                            int wikiId = jsonObject.getInt("id");
                            int start = jsonObject.getInt("start");
                            int end = jsonObject.getInt("end");
                            double rho = jsonObject.getDouble("rho");

                            annotations.add(new Annotation(wikiId, wikiTitle, start, end, rho));

                        }
                    } else {
                        System.err.println("ERROR: WAT could not find any annotations.");
                    }
                }
            } catch (JSONException e) {
                System.err.print("ERROR: JSONException");
                return annotations;
            }
            return annotations;
        }

        @NotNull
        public static ArrayList<Annotation> getAnnotations(String data, double rho) {
            ArrayList<Annotation> allAnnotations = getAnnotations(data);

            if (rho == 0.0d) {
                return allAnnotations;
            }

            ArrayList<Annotation> annotations = new ArrayList<>();
            for (Annotation annotation : allAnnotations) {
                if (annotation.getRho() >= rho) {
                    annotations.add(annotation);
                }
            }
            return annotations;
        }

        /**
         * Helper method to connect to the URL.
         * @param data String The text to annotate.
         * @return Document Jsoup document
         */

        private static Document getDocument(String data) {
            Document doc = null;
            try {
                doc = Jsoup.connect(URL)
                        .data("lang", "en")
                        .data("gcube-token", TOKEN)
                        .data("text", data)
                        .data("tokenizer", "nlp4j")
                        .data("debug", "9")
                        .data("method", "spotter:includeUserHint=true:includeNamedEntity=true:includeNounPhrase=true,prior:k=50,filter-valid,centroid:rescore=true,topk:k=5,voting:relatedness=lm,ranker:model=0046.model,confidence:model=pruner-wiki.linear")
                        .ignoreContentType(true)
                        .get();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return doc;
        }
    }

    /**
     * Inner class to represent the Entity Relatedness system in WAT.
     */

    public static class  EntityRelatedness {
        private final static String URL = "https://wat.d4science.org/wat/relatedness/graph";

        public static class Pair {

            private static class Entry {
                final int id;
                final String name;

                public Entry(int id, String name) {
                    this.id = id;
                    this.name = name;
                }

                public int getId() {
                    return id;
                }

                public String getName() {
                    return name;
                }

            }

            public static class Source extends Entry {

                public Source(int id, String name) {
                    super(id, name);
                }

                @Override
                public String toString() {
                    return "Source{" +
                            "id='" + id + '\'' +
                            ", name='" + name + '\'' +
                            '}';
                }
            }

            public static class Destination extends Entry {

                public Destination(int id, String name) {
                    super(id, name);
                }

                @Override
                public String toString() {
                    return "Destination{" +
                            "id='" + id + '\'' +
                            ", name='" + name + '\'' +
                            '}';
                }
            }

            private final Source source;
            private final Destination destination;
            private final double relatedness;

            public Pair(Source source, Destination destination, double relatedness) {
                this.source = source;
                this.destination = destination;
                this.relatedness = relatedness;
            }

            public Source getSource() {
                return source;
            }

            public Destination getDestination() {
                return destination;
            }

            public double getRelatedness() {
                return relatedness;
            }

            @Override
            public String toString() {
                return "Pair{" +
                        "source=" + source +
                        ", destination=" + destination +
                        ", relatedness=" + relatedness +
                        '}';
            }
        }

        /**
         * Get the relatedness measure between pairs of entities provided as argument.
         * @param relMeasure String Relatedness function to compute.
         *                   Accepted values are:
         *                   (1) mw (Milne-Witten)
         *                   (2) jaccard (Jaccard measure of pages outlinks)
         *                   (3) lm (language model)
         *                   (4) w2v (Word2Vect)
         *                   (5) conditionalprobability (Conditional Probability)
         *                   (6) barabasialbert (Barabasi-Albert on the Wikipedia Graph)
         *                   (7) pmi (Pointwise Mutual Information)
         * @param ids List of Wikipedia entity IDs.
         * @return List List of Pairs of entities with relatedness score.
         */

        @NotNull
        public static List<Pair> getRelatedness(String relMeasure, int ... ids) {
            List<Pair> relatedPairsList = new ArrayList<>();

            Document doc;

            try {

                doc = getDocument(relMeasure, ids);

                if (doc != null) {
                    JSONObject json = new JSONObject(doc.text());
                    if (json.has("pairs")) {
                        JSONArray jsonArray = json.getJSONArray("pairs");
                        for (int i = 0; i < jsonArray.length(); i++) {
                            String srcTitle = "", dstTitle = "";
                            int srcId = 0, dstId = 0;
                            double rel = 0.0d;

                            JSONObject jsonObject = jsonArray.getJSONObject(i);

                            if (jsonObject.has("src_title")) {
                                srcTitle = jsonObject.getJSONObject("src_title").getString("wiki_title");
                                srcId = jsonObject.getJSONObject("src_title").getInt("wiki_id");
                            }

                            if (jsonObject.has("dst_title")) {
                                dstTitle = jsonObject.getJSONObject("dst_title").getString("wiki_title");
                                dstId = jsonObject.getJSONObject("dst_title").getInt("wiki_id");
                            }

                            if (jsonObject.has("relatedness")) {
                                rel = jsonObject.getDouble("relatedness");
                            }

                            Pair.Source source = new Pair.Source(srcId, srcTitle);
                            Pair.Destination destination = new Pair.Destination(dstId, dstTitle);
                            Pair pair = new Pair(source, destination, rel);

                            relatedPairsList.add(pair);

                        }
                    } else {
                        System.err.println("ERROR: WAT could not find any annotations.");
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }

            return relatedPairsList;
        }

        /**
         * Get the Jsoup document.
         * @param relMeasure Relatedness function to compute.
         *                   Accepted values are:
         *                   (1) mw (Milne-Witten)
         *                   (2) jaccard (Jaccard measure of pages outlinks)
         *                   (3) lm (language model)
         *                   (4) w2v (Word2Vect)
         *                   (5) conditionalprobability (Conditional Probability)
         *                   (6) barabasialbert (Barabasi-Albert on the Wikipedia Graph)
         *                   (7) pmi (Pointwise Mutual Information)
         *
         * @param ids List of Wikipedia entity IDs.
         * @return Document Jsoup Document
         */

        private static Document getDocument(String relMeasure, @NotNull int[] ids) {
            Document doc = null;
            try {
                Connection con  = Jsoup.connect(URL)
                        .data("gcube-token", TOKEN)
                        .data("relatedness", relMeasure)
                        .ignoreContentType(true);

                for (int id : ids) {
                    con.data("ids",Integer.toString(id));
                }
                doc = con.get();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return doc;
        }
    }

    /**
     * Inner class to represent the Title Resolver in WAT.
     * This system takes a Wikipedia entity name and returns the corresponding ID.
     * May be used in conjunction with the Entity Relatedness system.
     */

    public static class TitleResolver {
        private final static String URL = "https://wat.d4science.org/wat/title";

        public static int getId(String title) {
            Document doc;
            int id = 0;

            try {

                doc = getDocument(title);

                if (doc.text() != null) {
                    JSONObject json = new JSONObject(doc.text());
                    if (json.has("wiki_id")) {
                        id = json.getInt("wiki_id");
                    } else {
                        System.err.println("ERROR: WAT could not find any annotations.");
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
            return id;

        }

        private static Document getDocument(String data) {
            Document doc = null;
            try {
                doc = Jsoup.connect(URL)
                        .data("lang", "en")
                        .data("gcube-token", TOKEN)
                        .data("title", data)
                        .ignoreContentType(true)
                        .get();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return doc;
        }
    }

    /**
     * Inner class to represent the Entity Surface Form Information system in WAT.
     */

    public static class EntitySurfaceFormInformation {
        private final static String URL = "https://wat.d4science.org/wat/sf/sf";

        public static class SurfaceForm {
            private final int id;
            private final double linkProbability, termProbability, documentProbability, idf, tf_idf;
            private final int tf, df;
            private final ArrayList<LinkedEntity> linkedEntities;

            /**
             * Represents an entity that the surface form points to in Wikipedia.
             */
            private static class LinkedEntity {
                private final int wikiId, numLinks;
                private final double probability;

                public LinkedEntity(int wikiId, int numLinks, double probability) {
                    this.wikiId = wikiId;
                    this.numLinks = numLinks;
                    this.probability = probability;
                }

                public int getWikiId() {
                    return wikiId;
                }

                public int getNumLinks() {
                    return numLinks;
                }

                public double getProbability() {
                    return probability;
                }

                @Override
                public String toString() {
                    return "LinkedEntity{" +
                            "wikiId=" + wikiId +
                            ", numLinks=" + numLinks +
                            ", probability=" + probability +
                            '}';
                }
            }

            public SurfaceForm(int id, double linkProbability, double termProbability, double documentProbability,
                               double idf, double tf_idf, int tf, int df, ArrayList<LinkedEntity> linkedEntities) {
                this.id = id;
                this.linkProbability = linkProbability;
                this.termProbability = termProbability;
                this.documentProbability = documentProbability;
                this.idf = idf;
                this.tf_idf = tf_idf;
                this.tf = tf;
                this.df = df;
                this.linkedEntities = linkedEntities;
            }

            public int getId() {
                return id;
            }

            public double getLinkProbability() {
                return linkProbability;
            }

            public double getTermProbability() {
                return termProbability;
            }

            public double getDocumentProbability() {
                return documentProbability;
            }

            public double getIdf() {
                return idf;
            }

            public double getTf_idf() {
                return tf_idf;
            }

            public int getTf() {
                return tf;
            }

            public int getDf() {
                return df;
            }

            public ArrayList<LinkedEntity> getLinkedEntities() {
                return linkedEntities;
            }

            @Override
            public String toString() {
                return "SurfaceForm{" +
                        "id='" + id + '\'' +
                        ", linkProbability=" + linkProbability +
                        ", termProbability=" + termProbability +
                        ", documentProbability=" + documentProbability +
                        ", idf=" + idf +
                        ", tf_idf=" + tf_idf +
                        ", tf=" + tf +
                        ", df=" + df +
                        ", linkedEntities=" + linkedEntities +
                        '}';
            }
        }

        @NotNull
        @Contract("_ -> new")
        public static SurfaceForm getInformation(String data) {

            Document doc;
            int id = 0;
            double linkProbability = 0.0d, termProbability = 0.0d, documentProbability = 0.0d, idf = 0.0d, tf_idf = 0.0d;
            int tf = 0, df = 0;
            ArrayList<SurfaceForm.LinkedEntity> linkedEntities = new ArrayList<>();
            try {
                doc = getDocument(data);
                if (doc.text() != null) {
                    JSONObject json = new JSONObject(doc.text());
                    id = json.has("id")
                            ? json.getInt("id")
                            : 0;
                    linkProbability = json.has("link_probability")
                            ? json.getDouble("link_probability")
                            : 0.0d;
                    termProbability = json.has("term_probability")
                            ? json.getDouble("term_probability")
                            : 0.0d;
                    documentProbability = json.has("document_probability")
                            ? json.getDouble("document_probability")
                            : 0.0d;
                    idf = json.has("idf")
                            ? json.getDouble("idf")
                            : 0.0d;
                    tf_idf = json.has("tf_idf")
                            ? json.getDouble("tf_idf")
                            : 0.0d;
                    tf = json.has("tf")
                            ? json.getInt("tf")
                            : 0;
                    df = json.has("df")
                            ? json.getInt("df")
                            : 0;
                    JSONArray entities = json.getJSONArray("entities");
                    for (int i = 0; i < entities.length(); i++) {
                        JSONObject entity = entities.getJSONObject(i);
                        int wikiId = entity.has("wiki_id")
                                ? entity.getInt("wiki_id")
                                : 0;
                        int numLinks = entity.has("num_links")
                                ? entity.getInt("num_links")
                                : 0;
                        double prob = entity.has("probability")
                                ? entity.getDouble("probability")
                                : 0;
                        linkedEntities.add(new SurfaceForm.LinkedEntity(wikiId, numLinks, prob));
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }


            return new SurfaceForm(id, linkProbability, termProbability, documentProbability,
                    idf, tf_idf, tf, df, linkedEntities);
        }

        private static Document getDocument(String data) {
            Document doc = null;
            try {
                doc = Jsoup.connect(URL)
                        .data("gcube-token", TOKEN)
                        .data("text", data)
                        .ignoreContentType(true)
                        .get();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return doc;
        }

    }

    /**
     * Utility method to find the Wikipedia page title corresponding to the id.
     * Sort of a hack!!
     * @param id Integer Wikipedia Page ID.
     * @return String Wikipedia Page Title.
     */

    public static String pageIdToTitle(int id) {
        List<EntityRelatedness.Pair> pairs = EntityRelatedness.getRelatedness("mw",id, 0);
        return  pairs.get(0).getDestination().getName();
    }

    /**
     * Main method to test the above.
     * @param args Command line arguments.
     */

    public static void main(@NotNull String[] args) {
        int id = 1845551;
        String title = pageIdToTitle(id);
        System.out.println("ID: " + id + "\t" + "Title: " + title);

    }


}
