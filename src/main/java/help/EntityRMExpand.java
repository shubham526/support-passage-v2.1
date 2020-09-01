package help;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.TermQuery;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EntityRMExpand {
    @Nullable
    public static <K,V> BooleanQuery toEntityRmQuery(String queryStr,
                                               List<Map.Entry<K, V>> expansionEntities,
                                               boolean omitQueryTerms,
                                               String searchField,
                                               Analyzer analyzer) throws IOException {

        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        List<String> tokens = new ArrayList<>(64);
        int w1; double w2;
        if (!omitQueryTerms) {
            tokens = tokenizeQuery(queryStr, searchField, analyzer);
            for (String token : tokens) {
                booleanQuery.add(new BoostQuery(new TermQuery(new Term(searchField, token)), 1.0f),
                        BooleanClause.Occur.SHOULD);
            }
        }




        // add Entity RM terms
        for (Map.Entry<K,V> stringDoubleEntry : expansionEntities.subList(0, Math.min(expansionEntities.size(), (64 - tokens.size())))) {
            String e = (String) stringDoubleEntry.getKey();
            e = Utilities.process(e).replaceAll("_", " ");
            List<String> entityToks = tokenizeQuery(e, searchField, analyzer);
            for (String entity : entityToks) {
                double weight = (Double) stringDoubleEntry.getValue();
                booleanQuery.add(new BoostQuery(new TermQuery(new Term(searchField, entity)), (float) weight),
                        BooleanClause.Occur.SHOULD);
            }
        }
        return booleanQuery.build();
    }

    public static BooleanQuery toEntityRmQuery(String queryStr,
                                        List<Map.Entry<String, Float>> entityRelevanceModel,
                                        boolean omitQueryTerms,
                                        String textSearchField,
                                        String entitySearchField,
                                        Analyzer analyzer) throws IOException {


        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        List<String> tokens = new ArrayList<>(64);
        List<String> searchFields = new ArrayList<>();
        searchFields.add(textSearchField);
        searchFields.add(entitySearchField);
        if(!omitQueryTerms) {
            tokens = tokenizeQuery(queryStr, textSearchField, analyzer);

            for (String searchField : searchFields) {
                for (String token : tokens) {
                    booleanQuery.add(new BoostQuery(new TermQuery(new Term(searchField, token)), 1.0f),
                            BooleanClause.Occur.SHOULD);
                }
            }
        }

        // add Entity RM terms

        for (Map.Entry<String, Float> stringFloatEntry : entityRelevanceModel.subList(0, Math.min(entityRelevanceModel.size(), (64-tokens.size())))) {
            List<String> entityToks = tokenizeQuery(stringFloatEntry.getKey(), entitySearchField, analyzer);
            for(String entity: entityToks) {
                float weight = stringFloatEntry.getValue();
                booleanQuery.add(new BoostQuery(new TermQuery(new Term(entitySearchField, entity)),weight),
                        BooleanClause.Occur.SHOULD);
            }
        }


        return booleanQuery.build();
    }
    @NotNull
    private static List<String> tokenizeQuery(String queryStr,
                                              String searchField,
                                              @NotNull Analyzer analyzer) throws IOException {
        TokenStream tokenStream = analyzer.tokenStream(searchField, new StringReader(queryStr));
        List<String> tokens = new ArrayList<>();
        tokenStream.reset();
        while (tokenStream.incrementToken() && tokens.size() < 64) {
            final String token = tokenStream.getAttribute(CharTermAttribute.class).toString();
            tokens.add(token);
        }
        tokenStream.end();
        tokenStream.close();
        return tokens;
    }
}



