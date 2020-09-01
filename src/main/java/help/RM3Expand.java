package help;

import lucene.RAMIndex;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.jetbrains.annotations.NotNull;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * Expand a query with words using RM3.
 * @author Shubham Chatterjee
 * @version 06/9/2019
 * The original code was due to Dr. Laura Dietz, University of New Hampshire, USA.
 * The code has been adapted by the author and modified to suit the needs of the project.
 */

public class RM3Expand {

    private static void tokenizeQuery(String queryStr, String searchField, @NotNull List<String> tokens, @NotNull Analyzer analyzer) throws IOException {
        TokenStream tokenStream = analyzer.tokenStream(searchField, new StringReader(queryStr));
        tokenStream.reset();
        tokens.clear();
        while (tokenStream.incrementToken() && tokens.size() < 64)
        {
            final String token = tokenStream.getAttribute(CharTermAttribute.class).toString();
            tokens.add(token);
        }
        tokenStream.end();
        tokenStream.close();
    }

    public static BooleanQuery toRm3Query(String queryStr,
                                          List<Map.Entry<String, Float>> relevanceModel,
                                          boolean omitQueryTerms,
                                          String searchField,
                                          Analyzer analyzer) throws IOException {
        List<String> tokens = new ArrayList<>();
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();

        if (!omitQueryTerms) {
            tokenizeQuery(queryStr, searchField, tokens, analyzer);
            for (String token : tokens) {
                booleanQuery.add(new BoostQuery(new TermQuery(new Term(searchField, token)), 1.0f),
                        BooleanClause.Occur.SHOULD);
            }
        }

        // add RM3 terms
        for (Map.Entry<String, Float> stringFloatEntry : relevanceModel.subList(0, Math.min(relevanceModel.size(), (64 - tokens.size())))) {
            String token = stringFloatEntry.getKey();
            float weight = stringFloatEntry.getValue();
            booleanQuery.add(new BoostQuery(new TermQuery(new Term("text", token)),weight), BooleanClause.Occur.SHOULD);
        }
        return booleanQuery.build();
    }

    public static BooleanQuery toQuery(String queryStr, Analyzer analyzer) throws IOException {
        List<String> tokens = new ArrayList<>();

        tokenizeQuery(queryStr, "text", tokens, analyzer);
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();

        for (String token : tokens) {
            booleanQuery.add(new TermQuery(new Term("text", token)), BooleanClause.Occur.SHOULD);
        }
        return booleanQuery.build();
    }



}

