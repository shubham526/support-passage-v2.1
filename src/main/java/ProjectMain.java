import experiments.relatedness.Experiment1;
import experiments.relatedness.Experiment2;
import experiments.relatedness.Experiment3;
import experiments.relatedness.Experiment4;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.jetbrains.annotations.NotNull;

public class ProjectMain {
    public static void main(@NotNull String[] args) {
        if (args.length == 0) {
            help();
            System.exit(-1);
        }
        String command = args[0];
        if (command.equalsIgnoreCase("-h") || command.equalsIgnoreCase("--help")) {
            help();
            System.exit(-1);
        }

        if (command.equalsIgnoreCase("-o") || command.equalsIgnoreCase("--options")) {
            options();
            System.exit(-1);
        }

        if (command.equalsIgnoreCase("--exp1")) {
            exp1(args);
        } else if (command.equalsIgnoreCase("--exp2")) {
            exp2(args);
        } else if (command.equalsIgnoreCase("--exp3")) {
            exp3(args);
        } else if (command.equalsIgnoreCase("--exp4")) {
            exp4(args);
        }

    }
    private static void exp1(@NotNull String[] args) {

        System.out.println("Performing Experiment-1");
        Similarity similarity = null;
        Analyzer analyzer = null;
        boolean omit;
        String s1 = null, s2;

        String paraIndexDir = args[1];
        String pageIndexDir = args[2];
        String mainDir = args[3];
        String outputDir = args[4];
        String dataDir = args[5];
        String idFile = args[6];
        String entityRunFile = args[7];
        String entityQrel = args[8];
        String relType = args[9];
        int takeKEntities = Integer.parseInt(args[10]);
        String o = args[11];
        omit = o.equalsIgnoreCase("y") || o.equalsIgnoreCase("yes");
        String a = args[12];
        String sim = args[13];

        System.out.printf("Using %d entities for query expansion\n", takeKEntities);

        if (omit) {
            System.out.println("Using RM1");
            s2 = "rm1";
        } else {
            System.out.println("Using RM3");
            s2 = "rm3";
        }


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
        switch (sim) {
            case "BM25" :
            case "bm25":
                similarity = new BM25Similarity();
                System.out.println("Similarity: BM25");
                s1 = "bm25";
                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                float lambda;
                try {
                    lambda = Float.parseFloat(args[14]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                    s1 = "lmjm";
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Missing lambda value for similarity LM-JM");
                    System.exit(1);
                }
                break;
            case "LMDS":
            case "lmds":
                System.out.println("Similarity: LMDS");
                similarity = new LMDirichletSimilarity();
                s1 = "lmds";
                break;

            default:
                System.out.println("Wrong choice of similarity! Exiting.");
                System.exit(1);
        }
        String outFile = "qe-rel-ent-page" + "-" + s1 + "-" + s2 + ".run";

        new Experiment1(paraIndexDir, pageIndexDir, mainDir, outputDir, dataDir, idFile, entityRunFile,
                outFile, entityQrel, relType, takeKEntities, omit, analyzer, similarity);

    }
    private static void exp2(@NotNull String[] args) {
        System.out.println("Performing Experiment-2");
        String indexDir = args[1];
        String mainDir = args[2];
        String outputDir = args[3];
        String dataDir = args[4];
        String paraRunFile = args[5];
        String entityRunFile = args[6];
        String idFile = args[7];
        String entityQrel = args[8];
        String relType = args[9];
        String p = args[10];
        String a = args[11];
        String sim = args[12];

        Analyzer analyzer = null;
        Similarity similarity = null;
        String s1 = null;
        boolean parallel = false;

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
        switch (sim) {
            case "BM25" :
            case "bm25":
                similarity = new BM25Similarity();
                System.out.println("Similarity: BM25");
                s1 = "bm25";
                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                float lambda;
                try {
                    lambda = Float.parseFloat(args[13]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                    s1 = "lmjm";
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Missing lambda value for similarity LM-JM");
                    System.exit(1);
                }
                break;
            case "LMDS":
            case "lmds":
                System.out.println("Similarity: LMDS");
                similarity = new LMDirichletSimilarity();
                s1 = "lmds";
                break;

            default:
                System.out.println("Wrong choice of similarity! Exiting.");
                System.exit(1);
        }

        String outFile = "ecn-rel-ent-" + s1 + "-" + relType + ".run";

        if (p.equalsIgnoreCase("true")) {
            parallel = true;
        }


        new Experiment2(indexDir, mainDir, outputDir, dataDir, paraRunFile, entityRunFile, idFile,
                outFile, entityQrel, relType, parallel, analyzer, similarity);

    }
    private static void exp3(@NotNull String[] args) {
        System.out.println("Performing Experiment-3");
        Similarity similarity = null;
        Analyzer analyzer = null;
        boolean omit, useFrequency;
        String s1 = null, s2;

        String indexDir = args[1];
        String mainDir = args[2];
        String outputDir = args[3];
        String dataDir = args[4];
        String idFile = args[5];
        String relFile = args[6];
        String paraRunFile = args[7];
        String entityRunFile = args[8];
        String entityQrel = args[9];
        int takeKEntities = Integer.parseInt(args[10]);
        String o = args[11];
        String uf = args[12];
        String relType = args[13];
        String a = args[14];
        String sim = args[15];

        System.out.printf("Using %d entities for query expansion\n", takeKEntities);
        omit = o.equalsIgnoreCase("y") || o.equalsIgnoreCase("yes");
        useFrequency = uf.equalsIgnoreCase("true");

        if (omit) {
            System.out.println("Using RM1");
            s2 = "rm1";
        } else {
            System.out.println("Using RM3");
            s2 = "rm3";
        }


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
        switch (sim) {
            case "BM25" :
            case "bm25":
                similarity = new BM25Similarity();
                System.out.println("Similarity: BM25");
                s1 = "bm25";
                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                float lambda;
                try {
                    lambda = Float.parseFloat(args[16]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                    s1 = "lmjm";
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Missing lambda value for similarity LM-JM");
                    System.exit(1);
                }
                break;
            case "LMDS":
            case "lmds":
                System.out.println("Similarity: LMDS");
                similarity = new LMDirichletSimilarity();
                s1 = "lmds";
                break;

            default:
                System.out.println("Wrong choice of similarity! Exiting.");
                System.exit(1);
        }
        String outFile = "qe-rel-ent-context" + "-" + s1 + "-" + s2 + "-" + relType;
        if (useFrequency) {
            System.out.println("Using frequency of co-occurring entities: Yes");
            outFile += "-" + "freq-true";
        } else {
            System.out.println("Using frequency of co-occurring entities: No");
            outFile += "-" + "freq-false";
        }

        outFile += ".run";

        new Experiment3(indexDir, mainDir, outputDir, dataDir, idFile, relFile, paraRunFile, entityRunFile, outFile,
                entityQrel, takeKEntities, omit, useFrequency, relType, analyzer, similarity);


    }
    private static void exp4(@NotNull String[] args) {
        String indexDir = args[1];
        String mainDir = args[2];
        String outputDir = args[3];
        String dataDir = args[4];
        String passageRunFile = args[5];
        String entityRunFile = args[6];
        String idFile = args[7];
        String entityQrelFilePath = args[8];
        String relType = args[9];
        String candidateType = args[10];
        String p = args[11];
        String a = args[12];
        String sim = args[13];

        Analyzer analyzer = null;
        Similarity similarity = null;
        String s1 = null;
        boolean parallel = false;
        boolean usePsgCandidate = false;

        if (candidateType.equalsIgnoreCase("passage")) {
            usePsgCandidate = true;
        }

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
        switch (sim) {
            case "BM25" :
            case "bm25":
                similarity = new BM25Similarity();
                System.out.println("Similarity: BM25");
                s1 = "bm25";
                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                float lambda;
                try {
                    lambda = Float.parseFloat(args[14]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                    s1 = "lmjm";
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Missing lambda value for similarity LM-JM");
                    System.exit(1);
                }
                break;
            case "LMDS":
            case "lmds":
                System.out.println("Similarity: LMDS");
                similarity = new LMDirichletSimilarity();
                s1 = "lmds";
                break;

            default:
                System.out.println("Wrong choice of similarity! Exiting.");
                System.exit(1);
        }

        String outFile = "rel-all-ent-cand-set-" + candidateType + "-" + s1 + "-" + relType + ".run";
        if (p.equalsIgnoreCase("true")) {
            parallel = true;
        }
        new Experiment4(indexDir, mainDir, outputDir, dataDir, passageRunFile, entityRunFile, idFile, outFile,
                entityQrelFilePath, relType, usePsgCandidate, parallel, analyzer, similarity);

    }
    private static void options() {
        System.out.println("The following options are available: exp1, exp2, exp3");
        System.out.println("Use the --desc flag with the option (such as --exp1 --desc) " +
                "to view a description of the method.");
        System.out.println("Use the --use flag with the option (such as --exp1 --use) " +
                "to view a description of the command line arguments for the option.");
    }
    private static void help() {

        System.out.println("--exp1 (paraIndexDir|pageIndexDir|mainDir|outputDir|dataDir|idFile|entityRunFile|" +
                "entityQrelFilePath|takeKEntities|motQueryTerms|analyzer|similarity)");

        System.out.println("--exp2 (indexDir|mainDir|outputDir|dataDir|paraRunFile|entityRunFile|idFile|outFile|entityQrelPath" +
                "useRelatedness|analyzer|similarity)");

        System.out.println("--exp3 (indexDir|mainDir|outputDir|dataDir|idFile|relFile|paraRunFile|entityRunFile|outFile|" +
                "entityQrelFilePath|takeKEntities|omitQueryTerms|useFrequency|relType|analyzer|similarity)");

        System.out.println("--exp4 (indexDir|mainDir|outputDir|dataDir|passageRunFile|entityRunFile|idFile, outFile|" +
                "entityQrelFilePath|takeKPassages|relType|analyzer|similarity)");

    }

}
