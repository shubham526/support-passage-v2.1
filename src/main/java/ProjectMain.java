import experiments.baselines.EntityStat;
import experiments.relatedness.*;
import experiments.wiki.WikiTerms;
import help.GetRelatedness;
import lucene.Index;
import lucene.PageIndex;
import lucene.ParagraphIndex;
import lucene.StanfordNERIndex;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.jetbrains.annotations.NotNull;
import random.GetEntityId;
import random.MakePageMap;
import random.SWATAnnotate;

import java.io.IOException;

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

        if (command.equalsIgnoreCase("--qe-wiki-ent")) {
            qeWikiEnt(args);
        } else if (command.equalsIgnoreCase("--ecn-rel")) {
            ecnRel(args);
        } else if (command.equalsIgnoreCase("--exp4")) {
            exp4(args);
        } else if (command.equalsIgnoreCase("--page-index")) {
            pageIndex(args);
        } else if (command.equalsIgnoreCase("--para-index")) {
            paraIndex(args);
        } else if (command.equalsIgnoreCase("--get-ent-id")) {
            getEntId(args);
        } else if (command.equalsIgnoreCase("--get-rel")) {
            getRel(args);
        }   else if (command.equalsIgnoreCase("--ecn-weighted")) {
            ecnWeighted(args);
        } else if (command.equalsIgnoreCase("--wiki-entity")) {
            wikiEntity(args);
        } else if (command.equalsIgnoreCase("--swat-annotate")) {
            swatAnnotate(args);
        } else if (command.equalsIgnoreCase("--entity-stat")) {
            entityStat(args);
        } else if (command.equalsIgnoreCase("--wiki-terms")) {
            wikiTerms(args);
        } else if (command.equalsIgnoreCase("--stanford-ner-index")) {
            stanfordNER(args);
        } else if (command.equalsIgnoreCase("--qe-rel-ecd-ent")) {
            qeRelEcdEnt(args);
        }  else if (command.equalsIgnoreCase("--make-page-map")) {
            try {
                makePageMap(args);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    private static void qeRelEcdEnt(@NotNull String[] args) {
        Similarity similarity = null;
        Analyzer analyzer = null;
        String s1 = null, s2, s3;

        String indexDir = args[1];
        String mainDir = args[2];
        String outputDir = args[3];
        String dataDir = args[4];
        String relFile = args[5];
        String paraRunFile = args[6];
        String entityRunFile = args[7];
        String entityQrel = args[8];
        int takeKEntities = Integer.parseInt(args[9]);
        int takeKDocs = Integer.parseInt(args[10]);
        boolean omit = args[11].equalsIgnoreCase("yes");
        boolean parallel = args[12].equalsIgnoreCase("true");
        boolean useEcd = args[13].equalsIgnoreCase("true");
        String relType = args[14];
        String a = args[15];
        String sim = args[16];

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
                    lambda = Float.parseFloat(args[17]);
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

        if (useEcd) {
            System.out.println("Using ECD Index");
            s3 = "ecd-index";
        } else {
            System.out.println("Using Paragraph Index");
            s3 = "para-index";
        }
        String outFile = "QERelECDEntities" + "-" + s1 + "-" + s2 + "-" + relType + "-" + s3 +".run";


        new QERelECDEntities(indexDir, mainDir, outputDir, dataDir, relFile, paraRunFile, entityRunFile, outFile,
                entityQrel, takeKEntities, takeKDocs, omit, parallel, useEcd, relType, analyzer, similarity);
    }

    private static void getRel(@NotNull String[] args) {
        String indexDir = args[1];
        String paraRunFilePath = args[2];
        String entityRunFilePath = args[3];
        String entityQrelFilePath = args[4];
        String idFilePath = args[5];
        String outputFilePath = args[6];
        String relType = args[7];
        String type = args[8];
        boolean parallel = args[9].equalsIgnoreCase("true");

        if (type.equalsIgnoreCase("page")) {
            System.out.println("Getting page entity relatedness.");
        } else if(type.equalsIgnoreCase("ecd")) {
            System.out.println("Getting ECD entity relatedness.");
        } else {
            System.err.println("ERROR! WRONG TYPE!");
            System.exit(-1);
        }

        new GetRelatedness(indexDir, paraRunFilePath, entityRunFilePath, entityQrelFilePath, idFilePath,
                outputFilePath, relType, type, parallel);
    }

    private static void makePageMap(@NotNull String[] args) throws IOException {
        String pageCborFile = args[1];
        String outputFile = args[2];
        boolean parallel = args[3].equalsIgnoreCase("true");

        MakePageMap.makeMap(pageCborFile, outputFile, parallel);

    }

    private static void stanfordNER(@NotNull String[] args) {
        String cborFile = args[1];
        String indexDir = args[2];
        String stanfordFile = args[3];
        String a = args[4];
        boolean parallel = args[5].equalsIgnoreCase("true");

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
            StanfordNERIndex.createIndex(cborFile, indexDir, stanfordFile, analyzer, parallel);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void wikiTerms(@NotNull String[] args) {
        Similarity similarity = null;
        Analyzer analyzer = null;

        String pageIndexDir = args[1];
        String paraIndexDir = args[2];
        String mainDir = args[3];
        String dataDir = args[4];
        String outputDir = args[5];
        String paraRunFile = args[6];
        String entityRunFile = args[7];
        String entityQrelFile = args[8];
        String stopWordsFilePath = args[9];
        String p = args[10];
        boolean useProb = args[11].equalsIgnoreCase("true");
        String a = args[12];
        String sim = args[13];

        boolean parallel = false;

        switch (a) {
            case "std" :
                analyzer = new StandardAnalyzer();
                System.out.println("Analyzer: Standard");
                break;
            case "eng":
                analyzer = new EnglishAnalyzer();
                System.out.println("Analyzer: English");
                break;
            default:
                System.out.println("Wrong choice of analyzer! Exiting.");
                System.exit(1);
        }
        switch (sim) {
            case "BM25" :
            case "bm25":
                System.out.println("Similarity: BM25");
                similarity = new BM25Similarity();
                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                try {
                    float lambda = Float.parseFloat(args[14]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("No lambda value for similarity LM-JM.");
                    System.exit(1);
                }
                break;
            case "LMDS":
            case "lmds":
                System.out.println("Similarity: LMDS");
                similarity = new LMDirichletSimilarity();
                break;

            default:
                System.out.println("Wrong choice of similarity! Exiting.");
                System.exit(1);
        }
        String outFile = "WikiTerms.run";

        if (p.equalsIgnoreCase("true")) {
            parallel = true;
        }

        new WikiTerms(pageIndexDir,paraIndexDir, mainDir, dataDir, outputDir, paraRunFile, entityRunFile,
                entityQrelFile, outFile, stopWordsFilePath, parallel, useProb, analyzer, similarity);
    }

    private static void entityStat(@NotNull String[] args) {
        String cbor, passageRunFile;
        boolean parallel;

        String indexDir = args[1];
        String idField = args[2];
        String entityField = args[3];
        String entityPoolFile = args[4];
        String outputFile = args[5];
        String type = args[6];
        if (type.equalsIgnoreCase("corpus")) {
            cbor = args[7];
            passageRunFile = args[8];
            parallel = args[9].equalsIgnoreCase("true");
            new EntityStat(indexDir, idField, entityField, entityPoolFile, outputFile, cbor, passageRunFile, parallel);
        } else if (type.equalsIgnoreCase("run")) {
            passageRunFile = args[7];
            parallel = args[8].equalsIgnoreCase("true");
            new EntityStat(indexDir, idField, entityField, entityPoolFile, outputFile, passageRunFile, parallel);
        }
    }

    private static void swatAnnotate(@NotNull String[] args) {
        boolean cbor = args[1].equalsIgnoreCase("cbor");
        String inFile, outFile, type, indexDir;
        IndexSearcher searcher;
        if (cbor) {
            inFile = args[2];
            outFile = args[3];
            type = args[4];
            new SWATAnnotate(inFile, outFile, type);
        } else {
            indexDir = args[2];
            searcher = new Index.Setup(indexDir).getSearcher();
            inFile = args[3];
            outFile = args[4];
            type = args[5];
            new SWATAnnotate(inFile, outFile, type, searcher);
        }
    }

    private static void wikiEntity(@NotNull String[] args) {

        System.out.println("Performing WikiEntity.");
        Similarity similarity = null;
        Analyzer analyzer = null;

        String paraIndexDir = args[1];
        String pageIndexDir = args[2];
        String mainDir = args[3];
        String outputDir = args[4];
        String dataDir = args[5];
        String relFile = args[6];
        String paraRunFile = args[7];
        String entityRunFile = args[8];
        String entityQrel = args[9];
        String relType = args[10];
        boolean parallel = args[11].equalsIgnoreCase("true");
        String a = args[12];
        String sim = args[13];




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
                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                float lambda;
                try {
                    lambda = Float.parseFloat(args[14]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Missing lambda value for similarity LM-JM");
                    System.exit(1);
                }
                break;
            case "LMDS":
            case "lmds":
                System.out.println("Similarity: LMDS");
                similarity = new LMDirichletSimilarity();
                break;

            default:
                System.out.println("Wrong choice of similarity! Exiting.");
                System.exit(1);
        }
        String outFile = "WikiEntities.run";

        new WikiEntities(paraIndexDir, pageIndexDir, mainDir, outputDir, dataDir, relFile, paraRunFile, entityRunFile,
                outFile, entityQrel, relType, parallel, analyzer, similarity);
    }

    private static void ecnWeighted(@NotNull String[] args) {

        System.out.println("Performing ECNWeighted.");
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

        String outFile = "ECNWeighted-" + s1 + "-" + relType + ".run";
        if (p.equalsIgnoreCase("true")) {
            parallel = true;
        }


        new ECNWeighted(indexDir, mainDir, outputDir, dataDir, paraRunFile, entityRunFile, idFile,
                outFile, entityQrel, relType, parallel, analyzer, similarity);
    }

    private static void getEntId(@NotNull String[] args) {
        String pageIndexDir = args[1];
        String paraIndexDir = args[2];
        String mainDir = args[3];
        String dataDir = args[4];
        String outputDir = args[5];
        String paraRunFile = args[6];
        String entityRunFile = args[7];
        String entityQrelFile = args[8];
        String outputFile = args[9];
        String mode = args[10];
        boolean parallel = args[11].equalsIgnoreCase("true");
        String a = args[12];
        String sim = args[13];

        Analyzer analyzer = null;
        Similarity similarity = null;

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

                break;
            case "LMJM":
            case "lmjm":
                System.out.println("Similarity: LMJM");
                float lambda;
                try {
                    lambda = Float.parseFloat(args[14]);
                    System.out.println("Lambda = " + lambda);
                    similarity = new LMJelinekMercerSimilarity(lambda);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Missing lambda value for similarity LM-JM");
                    System.exit(1);
                }
                break;
            case "LMDS":
            case "lmds":
                System.out.println("Similarity: LMDS");
                similarity = new LMDirichletSimilarity();
                break;

            default:
                System.out.println("Wrong choice of similarity! Exiting.");
                System.exit(1);
        }

        new GetEntityId(pageIndexDir,paraIndexDir, mainDir, dataDir,outputDir, paraRunFile, entityRunFile,
                entityQrelFile, outputFile, mode, parallel, analyzer, similarity);

    }

    private static void paraIndex(@NotNull String[] args) {
        String cborFile = args[1];
        String indexDir = args[2];
        String a = args[3];

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
            ParagraphIndex.createIndex(cborFile, indexDir, analyzer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void pageIndex(@NotNull String[] args) {
        String cborFile = args[1];
        String indexDir = args[2];
        String a = args[3];

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
            PageIndex.createIndex(cborFile, indexDir, analyzer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void qeWikiEnt(@NotNull String[] args) {

        Similarity similarity = null;
        Analyzer analyzer = null;

        String s1 = null, s2, s3;

        String paraIndexDir = args[1];
        String pageIndexDir = args[2];
        String mainDir = args[3];
        String outputDir = args[4];
        String dataDir = args[5];
        String relFile = args[6];
        String paraRunFile = args[7];
        String entityRunFile = args[8];
        String entityQrel = args[9];
        String relType = args[10];
        int takeKEntities = Integer.parseInt(args[11]);
        int takeKDocs = Integer.parseInt(args[12]);
        boolean omit = args[13].equalsIgnoreCase("yes");
        boolean parallel = args[14].equalsIgnoreCase("true");
        boolean useEcdIndex = args[15].equalsIgnoreCase("true");
        String a = args[16];
        String sim = args[17];

        System.out.printf("Using %d entities for query expansion\n", takeKEntities);

        if (useEcdIndex) {
            System.out.println("Using ECD Index");
            s3 = "ecd-index";
        } else {
            System.out.println("Using Paragraph Index");
            s3 = "para-index";
        }

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
                    lambda = Float.parseFloat(args[18]);
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
        String outFile = "QEWikiEntities" + "-" + s1 + "-" + s2 + "-" + relType + "-" + s3 +".run";

        new QEWikiEntities(paraIndexDir, pageIndexDir, mainDir, outputDir, dataDir, relFile, paraRunFile, entityRunFile,
                outFile, entityQrel, relType, takeKEntities, takeKDocs, omit, parallel, useEcdIndex,
                analyzer, similarity);


    }
    private static void ecnRel(@NotNull String[] args) {
        System.out.println("Performing ECNRel");
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

        String outFile = "ECNRel-" + s1 + "-" + relType + ".run";

        if (p.equalsIgnoreCase("true")) {
            parallel = true;
        }


        new ECNRel(indexDir, mainDir, outputDir, dataDir, paraRunFile, entityRunFile, idFile,
                outFile, entityQrel, relType, parallel, analyzer, similarity);

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
        System.out.println("The following options are available: exp1, exp2, qeRelEcdEntities");
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

        System.out.println("--qeRelEcdEntities (indexDir|mainDir|outputDir|dataDir|idFile|relFile|paraRunFile|entityRunFile|outFile|" +
                "entityQrelFilePath|takeKEntities|omitQueryTerms|useFrequency|relType|analyzer|similarity)");

        System.out.println("--exp4 (indexDir|mainDir|outputDir|dataDir|passageRunFile|entityRunFile|idFile, outFile|" +
                "entityQrelFilePath|takeKPassages|relType|analyzer|similarity)");

    }

}
