package experiments.baselines;


import org.jetbrains.annotations.NotNull;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Various entity based support passage ranking methods described in Blanco et al.
 * @author Shubham Chatterjee
 * @version 6/30/2020
 */

public class EntityRanking {
    private final static int paragraphCorpusSize = 29794697; // Number of paragraphs in the TREC-CAR corpus

    /**
     * Rank by KL-Divergence.
     * @param runStats Map of entity to number of topK query-relevant passages mentioning the entity.
     * @param corpusStats Map of entity to number of passages in entire corpus mentioning the entity.
     * @param entityPool Pool of all candidate entities.
     * @param topKPassages Number of topK query-relevant passages.
     * @return Distribution over entities by KLD.
     */

    @NotNull
    public static Map<String, Double> rankByKLD(Map<String, Integer> runStats,
                                                Map<String, Integer> corpusStats,
                                                @NotNull Set<String> entityPool,
                                                int topKPassages) {

        Map<String, Double> distribution = new HashMap<>();

        for (String entity : entityPool) {

            double probabilityOfEntityGivenQueryModel = findProbabilityOfEntityGivenQueryModel(entity, runStats, topKPassages);

            if (probabilityOfEntityGivenQueryModel == 0.0d) {
                distribution.put(entity, 0.0d);
            } else {

                double probabilityOfEntityGivenPassageModel = findProbabilityOfEntityGivenPassageModel(entity,
                        corpusStats);

                double entityScore = probabilityOfEntityGivenQueryModel *
                        Math.log(probabilityOfEntityGivenQueryModel / probabilityOfEntityGivenPassageModel);

                distribution.put(entity, entityScore);
            }
        }

        return distribution;
    }

    private static double findProbabilityOfEntityGivenPassageModel(String entity,
                                                                   @NotNull Map<String, Integer> entityStatMap) {
        int numOfPassagesContainingEntity = entityStatMap.getOrDefault(entity, 0);
        return ((double) numOfPassagesContainingEntity / paragraphCorpusSize);
    }

    private static double findProbabilityOfEntityGivenQueryModel(String entity,
                                                                 @NotNull Map<String, Integer> runStats,
                                                                 int topKPassages) {
        int stat = runStats.getOrDefault(entity, 0);
        return  (double) stat / topKPassages;

    }

    /**
     * Rank by rarity.
     * @param entityStatMap Map of entity to number of passages in entire corpus mentioning the entity.
     * @param entityPool Pool of all candidate entities.
     * @return Distribution over entities by rarity.
     */

    @NotNull
    public static Map<String, Double> rankByRarity(@NotNull Map<String, Integer> entityStatMap,
                                                   @NotNull Set<String> entityPool) {

        Map<String, Double> distribution = new HashMap<>();
        int numOfPassagesContainingEntity;


        for (String entity : entityPool) {
            numOfPassagesContainingEntity = entityStatMap.getOrDefault(entity, 0);

            double entityScore = numOfPassagesContainingEntity == 0
                    ? 0.0d
                    :(double) paragraphCorpusSize / numOfPassagesContainingEntity;
            distribution.put(entity, entityScore);
        }

        return distribution;
    }

    /**
     * Rank bt frequency.
     * @param runStats Map of entity to number of topK query-relevant passages mentioning the entity.
     * @param entityPool Pool of all candidate entities.
     * @return Distribution over entities by frequency.
     */

    @NotNull
    public static Map<String, Double> rankByFrequency(Map<String, Integer> runStats, @NotNull Set<String> entityPool) {
        Map<String, Integer> ranking = new HashMap<>();

        for (String entity : entityPool) {
            if (runStats.containsKey(entity)) {
                ranking.put(entity, runStats.get(entity));
            } else {
                runStats.put(entity, 0);
            }
        }

        return toDistribution(ranking);
    }

    @NotNull
    private static Map<String, Double> toDistribution (@NotNull Map<String, Integer> freqMap) {
        Map<String, Double> dist = new HashMap<>();
        DecimalFormat df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);

        // Calculate the normalizer
        int norm = 0;
        for (int val : freqMap.values()) {
            norm += val;
        }

        // Normalize the map
        for (String word: freqMap.keySet()) {
            int freq = freqMap.get(word);
            double normFreq = (double) freq / norm;
            normFreq = Double.parseDouble(df.format(normFreq));
            dist.put(word, Math.log(normFreq));
        }
        return dist;
    }

    /**
     * Rank by combination of frequency and rarity.
     * @param runStats Map of entity to number of topK query-relevant passages mentioning the entity.
     * @param corpusStats Map of entity to number of passages in entire corpus mentioning the entity.
     * @param entityPool Pool of all candidate entities.
     * @return Distribution over entities by combination.
     */

    @NotNull
    public static Map<String, Double> rankByComb(Map<String, Integer> runStats,
                                                 Map<String, Integer> corpusStats,
                                                 Set<String> entityPool) {

        Map<String, Double> distribution = new HashMap<>();
        Map<String, Double> rankByFreqMap = rankByFrequency(runStats, entityPool);
        Map<String, Double> rankByRarityMap = rankByRarity(corpusStats,entityPool);

        for (String entity: entityPool) {
            if (rankByFreqMap.containsKey(entity) && rankByRarityMap.containsKey(entity)) {
                double s1 = Math.exp(rankByFreqMap.get(entity));
                double s2 = rankByRarityMap.get(entity);
                double entityScore = s1 * s2;
                distribution.put(entity, entityScore);
            }
        }

        return distribution;
    }
}
