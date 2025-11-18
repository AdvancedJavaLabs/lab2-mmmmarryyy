package util;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TextProcessor {
    private static final Pattern WORD = Pattern.compile("\\b[\\p{L}']+\\b");
    private static final Pattern NAME_CANDIDATE = Pattern.compile("\\b([A-Z][a-z]+)\\b");

    private static final Set<String> POS = Set.of(
            "good", "excellent", "happy", "joy", "love", "like", "positive", "great", "awesome",
            "amazing", "fantastic", "wonderful", "brilliant", "fabulous", "marvelous", "perfect", "ideal", "outstanding",
            "superb", "incredible", "phenomenal", "magnificent", "splendid", "terrific", "stellar", "exceptional",
            "satisfied", "pleased", "content", "delighted", "gratified", "nice", "pleasant", "agreeable",
            "adore", "cherish", "treasure", "fond", "affection", "passion", "devotion", "infatuation",
            "success", "triumph", "victory", "achievement", "accomplishment", "progress", "breakthrough",
            "win", "winner", "champion", "masterpiece", "best", "top", "prime", "peak",
            "beautiful", "pretty", "lovely", "gorgeous", "stunning", "attractive", "handsome", "cute",
            "interesting", "fascinating", "captivating", "engaging", "enthralling", "absorbing", "gripping",
            "grateful", "thankful", "appreciative", "thanks", "appreciation", "gratitude",
            "inspired", "motivated", "empowered", "encouraged", "uplifted", "energized", "determined",
            "kind", "kindhearted", "generous", "benevolent", "compassionate", "thoughtful", "considerate",
            "strong", "powerful", "robust", "reliable", "dependable", "trustworthy", "solid", "secure"
    );
    private static final Set<String> NEG = Set.of(
            "bad", "sad", "angry", "hate", "terrible", "negative", "worse", "awful", "horrible",
            "mad", "furious", "enraged", "irritated", "annoyed", "aggravated", "frustrated", "resentful",
            "unhappy", "depressed", "miserable", "sorrowful", "gloomy", "melancholy", "heartbroken", "devastated",
            "despondent", "disheartened", "downcast", "forlorn", "dismal", "bleak", "grim",
            "afraid", "scared", "frightened", "terrified", "panicked", "anxious", "worried", "nervous",
            "disgust", "disgusting", "revolting", "repulsive", "nauseating", "sickening", "contempt", "despise",
            "disappointed", "displeased", "dissatisfied", "discontent", "disenchanted", "disillusioned", "letdown",
            "ashamed", "guilty", "embarrassed", "humiliated", "mortified", "remorseful", "regretful",
            "pain", "painful", "hurt", "suffering", "agony", "anguish", "torment", "torture", "misery",
            "stressed", "overwhelmed", "burdened", "pressured", "tense", "strained", "frazzled", "burnedout",
            "lonely", "alone", "isolated", "abandoned", "forsaken", "rejected", "excluded", "alienated",
            "hopeless", "desperate", "despair", "helpless", "powerless", "defeated", "crushed",
            "difficult", "hard", "challenging", "complicated", "complex", "problematic", "troublesome",
            "wrong", "incorrect", "mistake", "error", "fault", "flaw", "defect", "failure", "defeat", "loss",
            "ugly", "unattractive", "hideous", "grotesque", "repellent", "unsightly", "unpleasant",
            "weak", "feeble", "fragile", "brittle", "flimsy", "unreliable", "untrustworthy", "shaky", "unstable"
    );

    public static long wordCount(String text) {
        Matcher matcher = WORD.matcher(text);
        long counter = 0;

        while (matcher.find()) {
            counter++;
        }

        return counter;
    }

    public static Map<String,Long> topCounts(String text) {
        Matcher matcher = WORD.matcher(text.toLowerCase());
        Map<String, Long> wordCounter = new HashMap<>();

        while (matcher.find()) {
            String word = matcher.group();
            wordCounter.merge(word, 1L, Long::sum);
        }

        return wordCounter.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a,b)->a, HashMap::new));
    }

    public static long[] lexiconSentiment(String text) {
        Matcher matcher = WORD.matcher(text.toLowerCase());
        long positive = 0, negative = 0;

        while (matcher.find()) {
            String w = matcher.group();

            if (POS.contains(w)) {
                positive++;
            }
            if (NEG.contains(w)) {
                negative++;
            }
        }

        return new long[]{positive, negative};
    }

    public static double lexiconSentiment(long positive, long negative) {
        if (positive + negative == 0) {
            return 0.0;
        }

        return 1.0 * (positive - negative) / (positive + negative);
    }

    public static String replaceNames(String text, String placeholder) {
        Matcher matcher = NAME_CANDIDATE.matcher(text);
        StringBuilder stringBuilder = new StringBuilder();

        while (matcher.find()) {
            matcher.appendReplacement(stringBuilder, placeholder);
        }

        matcher.appendTail(stringBuilder);
        return stringBuilder.toString();
    }

    public static List<String> sortSentencesByLength(String text) {
        String[] sentences = text.split("(?<=[.!?])\\s+");

        return Arrays.stream(sentences)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .sorted(Comparator.comparingInt(String::length))
                .collect(Collectors.toList());
    }
}
