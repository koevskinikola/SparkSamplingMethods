import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.dstream.RawNetworkReceiver;
import org.apache.spark.streaming.receiver.BlockGenerator;
import org.apache.spark.streaming.receiver.BlockGeneratorListener;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;
import twitter4j.*;
import twitter4j.auth.Authorization;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * Created by nikola on 26.4.16.
 */
public class CustomTwitterReceiver extends Receiver<Status> {
    Authorization twitterAuth;
    String[] filters;
    TwitterStream twitterStream;
    Integer sampleSize;
    Random random;

    final Map<String, Integer> langMap;
    final Map<String, Integer> placeMap;
    final Map<Tuple2<String, String>, Integer> langPlaceMap;
    final Map<Tuple2<String, String>, Float> probabilityMap;

    public CustomTwitterReceiver(Integer sampleSize, String[] filters, StorageLevel storageLevel) {
        super(storageLevel);
        this.filters = filters;
        this.sampleSize = sampleSize;

        langMap = new HashMap<>();
        placeMap = new HashMap<>();
        langPlaceMap = new HashMap<>();
        probabilityMap = new HashMap<>();
        random = new Random();
    }

    @Override
    public void onStart() {
        twitterStream = TwitterStreamFactory.getSingleton();

        twitterStream.addListener(new StatusListener() {
            @Override
            public void onStatus(final Status status) {
                String language = status.getLang();
                String place = status.getPlace().getName();
                Tuple2<String, String> langPlace = new Tuple2<String, String>(language, place);

                if (langMap.containsKey(language))
                    langMap.put(language, langMap.get(language) + 1);
                else
                    langMap.put(language, 1);

                if (placeMap.containsKey(place))
                    placeMap.put(place, placeMap.get(place) + 1);
                else
                    placeMap.put(place, 1);

                if (langPlaceMap.containsKey(langPlace))
                    langPlaceMap.put(langPlace, langPlaceMap.get(langPlace) + 1);
                else
                    langPlaceMap.put(langPlace, 1);

                int groupingLangCount = langMap.size();
                int groupingPlaceCount = placeMap.size();

                float totalMax = 0;

                for (Map.Entry<Tuple2<String, String>, Integer> group: langPlaceMap.entrySet()) {
                    int gCount = group.getValue();
                    Tuple2<String, String> attributes = group.getKey();

                    float sampleSizeLang = gCount;
                    float sampleSizePlace = gCount;

                    for (Map.Entry<String, Integer> langGroup: langMap.entrySet()) {
                        if (langGroup.getKey().equals(attributes._1())) {
                            sampleSizeLang = (sampleSize * gCount * 1.0f) / (groupingLangCount * langGroup.getValue());
                        }
                    }

                    for (Map.Entry<String, Integer> placeGroup: placeMap.entrySet()) {
                        if (placeGroup.getKey().equals(attributes._2())) {
                            sampleSizePlace = (sampleSize * gCount) / (groupingPlaceCount * placeGroup.getValue());
                        }
                    }

                    float sampleSizeMax = Math.max(sampleSizeLang, sampleSizePlace);
                    totalMax += sampleSizeMax;
                    probabilityMap.put(group.getKey(), sampleSizeMax);
                }

                for (Map.Entry<Tuple2<String, String>, Float> group: probabilityMap.entrySet()) {
                    float sampleSizeG = (sampleSize * group.getValue()) / totalMax;
                    float groupProbability = sampleSizeG / langPlaceMap.get(group.getKey());
                    group.setValue(groupProbability);
                };

                float pr = probabilityMap.get(langPlace);
                System.out.println("----------------------------------------------------------");
                System.out.println("(" + language + ", " + place + ") -> Probability: " + pr);
                System.out.println("----------------------------------------------------------");

                float r = random.nextFloat();
                if (r <= pr) {
                    store(status);
                };
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {

            }
        });

        twitterStream.sample();
    }

    @Override
    public void onStop() {
        twitterStream.shutdown();
    }

    private void receive(Status status) {

    }
}
