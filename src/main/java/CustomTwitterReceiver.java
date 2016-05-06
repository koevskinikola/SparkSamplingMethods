import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.dstream.RawNetworkReceiver;
import org.apache.spark.streaming.receiver.BlockGenerator;
import org.apache.spark.streaming.receiver.BlockGeneratorListener;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;
import twitter4j.*;
import twitter4j.auth.Authorization;

import java.util.*;

/**
 * Created by nikola on 26.4.16.
 */
public class CustomTwitterReceiver extends Receiver<Status> {
    Authorization twitterAuth;
    String[] filters;
    List<String> groupList;
    TwitterStream twitterStream;
    Integer sampleSize;
    Random random;
    TwitterCongressionalSample tcSampler;

    final Map<String, Integer> langMap;
    final Map<String, Integer> retweetMap;
    final Map<Tuple2<String, String>, Integer> langPlaceMap;
    final Map<Tuple2<String, String>, Float> probabilityMap;

    public CustomTwitterReceiver(Integer sampleSize, String[] filters, StorageLevel storageLevel) {
        super(storageLevel);
        this.filters = filters;
        this.sampleSize = sampleSize;

        langMap = new HashMap<>();
        retweetMap = new HashMap<>();
        langPlaceMap = new HashMap<>();
        probabilityMap = new HashMap<>();
        random = new Random();
        groupList = new ArrayList<>();
        groupList.add("favourite");
        groupList.add("retweet");
        tcSampler = new TwitterCongressionalSample(sampleSize, groupList);
    }

    @Override
    public void onStart() {
        twitterStream = TwitterStreamFactory.getSingleton();

        twitterStream.addListener(new StatusListener() {
            @Override
            public void onStatus(final Status status) {

                if (tcSampler.getCount() >= sampleSize * 2) {
//                    new Thread()  {
//                        @Override public void run() {
//                            List<Status> sampleList = tcSampler.getSample();
//                            System.out.println("Sample size: " + sampleList.size());
//                            for (Status item:sampleList) {
//                                store(item);
//                            }
//                            tcSampler = new TwitterCongressionalSample(sampleSize, groupList);
//                        }
//                    }.start();

                    List<Status> sampleList = tcSampler.getSample();
                    System.out.println("Sample size: " + sampleList.size());
                    for (Status item:sampleList) {
                        store(item);
                    }
                    tcSampler = new TwitterCongressionalSample(sampleSize, groupList);

                } else {
                    tcSampler.singleSample(status);
                }
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
