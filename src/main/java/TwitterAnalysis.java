import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

/**
 * Created by nikola on 25.4.16.
 */
public class TwitterAnalysis {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Error: TwitterAnalysis.jar <receiver_type> <window_interval> <sample_size>");
            System.exit(1);
        };

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Twitter Analysis");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(Integer.valueOf(args[1])));

        // Configuring Twitter credentials
        String consumerKey = "s8FEIf6BRFSRMz2wfKeIiwb5R";
        String consumerSecret = "aZgaZ7Lov2ApAdM5byltJhcA2xjjaf6oc9696r0cjVK2jsdsKu";
        String accessToken = "489365830-WAHpGCSbVw2DR37MQqIwSjdaL9cPWuXtwhriUaGw";
        String accessTokenSecret = "rDG45GEXPbmA7F8jfJcSNRu21CgL2286rZl2CNEVTfaoz";
        Helper.configureTwitterCredentials(consumerKey, consumerSecret, accessToken, accessTokenSecret);

        JavaDStream<Status> tweets = ssc.receiverStream(new CustomTwitterReceiver(Integer.valueOf(args[2]), null,StorageLevels.MEMORY_AND_DISK_2));
        switch (args[0]) {
            case "sample":
                tweets = ssc.receiverStream(new CustomTwitterReceiver(Integer.valueOf(args[2]), null,StorageLevels.MEMORY_AND_DISK_2));
                break;
            case "normal":
                tweets = TwitterUtils.createStream(ssc);
                break;
            default:
                tweets = ssc.receiverStream(new CustomTwitterReceiver(Integer.valueOf(args[2]), null,StorageLevels.MEMORY_AND_DISK_2));
                break;
        }

        JavaDStream totalCount = tweets.count();
        JavaDStream spanishCount = tweets.filter(new Function<Status, Boolean>() {
            @Override
            public Boolean call(Status status) throws Exception {
                return (status.getLang().equals("es"));
            }
        }).count();

        ssc.start();
        ssc.awaitTermination();

    }
}
