import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

/**
 * Created by nikola on 6.5.16.
 */
public class TextFileSampling {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Error: TwitterAnalysis.jar <receiver_type> <window_interval> <sample_size> <file_directory>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Text File Sampling");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(Integer.valueOf(args[1])));

        JavaDStream<String> textFile;
        switch (args[0]) {
            case "sample":
//                textFile = ssc.receiverStream(new CustomTwitterReceiver(Integer.valueOf(args[2]), null, StorageLevels.MEMORY_AND_DISK_2));
                break;
            case "normal":
                textFile = ssc.textFileStream(args[3]);
                break;
            default:
//                textFile = ssc.receiverStream(new CustomTwitterReceiver(Integer.valueOf(args[2]), null,StorageLevels.MEMORY_AND_DISK_2));
                break;
        }
    }
}
