import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import twitter4j.Status;

/**
 * Created by nikola on 25.4.16.
 */
public class TwitterAnalysis {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Error: TwitterAnalysis.jar <window_interval> <sample_size>");
            System.exit(1);
        };

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Twitter Analysis");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(Integer.valueOf(args[0])));

        // Configuring Twitter credentials
        String consumerKey = "s8FEIf6BRFSRMz2wfKeIiwb5R";
        String consumerSecret = "aZgaZ7Lov2ApAdM5byltJhcA2xjjaf6oc9696r0cjVK2jsdsKu";
        String accessToken = "489365830-WAHpGCSbVw2DR37MQqIwSjdaL9cPWuXtwhriUaGw";
        String accessTokenSecret = "rDG45GEXPbmA7F8jfJcSNRu21CgL2286rZl2CNEVTfaoz";

        Helper.configureTwitterCredentials(consumerKey, consumerSecret, accessToken, accessTokenSecret);


        JavaDStream<Status> tweets = ssc.receiverStream(new CustomTwitterReceiver(Integer.valueOf(args[1]), null,StorageLevels.MEMORY_AND_DISK_2));
        tweets.map(new Function<Status, String>() {
            @Override
            public String call(Status status) throws Exception {
                return status.getText();
            }
        }).print();
//        JavaDStream<Status> tweets = TwitterUtils.createStream(ssc);

//        //For Attribute1
//
//        final JavaPairDStream<String, Integer> groupingTweets = tweets.flatMapToPair(new PairFlatMapFunction<Status, String, Integer>() {
//            @Override
//            public Iterable<Tuple2<String, Integer>> call(Status status) throws Exception {
//
//                return Arrays.asList(new Tuple2<String, Integer>(status.getLang()))
//            }
//        })
//
//        //Get the number of tuples per Value
//        final JavaPairDStream<String, Integer> groupingLangTweets = tweets.mapToPair(new PairFunction<Status, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(Status status) throws Exception {
//                return new Tuple2<String, Integer>(status.getLang(), 1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        });
//        //Get the number of groups per Attribute
//        final JavaDStream groupingLangCount = groupingLangTweets.count();
//
//        groupingLangTweets.print();
//
//        //For Attribute2
//
//        //Get the number of tuples per Value
//        final JavaPairDStream<String, Integer> groupingPlaceTweets = tweets.mapToPair(new PairFunction<Status, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(Status status) throws Exception {
//                return new Tuple2<String, Integer>(status.getPlace().getName(), 1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        });
//        //Get the number of groups per Attribute
//        JavaDStream groupingPlaceCount = groupingPlaceTweets.count();
//
//        //Get the number of tuples per Group
//        final JavaPairDStream<Tuple2<String, String>, Integer> groupTweets = tweets.mapToPair(new PairFunction<Status, Tuple2<String, String>, Integer>() {
//            @Override
//            public Tuple2<Tuple2<String, String>, Integer> call(Status status) throws Exception {
//                return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(status.getLang(), status.getPlace().getName()), 1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        });
//        JavaDStream<Long> groupCount = groupTweets.count();
//
//        groupingPlaceTweets.print();



//        groupCount.foreachRDD(new Function<JavaRDD<Long>, Void>() {
//            @Override
//            public Void call(JavaRDD<Long> rdd) {
//                final long groupsCount = rdd.count();
//
//                final JavaPairDStream<String, Integer> sampleSizeLang = groupTweets.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>() {
//                    @Override
//                    public Tuple2<String, Integer> call(Tuple2<Tuple2<String, String>, Integer> group) throws Exception {
//                        return new Tuple2<String, Integer>(group._1()._1(), group._2());
//                    }
//                }).join(groupingLangTweets).mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Integer>() {
//                    @Override
//                    public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Integer>> langGroup) throws Exception {
//                        return new Tuple2<String, Integer>(langGroup._1(), ((sampleSize * langGroup._2()._1()) / ((int)groupsCount * langGroup._2()._2())));
//                    }
//                });
//
//                sampleSizeLang.print();
//
//                JavaPairDStream<String, Integer> sampleSizePlace = groupTweets.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>() {
//                    @Override
//                    public Tuple2<String, Integer> call(Tuple2<Tuple2<String, String>, Integer> group) throws Exception {
//                        return new Tuple2<String, Integer>(group._1()._2(), group._2());
//                    }
//                }).join(groupingPlaceTweets).mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Integer>() {
//                    @Override
//                    public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Integer>> langGroup) throws Exception {
//                        return new Tuple2<String, Integer>(langGroup._1(), ((sampleSize * langGroup._2()._1()) / ((int)groupsCount * langGroup._2()._2())));
//                    }
//                });
//
//                sampleSizePlace.print();
//
//                return null;
//            };
//        });

        ssc.start();
        ssc.awaitTermination();

    }
}
