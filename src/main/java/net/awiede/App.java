package net.awiede;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	String consumerKey = "eN9SQUWIl7NFuUrsEfDydDa7r";
    	String consumerSecret = "qDRhCH7K58nNUlLHc2chL6G8pdzRrJoc5BDQKCV54ycqxqOxui";
    	String accessToken = "456113064-dLRihnwCgwTSLCbVQkpOG5CwXO6yyK6RP7GDHqTj";
    	String accessTokenSecret = "0sQ9sKnDdjW2Cxlnr46hRmgwmmKuu7Oc4QYfOkttIXmIJ";
    	
    	String[] filters = new String[] {consumerKey, consumerSecret, accessToken, accessTokenSecret};
    	
        SparkConf conf = new SparkConf().setAppName("awiede-twitter-stream").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        
        JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(jssc, filters);
    }
}
