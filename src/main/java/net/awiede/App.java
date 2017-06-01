package net.awiede;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

/**
 * <a href="https://spark.apache.org/docs/1.6.2/streaming-programming-guide.html">Spark Programming guide</a>
 * 
 * <a href="https://snap.stanford.edu/data/">Stanford SNAP data</a>
 * 
 * @author andreas.s.wiede
 *
 */
public class App 
{
	
	private static boolean filterByCountry(Status status) {
        if (status.getPlace() != null) {
            return true;
        } else {
            return false;
        }		
	}
	
    public static void main( String[] args )
    {

    	System.setProperty("twitter4j.oauth.consumerKey", System.getProperty("CONSUMER_KEY"));
        System.setProperty("twitter4j.oauth.consumerSecret", System.getProperty("CONSUMER_SECRET"));
        System.setProperty("twitter4j.oauth.accessToken", System.getProperty("ACCESS_TOKEN"));
        System.setProperty("twitter4j.oauth.accessTokenSecret", System.getProperty("ACCESS_TOKEN_SECRET"));
        Logger.getRootLogger().setLevel(Level.ERROR);
    	
        SparkConf conf = new SparkConf().setAppName("awiede-twitter-stream").setMaster("local[2]");
        
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
        
        JavaDStream<Status> twitterStream = TwitterUtils.createStream(jssc);
        
//        JavaDStream<String> statuses = twitterStream.map(new Function<Status, String>() {
//
//			private static final long serialVersionUID = -5732802589701744031L;
//
//			public String call(Status status) throws Exception {
//				return status.getText();
//			}
//		});
        
	      JavaDStream<Status> tweetsForLocation = twitterStream.filter(
	                new Function<Status, Boolean>() {

	                	private static final long serialVersionUID = -4258850438617506640L;

						public Boolean call(Status status){
	                        return filterByCountry(status);
	                    }
	                }
	        );
        
	      tweetsForLocation.foreach(new Function<JavaRDD<Status>, Void>() {

				private static final long serialVersionUID = -8516662759985290176L;

				public Void call(JavaRDD<Status> rdd) throws Exception {
					
					List<Status> results = rdd.collect();
					for (Status result : results) {
						String countryCode = (result.getPlace() == null) ? "N/A" : result.getPlace().getCountryCode();
						System.out.println("Country: "+countryCode+" Text: "+result.getText());
					}
					
					return null;
				}
			});
	      
        jssc.start();
        
    }
}
