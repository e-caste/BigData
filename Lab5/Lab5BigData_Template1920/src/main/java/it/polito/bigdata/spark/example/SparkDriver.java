package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		String inputPath = args[0];
		String outputPath = args[1];
		String prefix = args[2];
	
		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);

		// Task 1
		System.out.println("Total number of lines: " + wordFreqRDD.count());
		JavaRDD<String> wordFreqPrefixRDD = wordFreqRDD.filter(x -> x.startsWith(prefix));
		System.out.println("Number of lines beginning with prefix " + prefix + ": " + wordFreqPrefixRDD.count());
		long maxFreq = wordFreqPrefixRDD.map(x -> Integer.parseInt(x.split("\t")[1])).top(1).get(0);
		System.out.println("Maximum frequency of top used word beginning with prefix " + prefix + ": " + maxFreq);

		// Task 2
		JavaRDD<String> wordFreqPrefixTop80RDD = wordFreqPrefixRDD.filter(x -> Integer.parseInt(x.split("\t")[1]) >= 0.8 * maxFreq);
		wordFreqPrefixTop80RDD.map(x -> x.split("\t")[0]).saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
