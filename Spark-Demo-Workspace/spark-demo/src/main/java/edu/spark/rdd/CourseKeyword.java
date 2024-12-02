package edu.spark.rdd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/*
 * Take the subtitles for a course
 * Find top 10 keywords for the course by finding the most frequent words used
 * Filter out filler words
 */
public class CourseKeyword {
	public static void sparkProgram()
	{
	Logger.getLogger("org.apache").setLevel(Level.WARN);
	JavaSparkContext context = null; 
		try
		{
		SparkConf conf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");
		context = new JavaSparkContext(conf);
		process(context);
		}
		finally
		{
		context.close();
		}
	}
	
	public static void process(JavaSparkContext context)
	{
		JavaRDD<String> inputRdd = context.textFile("src\\main\\resources\\subtitles.txt");
		Set<String> nonKeyWords = loadNonKeyWords();
		
		inputRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
				.map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase()) //Remove all non characters and convert to smaller case
				.filter(word -> word.trim().length() > 1)
				.filter(word -> !nonKeyWords.contains(word))
				.mapToPair(word -> new Tuple2<String, Integer>(word,1))
				.reduceByKey((cnt1,cnt2) -> cnt1+cnt2)
				.mapToPair(t -> new Tuple2<Integer,String>(t._2,t._1))
				.sortByKey(false)
				.take(10)
				.forEach(System.out::println);
	}
	
	public static Set<String> loadNonKeyWords()
	{
		BufferedReader br;
		Set<String> set = new HashSet<>();
		try {
			br = new BufferedReader(new FileReader(new File("src\\main\\resources\\nonkeywords.txt")));
			br.lines().forEach(set::add);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return set;
	}
	
	public static void main(String[] args) {
		sparkProgram();
	}
	
	
}


