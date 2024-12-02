package edu.spark.rdd;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/*
 * if user views more than 90% of course , course gets 10 points
 * if user views more than 50% of course , course gets 4 points
 * if user views more than 25% of course , course gets 2 points 
 */
public class CourseRankByViews {
	
	public static void sparkProgram()
	{
	Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(conf);
		process(context);
		context.close();
	}
	
	private static void process(JavaSparkContext context)
	{
		//Tuple(userId,ChapterId)
		JavaPairRDD<Integer, Integer> chapterViewdata = getChapterViewData(context);
		//Tuple(ChapterId,CourseId)
		JavaPairRDD<Integer, Integer> chapterCoursedata = getChapterCourseData(context);
		//Tuple(CourseId,CourseTitle)
		JavaPairRDD<Integer, String> titledata = getCourseTitleData(context);
		
		//Tuple(courseId,ChapterCount)
		JavaPairRDD<Integer, Long> courseChapterCount = chapterCoursedata.mapToPair(
				t -> {return new Tuple2<>(t._2,1L);}).reduceByKey((val1,val2) -> val1 + val2);
		
		// Step 1 - remove any duplicated views
		chapterViewdata = chapterViewdata.distinct();
		
		//Convert to tuple(chapterId,userId)
		chapterViewdata = chapterViewdata.mapToPair(t -> {return new Tuple2<>(t._2,t._1);});
		
		//After join tuple(chapterId, Tuple(userId,courseId))
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> chapterCourseViewData = chapterViewdata.join(chapterCoursedata);
		 
		 //Map to Tuple(Tuple(userId,courseId),1L)
		 JavaPairRDD<Tuple2<Integer,Integer>,Long> userCourseViewData = chapterCourseViewData.mapToPair(t -> {
			 Integer user = t._2._1;
			 Integer course = t._2._2;
			 return new Tuple2<>(new Tuple2<>(user,course),1L);
		 });
		 
		 //Map to Tuple(Tuple(userId,courseId),view Count)
		 JavaPairRDD<Tuple2<Integer, Integer>, Long> userCourseAggViewData = userCourseViewData.reduceByKey((val1,val2) -> val1+val2);
		 
		//Map to Tuple(courseId,view Count)
		 JavaPairRDD<Integer, Long> courseonlyView = userCourseAggViewData.mapToPair(t -> {
			 return new Tuple2<>(t._1._2,t._2);
		 });
		 
		//Map to Tuple(courseId,tuple(user view Count,chapter count))
		 JavaPairRDD<Integer, Tuple2<Long, Long>> courseViewChapterCount = courseonlyView.join(courseChapterCount);
		
		//Map to Tuple(courseId,user view Count / chapter count))
		 JavaPairRDD<Integer, Double> courseViewPercentage = courseViewChapterCount.mapValues(t -> (double)t._1/t._2);
		 
		//Map to Tuple(courseId,view weightage))
		 JavaPairRDD<Integer, Long> courseViewPoints = courseViewPercentage.mapValues(t -> {
				if (t > 0.9) return 10L;
				if (t > 0.5) return 4L;
				if (t > 0.25) return 2L;
				return 0L;
			});
		 
		 //reduce view weightage cumulative by course
		 JavaPairRDD<Integer, Long> courseViewPointsAgg = courseViewPoints.reduceByKey((val1,val2) -> val1 + val2);
		 
		 //Map (Courseid, Tuple(Sum View Weightage, Course title))
		 JavaPairRDD<Integer, Tuple2<Long, String>> courseViewPointsAggWithTitle = courseViewPointsAgg.join(titledata);
		 
		 //Tuple(Sum View Weightage, Course title)
		 JavaPairRDD<Long, String> aggViewCouurse = courseViewPointsAggWithTitle.mapToPair(t -> {
			 return t._2;
		 });
		 
		 //Sort By Sum View Weightage descending and print top 5
		 aggViewCouurse.sortByKey(false).take(5).forEach(System.out::println);
		
		
	}
	
/*
 * Contains view data for each course
 * Contains userId,chapterId (user viewed chapter)
 * returns Tuple(userid,ChapterId)
 */
private static JavaPairRDD<Integer, Integer> getChapterViewData(JavaSparkContext context)
{
	JavaRDD<String> lines = context.textFile("src\\main\\resources\\views-*.csv");
	return lines.mapToPair(s -> {
		String[] split = s.split(",");
		return new Tuple2<Integer,Integer>(new Integer(split[0]),new Integer(split[1]));
	});
}

/*
 * which chapter belongs to which course
 * Returns tuple(ChapterId, courseId)  
 */
private static JavaPairRDD<Integer, Integer> getChapterCourseData(JavaSparkContext context)
{
	JavaRDD<String> lines = context.textFile("src\\main\\resources\\chapters.csv");
	return lines.mapToPair(s -> {
		String[] split = s.split(",");
		return new Tuple2<Integer,Integer>(new Integer(split[0]),new Integer(split[1]));
	});
}

/*
 * return Tuple(courseId,CourseTitle)
 */
private static JavaPairRDD<Integer, String> getCourseTitleData(JavaSparkContext context)
{
	JavaRDD<String> lines = context.textFile("src\\main\\resources\\titles.csv");
	return lines.mapToPair(s -> {
		String[] split = s.split(",");
		return new Tuple2<Integer,String>(new Integer(split[0]),split[1]);
	});
}

public static void main(String[] args) {
	sparkProgram();
}



}
