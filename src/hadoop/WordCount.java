package hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount {
	private static Set<String> uniqueWord;
	//Mapper class 
	   public static class Map extends MapReduceBase implements 
	   Mapper<LongWritable ,/*Input key Type */ 
	   Text,                /*Input value Type*/ 
	   Text,                /*Output key Type*/ 
	   IntWritable>        /*Output value Type*/ 
	   { 
	      
	      //Map function 
	      public void map(LongWritable key, Text value, 
	      OutputCollector<Text, IntWritable> output,   
	      Reporter reporter) throws IOException 
	      { 
	          
	      } 
	   } 
	   
	   
	   //Reducer class 
	   public static class Reduce extends MapReduceBase implements 
	   Reducer< Text, IntWritable, Text, IntWritable > 
	   {  
	   
	      //Reduce function 
	      public void reduce( Text key, Iterator <IntWritable> values, 
	         OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
	         { 
	            
	 
	         } 
	   }  

	public static void main(String[] args) throws IOException {
		uniqueWord = new HashSet<>();
		uniqueWord.add("education");
		uniqueWord.add("politics");
		uniqueWord.add("sports");
		uniqueWord.add("agriculture");
		
		System.out.println("Hello World");
		JobConf conf = new JobConf(WordCount.class); 

		conf.setJobName("max_eletricityunits"); 
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class); 
		conf.setMapperClass(Map.class); 
		conf.setCombinerClass(Reduce.class); 
		conf.setReducerClass(Reduce.class); 
		conf.setInputFormat(TextInputFormat.class); 
		conf.setOutputFormat(TextOutputFormat.class); 

		FileInputFormat.setInputPaths(conf, new Path(args[0])); 
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); 

		JobClient.runJob(conf); 

	}

}
