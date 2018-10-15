package hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	private static final Pattern DISREGARD = Pattern.compile("[(){},.;!+\"?<>%]");
	private final static IntWritable one = new IntWritable(1);
	private static String elements[] = { "education", "politics", "sports", "agriculture" }; 
	private static HashSet<String> uniqueWord = new HashSet<String>(Arrays.asList(elements));


	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {

		private Text word = new Text();
		private Text filenameKey;
		@Override

		protected void setup(Context context) throws IOException,
		InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new Text(path.toString());
		}

		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				value.set(tokenizer.nextToken());
				String cleanWord = DISREGARD.matcher(word.toString()).replaceAll("");
				if(uniqueWord.contains(cleanWord)) {
					context.write(new Text(cleanWord), one);
				}  

			}
		}
	}

	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {

			int sum=0;
			for(IntWritable x: values)
			{
				sum+=x.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}


	public static void main(String[] args) throws IOException {

		System.out.println("Hello World");
		Configuration conf= new Configuration();
		Job job = new Job(conf,"My Word Count Program");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		Path outputPath = new Path(args[1]);

		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

	}

}
