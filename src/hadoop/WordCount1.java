package hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount1 {

	private static final String INTERMEDIATE = "/intermediate";

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text file = new Text();
		private List<String> words = Arrays.asList("education", "politics", "sports", "agriculture");

		@Override
		protected void setup(Context context)throws IOException, InterruptedException{
			file.set(((FileSplit) context.getInputSplit()).getPath().toString());
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				if (words.stream().anyMatch(word.toString()::contains)) {
					context.write(file, word);
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			HashMap<String, Integer> wordCountMap = new HashMap<String, Integer>();
			values.forEach(value ->{ 
				String currWord = value.toString();
				if (!wordCountMap.containsKey(currWord)) {
					wordCountMap.put(currWord, 1);
				} else {
					wordCountMap.put(currWord, wordCountMap.get(currWord) + 1);
				}});

			int max = 0;
			String mostFreqWord = null;
			for (String keyWord : wordCountMap.keySet()) {
				if (wordCountMap.get(keyWord) > max) {
					max = wordCountMap.get(keyWord);
					mostFreqWord = keyWord;
				}
			}
			context.write(key, new Text(mostFreqWord));
		}
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				context.write(new Text(itr.nextToken()), new IntWritable(1));
			}
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Job 1");
		job.setJarByClass(WordCount.class);
		//job.setJar("WordCount.jar");
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(INTERMEDIATE));
		job.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "Job 2");
		job2.setJarByClass(WordCount.class);
		//job2.setJar("WordCount.jar");
		job2.setMapperClass(MyMapper.class);
		job2.setCombinerClass(MyReducer.class);
		job2.setReducerClass(MyReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(INTERMEDIATE));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
