package hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
* @author  krithika
* Counts the number of occurences of the words 
* education
* agriculture
* politics
* sports
* in all of the 50 wikipedia state web pages
*/

public class WordCount2 extends Configured
implements Tool {

  private static final Pattern UNDESIRABLES = Pattern.compile("[(){},.;!+\"?<>%]");

  public static class WCMapper 
    extends Mapper<Object, BytesWritable , Text, IntWritable>{

      private Text filenameKey;
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
      private String elements[] = { "education", "politics", "sports", "agriculture" }; 
      private HashSet<String> dict = new HashSet<String>(Arrays.asList(elements));

      @Override

      protected void setup(Context context) throws IOException,
      InterruptedException {
          InputSplit split = context.getInputSplit();
          Path path = ((FileSplit) split).getPath();
          filenameKey = new Text(path.toString());
      }      

      public void map(Object key, BytesWritable value, Context context
      ) throws IOException, InterruptedException {
          byte[] bytes = value.getBytes();
          String str = new String(bytes);
          //default parsing - /n /t carriage return are the delimiters set
          StringTokenizer itr = new StringTokenizer(str.toLowerCase());
          while (itr.hasMoreTokens()) {
              word.set(itr.nextToken());
              //remove unwanted characters 
              //to count words that end or start with special character
              String cleanWord = UNDESIRABLES.matcher(word.toString()).replaceAll("");
              if(dict.contains(cleanWord)) {
                context.write(new Text(cleanWord), one);
              }      
          }
      }
  }

  public static class WCReducer 
  extends Reducer<Text,IntWritable,Text,IntWritable> {
      private IntWritable result = new IntWritable();

      public void reduce(Text key, Iterable<IntWritable> values, 
        Context context
        ) throws IOException, InterruptedException {
          int sum = 0;
          for (IntWritable val : values) {
              sum += val.get();
          }
          result.set(sum);
          context.write(key, result);
      }
  }


  public int run(String[] args) throws Exception {

      // Configuration processed by ToolRunner
      Configuration conf = getConf();

      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length != 2) {
          System.err.println("Usage: wordcount <in> <out>");
          System.exit(2);
      }
      Job job = new Job(conf, "word count");
      if (job == null) {
          return -1;
      }

      job.setJarByClass(WordCount.class);
      //job.setInputFormatClass(WholeFileInputFormat.class);
      job.setMapperClass(WCMapper.class);
      job.setReducerClass(WCReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
      return job.waitForCompletion(true) ? 0 : 1;

  }

  public static void main(String[] args) throws Exception {
      int exitCode = ToolRunner.run(new WordCount(), args);
      System.exit(exitCode);
  }

}


