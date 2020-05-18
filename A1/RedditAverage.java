import java.io.IOException;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;


public class RedditAverage extends Configured implements Tool {

	public static class AssignMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private Text word = new Text();
		private LongPairWritable pair = new LongPairWritable();
		//@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			JSONObject record = new JSONObject(value.toString());	//JSON Object creation
			word.set((String) record.get("subreddit"));	//Subreddit values are set to word
			pair.set((long) 1,(Integer) record.get("score"));	//LongWritablePair set to 1 and value of score
			context.write(word,pair);	//Mapper output is subreddit and 1,score
		}
	}
	public static class Combiner extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		private LongPairWritable pair1 = new LongPairWritable();
		//@Override
		public void combine(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			int add=0;
			
			for (LongPairWritable val : values) {
				add += val.get_0();
				sum += val.get_1();
			}
			pair1.set(add,sum);	//LongPairWritable has count and sum of score
			context.write(key,pair1);
		}
	}

	public static class AssignReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		//@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			double sum = 0;
			double add=0;
			
			for (LongPairWritable val : values) {
				add += val.get_0();
				sum += val.get_1();
			}

			double average = ((Double) sum/add);	//Average is calculated
			result.set(average);
			context.write(key, result);	//Subreddit and average of score are the output
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "RedditAverage");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(AssignMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(AssignReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
