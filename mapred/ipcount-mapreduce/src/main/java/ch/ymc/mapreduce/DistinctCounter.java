package ch.ymc.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Counting distinct IP's in weblog data.
 * 
 * @author cguegi
 *
 */
public class DistinctCounter extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: DistinctCounter <input dir> <output dir>\n");
			return -1;
		}

		Job job = Job.getInstance(getConf());
		job.setJarByClass(DistinctCounter.class);
		job.setJobName("Word Count");

		/*
		 * Specify input and output format
		 */
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		/*
	     * Specify the mapper and reducer classes.
	     */
		job.setMapperClass(DistinctMapper.class);
		job.setReducerClass(SumReducer.class);
		
	    /*
	     * Specify the job's output key and value classes.
	     */
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    
	    /*
	     * FIXME: default number of reducers in YARN?
	     */
	    job.setNumReduceTasks(1);


		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new DistinctCounter(), args);
		System.exit(exitCode);
	}
}
