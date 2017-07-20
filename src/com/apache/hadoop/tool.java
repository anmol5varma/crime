package com.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class tool extends Configured implements Tool {
	   
	    public static void main(String[] args) throws Exception {
	        // TODO code application logic here
	    /*Configuration conf = new Configuration();
	    Job job=Job.getInstance(conf, "word count");*/
	    	System.out
			.println("************************** In main method ****************");

	Configuration configuration = new Configuration();
tool dateTR = new tool();

	int exitCode = ToolRunner.run(configuration, dateTR, args);
	System.exit(exitCode);
	    }

		@Override
		public int run(String[] args) throws Exception {
			// TODO Auto-generated method stub
			Job job = new Job(new Configuration());
		    job.setJarByClass(tool.class);
		    job.setJobName("crime by month");
		    job.setMapperClass(mapper.class);
		    job.setCombinerClass(reducer.class);
		    job.setReducerClass(reducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
			return 0;
		}
}
