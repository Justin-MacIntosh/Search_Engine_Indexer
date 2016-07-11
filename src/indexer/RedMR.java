package indexer;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class RedMR  extends Configured implements Tool{
	
	@Override
	public int run(String[] arg0) throws Exception {
	    @SuppressWarnings("deprecation")
		Job job=new Job();
	    
	    job.setJobName("Red Map Reduce");
	    job.setJarByClass(InitializeMR.class);
	    
	    //set map reduce class
	    job.setMapperClass(RedMRMapper.class);
	    job.setReducerClass(RedMRReducer.class);
	    
	    //set output format
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	  
	    //set output path
	    FileInputFormat.setInputPaths(job, new Path(arg0[0]));
	    FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
	    
		return job.waitForCompletion(true)?0:1;
	}
	
	public static class RedMRMapper extends Mapper<LongWritable,Text,Text,Text> {
		@Override 
		public void map(LongWritable key ,Text values,Context context) throws IOException, InterruptedException {
			//send data as is, do not edit the data
			String v = values.toString();
			String[] parts = v.split("\t",2);
			
			//write ((KEY) : (DOCNAME : WEIGHT))
			context.write(new Text(parts[0]), new Text(parts[1]));
		}
	}

	public static class RedMRReducer extends Reducer<Text,Text,Text,Text>{
		@Override 
		public void reduce(Text key ,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			//join the values into one string
			String out_value = "|";
			for (Text t : values) {
				out_value += t.toString() + "|";
			}
			
			//write ((WORD) : (|DOCNAME : WEIGHT|DOCNAME : WEIGHT|DOCNAME : WEIGHT|...))
			context.write(key, new Text(out_value));
		}
	}
}
