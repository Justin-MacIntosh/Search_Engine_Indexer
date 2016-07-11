package indexer;

import java.io.IOException;
import java.util.ArrayList;

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

public class IndexMR extends Configured implements Tool{
	static double TOTAL_DOCS = 101007.0; //TOTAL NUMBER OF DOCS: EDIT YOURSELF
	static double a = .45;
	
	@Override
	public int run(String[] arg0) throws Exception {
	    @SuppressWarnings("deprecation")
		Job job=new Job();
	    
	    job.setJobName("Index Map Reduce");
	    job.setJarByClass(InitializeMR.class);
	    
	    //set map reduce class
	    job.setMapperClass(IndexMRMapper.class);
	    job.setReducerClass(IndexMRReducer.class);
	    
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
	
	public static class IndexMRMapper extends Mapper<LongWritable,Text,Text,Text> {
		@Override 
		public void map(LongWritable key ,Text values,Context context) throws IOException, InterruptedException {
			//send data as is, do not edit the data
			String v = values.toString();
			String[] parts = v.split("\t",2);
			
			//write ((WORD), (DOCNAME : FREQUENCY))
			context.write(new Text(parts[0]), new Text(parts[1]));
		}
	}

	public static class IndexMRReducer extends Reducer<Text,Text,Text,Text>{
		@Override 
		public void reduce(Text key ,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			//get list of values
			ArrayList<String> vals = new ArrayList<String>();
			for (Text t : values) { vals.add(t.toString()); }
			
			//document frequency
			int df = vals.size();
			
			//names -> DOCNAMES, frequencies -> FREQUENCIES
			String[] names       = new String[df];
			double[] frequencies = new double[df];
			
			//split vals to populate the arrays
			double max_freq = 0;
			for (int i = 0; i < vals.size(); i++) {
				String cur = vals.get(i);
				names[i]       = cur.split(" : ", 2)[0];
				frequencies[i] = Double.parseDouble(cur.split(" : ", 2)[1]);
				if (frequencies[i] > max_freq) {
					max_freq = frequencies[i];
				}
			}
			
			//compute idf
			double idf = Math.log10(TOTAL_DOCS/df);
			
			//arrays to store tf and w values
			double[] tf_values = new double[df];
			double[] w_values  = new double[df];
			
			//loop through all docs
			for(int j = 0; j < df; j++) {
				tf_values[j] = a + (1.0-a) * (frequencies[j]/max_freq);
				w_values[j]  = idf * tf_values[j];
				String output_value =  names[j] + " : " + Double.toString(w_values[j]);
				
				//write ((WORD), (DOCNAME : WEIGHT))
				context.write(new Text(key), new Text(output_value));
			}
		}	
	}
}
