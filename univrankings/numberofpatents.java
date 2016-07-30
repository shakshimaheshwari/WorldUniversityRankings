import java.io.IOException;
import java.util.HashMap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class numberofpatents{

    private static Map<String, Integer>myMap = new HashMap<String, Integer>();

    public static class numberofPatentsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String lineFields = value.toString();
	        String[] tokens = lineFields.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1); 
		
		

		if(tokens[1].equals("institution") || tokens[11].equals("patents"))
				return;
		String institution= tokens[1].trim();
		int patents = Integer.parseInt(tokens[11].trim());
		if(myMap.containsKey(institution))
		{
			int val = myMap.get(institution);
			myMap.put(institution, val+patents);
		}
		else
		  myMap.put(institution, patents);
		context.write(new Text(), new Text());
	}
    }

    public static class numberofPatentsReducer extends Reducer<Text, Text, Text, IntWritable> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	 Set<Entry<String, Integer>>set = myMap.entrySet();
	  List< Entry<String, Integer>>list = new ArrayList<Entry<String, Integer>>(set);
	  Collections.sort(list, new Comparator<Map.Entry<String, Integer>>()
	  {
		public int compare(Map.Entry<String, Integer>o1, Map.Entry<String, Integer>o2)
		{
			return(o2.getValue()).compareTo(o1.getValue());
		}
	  });
	 for(Map.Entry<String, Integer> entry:list){
            context.write(new Text(entry.getKey()),new IntWritable(entry.getValue()));
        }
	  
	}
    }

    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(numberofpatents.class);
    job.setMapperClass(numberofPatentsMapper.class);
    job.setReducerClass(numberofPatentsReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

