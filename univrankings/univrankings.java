import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class univrankings{
private static Map< String, HashMap<String, Integer>> outerMap = new HashMap< String, HashMap<String, Integer>>();
public static class MapClass extends Mapper<LongWritable,Text,Text,Text> {
        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		    String lineFields = value.toString();
		    String[] tokens = lineFields.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1); 
		    String year = tokens[13];
		    String country = tokens[2];
		    if(tokens[2].equals("country")|| tokens[13].equals("year"))
				return;

		    if(!outerMap.containsKey(year))
		    {
			HashMap<String,Integer> innerMap = new HashMap<String, Integer>();
			if(!innerMap.containsKey(country))
			innerMap.put(country,1);
			else
			{
			   innerMap.put(country,innerMap.get(country)+1);
			}
			outerMap.put(year,innerMap);
		    }
		    else
		    {
			HashMap<String,Integer>innerMap = outerMap.get(year);
			if(!innerMap.containsKey(country))
			innerMap.put(country,1);
			else
			{
			   innerMap.put(country,innerMap.get(country)+1);
			}
		    }
		    /*else
			{
				if(innerMap.containsKey(country))
				{
					int val = innerMap.get(country);
					innerMap.put(country,val+1);
				}
				else
				{
					innerMap.put(country,1);
				}
			}

		  static
		 {
		   yearlist.add(year);
		 }*/
		  
		   context.write(new Text(), new Text());
	   
        }
    } 

   
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException,InterruptedException {

		/*for(int i=0;i<yearlist.size();i++)
		{
			if(outerMap.containsKey(yearlist.get(i)))
				continue;
			   else
				outerMap.put(yearlist.get(i),innerMap);
		}*/
		for(Entry<String, HashMap<String,Integer>> entry : outerMap.entrySet()){
			HashMap<String, Integer> myMap = entry.getValue();
			for(Entry<String,Integer> entry2 : myMap.entrySet()){
			String ChildKey = entry2.getKey();
			Integer childValue = entry2.getValue();
			//System.out.println(entry.getKey() + "  " + ChildKey + "    "+ childValue.toString());
			context.write(new Text(entry.getKey() + "  " + ChildKey), new Text(childValue.toString()));
		}
	}

     }
    }   

    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "university ranking");
    job.setJarByClass(univrankings.class);
    job.setMapperClass(MapClass.class);
    //job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

