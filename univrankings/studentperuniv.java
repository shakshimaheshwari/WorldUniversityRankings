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


public class studentperuniv{
private static Map< String, HashMap<String, Integer>> outerMap = new HashMap< String, HashMap<String, Integer>>();
public static class MapClass extends Mapper<LongWritable,Text,Text,Text> {
        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		    String lineFields = value.toString();
		    String[] tokens = lineFields.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1); 
		    String year = tokens[13];
		    String country = tokens[2];
		    String num_students = tokens[9];
		    if(tokens[2].equals("country") || tokens[9].equals("num_students")|| tokens[13].equals("year"))
				return;

		    if(!outerMap.containsKey(year))
		    {
			HashMap<String,Integer> innerMap = new HashMap<String, Integer>();
			if(!innerMap.containsKey(country))
			{
				if(num_students.equals(""))
				{
				   innerMap.put(country,0);
				}
				else{
					String studentcount = tokens[9].replace(",","");
					studentcount = studentcount.replace("\"","");
					Integer num_of_students = Integer.parseInt(studentcount.trim());
					innerMap.put(country,num_of_students);
				}
				
			}
			else
			{
			        if(num_students.equals(""))
				{
				   innerMap.put(country,innerMap.get(country)+0);
				}
				else{
					String studentcount = tokens[9].replace(",","");
					studentcount = studentcount.replace("\"","");
					Integer num_of_students = Integer.parseInt(studentcount.trim());
					innerMap.put(country,innerMap.get(country)+num_of_students);
				}
			}
			outerMap.put(year,innerMap);
		    }
		    else
		    {
			HashMap<String,Integer>innerMap = outerMap.get(year);
			if(!innerMap.containsKey(country))
			{
				if(num_students.equals(""))
				{
				   innerMap.put(country,0);
				}
				else{
					String studentcount = tokens[9].replace(",","");
					studentcount = studentcount.replace("\"","");
					Integer num_of_students = Integer.parseInt(studentcount.trim());
					innerMap.put(country,num_of_students);
				}
				
			}
			else
			{
			        if(num_students.equals(""))
				{
				   innerMap.put(country,innerMap.get(country)+0);
				}
				else{
					String studentcount = tokens[9].replace(",","");
					studentcount = studentcount.replace("\"","");
					Integer num_of_students = Integer.parseInt(studentcount.trim());
					innerMap.put(country,innerMap.get(country)+num_of_students);
				}
			}
		    }

		context.write(new Text(), new Text());
        }
    }   
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException,InterruptedException {

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
    job.setJarByClass(studentperuniv.class);
    job.setMapperClass(MapClass.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

