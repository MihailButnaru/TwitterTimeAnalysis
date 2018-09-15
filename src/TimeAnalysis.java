import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
*     MIHAIL BUTNARU 150186618
*/

/*
* TimeAnalysis Class is the main class that is running the runJob method.
* The main method configures the Hadoop job through a Job Configuration object.
* The method runJob calls the mapper and the reducer, the path to the input files.
*/

public class TimeAnalysis {
  public static void runJob(String[] input, String output) throws Exception {
      Configuration conf = new Configuration();

      Job job = new Job(conf);

      job.setJarByClass(TimeAnalysis.class);
      job.setReducerClass(CountReducer.class);
      job.setCombinerClass(CountReducer.class);
      job.setMapperClass(Time.class);

      //Job set types of the MapOutKeys
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      //Set number of reducers to 2
      job.setNumReduceTasks(2);

      Path outputPath = new Path(output);
      FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
      FileOutputFormat.setOutputPath(job, outputPath);
      outputPath.getFileSystem(conf).delete(outputPath,true);
      job.waitForCompletion(true);
  }
  //Main method that calls the runJob method
  public static void main(String[] args) throws Exception {
    runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
  }
}
