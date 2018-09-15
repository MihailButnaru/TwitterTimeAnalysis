import java.io.IOException;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/*
*     MIHAIL BUTNARU 150186618
*/

/*
* Time extends the Mapper.
* The the mapper phase is to count number of occurances of each tweet from API and it prepare a list in the form of <word, frequency>
* It gets the number of Tweets that were posted each hour of the event.
*/

public class Time extends Mapper<Object, Text, Text, IntWritable> {

  private final IntWritable one = new IntWritable(1);
  private Text data = new Text();

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String[] list = value.toString().split(";");

          try{
            // Takes the time using the LocalDateTime API and because the RIO Olympic 2016 was in Brazil(RIO De Janeiro is -3 hours)
            LocalDateTime time = LocalDateTime.ofEpochSecond((Long.parseLong(list[0])/1000),0,ZoneOffset.ofHours(-3));
            System.out.println(time);
            int hour = time.getHour();
            //If the length is 4 then will data.set the hour and it will write the hour and the number of tweets
            if(list.length == 4){
              data.set("" + hour);
              context.write(data, one);
            }
          }catch(Exception e){
            // Error, Catch an exeception
          }
      }
  }
