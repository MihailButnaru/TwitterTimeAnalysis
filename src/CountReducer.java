import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
*     MIHAIL BUTNARU 150186618
*/

/* CountReducer class is the Reducer
*  The reducer combines values from Shuffling phase and returns a single output value. It summarizes the complete dataset.
*/

public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  private IntWritable count = new IntWritable();
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

  int sum = 0;
  for(IntWritable value : values){
        sum = sum + value.get();
  }
    // count objects it sets the sum.
    count.set(sum);
    //Context writes the key and the count.
    context.write(key, count);
  }
}
