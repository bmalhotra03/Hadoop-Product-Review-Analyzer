package stubs;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HW1_reducer extends Reducer<Text, FloatWritable, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<FloatWritable> values, Context context)
      throws IOException, InterruptedException {
	  
	  // Keep track of number of reviews and also sum them up to get the average later
	  int totalReviews = 0;
	  float sumRatings = 0;
	  
	  for (FloatWritable value : values) {
	  totalReviews++;
	  sumRatings += value.get();
	  }
	  
	  float avgReview = totalReviews > 0 ? sumRatings / totalReviews : 0;
	  
	  // Concatenate total reviews and average reviews so that one output is there for two
	  String output = totalReviews + ", " + String.format("%.2f", avgReview);
	  context.write(key, new Text(output));
  }
}