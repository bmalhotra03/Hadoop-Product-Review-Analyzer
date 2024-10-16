package stubs;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HW1_mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException { 
	  
	  // Convert string into an array to make it easier to parse through fields
	  String [] products = value.toString().split(",");
	  
	  if (products.length >= 3) {
		  
		  // Select the first and third element to be used as the appropriate key and value
		  String productID = products[0];
		  float rating = Float.parseFloat(products[2].trim());
		  
		  context.write(new Text(productID), new FloatWritable(rating));
	  }
  }
}
