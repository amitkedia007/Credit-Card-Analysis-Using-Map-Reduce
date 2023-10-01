package org.myorg;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Define the CreditReducer class extending the Reducer class
public class CreditReducer extends Reducer<Text, LongWritable, Text, Text> {

    // Override the reduce method that processes intermediate key-value pairs and generates the final key-value pairs
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        
        // Initialize the sum and count variables for aggregation
        long sum = 0;
        int count = 0;
        
        // Iterate over the values (amounts) associated with the same key (city) and aggregate them
        for (LongWritable value : values) {
            sum += value.get(); // Add the value to the sum
            count++; // Increment the count by 1
        }
        
        // Write the final key-value pair (city, sum and count) to the context object
        context.write(key, new Text(sum + "," + count));
    }
}
