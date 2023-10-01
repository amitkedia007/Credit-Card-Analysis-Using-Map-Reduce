// Import required packages
package org.myorg;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Define the CreditMapper2 class extending the Mapper class
public class CreditMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    // Override the map method that processes input key-value pairs and generates intermediate key-value pairs
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // Write the input value as the key and an empty string as the value to the context object
        context.write(value, new Text(""));
    }
}
