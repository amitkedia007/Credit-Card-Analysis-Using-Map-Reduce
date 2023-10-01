package org.myorg;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Define the CreditMapper class extending the Mapper class
public class CreditMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    // Override the map method that processes input key-value pairs and generates intermediate key-value pairs
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // Split the input text line (CSV) into an array of column values
        String[] columns = value.toString().split(",");
        
        // Extract the city value from the second column (index 1) and remove double quotes
        String city = columns[1].replaceAll("\"", "");
        
        // Parse the amount value from the seventh column (index 6) as a long
        long amount = Long.parseLong(columns[6]);
        
        // Write the intermediate key-value pair (city, amount) to the context object
        context.write(new Text(city), new LongWritable(amount));
    }
}
