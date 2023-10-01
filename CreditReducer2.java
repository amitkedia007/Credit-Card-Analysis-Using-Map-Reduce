// Import required packages
package org.myorg;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Define the CreditReducer2 class extending the Reducer class
public class CreditReducer2 extends Reducer<Text, Text, Text, Text> {
    // Initialize a TreeMap to store the top cities by average spent
    private TreeMap<Double, String> topCities = new TreeMap<>();

    // Override the reduce method that processes intermediate key-value pairs and generates the final key-value pairs
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Split the input key text into city, sum, and count parts
        String[] parts = key.toString().split("\t");
        String city = parts[0];
        String[] sumAndCount = parts[1].split(",");
        double sum = Double.parseDouble(sumAndCount[0]);
        int count = Integer.parseInt(sumAndCount[1]);
        
        // Calculate the average spent for the city
        double average = sum / count;

        // Add the city and its average to the TreeMap
        topCities.put(average, city);

        // If the TreeMap has more than 3 entries, remove the lowest average
        if (topCities.size() > 3) {
            topCities.remove(topCities.firstKey());
        }
    }

    // Override the cleanup method, which runs after the reduce method is finished
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Initialize a StringBuilder to store the result string
        StringBuilder result = new StringBuilder();
        result.append("The Top 3 cities with highest average spent is: ");
        
        int count = 0;
        // Iterate over the TreeMap in descending order and append the city and average to the result string
        for (Double average : topCities.descendingKeySet()) {
            String city = topCities.get(average);
            result.append("\"").append(city).append("\" : \"").append(String.format("%.2f", average)).append("\"");
            count++;
            if (count < topCities.size()) {
                result.append(", ");
            }
        }

        // Write the result string as the key and an empty string as the value to the context object
        context.write(new Text(result.toString()), new Text(""));
    }
}
