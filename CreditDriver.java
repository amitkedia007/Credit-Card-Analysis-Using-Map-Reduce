// Import required packages
package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// Define the CreditDriver class
public class CreditDriver {
    public static void main(String[] args) throws Exception {
        // Check if the required number of arguments is provided
        if (args.length != 4) {
            System.err.println("Usage: CreditDriver <input path> <output path for city-wise average> <output path for top 3 cities>");
            System.exit(-1);
        }

        // Initialize the Hadoop Configuration object
        Configuration conf = new Configuration();

        // Create and configure the first job (City Amount)
        Job job1 = Job.getInstance(conf, "City Amount");
        job1.setJarByClass(CreditDriver.class);

        job1.setMapperClass(CreditMapper.class);
        job1.setReducerClass(CreditReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // Set input and output paths for the first job
        TextInputFormat.addInputPath(job1, new Path(args[0]));
        TextOutputFormat.setOutputPath(job1, new Path(args[1]));

        // Execute the first job and wait for completion
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Create and configure the second job (City Wise Average)
        Job job2 = Job.getInstance(conf, "City Wise Average");
        job2.setJarByClass(CreditDriver.class);

        job2.setMapperClass(CreditMapper2.class);
        job2.setReducerClass(CreditReducer2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Set input and output paths for the second job
        TextInputFormat.addInputPath(job2, new Path(args[1]));
        TextOutputFormat.setOutputPath(job2, new Path(args[2]));

        // Execute the second job and wait for completion, then exit with an appropriate status code
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
