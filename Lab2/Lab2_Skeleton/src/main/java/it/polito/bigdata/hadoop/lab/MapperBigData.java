package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String startsWith = context.getConfiguration().get("startsWith");

        String[] lines = value.toString().split("\n");
        for (String line : lines) {
//            if (line.split("\t")[0].startsWith(startsWith))
            String words = line.split("\t")[0];
              if (words.split(" ")[0].equals(startsWith)
                  || words.split(" ")[1].equals(startsWith))
                context.write(new Text(line), NullWritable.get());
        }
    }
}
