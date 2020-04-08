package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {// Output value type

    TopKVector<WordCountWritable> top100;

    protected void setup(Context context) {
        top100 = new TopKVector<>(100);
    }

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */
        String[] lines = value.toString().split("\n");
        for (String line : lines) {
            String products = line.split("\t")[0];
            Integer count = Integer.valueOf(line.split("\t")[1]);
            System.out.println(products + " " + count);
            top100.updateWithNewElement(new WordCountWritable(products, count));
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (WordCountWritable topX : top100.getLocalTopK()) {
            context.write(NullWritable.get(), topX);
        }
    }
}
