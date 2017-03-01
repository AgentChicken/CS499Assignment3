package xyz.matthewstevens; /**
 * Created by matthew on 2/28/17.
 */
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapClass extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text word = new Text();
    private DoubleWritable count = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // value is tab separated values: word, year, occurrences, #books, #pages
        // we project out (word, occurrences) so we can sum over all years
        String[] split = value.toString().split(",");
        word.set(split[1]);
        if (split.length > 2) {
            try {
                count.set(Double.parseDouble(split[2]));
                context.write(word, new DoubleWritable(1));
            } catch (NumberFormatException e) {
                // cannot parse - ignore
            }
        }
    }
}