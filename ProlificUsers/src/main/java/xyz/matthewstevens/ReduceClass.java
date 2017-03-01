package xyz.matthewstevens; /**
 * Created by matthew on 2/28/17.
 */
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass<KEY> extends Reducer<KEY, DoubleWritable,
        KEY,DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(KEY key, Iterable<DoubleWritable> values,
                       Context context) throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }

}