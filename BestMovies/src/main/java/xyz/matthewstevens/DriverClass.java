package xyz.matthewstevens; /**
 * Created by matthew on 2/28/17.
 */

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.TreeMap;
import java.util.Vector;

public class DriverClass extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(getClass());
        job.setJobName(getClass().getSimpleName());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MapClass.class);
        job.setCombinerClass(ReduceClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DriverClass(), args);
        Vector vector = new Vector();
        try{
            BufferedReader buf = new BufferedReader(new FileReader(args[1] + "/part-r-00000"));
            TreeMap<Double, String> treeMap = new TreeMap<>(Collections.<Double>reverseOrder());
            String lineJustFetched;
            String[] wordsArray;

            while(true){
                lineJustFetched = buf.readLine();
                if(lineJustFetched == null){
                    break;
                }else{
                    wordsArray = lineJustFetched.split("\t");
                    treeMap.put(Double.parseDouble(wordsArray[1]), wordsArray[0]);
                }
            }

            vector = new Vector(treeMap.values());

            buf.close();

        }catch(Exception e){
            e.printStackTrace();
        }

        TreeMap<String, String> movieTitles = new TreeMap<>();

        try{
            BufferedReader buf = new BufferedReader(new FileReader(args[2]));
            String lineJustFetched;
            String[] wordsArray;

            while(true){
                lineJustFetched = buf.readLine();
                if(lineJustFetched == null){
                    break;
                }else{
                    wordsArray = lineJustFetched.split(",(?! )");
                    movieTitles.put(wordsArray[0], wordsArray[2]);
                }
            }

            buf.close();

        }catch(Exception e){
            e.printStackTrace();
        }

        for(int i = 0; i < 10; i++)
        {
            System.out.println((i+1) + ") " + movieTitles.get(vector.get(i)));
        }
    }
}