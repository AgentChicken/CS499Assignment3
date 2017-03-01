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
import java.util.*;

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
        ArrayList movieIds = new ArrayList();
        try{
            BufferedReader buf = new BufferedReader(new FileReader(args[1] + "/part-r-00000"));
            HashMap<String, Double> hashMap = new HashMap<>();
            String lineJustFetched;
            String[] wordsArray;

            while(true){
                lineJustFetched = buf.readLine();
                if(lineJustFetched == null){
                    break;
                }else{
                    wordsArray = lineJustFetched.split("\t");
                    hashMap.put(wordsArray[0], Double.parseDouble(wordsArray[1]));
                }
            }

            List<Map.Entry<String, Double>> greatest = findGreatest(hashMap, 10);

            for(int i = 0; i < 10; i++)
            {
                movieIds.add(greatest.get(i).getKey());
            }

            //System.out.println(greatest);

            vector = new Vector(hashMap.values());

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

        for(int i = 9; i > -1; i--)
        {
            System.out.println((10 - i) + ") " + movieTitles.get(movieIds.get(i)));
        }
    }

    //found this on Stack Overflow, gets largestentries in a list
    private static <K, V extends Comparable<? super V>> List<Map.Entry<K, V>>
    findGreatest(Map<K, V> map, int n)
    {
        Comparator<? super Map.Entry<K, V>> comparator =
                (Comparator<Map.Entry<K, V>>) (e0, e1) -> {
                    V v0 = e0.getValue();
                    V v1 = e1.getValue();
                    return v0.compareTo(v1);
                };
        PriorityQueue<Map.Entry<K, V>> highest =
                new PriorityQueue<>(n, comparator);
        for (Map.Entry<K, V> entry : map.entrySet())
        {
            highest.offer(entry);
            while (highest.size() > n)
            {
                highest.poll();
            }
        }

        List<Map.Entry<K, V>> result = new ArrayList<>();
        while (highest.size() > 0)
        {
            result.add(highest.poll());
        }
        return result;
    }
}
