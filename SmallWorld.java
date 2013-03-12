/*
 *
 * CS61C Spring 2013 Project 2: Small World
 *
 * Partner 1 Name: Edward Groshev
 * Partner 1 Login: cs61c-gq
 *
 * Partner 2 Name: Raemond Bergstrom-Wood
 * Partner 2 Login: cs61c-jz
 *
 * REMINDERS: 
 *
 * 1) YOU MUST COMPLETE THIS PROJECT WITH A PARTNER.
 * 
 * 2) DO NOT SHARE CODE WITH ANYONE EXCEPT YOUR PARTNER.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum depth for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Example writable type
    public static class EValue implements Writable {

        public HashSet<Long> destinations;
        public HashMap<Long,String> distances;

        public EValue(HashSet<Long> set, HashMap<Long,String> map){
            if (set == null){
                destinations = new HashSet<Long>();
            } else {
                destinations = set;              
            }
            if (map == null){
                distances = new HashMap<Long,String>();
            } else {
                distances = map;
            }
        }

        public EValue(long[] destinations, long[] sources, long[] distances, boolean[] curr){
            this.destinations = new HashSet<Long>();
            if(destinations != null){
                for(long destination : destinations){
                    this.destinations.add(destination);
                }               
            }

            int length = 0;
            if(sources != null && distances != null){
                length = sources.length;
            }
            this.distances = new HashMap<Long,String>();
            String bool;
            for(int i = 0; i < length; i++){
                if(curr[i]){
                    bool = "t";
                } else {
                    bool = "f";
                }
                this.distances.put(sources[i],bool+distances[i]);
            }
        }

        public EValue() {
            // does nothing
        }

        public boolean containsDestination(long destination){
            return destinations.contains(destination);
        }

        public Set<Long> listOfDestinations(){
            return destinations;
        }

        public boolean atCurrentDepth(long source){
            String value = distances.get(source);
            if(value.charAt(0) == 't'){
                return true;
            } else {
                return false;
            }
        }

        public long getDistance(long source){
            String value = distances.get(source);
            return Long.parseLong(value.substring(1));
        }

        public void updateSource(long source, boolean b, long distance){
            String bool;
            if(b){
                bool = "t";
            } else {
                bool = "f";
            }
            distances.put(source,bool+distance);
        }

        public Set<Long> listOfSources(){
            return distances.keySet();
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            //Serialize the HashSet
            // It's a good idea to store the length explicitly
            int length = 0;
            if (destinations != null){
                length = destinations.size();
            }
            // Always write the length, since we need to know
            // even when it's zero
            out.writeInt(length);
            for (long destination : destinations){
                out.writeLong(destination);
            }

            // Serialize the HashMap
            length = 0;
            if (distances != null){
                length = distances.size();
            }
            out.writeInt(length);
            for (Map.Entry<Long,String> entry : distances.entrySet()){
                out.writeLong(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            // Rebuilding the HashSet from the serialized object
            int length = in.readInt();
            destinations = new HashSet<Long>();
            for(int i = 0; i < length; i++){
                destinations.add(in.readLong());
            }

            // Rebuilding the HashMap from the serialized object
            length = in.readInt();
            distances = new HashMap<Long,String>();
            for(int i = 0; i < length; i++){
                distances.put(in.readLong(),in.readUTF());
            }
        }

        public String toString() {
            String result = "";
            for (long destination : destinations){
                result += destination + ",";
            }
            result += "     {";
            for (Map.Entry<Long,String> entry : distances.entrySet()){
                result += "("+entry.getKey()+","+entry.getValue().substring(1)+","+entry.getValue().substring(0,1)+"), ";
            }
            result += "}";
            return result;
        }
    }


    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {
            // example of getting value passed from main
            //int inputValue = Integer.parseInt(context.getConfiguration().get("inputValue"));
            context.write(key, value);
        }
    }


    /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class LoaderReduce extends Reducer<LongWritable, LongWritable, 
        LongWritable, EValue> {

        public long denom;

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            // We can grab the denom field from context: 
            denom = Long.parseLong(context.getConfiguration().get("denom"));

            // Example of iterating through an Iterable
            HashSet<Long> tempDest = new HashSet<Long>();
            HashMap<Long,String> tempDist = new HashMap<Long,String>();
            long newVal;
            Random probability = new Random();
            boolean include = probability.nextInt((int)denom) == 0;
            //boolean include = (long)(probablility.nextDouble()*denom) == 0;
            for (LongWritable value : values) {
                newVal = value.get();
                tempDest.add(newVal);
            }
            if (include) {
                tempDist.put((long)key.get(),"t0");
            }
            EValue newEvalue = new EValue(tempDest, tempDist);
            context.write(key,newEvalue);        
        }
    }


    // ------- Add your additional Mappers and Reducers Here ------- //
    // The second mapper. Part of the BFS process
    public static class BFSMap extends Mapper<LongWritable, EValue, 
        LongWritable, EValue> {

        public void map(LongWritable key, EValue value, Context context)
                throws IOException, InterruptedException {

            if (value.atCurrentDepth((long)key.get())){
                EValue newValue;
                for (long node : value.listOfDestinations()){
                    LongWritable tempNode = new LongWritable(node);
                    context.write(tempNode, newValue);
                }
            }
            context.write(key, value);
        }
    }


    public static class DistanceMap extends Mapper<LongWritable, LongWritable,
            LongWritable, EValue> {
        
        public void map(LongWritable key, EValue value, Context context) 
                throws IOException, InterruptedException {
            

        }
    }












    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Pass in denom command line arg:
        conf.set("denom", args[2]);

        // Sample of passing value from main into Mappers/Reducers using
        // conf. You might want to use something like this in the BFS phase:
        // See LoaderMap for an example of how to access this value
        conf.set("inputValue", (new Integer(5)).toString());

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");

        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(EValue.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        /*job.setInputFormatClass(SequenceFileInputFormat.class);//temorarilly commented out
        job.setOutputFormatClass(SequenceFileOutputFormat.class);*/
        job.setInputFormatClass(SequenceFileInputFormat.class);//Added to output the result so far
        job.setOutputFormatClass(TextOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Repeats your BFS mapreduce
        /*int i = 0;
        while (i < MAX_ITERATIONS) {
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            // Feel free to modify these four lines as necessary:
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(EValue.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(EValue.class);

            // You'll want to modify the following based on what you call
            // your mapper and reducer classes for the BFS phase.
            job.setMapperClass(BFSMap.class); // currently the default Mapper
            job.setReducerClass(BFSReducer.class); // currently the default Reducer

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        // Feel free to modify these two lines as necessary:
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        // DO NOT MODIFY THE FOLLOWING TWO LINES OF CODE:
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // You'll want to modify the following based on what you call your
        // mapper and reducer classes for the Histogram Phase
        job.setMapperClass(Mapper.class); // currently the default Mapper
        job.setReducerClass(Reducer.class); // currently the default Reducer

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);*/
    }
}
