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
        public HashMap<Long,Long> distances;
        public HashSet<Long> activeNodes;

        public EValue(HashSet<Long> dest, HashMap<Long,Long> map, HashSet<Long> curr){
            if (dest == null){
                destinations = new HashSet<Long>();
            } else {
                destinations = dest;              
            }
            if (map == null){
                distances = new HashMap<Long,Long>();
            } else {
                distances = map;
            }
            if (curr == null){
                activeNodes = new HashSet<Long>();
            } else {
                activeNodes = curr;              
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

        public Set<Long> listOfDistanceSources(){
            return distances.keySet();
        }

        public Set<Long> listOfActiveNodes(){
            return activeNodes;
        }

        public long getDistance(long source){
            return distances.get(source);
        }

        public void updateDistance(long source, long distance){
            distances.put(source,distance);
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            // Serialize the HashSet of destinations
            // It's a good idea to store the length explicitly
            int length = destinations.size();
            // Always write the length, since we need to know
            // even when it's zero
            out.writeInt(length);
            for (long destination : destinations){
                out.writeLong(destination);
            }

            // Serialize the HashMap
            length = distances.size();
            out.writeInt(length);
            for (Map.Entry<Long,Long> entry : distances.entrySet()){
                out.writeLong(entry.getKey());
                out.writeLong(entry.getValue());
            }

            // Serialize the HashSet of activeNodes
            length = activeNodes.size();
            out.writeInt(length);
            for (long node : activeNodes){
                out.writeLong(node);
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
            distances = new HashMap<Long,Long>();
            for(int i = 0; i < length; i++){
                distances.put(in.readLong(),in.readLong());
            }

            // Rebuilding the 2nd HashSet from the serialized object
            length = in.readInt();
            activeNodes = new HashSet<Long>();
            for(int i = 0; i < length; i++){
                activeNodes.add(in.readLong());
            }
        }

        public String toString() {
            String result = "";
            for (long destination : destinations){
                result += destination + ",";
            }
            result += "      {";
            for (Map.Entry<Long,Long> entry : distances.entrySet()){
                result += "("+entry.getKey()+","+entry.getValue()+"), ";
            }
            result += "}      [";
            for (long node : activeNodes){
                result += node + ",";
            }
            result += "]";
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

        public int denom;

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            // We can grab the denom field from context: 
            denom = Integer.parseInt(context.getConfiguration().get("denom"));

            HashSet<Long> tempDest = new HashSet<Long>();
            HashMap<Long,Long> tempDist = new HashMap<Long,Long>();
            HashSet<Long> tempActive = new HashSet<Long>();
            boolean include = new Random().nextInt(denom) == 0;
            for (LongWritable value : values) {
                tempDest.add(value.get());
            }
            if (include) {          
                tempDist.put(key.get(),0L);
                tempActive.add(key.get());
            }
            EValue newVal = new EValue(tempDest, tempDist, tempActive);
            context.write(key,newVal);        
        }
    }


    // ------- Add your additional Mappers and Reducers Here ------- //
    // The second mapper. Part of the BFS process
    public static class BFSMap extends Mapper<LongWritable, EValue, 
        LongWritable, EValue> {

        public void map(LongWritable key, EValue value, Context context)
                throws IOException, InterruptedException {

            context.write(key, value);
        }
    }

    

    //----------- Final Mapper and Reducer Functions. -------------- //
    // Taking the output distances and using them to create the final histogram
    public static class DistanceMap extends Mapper<LongWritable, EValue,
            LongWritable, LongWritable> {
        
        public void map(LongWritable key, EValue value, Context context) 
                throws IOException, InterruptedException {
            for (long keys : value.distances.keySet()) {
                context.write(new LongWritable(value.distances.get(keys)), new LongWritable(1L));
            }
        }
    }

    public static class DistanceReduce extends Reducer<LongWritable, LongWritable,
            LongWritable, LongWritable> {
        public void reduce(LongWritable key,Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long total = 0;
            for (LongWritable value : values) {
                total += (long) value.get();
            }
            context.write(key,new LongWritable(total));
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
