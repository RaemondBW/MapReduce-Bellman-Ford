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
import java.io.File;
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

        public HashSet<Long> children;
        public HashMap<Long,Long> distances;
        public HashSet<Long> sourcesCurrentlyHere;

        public EValue(HashSet<Long> children, HashMap<Long,Long> distances, HashSet<Long> sourcesCurrentlyHere){
            if (children == null){
                this.children = new HashSet<Long>();
            } else {
                this.children = children;
            }
            if (distances == null){
                this.distances = new HashMap<Long,Long>();
            } else {
                this.distances = distances;
            }
            if (sourcesCurrentlyHere == null){
                this.sourcesCurrentlyHere = new HashSet<Long>();
            } else {
                this.sourcesCurrentlyHere = sourcesCurrentlyHere;
            }
        }

        public EValue() {
            // does nothing
        }

        public boolean hasChildren(){
            return children.isEmpty();
        }

        public boolean hasAtLeastOneVisitorSource(){
            return !sourcesCurrentlyHere.isEmpty();
        }

        public Set<Long> listOfChildren(){
            return children;
        }

        public Set<Long> listOfSourcesVisitedBy(){
            return distances.keySet();
        }

        public Set<Long> listOfSourcesCurrentlyHere(){
            return sourcesCurrentlyHere;
        }

        public void clearListOfSourcesCurrentlyHere(){
            sourcesCurrentlyHere.clear();
        }

        public long getDistance(long source){
            return distances.get(source);
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            // Serialize the HashSet of children
            // It's a good idea to store the length explicitly
            int length = children.size();
            // Always write the length, since we need to know
            // even when it's zero
            out.writeInt(length);
            for (long child : children){
                out.writeLong(child);
            }

            // Serialize the HashMap
            length = distances.size();
            out.writeInt(length);
            for (Map.Entry<Long,Long> entry : distances.entrySet()){
                out.writeLong(entry.getKey());
                out.writeLong(entry.getValue());
            }

            // Serialize the HashSet of sourcesCurrentlyHere
            length = sourcesCurrentlyHere.size();
            out.writeInt(length);
            for (long source : sourcesCurrentlyHere){
                out.writeLong(source);
            }
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            // Rebuilding the HashSet from the serialized object
            int length = in.readInt();
            children = new HashSet<Long>();
            for(int i = 0; i < length; i++){
                children.add(in.readLong());
            }

            // Rebuilding the HashMap from the serialized object
            length = in.readInt();
            distances = new HashMap<Long,Long>();
            for(int i = 0; i < length; i++){
                distances.put(in.readLong(),in.readLong());
            }

            // Rebuilding the 2nd HashSet from the serialized object
            length = in.readInt();
            sourcesCurrentlyHere = new HashSet<Long>();
            for(int i = 0; i < length; i++){
                sourcesCurrentlyHere.add(in.readLong());
            }
        }

        public String toString() {
            String result = "";
            for (long child : children){
                result += child + ",";
            }
            result += "      {";
            for (Map.Entry<Long,Long> entry : distances.entrySet()){
                result += "("+entry.getKey()+","+entry.getValue()+"), ";
            }
            result += "}      [";
            for (long source : sourcesCurrentlyHere){
                result += source + ",";
            }
            result += "]";
            return result;
        }
    }


    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            // example of getting value passed from main
            //int inputValue = Integer.parseInt(context.getConfiguration().get("inputValue"));
            
            context.write(key, value);
            // Write the value as the key and -1 as the child
            // to indicate that the node exists but doesn't point anywhere
            context.write(value, new LongWritable(-1L));
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

            // Create temp HashSets and HashMaps which are used to
            // create a new EValue object
            HashSet<Long> listOfChildren = new HashSet<Long>();
            HashMap<Long,Long> tempDistances = new HashMap<Long,Long>();
            HashSet<Long> tempSourcesCurrenlyHere = new HashSet<Long>();

            // Random boolean used to indicte if node is included or not
            boolean include = new Random().nextInt(denom) == 0;
            for (LongWritable value : values) {
                // If the value is -1 the key node has no children
                if (value.get() != -1){
                    listOfChildren.add(value.get());
                }
            }

            // If the "include" boolean is true, the current key node
            // is now considered to be a source and is marked as being
            // distance 0 from its self. 
            if (include/*key.get() == 9803315 || key.get() == 9512380 || key.get() == 9606399*/) {          
                tempDistances.put(key.get(),0L);
                tempSourcesCurrenlyHere.add(key.get());
            }

            EValue newVal = new EValue(listOfChildren, tempDistances, tempSourcesCurrenlyHere);
            context.write(key,newVal);        
        }
    }


    // ------- Add your additional Mappers and Reducers Here ------- //

    // The second mapper. Part of the BFS process.
    public static class BFSMap extends Mapper<LongWritable, EValue, 
        LongWritable, EValue> {

        public void map(LongWritable key, EValue value, Context context)
            throws IOException, InterruptedException {

            if(value.hasAtLeastOneVisitorSource()){
                HashMap<Long,Long> distancesOfChild = new HashMap<Long,Long>();
                HashSet<Long> sourcesNowAtChild = new HashSet<Long>();
                long currDist;
                // For all sources currenly visiting this source, build a
                // new distance list with respect to the children and 
                // move the sources currenly at this node, to be at the
                // child nodes.
                for(long source : value.listOfSourcesCurrentlyHere()){
                    currDist = value.getDistance(source);
                    distancesOfChild.put(source,currDist+1);
                    sourcesNowAtChild.add(source);
                }

                // For every child of this node, give them the updated 
                // list of distances and list of sources currenly visiting them
                LongWritable newKey = new LongWritable();
                EValue newVal = new EValue(null, distancesOfChild, sourcesNowAtChild);
                for(long child : value.listOfChildren()){
                    newKey.set(child);
                    context.write(newKey,newVal);
                }

                // Clear all the sources from the current node, because
                // they have now moved onto the children 
                value.clearListOfSourcesCurrentlyHere();
            }

            context.write(key, value);
        }
    }

    // The second reducer. Part of the BFS process.
    public static class BFSReduce extends Reducer<LongWritable, EValue, 
        LongWritable, EValue> {
    
        public void reduce(LongWritable key, Iterable<EValue> values, 
            Context context) throws IOException, InterruptedException {

            HashSet<Long> totalChildren = new HashSet<Long>();
            HashMap<Long,Long> mergedDistances = new HashMap<Long,Long>();
            HashSet<Long> sourcesCurrenlyHere = new HashSet<Long>();

            long tempDist;
            HashSet<Long> revisitedBy = new HashSet<Long>();
            for(EValue value : values) {
                // Merge all the children (note that only one of the values
                // will contain children every other value is an intermediate
                // one with NO children)
                for(long child : value.listOfChildren()){
                    totalChildren.add(child);
                }

                // Merge all of the distances of the key node with
                // with respect to each source
                for(long source : value.listOfSourcesVisitedBy()){
                    tempDist = value.getDistance(source);
                    if (mergedDistances.containsKey(source)){
                        // If the conflicting distances are NOT equal
                        // the source has looped back to this node
                        // therefore we should stop if from repeating
                        // working that it has already done
                        if (tempDist!=mergedDistances.get(source)){
                            revisitedBy.add(source);
                        }
                        // If there are conflicting distances,
                        // take the minimum one
                        tempDist = Math.min(mergedDistances.get(source),tempDist);
                    }
                    mergedDistances.put(source, tempDist);
                }

                // Merge all of the sources that are currenly at the key node
                for(long node : value.listOfSourcesCurrentlyHere()){
                    sourcesCurrenlyHere.add(node);
                }
            }
            // Removed sources that are revisiting this node
            for(long source : revisitedBy){
                sourcesCurrenlyHere.remove(source);
            }

            EValue newVal = new EValue(totalChildren, mergedDistances, sourcesCurrenlyHere);
            context.write(key,newVal);        
        }
    }


    //----------- Final Mapper and Reducer Functions. -------------- //
    // Taking the output distances and using them to create the final histogram
    public static class DistanceMap extends Mapper<LongWritable, EValue,
            LongWritable, LongWritable> {
        
        public static final LongWritable ONE = new LongWritable(1L);
        public void map(LongWritable key, EValue value, Context context) 
                throws IOException, InterruptedException {
            LongWritable distance = new LongWritable();
            for (long node : value.listOfSourcesVisitedBy()) {
                distance.set(value.getDistance(node));
                context.write(distance, ONE);
            }
        }
    }

    public static class DistanceReduce extends Reducer<LongWritable, LongWritable,
            LongWritable, LongWritable> {
        public void reduce(LongWritable key,Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long total = 0;
            for (LongWritable value : values) {
                total += value.get();
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

        /*job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);*/
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Repeats your BFS mapreduce
        int i = 0;
        boolean foundSame = false;
        while (i < MAX_ITERATIONS && !foundSame) {
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
            job.setReducerClass(BFSReduce.class); // currently the default Reducer

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));
            job.waitForCompletion(true);

            //Check to see if the files contain the same information, So that we know whether or not we need to run the bfs mapper and reducer for another iteration.
            long previousFile = (new File("bfs-"+(i-1)+"-out/part-r-00000")).length();
            long currentFile = (new File("bfs-"+(i)+"-out/part-r-00000")).length();
            if (previousFile == currentFile)
                foundSame = true;

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
        job.setMapperClass(DistanceMap.class); // currently the default Mapper
        job.setReducerClass(DistanceReduce.class); // currently the default Reducer

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
