package seqkmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class KMeansSeq extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(KMeansSeq.class);

    public static class KMeansSeqMapper extends
            Mapper<Object, Text, Text, Text> {

        private static List<ArrayList<Double>> records = new ArrayList<>();

        //Euclidean distance
        private static Double distance(ArrayList<Double> centroid, ArrayList<Double> record) {
            double sum = 0.0;
            int size = centroid.size();
            // ignoring the last elememt ... which is the actual label for now
            for (int i = 0; i < size - 1; i++) {
                sum += Math.pow(centroid.get(i) - record.get(i), 2);
            }
            return Math.sqrt(sum);
        }

        private static void selectRandomCentroids(List<ArrayList<Double>> centroids, int k) {
            // Selecting k centroids at random
            // Reference - https://stackoverflow.com/questions/12487592/randomly-select-an-item-from-a-list
            int i = 0;
            while (i < k){
                Random random = new Random();
                ArrayList<Double> centroid = records.get(random.nextInt(records.size()));
                centroids.add(centroid);
                i++;

            }
        }

        private static void evaluateClusterMap(HashMap<ArrayList<Double>, ArrayList<ArrayList<Double>>> clusterMap, List<ArrayList<Double>> centroids) {

            clusterMap.clear();

            // creating k entries in hashmap, one for each centroid
            for (ArrayList<Double> c: centroids) {
                clusterMap.put(c, new ArrayList<>());
            }
        }

        private static ArrayList<Double> initializeList(int size) {
            ArrayList<Double> a = new ArrayList<>();
            for(int j = 0; j < size; j++){
                a.add(0.0);
            }

            return a;
        }

        private static boolean evaluateCentroids(HashMap<ArrayList<Double>, ArrayList<ArrayList<Double>>> clusterMap,
                                              List<ArrayList<Double>> centroids, boolean runIteration, Double THRESHOLD){

            //Clear the previous centroids
            centroids.clear();

            for (ArrayList<Double> c: clusterMap.keySet()) {

                //create initial centroid
                ArrayList<Double> new_c = initializeList(clusterMap.get(c).get(0).size());

                for (ArrayList<Double> r: clusterMap.get(c)) {
                    //start from first column
                    int i = 0;

                    //loop until last column, each column denotes one feature
                    //ignore the last column which is the feature: label
                    while (i < r.size() - 1){

                        //calculate the average of the column under consideration
                        new_c.set(i, new_c.get(i) + (r.get(i)/clusterMap.get(c).size()));
                        i++;
                    }
                }

                //check for convergence
                if(distance(c,new_c) / distance(initializeList(c.size()), c) > THRESHOLD) {
                    runIteration = true;
                }

                centroids.add(new_c);
            }

            return runIteration;
        }

        private static void kMeans(HashMap<ArrayList<Double>, ArrayList<ArrayList<Double>>> clusterMap,
                                   List<ArrayList<Double>> centroids, boolean runIteration, Double THRESHOLD){
            // running k means

            //Run the iteration based on the flag set during convergence
            do {
                runIteration = false;
                for (ArrayList<Double> r : records) {

                    Double min_val = Double.MAX_VALUE;
                    ArrayList<Double> selected_c = null;
                    Double d;

                    for (ArrayList<Double> c : centroids) {
                        d = distance(c, r);
                        if (d < min_val) {
                            selected_c = c;
                            min_val = d;
                        }
                    }
                    clusterMap.get(selected_c).add(r);
                }

                //To recompute new centroids by averaging the records assigned to each
                //Sets the flag to false if converged
                runIteration = evaluateCentroids(clusterMap, centroids, runIteration, THRESHOLD);

                if(runIteration){
                    evaluateClusterMap(clusterMap, centroids);
                }


            } while(runIteration);
        }

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);

            records.clear();

            //Retrieve from local cache
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {

                //Read input to a bufferedReader
                BufferedReader br = new BufferedReader((new FileReader("normalized_data.csv")));

                String line;
                while ((line = br.readLine()) != null) {
                    String[] keyvalue = line.split(";");

                    ArrayList<Double> record = new ArrayList<>();
                    for (String s : keyvalue) {
                        record.add(Double.parseDouble(s));
                    }
                    records.add(record);
                }

                br.close();

            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            List<ArrayList<Double>> centroids = new ArrayList<>();

            //Threshold for convergence(set to 1%)
            final double THRESHOLD = 0.01;

            //Initialize the termination to false
            boolean runIteration = false;

            //private static HashMap<ArrayList<Double>, Double> centroidMap = new HashMap<>();
            HashMap<ArrayList<Double>, ArrayList<ArrayList<Double>>> clusterMap = new HashMap<>();

            selectRandomCentroids(centroids, Integer.parseInt(value.toString()));
            evaluateClusterMap(clusterMap, centroids);
            kMeans(clusterMap, centroids, runIteration, THRESHOLD);

            int i = 0;
            for (ArrayList<Double> k: clusterMap.keySet()) {
                for (ArrayList<Double> r: clusterMap.get(k)) {
                    StringBuilder out_record = new StringBuilder();
                    for (int j = 0; j < r.size()- 1; j++){
                         out_record.append(r.get(j) + ";");
                    }
                    out_record.append(r.get(r.size() - 1));
                    context.write(null, new Text(Integer.toString(i) + ":" + out_record.toString()));
                }
                i++;
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        int val;
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "K Mean Sequential");
        job.setJarByClass(KMeansSeq.class);

        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\n");

        //Add the input to cache file
        job.addCacheFile(new URI(args[0]+"/normalized_data.csv"));
        //set the sequential flow of job
        job.setMapperClass(KMeansSeqMapper.class);
        job.setNumReduceTasks(0);

        //Set the input path for job
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(args[2]));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);

        //Set the output path for job
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        //Set the output key and value type for this job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        val = job.waitForCompletion(true) ? 0 : 1;

        return val;
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
        }

        try {
            ToolRunner.run(new KMeansSeq(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
