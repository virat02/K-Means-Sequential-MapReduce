package seqkmeans;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class SeqKMeans {

    private static ArrayList<ArrayList<Double>> records = new ArrayList<>();
    private static ArrayList<ArrayList<Double>> centroids = new ArrayList<>();
    private static int k = 6;

    //Threshold for convergence(set to 1%)
    private static final double THRESHOLD = 0.01;

    //Initialize the termination to false
    private static boolean runIteration = false;

    //private static HashMap<ArrayList<Double>, Double> centroidMap = new HashMap<>();
    private static HashMap<ArrayList<Double>, ArrayList<ArrayList<Double>>> clusterMap = new HashMap<>();

//    // Manhattan distance
//    private static Double distance(ArrayList<Double> centroid, ArrayList<Double> record)
//    {
//        double sum = 0.0;
//        int size = centroid.size();
//        // ignoring the second last element ... which is the actual label for now
//        for (int i = 0; i < size - 1; i++) {
//            sum += Math.abs(centroid.get(i) - record.get(i));
//        }
//        return sum;
//    }

     //Euclidian distance
     private static Double distance(ArrayList<Double> centroid, ArrayList<Double> record) {
         double sum = 0.0;
         int size = centroid.size();
         // ignoring the last elememt ... which is the actual label for now
         for (int i = 0; i < size - 1; i++) {
         sum += Math.pow(centroid.get(i) - record.get(i), 2);
         }
         return Math.sqrt(sum);
     }

    private static void readFile() throws IOException {
        // Building our dataset in memory
        BufferedReader br = new BufferedReader(new FileReader("data/winequality-red.csv"));
        String line;
        while ((line = br.readLine()) != null){
            String[] recordValuesAsString = line.split(";");
            ArrayList<Double> record = new ArrayList<>();
            for (String s : recordValuesAsString) {
                record.add(Double.parseDouble(s));
            }
            records.add(record);
        }
        br.close();
    }

    private static void selectRandomCentroids(int k) {
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

    private static void evaluateClusterMap() {

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

    private static void evaluateCentroids(){

        //CLear the previous centroids
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
    }

    private static void kMeans(){
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
            evaluateCentroids();

            //Clear the previous contents of the clusterMap if we have a next iteration
            if(runIteration) {
                evaluateClusterMap();
            }


        } while(runIteration);
    }

    public static void main(String[] args) throws IOException {

        readFile();
        selectRandomCentroids(k);
        System.out.println("Random centroids");
        System.out.println(centroids);
        evaluateClusterMap();
        kMeans();

        for (ArrayList<Double> k: clusterMap.keySet()) {
            System.out.println();
            System.out.println(k);
            System.out.println();
            for (ArrayList<Double> r: clusterMap.get(k)) {
                System.out.println("---------------------------");
                System.out.println(r.get(r.size()- 1));
                System.out.println("---------------------------");
            }
        }

    }
}
