package seqkmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class SeqKMeans {

    private static ArrayList<ArrayList<Double>> records = new ArrayList<>();
    private static ArrayList<ArrayList<Double>> centroids = new ArrayList<>();
    private static int k = 5;
    private static HashMap<ArrayList<Double>, Double> centroidMap = new HashMap<>();
    private static HashMap<ArrayList<Double>, ArrayList<ArrayList<Double>>> clusterMap = new HashMap<>();

    // Manhattan distance
    private static Double distance(ArrayList<Double> centroid, ArrayList<Double> record)
    {
        double sum = 0.0;
        int size = centroid.size();
        // ignoring the second last element ... which is the actual label for now
        // and the last element which is the centroid to which the record belongs
        for (int i = 0; i < size - 2; i++) {
            sum += Math.abs(centroid.get(i) - record.get(i));
        }
        return sum;
    }

    private static void readFile() throws IOException {
        // Building our dataset in memory
        BufferedReader br = new BufferedReader(new FileReader("input/winequality-red.csv"));
        String line;
        while ((line = br.readLine()) != null){
            String[] recordValuesAsString = line.split(";");
            ArrayList<Double> record = new ArrayList<>();
            for (String s : recordValuesAsString) {
                record.add(Double.parseDouble(s));
            }
            record.add(0.0);
            records.add(record);
        }
        br.close();
    }

    private static void selectRandomCentroids() {
        // Selecting k centroids at random
        // Reference - https://stackoverflow.com/questions/12487592/randomly-select-an-item-from-a-list
        int i = 4;
        while (i <= 4 + k){
//            Random random = new Random();
//            ArrayList<Double> centroid = records.get(random.nextInt(records.size()));
            for (ArrayList<Double> r:records) {
                if (r.get(r.size()-2) == i){
                    centroids.add(r);
                    i++;
                    continue;
                }
            }
            i++;
        }
    }

    private static void fillCentroidMap() {
        // creating k entries in hashmap, one for each centroid
        Double j = 1.0;
        for (ArrayList<Double> c: centroids) {
            centroidMap.put(c, j++);
            clusterMap.put(c, new ArrayList<>());
        }

        // To check how many clusters were there after random picking
//        for (ArrayList<Double> a: centroidMap.keySet()) {
//            System.out.println(centroidMap.get(a));
//        }
    }

    private static void evaluateCentroids(){
        for (ArrayList<Double> c: clusterMap.keySet()) {
            ArrayList<Double> new_c = new ArrayList<>();
            for(int j = 0; j < clusterMap.get(c).get(0).size(); j++){
                new_c.add(0.0);
            }
            for (ArrayList<Double> r: clusterMap.get(c)) {
                int i = 0;
                while (i < r.size() - 2){
                    new_c.set(i, new_c.get(i) + (r.get(i)/clusterMap.get(c).size()));
                    i++;
                }
            }
            centroids.add(new_c);
        }
    }

    private static void kMeans(){
        // running k means
        int size = records.get(0).size();
        boolean flag = true;
        int z = 0;
        do{
            z++;
            for (ArrayList<Double> r: records) {

                Double min_val = Double.MAX_VALUE;
                ArrayList<Double> selected_c = null;
                Double d;

                for (ArrayList<Double> c: centroids) {
                    d = distance(c, r);
                    if (d < min_val){
                        selected_c = c;
                        min_val = d;
                    }
                }
//                System.out.println(selected_c);
                clusterMap.get(selected_c).add(r);
//                r.set(size-1, centroidMap.get(selected_c));
            }

            if (z == 1000)
                break;

            centroids.clear();
            evaluateCentroids(); // to recompute new centroids by averaging the records assigned to each
            clusterMap.clear();
            centroidMap.clear();
            fillCentroidMap();

        } while(flag);
    }

    public static void main(String[] args) throws IOException {

        readFile();
        selectRandomCentroids();
        System.out.println("Random centroids");
        System.out.println(centroids);
        fillCentroidMap();
        kMeans();

//        for (ArrayList<Double> r: records) {
//            System.out.println(r);
//        }

        for (ArrayList<Double> k: clusterMap.keySet()) {
            System.out.println();
            System.out.println(k);
            System.out.println();
            for (ArrayList<Double> r: clusterMap.get(k)) {
                System.out.println(r.get(r.size()- 2));
            }
        }

    }
}
