import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.*;


public class KMeansClustering {
    public static HashMap<Integer, List<Point>> cluster_points = new HashMap<Integer, List<Point>>();
    private static int numPoints = 300;
    private static int numClusters = 10;
    private static int MAX_ITER = 1000;
    private static List<Point> points; 
    private static List<Point> clusters;
    
    public static void main(String[] args) {
        points = generateRandomPoints(numPoints);
        clusters = clusterPoints(points, numClusters, MAX_ITER);
        // points = getPoints();

        for (int i = 0; i < clusters.size(); i++) {
            System.out.println("Cluster " + i + ": " + clusters.get(i));
        }
        int[] clusterSize = new int[10];
        Arrays.fill(clusterSize, 0);
        for (int i : clusterSize) {
            System.out.print(i+ " ");
            System.out.println();
        }
        for (int i =0; i < points.size(); i++) {
            System.out.println("Point " + i +": " + points.get(i) + " Cluster - " +points.get(i).clusterID);
            clusterSize[points.get(i).clusterID] ++;
        }
        for (int i : clusterSize) {
            System.out.print(i+ " ");
            System.out.println();
        }
        createClusterPoints(points);
        Point closestTo100 = findClosestPointInCluster(points.get(100), points.get(100).clusterID, cluster_points);
        System.out.println(closestTo100);
        // for (Map.Entry<Integer, List<Point>> entry : cluster_points.entrySet()) { // iterate over the key-value pairs
        //     Integer key = entry.getKey(); // get the key
        //     List<Point> value = entry.getValue(); // get the value
            
        //     System.out.print(key + " => ["); // print the key and start of value
        //     for (Point p : value) { // iterate over the List of Objects
        //         System.out.print(p + ", "); // print each Object
        //     }
        //     System.out.println("]"); // end of value
        // }
        
        
    }

    private static Point findClosestPointInCluster(Point point, int cluster_id, HashMap<Integer, List<Point>> map){
        List<Point> clusterPoints = map.get(cluster_id);
        Point closestPoint = clusterPoints.get(0);    
        double minDist = Double.MAX_VALUE;
        for(Point p: clusterPoints){
            double dist = point.distance(p);
            if (dist < minDist && dist !=0) {
                minDist = dist;
                closestPoint = p;
            }
        }
        return closestPoint;
    }

    private static void createClusterPoints(List<Point> points){

        List<Point>[] cluster_list = new ArrayList[numClusters];
        for (int i = 0; i < numClusters; i++) {
            cluster_list[i] = new ArrayList<Point>(); // create a new array with 10 elements
        }

        for(Point p: points){
            cluster_list[p.clusterID].add(p);
        }
        for(int i = 0; i< numClusters; i++){
            cluster_points.put(i, cluster_list[i]);
        }
    }

    private static List<Point> generateRandomPoints(int numPoints) {
        Random rand = new Random();
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < numPoints; i++) {
            double x = rand.nextDouble();
            double y = rand.nextDouble();
            points.add(new Point(x, y));
        }
        return points;
    }

    // public static void getPoints(){
    //     List<Point> points = new ArrayList<>();
    //     int numPoints = 10000;
    //     // Access the similarity metric from the files

    //     // Generate points;
    //     for (int i = 0; i < numPoints; i++) {
    //         double x = getSimilarityMetricReview();
    //         double y = getSimilarityMetricCategories();
    //         points.add(x, y)
    //     }
    // }

    private static List<Point> clusterPoints(List<Point> points, int numClusters, int MAX_ITER) {
        Random rand = new Random();
        // Create clusters
        List<Point> clusters = new ArrayList<>();
        for (int i = 0; i < numClusters; i++) {
            double x = rand.nextDouble();
            double y = rand.nextDouble();
            clusters.add(new Point(x, y));
        }
        // Cluster the points
        int iter = 0;
        while (true) {
            List<List<Point>> clustersList = new ArrayList<>(numClusters);
            for (int i = 0; i < numClusters; i++) {
                clustersList.add(new ArrayList<>());
            }
            for (Point point : points) {
                int nearestCluster = findNearestCluster(point, clusters);
                clustersList.get(nearestCluster).add(point);
            }
            List<Point> newClusters = new ArrayList<>();
            for (List<Point> cluster : clustersList) {
                if (cluster.isEmpty()) {
                    newClusters.add(clusters.get(rand.nextInt(numClusters)));
                } else {
                    newClusters.add(calculateMean(cluster));
                }
            }
            iter++;
            if (newClusters.equals(clusters) || iter > MAX_ITER) {
                break;
            } else {
                clusters = newClusters;
            }
        }
        return clusters;
    }

    // private static Point findNearestPoint(Point point, )

    private static int findNearestCluster(Point point, List<Point> clusters) {
        double minDist = Double.MAX_VALUE;
        int nearestCluster = -1;
        for (int i = 0; i < clusters.size(); i++) {
            double dist = point.distance(clusters.get(i));
            if (dist < minDist) {
                minDist = dist;
                nearestCluster = i;
            }
        }
        point.addToCluster(nearestCluster);
        return nearestCluster;
    }

    private static Point calculateMean(List<Point> points) {
        double sumX = 0.0;
        double sumY = 0.0;
        for (Point point : points) {
            sumX += point.x;
            sumY += point.y;
        }
        double meanX = sumX / points.size();
        double meanY = sumY / points.size();
        return new Point(meanX, meanY);
    }

    private static class Point {
        double x;
        double y;
        int clusterID;

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public double distance(Point other) {
            double dx = x - other.x;
            double dy = y - other.y;
            return Math.sqrt(dx * dx + dy * dy);
        }

        public void addToCluster(int clusterID){
            this.clusterID = clusterID;
        }

        @Override
        public String toString() {
            return "(" + x + ", " + y + ")";
        }
    }
}