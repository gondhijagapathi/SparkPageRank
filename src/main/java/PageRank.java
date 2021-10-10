import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import scala.Tuple2;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by tab(s).
 */
public final class PageRank {
    private static final Pattern TABS = Pattern.compile("\t");
    private static final HashMap<String,Integer> inLinksCount = new HashMap<>();
    private static final HashMap<String,Integer> outLinksCount = new HashMap<>();

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaPageRank")
                .config("spark.master", "local")
                .getOrCreate();

        String inputPath = "/Users/jg067071/Downloads/PR_Data-1.txt";
        String outputDir = "/Users/jg067071/Downloads/out";
        String newInputPath = convertInputFile(inputPath);

        // Loads in input file. It should be in format of:
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     ...
        JavaRDD<String> lines = spark.read().textFile(newInputPath).javaRDD();

        // Loads all URLs from input file and initialize their neighbors.
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
            String[] parts = TABS.split(s);
            return new Tuple2<>(parts[0], parts[1]);
        }).distinct().groupByKey().cache();

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        // Calculates URL contributions to the rank of other URLs.
        JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                .flatMapToPair(s -> {
                    int urlCount = Iterables.size(s._1());
                    List<Tuple2<String, Double>> results = new ArrayList<>();
                    for (String n : s._1) {
                        results.add(new Tuple2<>(n, s._2() / urlCount));
                    }
                    return results.iterator();
                });

        // Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 1 + sum * 0.85);

        // Collects all URL ranks and dump them to console.
        List<Tuple2<String, Double>> output = ranks.collect();
        LinkedHashMap<String,Double> linkRanks = new LinkedHashMap<>();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
            linkRanks.put((String) tuple._1(),(Double) tuple._2());
        }

        spark.stop();

        //sort linkRanks
        linkRanks = linkRanks.entrySet().stream().sorted(comparingByValue(Comparator.reverseOrder()))
                .limit(500)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

        //count inlinks and outlinks
        generateInAndOutLinks(inputPath);

        //write to output file
        generateOutput(linkRanks,outputDir);
    }

    //convert input file to required spark format
    /* example of spark required formate
    p1 p200
    p1 p201
    p2 p300
    p2 p303
     */
    private static String convertInputFile(String path) {
        //second input file which contains format needed for spark
        String newInput = path+System.lineSeparator()+"in.txt";
        try {
            FileInputStream fstream = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            File myObj = new File(newInput);
            FileWriter myWriter = new FileWriter(myObj);


            String strLine;

            while ((strLine = br.readLine()) != null) {
                String pageName = strLine.split("\t")[0];
                String[] outlinks = strLine.split("\t")[1].split(",");

                for (String outLink : outlinks) {
                    myWriter.write(pageName + "\t" + outLink);
                    myWriter.write("\n");
                }
            }
            myWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return newInput;
    }

    /**
     * generates output file
     */
    public static void generateOutput(LinkedHashMap<String,Double> sortedPages, String out){
        try {
            File myObj = new File(out + File.separator+"sparkOutput.txt");
            FileWriter myWriter = new FileWriter(myObj);
            myWriter.write("Page Name\tPage Rank\tout links\tin links\n");
            myWriter.write("__________________________________________\n\n");
            for (Map.Entry<String,Double> entry:sortedPages.entrySet()) {
                myWriter.write(entry.getKey()+"\t"+entry.getValue()+"\t"+outLinksCount.get(entry.getKey())+"\t"+inLinksCount.get(entry.getKey()));
                myWriter.write("\n");
            }
            myWriter.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void generateInAndOutLinks(String path){
        try{
            FileInputStream fstream = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String strLine;

            while ((strLine = br.readLine()) != null) {

                String[] outlinks = strLine.split("\t")[1].split(",");
                outLinksCount.put(strLine.split("\t")[0],outlinks.length);

                // p1  p2,p3,p4
                // hashmap => p1 3
                // inlink hashmap p2 = 3 , p3 =1 , p4 = 1
                for (String pageNum: outlinks) {
                    if (!inLinksCount.containsKey(pageNum)){
                        inLinksCount.put(pageNum , 1);
                    }else{
                        inLinksCount.put(pageNum , inLinksCount.get(pageNum)+1);
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}