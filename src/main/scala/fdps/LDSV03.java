package fdps;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

;

public class LDSV03 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter 05").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        System.out.println("Running Spark Version : " +ctx.version());
        ctx.addFile("data/Line_of_numbers.csv");
        //
        JavaRDD<String> lines = ctx.textFile(SparkFiles.get("Line_of_numbers.csv"));
        //
        JavaRDD<String[]> numbersStrRDD = lines.map(new Function<String,String[]>() {
            public String[] call(String line) {return line.split(",");}
        });
        List<String[]> val = numbersStrRDD.take(1);
        for (String[] e : val) {
            for (String s : e) {
                System.out.print(s+" ");
            }
            System.out.println();
        }
        //

    }
}