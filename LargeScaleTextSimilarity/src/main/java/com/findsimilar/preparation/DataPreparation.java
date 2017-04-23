package com.findsimilar.preparation;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.findsimilar.hash.Hasher;
import com.findsimilar.properties.ConstantProperties;

/**
 * This class use spark to convert each document in our data into k-shingle and
 * then hash each k-shingle into token. Like that, each document is converted
 * into a set of tokens.
 * 
 * @author KAKANAKOU Miguel Stephane (Skakanakou@gmail.com)
 */
public class DataPreparation {

    private JavaSparkContext sc;

    /**
     * the parameter k of k-shingles
     */
    private int k;

    /**
     * Main method to launch the conversion of the documents into tokens
     * 
     * @param args
     */
    public static void main(String[] args) {
        int k = 2;
        DataPreparation dataPreparation = new DataPreparation(k);
        long beginTime = System.currentTimeMillis();
        dataPreparation.preparationStep();
        long endTime = System.currentTimeMillis() - beginTime;
        System.out.println("Total time spend is " + endTime);
    }

    /**
     * Constructor that take the number of k-shingles
     * 
     * @param k
     */
    public DataPreparation(int k) {
        this.k = k;
        String master = "local[*]";
        SparkConf conf = new SparkConf().setAppName("FIND_SIMILAR").setMaster(master);
        sc = new JavaSparkContext(conf);
    }

    /**
     * Iterate through the data folder and convert each documents into tokens
     */
    public void preparationStep() {
        try {
            Path dir = FileSystems.getDefault().getPath(ConstantProperties.INITIAL_DATA_PATH);
            DirectoryStream<Path> stream = Files.newDirectoryStream(dir);

            int i = 0;

            for (Path path : stream) {
                if (!path.getFileName().toString().equals(".DS_Store")) {
                    System.out.println(++i);
                    String fileName = path.toString();
                    String shortName = path.getFileName().toString();
                    convertToTokens(fileName, k, shortName.substring(0, shortName.length() - 4));
                }
            }
            stream.close();

            System.out.println("End of the convertion to tokens");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Convert a specific file into tokens and saved it.
     * 
     * @param fileName
     * @param k
     * @param fileShortName
     */
    public void convertToTokens(String fileName, int k, String fileShortName) {
        JavaRDD<String> inputFile = sc.textFile(fileName);
        JavaRDD<Integer> tokensFile = inputFile.map(input -> input.replaceAll("\\s+", " ").trim()).flatMap(input -> {
            String[] words = input.split(" ");
            List<Integer> tokens = new ArrayList<>(words.length);
            StringBuilder strBuilder;
            for (int i = 0; i < words.length - k; i++) {
                strBuilder = new StringBuilder();
                for (int j = 0; j < k - 1; j++) {
                    strBuilder.append(words[i + j]);
                    strBuilder.append(" ");
                }
                strBuilder.append(words[i + k - 1]);
                tokens.add(Hasher.djbHashing(strBuilder.toString()));
            }
            return tokens.iterator();
        }).distinct();
        tokensFile.coalesce(1).saveAsTextFile(ConstantProperties.DATA_AS_TOKENS_PATH + fileShortName);
    }

}
