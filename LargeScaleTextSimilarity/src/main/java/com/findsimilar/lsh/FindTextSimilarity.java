package com.findsimilar.lsh;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.ListUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.findsimilar.hash.Hasher;
import com.findsimilar.properties.ConstantProperties;

import scala.Tuple2;

public class FindTextSimilarity implements Serializable {

    private static final long serialVersionUID = 3671144356818113562L;

    private static int minhash;

    private static int numberOfBands;

    private static double threshold;

    private static List<Hasher> hashFunctions;

    private static List<List<Integer>> data;

    private JavaSparkContext sc;

    private List<String> filesNames;

    private static List<String> names;

    private static List<Integer> zIndex;

    public static void main(String[] args) {
        int minHash = 500;
        int rowPerBand = 5;
        int threshold = 60;
        FindTextSimilarity findTextSimilarity = new FindTextSimilarity(minHash, rowPerBand, threshold);
        long beginTime = System.currentTimeMillis();
        findTextSimilarity.performSimilarSearch();
        long endTime = System.currentTimeMillis() - beginTime;
        System.out.println("Total time spend is " + endTime);
    }

    /**
     * Constructor that take the parameters that will be used during the LSH
     * 
     * @param minhash
     *            the number of minhash function
     * @param rowsPerband
     *            the number of row in each band
     * @param threshold
     *            the threshold of similarity
     */
    public FindTextSimilarity(int minhash, int rowsPerband, double threshold) {
        String master = "local[*]";
        SparkConf conf = new SparkConf().setAppName("FIND_SIMILAR").setMaster(master);
        sc = new JavaSparkContext(conf);

        FindTextSimilarity.minhash = minhash;
        hashFunctions = generateHashFunction();
        filesNames = new ArrayList<>();
        data = loadData();
        zIndex = new ArrayList<>();
        for (int i = 0; i < filesNames.size(); i++) {
            zIndex.add(i);
        }
        FindTextSimilarity.numberOfBands = minhash / rowsPerband;
        FindTextSimilarity.threshold = threshold;
        names = filesNames;
    }

    /**
     * This method use the LSH method performs the LSH algorithm, detect and
     * save the similar documents.
     */
    public void performSimilarSearch() {

        /* Compute the data RDD */
        JavaPairRDD<List<Integer>, Long> dataRDD = sc.parallelize(data).zipWithIndex().cache();

        /* Compute the signature */
        JavaPairRDD<Tuple2<Long, Integer>, Integer> signature = dataRDD.flatMapToPair(tuple -> {
            Stream<Hasher> hash = hashFunctions.parallelStream();
            Stream<Tuple2<Tuple2<Long, Integer>, Integer>> minhs = hash.map(hasher -> {
                return new Tuple2<Tuple2<Long, Integer>, Integer>(
                        new Tuple2<Long, Integer>(tuple._2(), hasher.getIndex() % numberOfBands),
                        hasher.minHash(tuple._1()));
            });
            List<Tuple2<Tuple2<Long, Integer>, Integer>> minHashValues = minhs.collect(Collectors.toList());
            return minHashValues.iterator();
        }).cache();

        /*
         * Separate each signature into band and compute the hash value of each
         * document
         */
        JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Long>> bands = signature.groupByKey().mapToPair(tuple -> {
            return new Tuple2<Tuple2<Integer, Integer>, Long>(
                    new Tuple2<Integer, Integer>(tuple._1()._2(), Hasher.djbHashing(tuple._2())), tuple._1()._1());
        }).groupByKey().cache();

        /*
         * Find the candidate pairs documents
         */
        JavaPairRDD<Integer, Integer> candidatePair = bands.flatMapToPair(tuple -> {
            List<Integer> list = new ArrayList<>();
            for (long idVector : tuple._2()) {
                list.add((int) idVector);
            }
            List<Tuple2<Integer, Integer>> candidatesTuples = new ArrayList<>();
            for (int i = 0; i < list.size() - 1; i++) {
                for (int j = i + 1; j < list.size(); j++) {
                    candidatesTuples.add(new Tuple2<Integer, Integer>(Math.min(list.get(i), list.get(j)),
                            Math.max(list.get(i), list.get(j))));
                }
            }
            return candidatesTuples.iterator();
        }).distinct().cache();

        /*
         * Find the similar pair of documents by computing the jaccard
         * similarity between each pairs of candidate pair
         */
        JavaPairRDD<String, String> similarDoc = candidatePair.filter(tuple -> {
            int interSize = ListUtils.intersection(data.get(tuple._1()), data.get(tuple._2())).size();
            int unionSize = data.get(tuple._1()).size() + data.get(tuple._2()).size() - interSize;
            double jaccardSimilarity = ((double) interSize) / unionSize;
            System.out.println("Jaccard is " + jaccardSimilarity);
            if (jaccardSimilarity >= (threshold / 100)) {
                return true;
            } else {
                return false;
            }
        }).mapToPair(tuple -> {
            return new Tuple2<String, String>(names.get(tuple._1()), names.get(tuple._2()));
        });

        /* save the similar pair of documents */
        similarDoc.coalesce(1).saveAsTextFile(ConstantProperties.RESULT_PATH);
    }

    /*
     * Generate the minhash functions that will be used
     * 
     * @return a list of hasher that represent the minhash functions
     */
    private List<Hasher> generateHashFunction() {
        int[] A = new int[minhash];
        int[] B = new int[minhash];
        List<Hasher> hashFuns = new ArrayList<>();
        generateMinHashFunctionParameters(A, B);
        for (int i = 0; i < minhash; i++) {
            hashFuns.add(new Hasher(i, A[i], B[i]));
        }
        return hashFuns;
    }

    /*
     * Generate the minhash function. This function use a local inner class name
     * Combinaison in order to avoid duplication.
     */
    private void generateMinHashFunctionParameters(int[] A, int[] B) {
        class Combinaison {
            int a;
            int b;

            public Combinaison(int a, int b) {
                this.a = a;
                this.b = b;
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + a;
                result = prime * result + b;
                return result;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj)
                    return true;
                if (obj == null)
                    return false;
                if (getClass() != obj.getClass())
                    return false;
                Combinaison other = (Combinaison) obj;
                if (a != other.a)
                    return false;
                if (b != other.b)
                    return false;
                return true;
            }
        }

        List<Integer> primes = loadPrimes();

        Random random = new Random();
        int size = primes.size();
        int a, b;
        int n = 0;
        Set<Combinaison> combinaisons = new HashSet<>();
        Combinaison c;
        while (n < minhash) {
            a = random.nextInt(size);
            b = random.nextInt(size);
            c = new Combinaison(a, b);
            if (!combinaisons.contains(c)) {
                combinaisons.add(c);
                A[n] = a;
                B[n] = b;
                n++;
            }
        }
    }

    /*
     * This method load the primes numbers that I have previously generated.
     */
    private List<Integer> loadPrimes() {
        List<Integer> primes = new ArrayList<>();
        try {
            File f = new File(ConstantProperties.PRIMES_FILE_PATH);
            BufferedReader b = new BufferedReader(new FileReader(f));
            String readLine = "";
            int p;
            while ((readLine = b.readLine()) != null) {
                if (!readLine.isEmpty()) {
                    try {
                        p = Integer.parseInt(readLine);
                        primes.add(p);
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                }
            }
            b.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return primes;
    }

    /*
     * Load the set of tokens of each document
     */
    private List<List<Integer>> loadData() {
        List<List<Integer>> docsAsTokens = new ArrayList<>();
        try {
            Path dir = FileSystems.getDefault().getPath(ConstantProperties.INITIAL_DATA_PATH);
            DirectoryStream<Path> stream = Files.newDirectoryStream(dir);

            int i = 0;
            String shortName;
            for (Path path : stream) {
                if (!path.getFileName().toString().equals(".DS_Store")) {
                    System.out.println(++i);
                    filesNames.add(path.getFileName().toString());
                    // System.out.println(path.getFileName().toString());
                    shortName = path.getFileName().toString();
                    docsAsTokens.add(getSetOfDoc(shortName.substring(0, shortName.length() - 4)));
                }
            }
            stream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return docsAsTokens;
    }

    /*
     * Given a specific file name, return the set of tokens that represent that
     * document.
     */
    private List<Integer> getSetOfDoc(String fileName) throws IOException {
        List<Integer> tokens = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(
                new FileReader(ConstantProperties.DATA_AS_TOKENS_PATH + fileName + "/part-00000"))) {
            String line;
            int token;
            while ((line = br.readLine()) != null) {
                if (!line.isEmpty()) {
                    token = Integer.parseInt(line);
                    tokens.add(token);
                }
            }
        }
        return tokens;
    }

}
