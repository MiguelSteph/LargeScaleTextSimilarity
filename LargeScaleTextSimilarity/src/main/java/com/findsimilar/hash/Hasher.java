package com.findsimilar.hash;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

/**
 * This class contains our hash methods. Here we use universal hashing h(k) =
 * (ak + b)mod p and also the DJB2 hashing method for string and for list
 * 
 * @author KAKANAKOU Miguel Stephane (Skakanakou@gmail.com)
 */
public class Hasher implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The index of the hash function
     */
    private final int index;

    /**
     * Parameter of the hash function
     */
    private final int a;

    /**
     * Parameter of the hash function
     */
    private final int b;

    public Hasher(int index, int a, int b) {
        this.index = index;
        this.a = a;
        this.b = b;
    }

    public int getIndex() {
        return index;
    }

    /**
     * Compute the minhash of a list. It fisrt hash each integer in the list and
     * then return the minimum hash value.
     * 
     * @param tokensVectors
     *            List of integer
     * @return return the minhash of the given list of integer.
     */
    public int minHash(List<Integer> tokensVectors) {
        Stream<Integer> stream = tokensVectors.stream();
        return stream.mapToInt(token -> hash(token)).min().getAsInt();
    }

    /**
     * Use the DJB2 method to hash the given string.
     * 
     * @param str
     *            String to hash
     * @return an integer that represent the hash value.
     */
    public int djbHashing(String str) {
        long hash = 5381;
        for (int i = 0; i < str.length(); i++) {
            hash = ((hash << 5) + hash) + str.charAt(i);
        }
        int hashVal = (int) (hash ^ (hash >>> 32));
        return (hashVal & 0x7FFFFFFF);
    }

    /**
     * Hash the given integer
     * @param val
     * @return
     */
    public int hash(int val) {
        int hash = a * val + b;
        return (hash & 0x7FFFFFFF);
    }

    /**
     * Hash the given array of integer
     * @param band
     * @return
     */
    public int djbHashing(int[] band) {
        long hash = 5381;

        for (int i : band) {
            hash = ((hash << 5) + hash) + i;
        }

        int hashVal = (int) (hash ^ (hash >>> 32));
        return (hashVal & 0x7FFFFFFF);
    }

    /**
     * 
     * @param minHashVal
     * @return
     */
    public static int djbHashing(Iterable<Integer> minHashVal) {
        long hash = 5381;

        for (int i : minHashVal) {
            hash = ((hash << 5) + hash) + i;
        }

        int hashVal = (int) (hash ^ (hash >>> 32));
        return (hashVal & 0x7FFFFFFF);
    }

}
