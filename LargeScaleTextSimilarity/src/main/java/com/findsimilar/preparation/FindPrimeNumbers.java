package com.findsimilar.preparation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import com.findsimilar.properties.ConstantProperties;

/**
 * The purpose of this class is to find and save the primes numbers that we will
 * use as parameters for the universal hashing.
 * 
 * @author KAKANAKOU Miguel Stephane (Skakanakou@gmail.com)
 */
public class FindPrimeNumbers {

    public static void main(String[] args) {
        FindPrimeNumbers findPrimeNumbers = new FindPrimeNumbers();
        findPrimeNumbers.findAndSavePrime();
    }
    
    private List<Integer> primes;

    public FindPrimeNumbers() {
        primes = new ArrayList<>();
        primes.add(2);
    }
    
    /**
     * Find and save all the primes smaller than 100000
     */
    private void findAndSavePrime() {
        File file = new File(ConstantProperties.PRIMES_FILE_PATH);

        try {
            FileOutputStream fos = new FileOutputStream(file);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

            try {

                bw.write("1");
                bw.newLine();
                bw.write("2");
                bw.newLine();

                for (int i = 3; i < 100000; i += 2) {
                    if (isPrime(i)) {
//                        System.out.println(i);
                        primes.add(i);
                        bw.write("" + i);
                        bw.newLine();
                    }
                }

                bw.close();
                fos.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    /*
     * Detect if a specific number is prime of not
     */
    private boolean isPrime(int k) {
        for (int i : primes) {
            if (k % i == 0) {
                return false;
            }
            if (k / i < i) {
                break;
            }
        }
        return true;
    }
    
}
