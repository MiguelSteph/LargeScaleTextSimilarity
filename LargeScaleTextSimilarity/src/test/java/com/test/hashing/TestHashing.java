package com.test.hashing;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.findsimilar.hash.Hasher;

public class TestHashing {

    @Test
    public void test() {
        String data = "hello world";
        assertEquals(1975908117, new Hasher(0, 1, 3).djbHashing(data));
    }
    
}
