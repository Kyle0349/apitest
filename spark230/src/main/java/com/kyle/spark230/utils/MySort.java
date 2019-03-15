package com.kyle.spark230.utils;

import java.io.Serializable;
import java.util.Comparator;

public class MySort implements Serializable, Comparator<Integer> {


    @Override
    public int compare(Integer o1, Integer o2) {
        return o2 - o1 ;
    }
}
