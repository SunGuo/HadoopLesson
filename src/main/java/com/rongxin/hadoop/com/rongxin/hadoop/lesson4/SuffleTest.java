package com.rongxin.hadoop.com.rongxin.hadoop.lesson4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

/**
 * Created by guoxian1 on 15/3/29.
 */
public class SuffleTest {

    public static void main(String [] args){
        ArrayList<String> list1 = new ArrayList<String>();
        list1.add("a");
        list1.add("b");
        list1.add("c");
      //  System.out.println(list1.toString());

        for(String o: list1){
            System.out.println(o);
        }
        System.out.println("..................");
        Collections.shuffle(list1);

        for(String o:list1){
            System.out.println(o);
        }
    }
}
