package com.rongxin.hadoop.lesson4.shuffle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by guoxian1 on 15/5/24.
 */
public class TestShuffle {

    public static void main(String [] args){
        List<Integer> list = new ArrayList<Integer>();
        list.add(2);
        list.add(1);
        list.add(4);
        list.add(5);
        for(Integer o : list){
            System.out.println(o);
        }
        System.out.println("#######");
        Collections.shuffle(list);

        for(Integer o : list){
            System.out.println(o);
        }
    }

}
