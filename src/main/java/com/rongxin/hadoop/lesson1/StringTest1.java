package com.rongxin.hadoop.lesson1;

/**
 * Created by guoxian1 on 15/5/16.
 */
public class StringTest1 {

    public static void main(String [] args){
        String t = "hello";
        String t1 = "hello";

        String t2 = new String("hello");

        System.out.println((t == "hello"));
        System.out.println(t.equals("hello"));

        System.out.println((t == t1));
        System.out.println(t.equals(t1))        ;

        System.out.println((t == t2));
        System.out.println(t.equals(t2));


    }
}
