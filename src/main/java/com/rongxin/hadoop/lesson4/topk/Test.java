package com.rongxin.hadoop.lesson4.topk;

import java.util.Iterator;
import java.util.TreeMap;

/**
 * Created by guoxian1 on 15/4/2.
 */

public class Test {

    public static void main(String [] args){
//
//        TreeMap<Integer, String> tmp = new TreeMap<Integer, String>();
//
//        tmp.put(1, "1");
//        tmp.put(1, "2");
//        tmp.put(2, "3");
//        tmp.put(5, "5");
//        tmp.put(4, "4");
//
//        Iterator<Integer> iter = tmp.keySet().iterator();
//
//        while(iter.hasNext()){
//            Integer key = iter.next();
//            System.out.println("key :" + key + ", value:" + tmp.get(key));
//        }

//        String tmp1 = new String("maolijuan");
//
//        String tmp2 = "maolijuan";
//        String tmp3 = "maolijuan";
//
//        System.out.println(tmp2 == tmp3);  //true
//        System.out.println(tmp2.equals(tmp3)); //true
//
//
//        System.out.println((tmp1 == tmp2)); //false
//        System.out.println(tmp1.equals(tmp2)); //true
//        System.out.println(tmp1.compareTo(tmp2)); // 0



        IpCountEntity ip1 = new IpCountEntity(1, "10.100.10.1");
        IpCountEntity ip2 = new IpCountEntity(1, "10.100.10.2");
        IpCountEntity ip3 = new IpCountEntity(3, "10.100.10.4");
        IpCountEntity ip4 = new IpCountEntity(2, "10.100.10.3");
        IpCountEntity ip5 = new IpCountEntity(10, "10.100.10.10");
        IpCountEntity ip6 = new IpCountEntity(9, "10.100.10.9");

        TreeMap<IpCountEntity, String> tmp1 = new TreeMap<IpCountEntity, String>();

        tmp1.put(ip1, "1");
        tmp1.put(ip2, "2");
        tmp1.put(ip3, "3");
        tmp1.put(ip4, "3");
        tmp1.put(ip5, "5");
        tmp1.put(ip6, "6");

        System.out.println(tmp1.size());
        System.out.println(ip1.hashCode() + " "+  ip2.hashCode());
        System.out.println(tmp1.firstKey().getIp());

        Iterator<IpCountEntity> iter1 = tmp1.keySet().iterator();

        while(iter1.hasNext()){
            IpCountEntity key = iter1.next();
            System.out.println("k: " + key.getPv() + " v: " + key.getIp());
        }

    }
}
