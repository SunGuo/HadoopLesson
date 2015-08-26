package com.rongxin.hadoop.lesson5;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by guoxian1 on 15/4/16.
 */
public class TestRegex {

    public static void main(String [] args){
        Pattern p=Pattern.compile("^([^\\s]*) ([^\\s]*) ([^\\s]*) \\[([^\\s]*) ([^\\s]*)\\] \"([^\\s]*) ([^\\s]*) ([^\\s]*)\" ([^\\s]*) ([^\\s]*) \"([^\\s]*)\" \"([^\\s]*) (.*) (.*)\" (.*)$");
        Matcher m=p.matcher( "220.181.124.79 - - [15/Apr/2015:17:05:20 +0800] \"POST " +
                "/macqqversion.txt?sv=10.10.3&md=iMac12,1&h=8542B6915B785FFBB441FE2F09EB0ACD&v=2" +
                ".8.86.400&wc=0&imli=(null)&- HTTP/1.0\" 200 0 \"-\" \"pinback/1 CFNetwork/720.3" +
        ".13 Darwin/14.3.0 (x86_64)\" CTC");

        boolean rs = m.find();
        System.out.println(rs);
        System.out.println(m.groupCount());
        for(int i=1;i<=m.groupCount();i++){
            System.out.println(m.group(i));
        }
    }
}
