package com.yyb.flink10.util1;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-20
 * @Time 16:09
 */
public class Demo {
    public static void main(String[] args){
        System.out.println(("LSJA24W96KS001123".hashCode() & Integer.MAX_VALUE )%1080 );
    }
}
