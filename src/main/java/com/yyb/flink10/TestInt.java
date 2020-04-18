package com.yyb.flink10;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-04-18
 * @Time 08:40
 */
public class TestInt {
    public static void main(String[] args){
        int[]  a = new int[2];
        a[0] = 1;
        a[1] = 2;
        test(a);
//        test1(a);
        System.out.println(a);
    }

    /**
     * 可变类型定义的是 Object，但是 传入的是 java 基本类型的数组
     * 为了保持一致，java 会把 基本类型的数组 整个看作一个 object （所以 length = 1）
     * @param a
     */
    public  static void test(Object ... a){
        System.out.println(a.length);
        System.out.println(a);
        for(int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
    }

    public  static void test1(Object[] a){
        System.out.println(a.length);
        System.out.println(a);
        for(int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
    }
}
