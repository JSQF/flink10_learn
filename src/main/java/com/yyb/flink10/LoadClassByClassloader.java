package com.yyb.flink10;

import com.yyb.flink10.util.GeneratorClassByASM;
import net.sf.cglib.core.ReflectUtils;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-12
 * @Time 13:44
 */
public class LoadClassByClassloader {
    public static void main(String[] args) throws Exception {
        String packageName = "com.yyb.flink10.xxx.";
        String className = "Pi";
        byte[] byteOfClass = GeneratorClassByASM.geneClassMain(packageName, className);
        Class pi = ReflectUtils.defineClass(packageName + className, byteOfClass, LoadClassByClassloader.class.getClassLoader());
        Class<?> xx = Class.forName(packageName + className);
        System.out.println(xx.newInstance());
    }
}
