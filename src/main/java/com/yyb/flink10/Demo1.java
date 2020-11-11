package com.yyb.flink10;

import java.lang.reflect.Field;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-11-08
 * @Time 21:48
 */
public class Demo1 {
    public static void main(String[] args){
        Field[] fields = A.B.class.getDeclaredFields();
        for(Field field : fields){
            System.out.println(field.getName());
        }

        System.out.println(new C().a());

    }

    static class C{
        public String a(){
            return Demo1.getCallLocationName();
        }
    }

    public static String getCallLocationName() {
        return getCallLocationName(4);
    }

    public static String getCallLocationName(int depth) {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

        if (stackTrace.length <= depth) {
            return "<unknown>";
        }

        StackTraceElement elem = stackTrace[depth];

        return String.format("%s(%s:%d)", elem.getMethodName(), elem.getFileName(), elem.getLineNumber());
    }
}
