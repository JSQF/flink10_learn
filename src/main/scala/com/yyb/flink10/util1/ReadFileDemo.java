package com.yyb.flink10.util1;

import java.io.*;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-09-15
 * @Time 13:31
 */
public class ReadFileDemo {
    public static void main(String[] args) throws IOException {
        write();
    }

    public static void read() throws IOException {
        File file = new File("./xxx.text/1.txt");
        FileInputStream fis = new FileInputStream(file);
        byte[] bytes = new byte[1024];
        //文件保存的是 字符的 asi2 的码值
        //{65, 0, 0, ... ,0 }
        int len = 0;
        while((len = fis.read(bytes)) >0){
            System.out.println(bytes);
        }
    }

    public static void write() throws IOException{
        File file = new File("./xxx.text/2.txt");
        if(file.exists()){
            file.delete();
        }
        FileOutputStream fos = new FileOutputStream(file);
        String a = "A";
        byte[] bytes = a.getBytes(); //获取对应的 （ascii） 码值
        fos.write(bytes);

        byte[] xxx = new byte[]{65, 66, 67};
        fos.write(xxx);
    }
}
