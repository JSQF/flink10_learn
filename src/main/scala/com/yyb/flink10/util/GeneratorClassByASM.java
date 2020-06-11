package com.yyb.flink10.util;

import com.yyb.flink10.table.blink.stream.kafka.ReadDataFromKafkaConnectorJava;
import scala.tools.asm.ClassWriter;
import scala.tools.asm.MethodVisitor;
import scala.tools.asm.Opcodes;
import scala.tools.asm.Type;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

import static scala.tools.asm.Opcodes.*;

/**
 * 解析配置 利用ASM技术 及时 生成 Class 对象
 * 注意 Class.forname 与 ClassLoader.loadClass 的区别
 *  Class.forname 用的是 jvm 的 native 方法产生的，
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-10
 * @Time 12:32
 */
public class GeneratorClassByASM {
    private static Map<Class, Integer> mappingReturns = new HashMap<>();
    private static Map<Class, Integer> mappingLoads = new HashMap<>();
    public static MyClassLoader cl = new MyClassLoader();
    static {
        mappingReturns.put(Integer.class, IRETURN);
        mappingLoads.put(Integer.class, ILOAD);

        mappingReturns.put(int.class, IRETURN);
        mappingLoads.put(int.class, ILOAD);

        mappingReturns.put(String.class, ARETURN);
        mappingLoads.put(String.class, ALOAD);

        mappingReturns.put(Long.class, LRETURN);
        mappingLoads.put(Long.class, LLOAD);

        mappingReturns.put(long.class, IRETURN);
        mappingLoads.put(long.class, ILOAD);

        mappingReturns.put(Double.class, DRETURN);
        mappingLoads.put(Double.class, DLOAD);

        mappingReturns.put(double.class, DRETURN);
        mappingLoads.put(double.class, DLOAD);

        mappingReturns.put(Float.class, FRETURN);
        mappingLoads.put(Float.class, FLOAD);

        mappingReturns.put(float.class, FRETURN);
        mappingLoads.put(float.class, FLOAD);
    }

    private static byte[] generate(){
        ClassWriter cw = new ClassWriter(0);
        // 定义对象头：版本号、修饰符、全类名、签名、父类、实现的接口
        cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC, "com/yyb/flink10/commonEntity/HelloWorld",
                null, "java/lang/Object", null);
        // 添加方法：修饰符、方法名、描述符、签名、抛出的异常
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC + Opcodes.ACC_STATIC, "main",
                "([Ljava/lang/String;)V", null, null);
        // 执行指令：获取静态属性
        mv.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
        // 加载常量 load constant
        mv.visitLdcInsn("HelloWorld!");
        // 调用方法
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V", false);
        // 返回
        mv.visitInsn(Opcodes.RETURN);
        // 设置栈大小和局部变量表大小
        mv.visitMaxs(2, 1);
        // 方法结束
        mv.visitEnd();
        // 类完成
        cw.visitEnd();
        // 生成字节数组
        return cw.toByteArray();
    }

    /**
     * ASM Class 的  set 方法
     * @param cw
     * @param fieldName
     * @param fileType
     * @param packageName 这里是 包 路径 /../../
     * @param className
     */
    private static void generatorSetMethod(ClassWriter cw, String fieldName, Class fileType, String packageName, String className){
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC , "set" + initUpper(fieldName),
                "("+ Type.getType(fileType).getDescriptor() +")V", null, null);
        mv.visitVarInsn(mappingLoads.get(fileType), 0);
        mv.visitVarInsn(mappingLoads.get(fileType), 1);
        mv.visitFieldInsn(Opcodes.PUTFIELD, packageName + className, fieldName, Type.getType(fileType).getDescriptor());
        mv.visitInsn(RETURN);
        mv.visitMaxs(2, 2);
        mv.visitEnd();
    }

    /**
     * ASM Class 的  get 方法
     * @param cw
     * @param fieldName
     * @param fileType
     * @param packageName 这里是 包 路径 /../../
     * @param className
     */
    private static void generatorGetMethod(ClassWriter cw, String fieldName, Class fileType, String packageName, String className){
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC , "get" + initUpper(fieldName),
                "()" + Type.getType(fileType).getDescriptor(), null, null);
        mv.visitVarInsn(mappingLoads.get(fileType), 0);
        mv.visitFieldInsn(GETFIELD, packageName  + className, fieldName, Type.getType(fileType).getDescriptor());
        mv.visitInsn(mappingReturns.get(fileType));
        mv.visitMaxs(1,1);
        mv.visitEnd();
    }

    /**
     * ASM Class 的无参构造方法
     * @param cw
     */
    private static void generatorInitmethos(ClassWriter cw){
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC , "<init>",
                "()V" , null, null);
        mv.visitVarInsn(ALOAD, 0);
        mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        mv.visitInsn(RETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();
    }

    /**
     * ASM Class 的  属性字段
     * @param cw
     * @param fieldName
     * @param fileType
     */
    private static void generatorFields(ClassWriter cw, String fieldName, Class fileType){
        cw.visitField(Opcodes.ACC_PRIVATE, fieldName, Type.getType(fileType).getDescriptor(), null, null);
    }

    private static String initUpper(String fieldName){
        return fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
    }

    private static byte[] geneClassMain(String packageName, String className){
        String packageName1 = packageName.replace(".", "/");
        System.out.println(packageName1);
        ClassWriter cw = new ClassWriter(0);
        // 定义对象头：版本号、修饰符、全类名、签名、父类、实现的接口
        cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC, packageName1 + className,
                null, "java/lang/Object", null);
        generatorInitmethos(cw);
        generatorFields(cw, "id", String.class);
        generatorFields(cw, "time", String.class);
        generatorSetMethod(cw, "id", String.class, packageName1, className);
        generatorGetMethod(cw, "id", String.class, packageName1, className);
        generatorSetMethod(cw, "time", String.class, packageName1, className);
        generatorGetMethod(cw, "time", String.class, packageName1, className);
        cw.visitEnd();
        return cw.toByteArray();
    }

    /**
     * 自定义ClassLoader以支持加载字节数组形式的字节码
     */


    public Class run(){
        byte[] bytes = generate();
        Class<?> clazz = cl.defineClass("com.yyb.flink10.commonEntity.HelloWorld", bytes);
        return clazz;
    }

    public  void run1() throws Exception {
//        String packageName = "com.yyb.flink10.xxx.";
//        String className = "User";
//        byte[] bytes = geneClassMain(packageName, className);
//        Class<?> clazz = cl.defineClass(packageName + className, bytes);
//        System.out.println(clazz.getName());
//        String method = (String)clazz.getDeclaredMethod("getId", null).invoke(clazz.newInstance(), null);
//        clazz.getClassLoader().loadClass("com.yyb.flink10.xxx.User").newInstance();
        MyThread myThread = new MyThread(cl);
        Thread thread = new Thread(myThread);
        thread.start();
        thread.join();

    }

    public static Class getPiClass(String packageName, String className) throws ClassNotFoundException, Exception {
        byte[] bytes = geneClassMain(packageName, className);
        System.out.println("getPiClass Thread.currentThread() :" + Thread.currentThread().getContextClassLoader());
        System.out.println("getPiClass:"+ cl);
        Class<?> clazz = cl.defineClass(packageName + className, bytes);

        String path = Thread.currentThread().getContextClassLoader().getResource("").toString().substring(5);
        System.out.println("xdsvrf:" + path);
        File one = new File(path + packageName.replace(".", "/"));
        if(!one.exists()){
            one.mkdirs();
        }
        File file  = new File(path + packageName.replace(".", "/") + className + ".class");
        System.out.println("xxx:" + file.getAbsolutePath());
        if(file.exists()){
            file.delete();
        }
        FileOutputStream fo = new FileOutputStream(file);
        fo.write(bytes);
        return clazz;
    }


    public static void main(String[] args) throws Exception {
//        GeneratorClassByASM generatorClassByASM = new GeneratorClassByASM();
//        generatorClassByASM.run1();

//        GeneratorClassByASM.getPiClass();

        System.out.println(Thread.currentThread().getContextClassLoader().getResource("").toString());

    }

    class MyThread implements Runnable{
        private ClassLoader classLoader;
        public MyThread(ClassLoader classLoader){
            this.classLoader = classLoader;
            Thread.currentThread().setContextClassLoader(this.classLoader);
        }
        @Override
        public void run() {
            try {
                System.out.println("Thread.currentThread() :" + Thread.currentThread().getContextClassLoader());
                System.out.println( "getClass : " + getClass().getClassLoader());
                ReadDataFromKafkaConnectorJava xxx = (ReadDataFromKafkaConnectorJava)this.classLoader
                        .loadClass("com.yyb.flink10.table.blink.stream.kafka.ReadDataFromKafkaConnectorJava").newInstance();

                xxx.main(new String[]{});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class MyClassLoader extends ClassLoader {
        public Class<?> defineClass(String name, byte[] b) {
            // ClassLoader是个抽象类，而ClassLoader.defineClass 方法是protected的
            // 所以我们需要定义一个子类将这个方法暴露出来
            return super.defineClass(name, b, 0, b.length);
        }

    }
}
