package com.cw.bigdata.java;

import java.util.Random;

/**
 * @author 陈小哥cw
 * @date 2020/7/8 15:41
 */
public class TestRandom {
    public static void main(String[] args) {
        Random random = new Random(10);

        for (int i = 0; i < 10; i++) {
            System.out.println(random.nextInt());
        }

        System.out.println("************************");

        // 种子相同，生成的随机数也相同
        random = new Random(10);
        for (int i = 0; i < 10; i++) {
            System.out.println(random.nextInt());
        }

        System.out.println("*************************");
        // 一般种子填写当前的时间戳
        random=new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            System.out.println(random.nextInt());
        }


        System.out.println("*************************");

        random=new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            System.out.println(random.nextInt());
        }


    }
}
