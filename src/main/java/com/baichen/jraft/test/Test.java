package com.baichen.jraft.test;

import java.util.concurrent.locks.LockSupport;

public class Test {

    public static void main(String[] atg) throws InterruptedException {

        final Object blocker = new Object();
        Thread thread = new Thread(() -> {
            System.out.println("before block");
            synchronized (blocker) {
                try {
                    blocker.wait();
                } catch (InterruptedException e) {

                }
            }

            System.out.println("passed");

        });
        thread.start();

        Thread.sleep(5000);
        System.out.println(" unblocking......");
        synchronized (blocker){
            blocker.notifyAll();

        }

        Thread.sleep(5000);

    }
}
