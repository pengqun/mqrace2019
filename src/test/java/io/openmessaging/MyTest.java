package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * @author .ignore 2019/8/25
 */
public class MyTest {

    private static byte[][] data =  {
            {1, 2, 3},
            {1, 2, 2}
    };

    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1,
            1800L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1), new ThreadPoolExecutor.CallerRunsPolicy());

    static ForkJoinPool commonPool = ForkJoinPool.commonPool();

    static ForkJoinPool myPool = new ForkJoinPool(20);

    public static void main(String args[]) throws Exception {

            System.out.println(commonPool.getParallelism());
        System.out.println(myPool.getParallelism());

//        threadPoolExecutor.execute(() -> {
//            System.out.println("running start");
//            LockSupport.parkNanos(5_000_000_000L);
//            System.out.println("running end");
//        });
//
//        threadPoolExecutor.execute(() -> {
//            System.out.println("running 2");
//        });
//
//        threadPoolExecutor.execute(() -> {
//            System.out.println("running 3");
//        });

//        for (int i = 0; i < data.length; i++) {
//            for (int j = 0; j < data[i].length; j++) {
//                System.out.print(data[i][j] + " ");
//            }
//            System.out.println("");
//        }

//        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(10240);
//
//        for (long i = 0; i <= 256; i++) {
//            byteBuffer.put((byte) i);
//        }
//
//        byteBuffer.flip();
//        for (long i = 0; i <= 256; i++) {
//            long r = byteBuffer.get() & 0xff;
//            System.out.println(r);
//        }
    }
}
