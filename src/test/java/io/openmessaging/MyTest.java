package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * @author pengqun.pq
 */
public class MyTest {

    private static byte[][] data =  {
            {1, 2, 3},
            {1, 2, 2}
    };

    public static void main(String args[]) {
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < data[i].length; j++) {
                System.out.print(data[i][j] + " ");
            }
            System.out.println("");
        }

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(10240);

        for (long i = 0; i <= 256; i++) {
            byteBuffer.put((byte) i);
        }

        byteBuffer.flip();
        for (long i = 0; i <= 256; i++) {
            long r = byteBuffer.get() & 0xff;
            System.out.println(r);
        }
    }
}
