package io.openmessaging;

/**
 * @author .ignore 2019/8/25
 */
public class MyTest {

    private static byte[][] data =  {
            {1, 2, 3},
            {1, 2, 2}
    };

    public static void main(String args[]) throws Exception {
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < data[i].length; j++) {
                System.out.print(data[i][j] + " ");
            }
            System.out.println("");
        }
    }
}
