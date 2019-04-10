package edu.buffalo.cse.cse486586.simpledht;

import java.io.File;

public class Testing {
    public static void main(String args[])
    {
/*        String msg = "17darshan221";
        int keylengthIndex = 1,keySize = Character.getNumericValue(msg.charAt(keylengthIndex)), keyStartIndex = keylengthIndex + 1, vallengthIndex = keyStartIndex+keySize;
        int valSize = Character.getNumericValue(msg.charAt(vallengthIndex)),valStartIndex = vallengthIndex+1;
        String key= msg.substring(keyStartIndex,keyStartIndex + keySize),value = msg.substring(valStartIndex, valStartIndex + valSize);
        System.out.print("Key = " + key+ "Value = " + value);*/

        File path = new File(".");

        File [] files = path.listFiles();
        for (int i = 0; i < files.length; i++){
            if (files[i].isFile()){
                System.out.println(files[i]);
            }
        }
    }
}
