package org.example.drs;

import org.example.drs.index.IndexDriver;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
        if(args.length < 2) {
            System.out.println("Invalid parameters!");
            System.exit(-1);
        }

        if(args[0].equals("index")) {
            boolean pathExist = IndexDriver.run(args[1]);
            if(!pathExist) {
                System.out.println("Failed to index!");
                System.exit(-1);
            } else {
                System.out.println("Index successfully!");
                System.exit(1);
            }
        }
    }
}
