package org.rnakra;

import org.rnakra.core.KeyValueStoreImpl;
import org.rnakra.merger.CompactAndMerge;
import org.rnakra.scheduler.MasterTask;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);
            MasterTask masterTask = new MasterTask();
            while(true) {
                System.out.println("Enter command: " +
                        "put | get | exit | merge");
                String command = scanner.nextLine();
                if(command.equals("exit")) {
                    break;
                }
                if(command.equals("put")) {
                    System.out.println("Enter key: ");
                    String key = scanner.nextLine();
                    System.out.println("Enter value: ");
                    String value = scanner.nextLine();
                    masterTask.submitWriteTask(key, value);
                }
                if(command.equals("get")) {
                    System.out.println("Enter key: ");
                    String key = scanner.nextLine();
                    masterTask.submitReadTask(key).thenAccept((value)-> {
                        System.out.println("Value for key: " + key + " is: " + value);
                    });
                }
                if(command.equals("merge")) {
                    System.out.println("Enter file name: ");
                    File dataDir = new File("data");
                    if(!dataDir.exists() || !dataDir.isDirectory()) {
                        System.out.println("No data directory found");
                        continue;
                    }
                    String[] files = Arrays.stream(dataDir.list()).filter(f -> f.endsWith(".db")).toArray(String[]::new);
                    Arrays.sort(files);
                    if(files.length < 2) {
                        System.out.println("Need at least 2 files to merge");
                        continue;
                    }
                    String file1 = files[0];
                    String file2 = files[1];
                    for (String file: files) {
                        System.out.println("File: " + file);
                    }
                    System.out.println("Merging files: " + file1 + " and " + file2);
//                    keyValueStore.compactAndMerge(file1, file2);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception: " + e.getMessage());
        }
    }
}