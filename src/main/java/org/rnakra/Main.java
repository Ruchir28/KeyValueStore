package org.rnakra;

import org.rnakra.core.KeyValueStoreImpl;

import java.io.FileNotFoundException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);
            KeyValueStoreImpl keyValueStore = new KeyValueStoreImpl();
            while(true) {
                System.out.println("Enter command: " +
                        "put | get | exit");
                String command = scanner.nextLine();
                if(command.equals("exit")) {
                    break;
                }
                if(command.equals("put")) {
                    System.out.println("Enter key: ");
                    String key = scanner.nextLine();
                    System.out.println("Enter value: ");
                    String value = scanner.nextLine();
                    keyValueStore.put(key, value);
                }
                if(command.equals("get")) {
                    System.out.println("Enter key: ");
                    String key = scanner.nextLine();
                    System.out.println("Value for key: " + key + " is: " + keyValueStore.get(key));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception: " + e.getMessage());
        }
    }
}