package com.fang;

public class Main {

    public static void main(String[] args) {

        IndexEngine indexEngine = new IndexEngine("../../ads_0502.txt",
                                                  "../../budget.txt",
                                                  "localhost",
                                                  11211,
                                                  "localhost:3306",
                                                  "mySql",
                                                  "root",
                                                  "root");
        indexEngine.init();
    }
}
