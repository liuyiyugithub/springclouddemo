package com.example.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import java.io.*;
import java.sql.*;

public class Utilities {
    public static int fileversion = 0;
    public static String connectStrPublic = "DefaultEndpointsProtocol=https;AccountName=stgbcrmfile;AccountKey=GI+hzLnWpKM7s5bkHIgKW4/lAGEVEac79DYYlDWFtIOwlE5h6NtO60b4vn0L79mQ5ZZxtzpt/TCLjY6y3O7E1A==;EndpointSuffix=core.windows.net";
    public static String connectStrVNET = "DefaultEndpointsProtocol=https;AccountName=stgbcrmfilevnet;AccountKey=IUDBF0XQoE3SZPI9TorzzHXEzz4pkNjX7HvW1M/AQn+Z56tnnj6kxTgQjKczW/BaePTIZb5evkADL0Kmy2GPIA==;EndpointSuffix=core.windows.net";
    public static String connectStrA = connectStrVNET;
    public static int getControlFileVersion(){
        String connectStr = Utilities.connectStrA;
        String containerName = "upload";
        String blobName = "controlfile.txt";
        String localFileName = "controlfile" + java.util.UUID.randomUUID() + ".txt";

        int filever = 0;

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectStr).buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.downloadToFile(localFileName);
        File downloadFile = new File (localFileName);

        BufferedReader reader;
        try{
            reader = new BufferedReader((new FileReader(localFileName)));
            String fileVersion = reader.readLine();
            System.out.println("fileversion:"+fileVersion);
            filever = Integer.parseInt(fileVersion);
            reader.close();
        } catch (Exception e){
            e.printStackTrace();
        }

        downloadFile.delete();

        return filever;
    }
    public static void downloadData(){
        String connectStr = Utilities.connectStrA;
        String containerName = "upload";
        String blobName = "data.csv";
        String localFileName = "data" + java.util.UUID.randomUUID() + ".csv";

        String jdbcURL = "jdbc:h2:mem:testdb";
        String username = "sa";
        String password = "password";


        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectStr).buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient blobClient = containerClient.getBlobClient(blobName);

        File downloadFile = new File (localFileName);
        blobClient.downloadToFile(localFileName);

        Connection connection = null;

        BufferedReader reader;
        try{
            reader = new BufferedReader((new FileReader(localFileName)));

            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(true);

            String sql;
            Statement statement;

            sql = "DROP TABLE IF EXISTS CUST";
            statement = connection.createStatement();
            statement.execute(sql);

            sql = "CREATE TABLE CUST(ID INT PRIMARY KEY, CREATEDATE DATE, CUSTID INT, CUSTTYPE VARCHAR(255), UPDATEDATE DATE);";
            statement.execute(sql);


            String line = reader.readLine();
            while (line != null){
                System.out.println(line);

                String[] data = line.split(",");
                String id = data[0];
                String createDate = data[1];
                String custId = data[2];
                String custType = data[3];
                String updateDate = data[4];

                sql = "INSERT INTO CUST VALUES( "+id+",'"+createDate+"',"+custId+",'"+custType+"','"+updateDate+"');";
                statement.execute(sql);
                System.out.println(sql);

                line = reader.readLine();
            }

            connection.close();
            reader.close();
            downloadFile.delete();
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    public static int getCount(){
        String jdbcURL = "jdbc:h2:mem:testdb";
        String username = "sa";
        String password = "password";
        Connection connection = null;

        String sql;
        Statement statement =  null;

        int rowcount=0;

        try{
            connection = DriverManager.getConnection(jdbcURL, username, password);
            statement = connection.createStatement();
            sql = "select count(*) from cust;";
            ResultSet rs = statement.executeQuery(sql);
            while(rs.next()){
                System.out.println(rs.getInt("count(*)"));
                rowcount = rs.getInt("count(*)");
            }
            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return rowcount;
    }
}
