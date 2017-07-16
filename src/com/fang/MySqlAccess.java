package com.fang;

import java.sql.*;

public class MySqlAccess {
    private Connection d_connect = null;
    private String d_user_name;
    private String d_password;
    private String d_server_name;
    private String d_db_name;

    MySqlAccess(String server, String db, String user, String pass) throws Exception {
        d_user_name = user;
        d_password = pass;
        d_server_name = server;
        d_db_name = db;
        // This will load the MySQL driver, each DB has its own driver
        Class.forName("com.mysql.jdbc.Driver");
        //"jdbc:mysql://127.0.0.1:3306/searchads?user=root&password=bittiger2017"
        String conn = "jdbc:mysql://" + d_server_name + "/" +
                d_db_name + "?user=" + d_user_name + "&password=" + d_password;
        System.out.println("Connecting to database: " + conn);
        d_connect = DriverManager.getConnection(conn);
        System.out.println("Connected to database");
    }

    void close() throws Exception {
        System.out.println("Close database");
        if (d_connect != null) {
            d_connect.close();
        }
    }

    private boolean isRecordExist(String sql_string) throws SQLException {
        PreparedStatement existStatement = null;
        boolean isExist = false;

        try {
            existStatement = d_connect.prepareStatement(sql_string);
            ResultSet result_set = existStatement.executeQuery();
            if (result_set.next()) {
                isExist = true;
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw e;
        } finally {
            if (existStatement != null) {
                existStatement.close();
            }
        }

        return isExist;
    }

    void addAdData(Ad ad) throws Exception {
        boolean isExist;
        String sql_string = "select adId from " + d_db_name + ".ad where adId=" + ad.adId;
        PreparedStatement ad_info = null;
        try {
            isExist = isRecordExist(sql_string);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw e;
        }

        if (isExist) {
            return;
        }

        sql_string = "insert into " + d_db_name + ".ad values(?,?,?,?,?,?,?,?,?,?,?)";
        try {
            ad_info = d_connect.prepareStatement(sql_string);
            ad_info.setLong(1, ad.adId);
            ad_info.setLong(2, ad.campaignId);
            String keyWords = Util.strJoin(ad.keyWords, ",");
            ad_info.setString(3, keyWords);
            ad_info.setDouble(4, ad.bidPrice);
            ad_info.setDouble(5, ad.price);
            ad_info.setString(6, ad.thumbnail);
            ad_info.setString(7, ad.description);
            ad_info.setString(8, ad.brand);
            ad_info.setString(9, ad.detail_url);
            ad_info.setString(10, ad.category);
            ad_info.setString(11, ad.title);
            ad_info.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw e;
        } finally {
            if (ad_info != null) {
                ad_info.close();
            }
        }
    }

    void addCampaignData(Campaign campaign) throws Exception {
        boolean isExist;
        String sql_string = "select campaignId from " + d_db_name + ".campaign where campaignId=" + campaign.campaignId;
        try {
            isExist = isRecordExist(sql_string);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw e;
        }

        if (isExist) {
            return;
        }
        PreparedStatement camp_info = null;
        sql_string = "insert into " + d_db_name + ".campaign values(?,?)";
        try {
            camp_info = d_connect.prepareStatement(sql_string);
            camp_info.setLong(1, campaign.campaignId);
            camp_info.setDouble(2, campaign.budget);
            camp_info.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw e;
        } finally {
            if (camp_info != null) {
                camp_info.close();
            }
        }
    }
}
