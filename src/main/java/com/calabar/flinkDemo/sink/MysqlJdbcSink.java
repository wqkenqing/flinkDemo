package com.calabar.flinkDemo.sink;

import com.calabar.flinkDemo.model.Toll;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2020/8/3
 * @desc
 */
public class MysqlJdbcSink extends RichSinkFunction<Toll> {
    private static final long serialVersionUID = 1L;

    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        // JDBC连接信息
        String USERNAME = "root";
        String PASSWORD = "125323wQ";
        String DRIVERNAME = "com.mysql.jdbc.Driver";
        String DBURL = "jdbc:mysql://localhost/flink";
        // 加载JDBC驱动
        Class.forName(DRIVERNAME);
        // 获取数据库连接
//        connection = DriverManager.getConnection(DBURL, USERNAME, PASSWORD);
        connection = DriverManager.getConnection(DBURL +
                        "?useUnicode=true" +
                        "&useSSL=false" +
                        "&characterEncoding=UTF-8",
                USERNAME,
                PASSWORD);
        String sql = "insert into toll_info(fId, passtype, modifyTime,plateNo,passTime,roadID,userName,feeType,vKindNo,plateColor) values (?,?,?,?,?,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        super.open(parameters);
    }

    @Override
    public void invoke(Toll toll) throws Exception {
        try {
            preparedStatement.setInt(1, toll.getFId());
            preparedStatement.setString(2, toll.getPassType());
            preparedStatement.setDate(3, toll.getModifyTime());
            preparedStatement.setString(4, toll.getPlateNo());
            preparedStatement.setDate(5, toll.getPassTime());
            preparedStatement.setInt(6, toll.getRoadId());
            preparedStatement.setString(7, toll.getUserName());
            preparedStatement.setString(8, toll.getFeeType());
            preparedStatement.setInt(9, toll.getVKindNo());
            preparedStatement.setString(10, toll.getPlateColor());

            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    ;

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}
