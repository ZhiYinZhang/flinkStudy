package datastream.operators.asyncIO;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import com.zaxxer.hikari.HikariConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class mysqlAsync extends RichAsyncFunction<String, String> {
    private transient HikariDataSource dataSource;
    private transient ExecutorService executorService;
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化数据库连接池
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:mysql://localhost:3306/test?serverTimezone=Asia/Shanghai");
        hikariConfig.setUsername("root");
        hikariConfig.setPassword("123456");
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.addDataSourceProperty("cachePrepStmts","true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize","250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit","2048");
        dataSource = new HikariDataSource(hikariConfig);

        //初始化线程池
        executorService = Executors.newFixedThreadPool(20);

    }
    @Override
    public void close() throws Exception {
        dataSource.close();
        executorService.shutdown();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        //将查询放到线程池
        Future<String> future = executorService.submit(() -> {
            return queryFromMysql(input);
        });
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try{
                    return future.get();
                }catch (Exception e){
                    return null;
                }
            }
        }).thenAccept((String dbResult)->{
              resultFuture.complete(Collections.singleton(dbResult));
        });
    }
    private String queryFromMysql(String param) throws SQLException{
        Connection connection =null;
        Statement stmt = null;
        ResultSet rs = null;

        String result = null;
        try{
            connection = dataSource.getConnection();
            stmt = connection.createStatement();
            rs = stmt.executeQuery("select * from temp where id="+param);
            while(rs.next()){
                result = rs.getString("name");
            }
        }finally {
            if(rs!=null) rs.close();
            if(stmt!=null) stmt.close();
            if(connection!=null) connection.close();
        }

        if(result!=null){
            //放入缓存中，前面查数据库前可以先查缓存
        }
        return result;
    }
}
