package datastream.operators.asyncIO;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class AsyncDatabaseRequest extends RichAsyncFunction<String, Tuple2<String,String>> {
    //带有回调的并发请求的数据库客户端
    //transient表示不序列化
    //伪代码
//    private transient DatabaseClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化数据库连接
    }

    @Override
    public void close() throws Exception {
        //关闭数据库连接
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<Tuple2<String, String>> resultFuture) {
        //异步查询,伪代码如下
//        Future<String> result = client.query(input)

        //请求查询完成触发
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try{
                    //返回查询的结果
                    //return result.get();
                    return "";
                }catch(Exception e){
                    return null;
                }
            }
        }).thenAccept((String dbResult) -> {
            //将结果放到collections里面
            resultFuture.complete(Collections.singleton(new Tuple2<>(input,dbResult)));
        });
    }
}