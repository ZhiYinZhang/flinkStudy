package datastream.transformation;

//封装数据的Bean
public class wordCounts {
    /*
    在大数据里面，属性可以不用私有，也不用get和set方法
    需要提供一个of方法，去获取实例
    这些参考flink的Tuple
     */
    public String word;
    public Long counts;
//    private String word;
//    private Long counts;

    public wordCounts() {
    }

    public wordCounts(String word, Long counts) {
        this.word = word;
        this.counts = counts;
    }
//
//    public String getWord() {
//        return word;
//    }
//
//    public void setWord(String word) {
//        this.word = word;
//    }
//
//    public Long getCounts() {
//        return counts;
//    }
//
//    public void setCounts(Long counts) {
//        this.counts = counts;
//    }

    public static wordCounts of(String word,Long counts){
        return new wordCounts(word,counts);
    }

    @Override
    public String toString() {
        return "wordCounts{" +
                "word='" + word + '\'' +
                ", counts=" + counts +
                '}';
    }
}
