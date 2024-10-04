import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    int sum;
    IntWritable v = new IntWritable();
    
    // 用于存储词频数据
    private Map<Text, Integer> frequencyMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        frequencyMap.put(new Text(key), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 1. 创建一个列表来存储 Map 的条目
        List<Map.Entry<Text, Integer>> list = new ArrayList<>(frequencyMap.entrySet());
        
        // 2. 对列表按值进行排序
        list.sort((a, b) -> b.getValue().compareTo(a.getValue()));
        
        // 3. 输出出现频率最高的 20 个关键词
        for (int i = 0; i < Math.min(20, list.size()); i++) {
            Map.Entry<Text, Integer> entry = list.get(i);
            context.write(entry.getKey(), new IntWritable(entry.getValue()));
        }
    }
}
