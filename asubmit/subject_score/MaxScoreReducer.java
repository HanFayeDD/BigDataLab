import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxScore = Integer.MIN_VALUE;

        // 遍历所有分数，找出最高分
        for (IntWritable val : values) {
            maxScore = Math.max(maxScore, val.get());
        }

        // 输出科目和最高分
        context.write(key, new IntWritable(maxScore));
    }
}
