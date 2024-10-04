import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxScoreMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text subject = new Text();
    private IntWritable score = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        String[] parts = line.split(" ");
        if (parts.length == 2) {
            String subj = parts[0]; 
            int scr;
            try {
                scr = Integer.parseInt(parts[1]); 
            } catch (NumberFormatException e) {
                return; 
            }

            subject.set(subj);
            score.set(scr);

            context.write(subject, score);
        }
    }
}
