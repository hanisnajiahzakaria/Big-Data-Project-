package topSellProd;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopSellingProductMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable quantity = new IntWritable(1);
    private Text productName = new Text();

   
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
        // Assuming CSV format: order_id, product name, quantity ordered, price each, order date, purchase address
        String[] tokens = value.toString().split(",");
        if (tokens.length >= 3 && !tokens[2].trim().equals("Quantity Ordered")) {
            productName.set(tokens[1].trim()); // Product Name as the key
            quantity.set(Integer.parseInt(tokens[2].trim())); // Quantity Ordered as the value
            context.write(productName, quantity);
        }
    }

}
