import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * PREPROCESSING MAPPER
 * ─────────────────────
 * Runs ONCE before the main iterations.
 * Reads raw edge list and enriches each line with an initial distance.
 *
 * INPUT  (one line):  "0  1  4"         → src=0, dst=1, weight=4
 * OUTPUT (one line):  "0  1  4  0"      → if src == SOURCE_NODE
 *                     "2  1  2  INF"    → if src != SOURCE_NODE
 */
public class PreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {

    // The node we start Dijkstra from
    private static final String SOURCE_NODE = "0";

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Step 1: Parse the line into parts
        // e.g. "0  1  4" → ["0", "1", "4"]
        String[] parts = value.toString().trim().split("\\s+");

        // Skip malformed lines
        if (parts.length < 3) return;

        String src    = parts[0];
        String dst    = parts[1];
        String weight = parts[2];

        // Step 2: Assign initial distance
        // Source node starts at 0, everything else is unreachable (INF)
        String distance = src.equals(SOURCE_NODE) ? "0" : "INF";

        // Step 3: Emit enriched line
        // Key   → source node ID
        // Value → "dst weight distance"
        // This is a Map-only job, so output goes directly to HDFS
        context.write(new Text(src), new Text(dst + " " + weight + " " + distance));
    }
}
