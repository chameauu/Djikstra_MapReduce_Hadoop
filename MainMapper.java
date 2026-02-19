import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * MAIN MAPPER — runs once per iteration
 * ──────────────────────────────────────
 * INPUT  (one line): "2  1  2  1"
 *                     src=2, dst=1, weight=2, dist[src]=1
 *
 * For each line, the Mapper does TWO things:
 *
 *  1. RELAX NEIGHBOR → emit a candidate distance to the destination node
 *     key=dst, value="DISTANCE|newDist"
 *     newDist = dist[src] + weight
 *
 *  2. PRESERVE EDGE  → re-emit the edge so graph structure survives to next iteration
 *     key=src, value="EDGE|dst|weight|dist[src]"
 *
 * The Reducer will collect all messages per node and decide the best distance.
 */
public class MainMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Step 1: Parse the enriched line
        // e.g. "2  1  2  1" → src=2, dst=1, weight=2, dist=1
        String[] parts = value.toString().trim().split("\\s+");

        if (parts.length < 4) return;

        String src    = parts[0];
        String dst    = parts[1];
        int    weight = Integer.parseInt(parts[2]);
        String distStr = parts[3];

        // Step 2: Preserve the edge — key is src
        // The Reducer for src node needs to know all its edges to rewrite them
        context.write(
            new Text(src),
            new Text("EDGE|" + dst + "|" + weight + "|" + distStr)
        );

        // Step 3: Relax the neighbor — only if this node has been reached
        if (!distStr.equals("INF")) {
            int dist = Integer.parseInt(distStr);
            int newDist = dist + weight;

            // Tell the destination node: "you can be reached in newDist steps via src"
            context.write(
                new Text(dst),
                new Text("DISTANCE|" + newDist)
            );
        }
        // If dist is INF, no relaxation — this node hasn't been reached yet
    }
}
