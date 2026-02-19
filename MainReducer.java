import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MAIN REDUCER — runs once per iteration
 * ────────────────────────────────────────
 * Receives all messages for a given node.
 *
 * INPUT:
 *   key   = nodeID (e.g. "1")
 *   values = mix of:
 *     "DISTANCE|3"         ← candidate distance from a neighbor
 *     "EDGE|3|1|INF"       ← edge this node owns (dst=3, weight=1, currentDist=INF)
 *
 * The Reducer does TWO things:
 *   1. Find the MINIMUM candidate distance → new best dist for this node
 *   2. Rewrite ALL edges with the updated distance → output for next iteration
 *
 * OUTPUT (same format as MainMapper input):
 *   key=src, value="dst weight newDist"
 */
public class MainReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Separate DISTANCE messages from EDGE messages
        List<String[]> edges = new ArrayList<>();
        int bestDist = Integer.MAX_VALUE; // start with INF

        for (Text val : values) {
            String str = val.toString();

            if (str.startsWith("DISTANCE|")) {
                // Parse candidate distance and track the minimum
                int candidateDist = Integer.parseInt(str.split("\\|")[1]);
                if (candidateDist < bestDist) {
                    bestDist = candidateDist;
                }

            } else if (str.startsWith("EDGE|")) {
                // Parse edge: EDGE|dst|weight|currentDist
                String[] parts = str.split("\\|");
                // parts[0]=EDGE, parts[1]=dst, parts[2]=weight, parts[3]=currentDist
                edges.add(new String[]{parts[1], parts[2], parts[3]});

                // Also consider the node's existing distance (from previous iteration)
                if (!parts[3].equals("INF")) {
                    int existingDist = Integer.parseInt(parts[3]);
                    if (existingDist < bestDist) {
                        bestDist = existingDist;
                    }
                }
            }
        }

        // Determine the final distance string
        String finalDist = (bestDist == Integer.MAX_VALUE) ? "INF" : String.valueOf(bestDist);

        // Track if distance changed for convergence detection
        if (!edges.isEmpty()) {
            String previousDist = edges.get(0)[2]; // Get distance from first edge
            if (!finalDist.equals(previousDist)) {
                context.getCounter("Dijkstra", "DistanceUpdates").increment(1);
            }
        }

        // Rewrite all edges with the updated distance
        // This output becomes the input for the next iteration
        for (String[] edge : edges) {
            String dst    = edge[0];
            String weight = edge[1];
            // Format: "dst weight newDist"
            context.write(key, new Text(dst + " " + weight + " " + finalDist));
        }
    }
}
