import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * DRIVER CLASS
 * ─────────────
 * Orchestrates the full Dijkstra MapReduce pipeline:
 *
 *  Phase 1: Preprocessing Job (Map-only)
 *           Raw edge list → enriched edge list with initial distances
 *
 *  Phase 2: Iterative Main Jobs
 *           Runs up to MAX_ITERATIONS times
 *           Each iteration relaxes distances one hop further
 *           Stops early if no changes detected (convergence)
 *
 * HOW TO RUN:
 *   hadoop jar dijkstra.jar Driver /input /output 5
 *   arg[0] = input path (raw edge list in HDFS)
 *   arg[1] = output base path
 *   arg[2] = number of iterations (optional, default=10)
 */
public class Driver {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: Driver <inputPath> <outputPath> [maxIterations]");
            System.exit(1);
        }

        String inputPath  = args[0];
        String outputPath = args[1];
        int maxIterations = args.length >= 3 ? Integer.parseInt(args[2]) : 10;

        Configuration conf = new Configuration();

        // ─────────────────────────────────────────
        // PHASE 1: Preprocessing Job (runs once)
        // ─────────────────────────────────────────
        System.out.println(">>> Starting Preprocessing Job...");

        Job preprocessJob = Job.getInstance(conf, "Dijkstra Preprocessing");
        preprocessJob.setJarByClass(Driver.class);

        preprocessJob.setMapperClass(PreprocessingMapper.class);

        // Map-only job — no Reducer
        preprocessJob.setNumReduceTasks(0);

        preprocessJob.setOutputKeyClass(Text.class);
        preprocessJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(preprocessJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(preprocessJob, new Path(outputPath + "/iter0"));

        // Wait for preprocessing to finish before starting iterations
        preprocessJob.waitForCompletion(true);
        System.out.println(">>> Preprocessing complete. Output at: " + outputPath + "/iter0");

        // ─────────────────────────────────────────
        // PHASE 2: Iterative Main Jobs
        // ─────────────────────────────────────────
        for (int i = 1; i <= maxIterations; i++) {

            String iterInput  = outputPath + "/iter" + (i - 1);
            String iterOutput = outputPath + "/iter" + i;

            System.out.println(">>> Starting Iteration " + i + "...");

            Job mainJob = Job.getInstance(conf, "Dijkstra Iteration " + i);
            mainJob.setJarByClass(Driver.class);

            mainJob.setMapperClass(MainMapper.class);
            mainJob.setReducerClass(MainReducer.class);

            mainJob.setOutputKeyClass(Text.class);
            mainJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(mainJob, new Path(iterInput));
            FileOutputFormat.setOutputPath(mainJob, new Path(iterOutput));

            mainJob.waitForCompletion(true);
            
            // Check for convergence
            long distanceUpdates = mainJob.getCounters()
                .findCounter("Dijkstra", "DistanceUpdates").getValue();
            
            System.out.println(">>> Iteration " + i + " complete. Output at: " + iterOutput);
            System.out.println(">>> Distance updates: " + distanceUpdates);

            // If no distances changed, we've converged
            if (distanceUpdates == 0) {
                System.out.println(">>> Converged! No distance changes detected.");
                System.out.println(">>> Final distances at: " + iterOutput);
                return;
            }
        }

        System.out.println(">>> Dijkstra complete!");
        System.out.println(">>> Final distances at: " + outputPath + "/iter" + maxIterations);
    }
}
