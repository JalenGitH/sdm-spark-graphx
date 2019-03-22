package exercise_4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Exercise_4 {

	private static Row splitWikiVertexString(String wikiVerticeLine){
		String[] parts = wikiVerticeLine.split("\t");
		return RowFactory.create(parts[0], parts[1]);
	}

	private static Row splitWikiEdgesString(String wikiVerticeLine){
		String[] parts = wikiVerticeLine.split("\t");
		return RowFactory.create(parts[0], parts[1]);
	}

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		Path wikiEdgesPath = Paths.get("src/main/resources/wiki-edges.txt");
		Path wikiVerticePath = Paths.get("src/main/resources/wiki-vertices.txt");
		try {
			// Vertice Creation
			List<Row> wikiVerticeList = Files.lines(wikiVerticePath).map(s -> splitWikiVertexString(s)).collect(Collectors.toList());
			JavaRDD<Row> wikiVerticeRDD = ctx.parallelize(wikiVerticeList);

			StructType wikiVerticeSchema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build())
			});
			Dataset<Row> wikiVertices = sqlCtx.createDataFrame(wikiVerticeRDD, wikiVerticeSchema);

			// Edges Creation
			List<Row> wikiEdgeList = Files.lines(wikiEdgesPath).map(s -> splitWikiEdgesString(s)).collect(Collectors.toList());
			JavaRDD<Row> wikiEdgesRDD = ctx.parallelize(wikiEdgeList);

			StructType wikiEdgesSchema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
			});
			Dataset<Row> wikiEdges = sqlCtx.createDataFrame(wikiEdgesRDD, wikiEdgesSchema);

			GraphFrame wikiGraph = GraphFrame.apply(wikiVertices, wikiEdges);

			System.out.println(wikiGraph);

			wikiGraph.edges().show();
			wikiGraph.vertices().show();
		} catch (IOException ex) {
			ex.printStackTrace();
		}


	}
	
}
