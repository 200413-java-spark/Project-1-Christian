package spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

@WebServlet("/pokemon")
public class PokemonServlet extends HttpServlet {
	JavaSparkContext sparkContext;
	List<String> names;

	@Override
	public void init() throws ServletException {
		SparkConf conf = new SparkConf().setAppName("PokemonDirectory").setMaster("local");
		sparkContext = new JavaSparkContext(conf);
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

    resp.getWriter().println("These are the names of the columns: \n");

	JavaRDD<String> pokemonRDD = sparkContext.textFile("test.csv");
    JavaRDD<String[]> raw = pokemonRDD.map((x) -> x.split(","));
    String[] header = raw.take(1).get(0);
	
    for (int i = 0; i < header.length; i++) {
		resp.getWriter().println(i + ". " + header[i]);
    }

}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		
		JavaRDD<String> pokemonRDD = sparkContext.textFile("pokemon.csv");
		String header = pokemonRDD.first();
		JavaRDD<String> data = pokemonRDD.filter(row -> !row.equals(header));
		JavaRDD<String[]> raw = data.map((x) -> x.split(","));

		String type = req.getParameter("type");
		String stat = req.getParameter("stat");
		String gen = req.getParameter("gen");
		String pokemon = req.getParameter("pokemon");


		if(type != null && gen != null){
			JavaRDD<String[]> sortType = raw.filter(x -> x[9].equals(type));
			JavaRDD<String[]> sortGen = sortType.filter(x -> x[10].equals(gen));
			JavaRDD<String> names = sortGen.cache().map(x -> x[4]);
			resp.getWriter().println(names.collect());
		}
		else if(pokemon != null)
		{
			JavaRDD<String[]> sortName = raw.filter(x -> x[4].equals(pokemon));
			JavaRDD<String> p_att = sortName.map(x -> x[0]);
			JavaRDD<String> p_def = sortName.map(x -> x[2]);
			JavaRDD<String> p_spatt = sortName.map(x -> x[6]);
			JavaRDD<String> p_spdef = sortName.map(x -> x[7]);
			JavaRDD<String> p_spd = sortName.map(x -> x[8]);
			JavaRDD<String> p_hp = sortName.map(x -> x[3]);
			JavaRDD<String> p_gen = sortName.map(x -> x[10]);

			resp.getWriter().println("Attack: " + p_att.collect());
			resp.getWriter().println("Defense: " + p_def.collect());
			resp.getWriter().println("SP Attack: " + p_spatt.collect());
			resp.getWriter().println("SP Defense: " + p_spdef.collect());
			resp.getWriter().println("Speed: " + p_spd.collect());
			resp.getWriter().println("HP: " + p_hp.collect());
			resp.getWriter().println("Generation: " + p_gen.collect());
			
		}
		else if(type != null && stat != null)
		{
			JavaRDD<String[]> sortType = raw.filter(x -> x[9].equals(type));
			JavaRDD<String> selectedStat = sortType.cache().map(x -> x[1]);
			JavaPairRDD<String, Integer> count =  selectedStat.mapToPair(x -> new Tuple2(x, 1))
			.reduceByKey((a, b) -> ((int) a + (int) b)).cache();
			List<Tuple2<String, Integer>> top = count
			.mapToPair(x -> x.swap()) 
			.mapToPair(x -> x.swap()) 
			.sortByKey(false)         
			.take(5);                
			/**
			List<String> highest = new ArrayList<String>();
			for (int i =0; i<5; i++){
				highest.add(top.get(i)._1);
			}
			for (int i = 0; i < highest.size();i++) 
	     	{ 		      
	          	System.out.println(highest.get(i)); 		
			}   
			
			String str[] = new String[highest.size()];
			for (int j = 0; j < highest.size(); j++) { 
				// Assign each value to String array 
				str[j] = highest.get(j); 
			}
			JavaRDD<String[]> filter = sortType.filter(x -> x[1].equals(str));
			resp.getWriter().println(filter.collect());
			**/
			for (int i =0; i< top.size(); i ++){
				resp.getWriter().format("%s",top.get(i)._1 + " ");
			}
		}
	}
}
