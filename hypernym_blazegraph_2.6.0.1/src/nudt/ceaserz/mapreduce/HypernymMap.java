package nudt.ceaserz.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

public class HypernymMap extends Mapper<LongWritable, Text, Text, Text> {

	private String sparqlEndPoint = "http://idc.answercow.org:3004/bigdata/namespace/lab681_KG/sparql";
	private SPARQLRepository repo = new SPARQLRepository(sparqlEndPoint);
	private String queryString = "SELECT ?s1 ?p3 ?s2 WHERE {?s1 ?p1 ?o1 .?s2 ?p2 ?o2 .?s1 ?p3 ?s2}";
	RepositoryConnection con = null;
	private ValueFactory factory = null;
	private TupleQuery tupleQuery = null;

	public void setup(Context context) {
		repo.setUsernameAndPassword("rdfmanage", "bjgdrdfManage123");
		try {
			repo.initialize();
			con = repo.getConnection();
			tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
			factory = repo.getValueFactory();
		} catch (RepositoryException e) {
			System.out.println("something wrong hadppend in initializing");
		} catch (MalformedQueryException e) {
			System.out.println("something wrong hadppend in prepareTupleQuery");
		}

	}

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] lineArray = line.split("\t");
		if (lineArray.length == 11) {
			String concept = lineArray[0];
			String entity = lineArray[1];
			tupleQuery.setBinding("o1", factory.createLiteral(entity));
			tupleQuery.setBinding("o2", factory.createLiteral(concept));
			try {
				TupleQueryResult result = tupleQuery.evaluate();
				while (result.hasNext()) {
					BindingSet bindingSet = result.next();
					String nodes = entity + "&&&&&" + concept + "&&&&&" + bindingSet.getValue("s1").toString() + "&&&&&"
							+ bindingSet.getValue("s2").toString();
					String edge = bindingSet.getValue("p3").toString();
					context.write(new Text(nodes), new Text(edge));
				}
				result.close();
			} catch (QueryEvaluationException e) {
				System.out.println("something wrong hadppend in QueryEvaluationException");
			}
		}
	}

	public void cleanup(Context context) {
		try {
			con.close();
			repo.shutDown();
		} catch (RepositoryException e) {
			System.out.println("something wrong hadppend in RepositoryException");
		}
	}
}
