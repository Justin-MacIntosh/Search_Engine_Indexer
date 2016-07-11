package indexer;

import org.apache.hadoop.util.ToolRunner;

public class Driver {

	static String input, tempput, output;
	static double num_docs;
	static int iter_round = 3;
	static double damping = 0.5;

	public static void main(String args[]) throws Exception {
		int res = 0;
		// read parameters
		if (args.length == 2) {
			input = args[0];
			output = args[1];
		} 
		else {
			System.out.println(args.length);
			for(String s:args)
				System.out.println(s);
			System.out.println("Need 2 parameters: input path, output path");
		}
		
		//---------------------INIT--------------------
		System.out.println("invoke initialize indexer");
		String init_out = input + "/init";
		String[] param_init = new String[] { input, init_out };
		res = ToolRunner.run(new InitializeMR(), param_init);
		if (res == 1) {
			System.out.println("failed at initializing");
			System.exit(res);
		}
		//-------------------END INIT------------------
		
		
		//--------------------INDEX--------------------
		System.out.println("invoke indexing");
		String ind_out = input + "/ind";
		String[] param_index = new String[] { init_out, ind_out };
		res = ToolRunner.run(new IndexMR(), param_index);
		if (res == 1) {
			System.out.println("failed at indexing");
			System.exit(res);
		}
		//------------------END INDEX------------------
		
		//-----------------COMBINATION-----------------
		System.out.println("invoke reducing");
		String[] param_red = new String[] { ind_out, output };
		res = ToolRunner.run(new RedMR(), param_red);
		if (res == 1) {
			System.out.println("failed at reducing");
			System.exit(res);
		}
		//---------------END COMBINATION---------------

		System.exit(0);
	}
}
