// Egg-based Plan Optimizer for T-SQL Queries (eggplant)
// Developed for CSEP 590D Spring 2022
// Karl Burtram, kburtram@uw.edu

use egg::{*};
use std::env;

// language definition for simplified SQL query execution plans
define_language! {
    enum QueryLanguage {        
        "select" = Select([Id; 1]),
        "hashJoin" = HashJoin([Id; 2]),
        "mergeJoin" = MergeJoin([Id; 2]),
        "nestedLoopsJoin" = NestedLoopsJoin([Id; 2]),
        "scan" = Scan([Id; 2]),
        "seek" = Seek([Id; 2]),
        Symbol(Symbol),
    }
}

fn main() {
    // print the application header
    println!("Egg-based Plan Optimizer for T-SQL Queries (eggplant)");

    // read the input string from the command-line
    let args: Vec<String> = env::args().collect();
    let input_expression = &args[1]; // e.g. "(select (hashJoin (scan tbl1 id)  (seek tbl2 fid)))" 

    // // declare an e-graph object for the execution plan query language
    // let egraph = egg::EGraph::<QueryLanguage, ()>::default();

    // rewrite rules vector
    let rules = vec![
        // join operations rewrite rules
        rewrite!("hashJoinMergeJoin"; "hashJoin"         => "mergeJoin"),
        rewrite!("hashJoinNestedLoopsJoin"; "hashJoin"   => "nestedLoopsJoin"),
        rewrite!("mergeJoinHashJoin"; "mergeJoin"        => "hashJoin"),
        rewrite!("mergeJoinNestedLoopsJoin"; "mergeJoin" => "nestedLoopsJoin"),
        rewrite!("nestedLoopsJoinMergeJoin"; "nestedLoopsJoin" => "mergeJoin"),
        rewrite!("nestedLoopsJoinHashJoin"; "nestedLoopsJoin" => "hashJoin"),
    ];

    // parse the input expression and create a runner object using rewrite rules
    let exp :RecExpr<QueryLanguage> = input_expression.trim().parse().unwrap();
    let runner: Runner<QueryLanguage, ()> = Runner::default()
        .with_iter_limit(1000)
        .with_expr(&exp)
        .run(&rules);

    // find the best expression using AstSize cost function (TODO create stats based cost function)
    let root = runner.roots[0];
    let best = Extractor::new(&runner.egraph, AstSize).find_best(root).1;
   
    // print the input and output expression
    println!("input   [{}] {}", exp.as_ref().len(), exp);
    println!("normal  [{}] {}", best.as_ref().len(), best);
}
