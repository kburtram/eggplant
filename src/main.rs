// Egg-based Plan Optimizer for T-SQL Queries (eggplant)
// Developed for CSEP 590D Spring 2022
// Karl Burtram, kburtram@uw.edu

use egg::{*};
use std::env;

// language definition for simplified SQL query execution plans
define_language! {
    enum PlanLanguage {        
        "select" = Select([Id; 1]),
        "hashJoin" = HashJoin([Id; 2]),
        "mergeJoin" = MergeJoin([Id; 2]),
        "nestedLoopsJoin" = NestedLoopsJoin([Id; 2]),
        "scan" = Scan([Id; 2]),
        "seek" = Seek([Id; 2]),
        Symbol(Symbol),
    }
}

pub struct PlanCostFunction;
impl egg::CostFunction<PlanLanguage> for PlanCostFunction {
    type Cost = usize;
    fn cost<C>(&mut self, enode: &PlanLanguage, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let op_cost = match enode {
            PlanLanguage::MergeJoin(..) => 5,
            PlanLanguage::HashJoin(..) => 3,
            PlanLanguage::NestedLoopsJoin(..) => 1,
            _ => 1,
        };
        enode.fold(op_cost, |sum, i| sum + costs(i))
    }
}

#[rustfmt::skip]
fn make_runner(exp: &RecExpr<PlanLanguage>) -> Runner<PlanLanguage, ()> {
    // rewrite rules vector
    let rules = vec![
        // join operations rewrite rules
        rewrite!("hash-join-merge-join"; "select (hashTable &t1 &t2)"         => "select (mergeTable &t1 &t2)"),
        // rewrite!("hash-join-nested-loops-join"; "hashJoin"   => "nestedLoopsJoin"),
        // rewrite!("merge-join-hash-join"; "mergeJoin"        => "hashJoin"),
        // rewrite!("merge-join-nested-loops-join"; "mergeJoin" => "nestedLoopsJoin"),
        // rewrite!("nested=-loops-join-merge-join"; "nestedLoopsJoin" => "mergeJoin"),
        // rewrite!("nested-loops-join-hash-join"; "nestedLoopsJoin" => "hashJoin"),
    ];

    let runner: Runner<PlanLanguage, ()> = Runner::default()
        .with_iter_limit(1000)
        .with_expr(&exp)
        .run(&rules);
    runner
}

fn main() {
    // print the application header
    println!("Egg-based Plan Optimizer for T-SQL Queries (eggplant)");

    // read the input string from the command-line
    let args: Vec<String> = env::args().collect();
    let input_expression = &args[1]; // e.g. "(select (hashJoin (scan tbl1 id)  (seek tbl2 fid)))" 
    //  (select (hashJoin (nestedLoopsJoin (scan tbl1 id) (seek tbl3 fid)) (seek tbl2 fid)))

    // parse the input expression and create a runner object using rewrite rules
    let exp :RecExpr<PlanLanguage> = input_expression.trim().parse().unwrap();

    let runner: Runner<PlanLanguage, ()> = make_runner(&exp);

    // find the best expression using AstSize cost function (TODO create stats based cost function)
    let root = runner.roots[0];
    let best = Extractor::new(&runner.egraph, PlanCostFunction).find_best(root).1;
   
    // print the input and output expression
    println!("input   [{}] {}", exp.as_ref().len(), exp);
    println!("normal  [{}] {}", best.as_ref().len(), best);
}

#[test]
fn test_join_operation_rewrites() {    
    let input_expression = "(select (hashJoin (mergeJoin (scan tbl1 id) (seek tbl3 fid)) (seek tbl2 fid)))";
    let exp :RecExpr<PlanLanguage> = input_expression.trim().parse().unwrap();
    let runner: Runner<PlanLanguage, ()> = make_runner(&exp);
    assert!(matches!(runner.stop_reason, Some(StopReason::Saturated)));

    let root = runner.roots[0];
    let best = Extractor::new(&runner.egraph, PlanCostFunction).find_best(root).1;
   
    // print the input and output expression
    println!("input   [{}] {}", exp.as_ref().len(), exp);
    println!("normal  [{}] {}", best.as_ref().len(), best);
}
