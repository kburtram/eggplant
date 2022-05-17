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

pub struct PlanCostFunction<'a> {
    egraph: &'a EGraph<PlanLanguage, ()>,
}

impl<'a> egg::CostFunction<PlanLanguage> for PlanCostFunction<'a> {
    type Cost = usize;
    fn cost<C>(&mut self, enode: &PlanLanguage, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let op_cost = match enode {
            PlanLanguage::MergeJoin(ids) => {

                for c in self.egraph.classes() {
                    if c.id == ids[0] || c.id == ids[1]  {
                        for n in c.iter() {
                            println!("node = {}", n)
                        }
                    }
                }
                10
            },
            PlanLanguage::HashJoin(ids) => {
                for c in self.egraph.classes() {
                    if c.id == ids[0] || c.id == ids[1]  {
                        for n in c.iter() {
                            println!("node = {}", n)
                        }
                    }
                }
                9
            },
            PlanLanguage::NestedLoopsJoin(ids) => {
                for c in self.egraph.classes() {
                    if c.id == ids[0] || c.id == ids[1]  {
                        for n in c.iter() {
                            println!("node = {}", n)
                        }
                    }
                }
                8
            },
            _ => 1,
        };
        let cost = enode.fold(op_cost, |sum, i| sum + costs(i));
        println!("Op_Cost = {}, Cost = {}", op_cost, cost);
        cost
    }
}

#[rustfmt::skip]
fn make_runner(exp: &RecExpr<PlanLanguage>) -> Runner<PlanLanguage, ()> {
    // rewrite rules vector
    let rules = vec![
        // join operations rewrite rules
        rewrite!("hash-join-merge-join"; "(hashJoin ?a ?b)"         => "(mergeJoin ?a ?b)"),
        rewrite!("hash-join-nested-loops-join"; "(hashJoin ?a ?b)"   => "(nestedLoopsJoin ?a ?b)"),
        rewrite!("merge-join-hash-join"; "(mergeJoin ?a ?b)"        => "(hashJoin ?a ?b)"),
        rewrite!("merge-join-nested-loops-join"; "(mergeJoin ?a ?b)" => "(nestedLoopsJoin ?a ?b)"),
        rewrite!("nested=-loops-join-merge-join"; "(nestedLoopsJoin ?a ?b)" => "(mergeJoin ?a ?b)"),
        rewrite!("nested-loops-join-hash-join"; "(nestedLoopsJoin ?a ?b)" => "(hashJoin ?a ?b)"),
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

    let root = runner.roots[0];
    let cost_func = PlanCostFunction { egraph: &runner.egraph };
    let best = Extractor::new(&runner.egraph, cost_func).find_best(root).1;
   
    // print the input and output expression
    println!("input   [{}] {}", exp.as_ref().len(), exp);
    println!("normal  [{}] {}", best.as_ref().len(), best);
}

#[test]
fn test_join_operation_rewrites() {    
    //let input_expression = "(select (hashJoin (mergeJoin (scan tbl1 id) (seek tbl3 fid)) (seek tbl2 fid)))";
    let input_expression = "(mergeJoin tbl1 tbl2))";
    let exp :RecExpr<PlanLanguage> = input_expression.trim().parse().unwrap();
    let runner: Runner<PlanLanguage, ()> = make_runner(&exp);
    assert!(matches!(runner.stop_reason, Some(StopReason::Saturated)));

    let root = runner.roots[0];
    let cost_func = PlanCostFunction { egraph: &runner.egraph };
    let best = Extractor::new(&runner.egraph, cost_func).find_best(root).1;
   
    // print the input and output expression
    println!("input   [{}] {}", exp.as_ref().len(), exp);
    println!("normal  [{}] {}", best.as_ref().len(), best);
}
