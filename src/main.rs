// Egg-based Plan Optimizer for T-SQL Queries (eggplant)
// Developed for CSEP 590D Spring 2022
// Karl Burtram, kburtram@uw.edu

use egg::{*};
use serde::Deserialize;
use serde_json;
use std::env;
use std::fs;

#[derive(Deserialize, Debug)]
struct InputMetadata {
    expression: String,
    tables: Vec<Table>,    
}

#[derive(Deserialize, Debug)]
struct Table {
    name: String,
    cardinality: u64,
}

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
    input_metadata: InputMetadata,
}

fn get_node_cost(egraph: &EGraph<PlanLanguage, ()>, input_metadata: &InputMetadata, id: &Id) -> u64 {
    let mut cost = 0;
    for c in egraph.classes() {
        if c.id == *id  {
            for n in c.iter() {
                for t in input_metadata.tables.iter() {
                    if t.name == n.to_string() {
                        println!("TABLE = {}", n);
                        cost = t.cardinality;
                    }
                }                
            }
        }
    }

    cost
}

impl<'a> egg::CostFunction<PlanLanguage> for PlanCostFunction<'a> {
    type Cost = usize;
    fn cost<C>(&mut self, enode: &PlanLanguage, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let op_cost = match enode {
            PlanLanguage::MergeJoin(ids) => {
                let mut cost = 1000;
                let cost1 = get_node_cost(&self.egraph, &self.input_metadata, &ids[0]);
                let cost2 = get_node_cost(&self.egraph, &self.input_metadata, &ids[1]);
                if cost1 > 50 && cost2 > 50 && cost1 + cost2 > 1000 {
                    cost = 1
                }
                cost
            },
            PlanLanguage::HashJoin(ids) => {
                let mut cost = 1100;
                let cost1 = get_node_cost(&self.egraph, &self.input_metadata, &ids[0]);
                let cost2 = get_node_cost(&self.egraph, &self.input_metadata, &ids[1]);
                println!("Hash costs 1={}, 2={}", cost1, cost2);
                if (cost1 < 50 || cost2 < 50) && cost1 + cost2 > 1000  {
                    cost = 2
                }
                cost
            },
            PlanLanguage::NestedLoopsJoin(ids) => {
                let mut cost = 1200;
                let cost1 = get_node_cost(&self.egraph, &self.input_metadata, &ids[0]);
                let cost2 = get_node_cost(&self.egraph, &self.input_metadata, &ids[1]);
                if cost1 + cost2 <= 1000 {
                    cost = 3
                }
                cost
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

    if args.len() < 2 {
        println!("Usage: eggplant input_file_path");
        return
    }

    let input_file = &args[1];
    let file_contents = fs::read_to_string(input_file)
        .expect("Could not read input file");
    
    let input_metadata = serde_json::from_str::<InputMetadata>(&file_contents).unwrap();
    println!("{:#?}", input_metadata);

    // (select (hashJoin (scan tbl1 id)  (seek tbl2 fid)))
    // (select (hashJoin (nestedLoopsJoin (scan tbl1 id) (seek tbl3 fid)) (seek tbl2 fid)))
    let input_expression = input_metadata.expression.to_string(); 
    
    // parse the input expression and create a runner object using rewrite rules
    let exp :RecExpr<PlanLanguage> = input_expression.trim().parse().unwrap();
    let runner: Runner<PlanLanguage, ()> = make_runner(&exp);

    let root = runner.roots[0];
    let cost_func = PlanCostFunction { egraph: &runner.egraph, input_metadata: input_metadata };
    let best = Extractor::new(&runner.egraph, cost_func).find_best(root).1;
   
    // print the input and output expression
    println!("input   [{}] {}", exp.as_ref().len(), exp);
    println!("normal  [{}] {}", best.as_ref().len(), best);
}

#[test]
fn test_join_operation_rewrites() {
    let input_metadata_json = r#"{
        "expression": "(mergeJoin tbl1 tbl2)",
        "tables": [ 
            {
                "name": "tbl1",
                "cardinality": 3
            },
            {
                "name": "tbl2",
                "cardinality": 20
            }
        ]
    }"#;
    
    let input_metadata = serde_json::from_str::<InputMetadata>(&input_metadata_json).unwrap();

    //let input_expression = "(select (hashJoin (mergeJoin (scan tbl1 id) (seek tbl3 fid)) (seek tbl2 fid)))";
    let input_expression = input_metadata.expression.to_string();
    let exp :RecExpr<PlanLanguage> = input_expression.trim().parse().unwrap();
    let runner: Runner<PlanLanguage, ()> = make_runner(&exp);
    assert!(matches!(runner.stop_reason, Some(StopReason::Saturated)));

    let root = runner.roots[0];
    let cost_func = PlanCostFunction { egraph: &runner.egraph, input_metadata: input_metadata };
    let best = Extractor::new(&runner.egraph, cost_func).find_best(root).1;
   
    // print the input and output expression
    println!("input   [{}] {}", exp.as_ref().len(), exp);
    println!("normal  [{}] {}", best.as_ref().len(), best);
}
