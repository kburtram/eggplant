// Egg Plan Transformer (eggplant)
// Developed for CSEP 590D Spring 2022
// Karl Burtram, kburtram@uw.edu

use egg::{*};
use serde::Deserialize;
use serde_json;
use std::env;
use std::fs;

// aliases for EGraph and Runner types with template parameters
type EPlanGraph = egg::EGraph<PlanLanguage, PlanAnalysis>;
type EPlanRunner = egg::Runner<PlanLanguage, PlanAnalysis>;

// global variable to store the input metadata, this is needed to be able
// to access the data from anywhere in the application, particularly analysis methods
static mut GLOBAL_METADATA: Option<InputMetadata> = None;

// data structures for the input JSON files, basically the expression
// to rewrite and related table metadata
#[derive(Deserialize, Debug, Clone)]
struct InputMetadata {
    expression: String,
    tables: Vec<Table>,    
}

#[derive(Deserialize, Debug, Clone)]
struct Table {
    name: String,
    cardinality: usize,
}

// plan analysis classes
#[derive(Default, Clone)]
struct PlanAnalysis;

// auxillary data that is attached to egraph nodes
#[derive(Debug)]
struct Data {
    cardinality: usize
}

impl Analysis<PlanLanguage> for PlanAnalysis {
    type Data = Data;
    fn merge(&mut self, to: &mut Data, from: Data) -> DidMerge {
        println!("analysis merge");
        DidMerge(false, false)   
    }

    fn make(egraph: &EPlanGraph, enode: &PlanLanguage) -> Data {
        println!("analysis make -> {}", enode);
        let mut cardinality = 1;
        match enode {            
            PlanLanguage::Symbol(sym) => {
                let table_name = sym.to_string();
                let table = global_get_table(table_name);
                cardinality = table.unwrap().cardinality;    
            },
            PlanLanguage::MergeJoin([table1_id, table2_id]) => {
                let t1_cardinality = egraph[*table1_id].data.cardinality;
                let t2_cardinality = egraph[*table2_id].data.cardinality;
                println!("merge t1={}, t2={}", t1_cardinality, t2_cardinality);
                cardinality = t1_cardinality + t2_cardinality;           
            },
            PlanLanguage::HashJoin([table1_id, table2_id]) => {
                let t1_cardinality = egraph[*table1_id].data.cardinality;
                let t2_cardinality = egraph[*table2_id].data.cardinality;
                println!("hash t1={}, t2={}", t1_cardinality, t2_cardinality);
                cardinality = t1_cardinality + t2_cardinality;
                
            },            
            PlanLanguage::NestedLoopsJoin([table1_id, table2_id]) => {
                let t1_cardinality = egraph[*table1_id].data.cardinality;
                let t2_cardinality = egraph[*table2_id].data.cardinality;
                println!("nested t1={}, t2={}", t1_cardinality, t2_cardinality);
                cardinality = t1_cardinality + t2_cardinality;
                              
            },
            _ => { }
        };
        Data { cardinality }
    }

    fn modify(egraph: &mut EPlanGraph, id: Id) {
        println!("analysis modify  --> {}", id);
    }
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
    egraph: &'a EPlanGraph
}

fn global_get_table(table_name: String) -> Option<Table> {
    unsafe {
        if GLOBAL_METADATA.is_some() {
            let metadata: &InputMetadata = GLOBAL_METADATA.as_ref().unwrap();
            for t in metadata.tables.iter() {
                if t.name == table_name {
                    return Some(t.clone());
                }
            }
        }
    }
    None
}

fn get_symbol_cost(sym: &Symbol) -> usize {
    println!("symcost = {}", sym.to_string());    
    let table = global_get_table(sym.to_string());
    if table.is_some() {
        return table.unwrap().cardinality;
    }
    1
}

impl<'a> egg::CostFunction<PlanLanguage> for PlanCostFunction<'a> {
    type Cost = usize;
    fn cost<C>(&mut self, enode: &PlanLanguage, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let op_cost = match enode {            
            PlanLanguage::MergeJoin([table1_id, table2_id]) => {
                println!("---merge");
                let mut rank = 1000;
                let t1_cardinality = self.egraph[*table1_id].data.cardinality;
                let t2_cardinality = self.egraph[*table2_id].data.cardinality;
                if t1_cardinality > 50 && t2_cardinality > 50 && t1_cardinality + t2_cardinality > 1000 {
                    rank = 1
                }
                rank
            },
            PlanLanguage::HashJoin([table1_id, table2_id]) => {
                println!("---hash");
                let mut rank = 1100;
                let t1_cardinality = self.egraph[*table1_id].data.cardinality;
                let t2_cardinality = self.egraph[*table2_id].data.cardinality;
                if (t1_cardinality < 50 || t2_cardinality < 50) && t1_cardinality + t2_cardinality > 1000 {
                    rank = 2
                }
                rank
            },            
            PlanLanguage::NestedLoopsJoin([table1_id, table2_id]) => {
                println!("---nested");
                let mut rank = 1200;
                let t1_cardinality = self.egraph[*table1_id].data.cardinality;
                let t2_cardinality = self.egraph[*table2_id].data.cardinality;
                if t1_cardinality + t2_cardinality <= 1000 {
                    rank = 3
                }
                rank                
            },
            PlanLanguage::Symbol(sym) => {
                let cost = get_symbol_cost(&sym);
                cost
            }
            _ => 1,
        };
       
        let cost = enode.fold(op_cost, |sum, i| sum + costs(i));
        println!("Op_Cost = {}, Cost = {}", op_cost, cost);
        cost
    }
}

// create a plan runner with the query plan rewrite rules
#[rustfmt::skip]
fn make_runner(exp: &RecExpr<PlanLanguage>) -> EPlanRunner {
    // rewrite rules vector
    let rules = vec![
        // join operations rewrite rules
        rewrite!("hash-join-merge-join"; "(hashJoin ?a ?b)"         => "(mergeJoin ?a ?b)"),
        rewrite!("hash-join-nested-loops-join"; "(hashJoin ?a ?b)"   => "(nestedLoopsJoin ?a ?b)"),
        rewrite!("merge-join-hash-join"; "(mergeJoin ?a ?b)"        => "(hashJoin ?a ?b)"),
        rewrite!("merge-join-nested-loops-join"; "(mergeJoin ?a ?b)" => "(nestedLoopsJoin ?a ?b)"),
        rewrite!("nested-loops-join-merge-join"; "(nestedLoopsJoin ?a ?b)" => "(mergeJoin ?a ?b)"),
        rewrite!("nested-loops-join-hash-join"; "(nestedLoopsJoin ?a ?b)" => "(hashJoin ?a ?b)"),
    ];

    let runner: EPlanRunner =  Runner::default()
        .with_iter_limit(1000)
        .with_expr(&exp)
        .run(&rules);
    runner
}

// entry point into the plan rewriter application
fn main() {
    // print the application header
    println!("Egg-based Plan Optimizer for T-SQL Queries (eggplant)");

    // read the input string from the command-line
    let args: Vec<String> = env::args().collect();

    // check that the path to input metadata file was provided
    if args.len() < 2 {
        println!("Usage: eggplant input_file_path");
        return
    }

    // read the input file
    let input_file = &args[1];
    let file_contents = fs::read_to_string(input_file)
        .expect("Could not read input file");
    
    // deserialize the input JSON
    let input_metadata = serde_json::from_str::<InputMetadata>(&file_contents).unwrap();
    println!("{:#?}", input_metadata);

    unsafe {
        // store the metadata input global variable to access it later
        GLOBAL_METADATA = Some(input_metadata.clone());
    }

    // get the expression from the input metadata object
    let input_expression = input_metadata.expression.to_string(); 
    
    // parse the input expression and create a runner object using rewrite rules
    let exp :RecExpr<PlanLanguage> = input_expression.trim().parse().unwrap();
    let runner: EPlanRunner = make_runner(&exp);

    // extract the best query plan expression using the cost function
    let root = runner.roots[0];
    let cost_func = PlanCostFunction { egraph: &runner.egraph  };
    let best = Extractor::new(&runner.egraph, cost_func).find_best(root).1;
   
    // print the input and output expression
    println!("input   [{}] {}", exp.as_ref().len(), exp);
    println!("normal  [{}] {}", best.as_ref().len(), best);
}

// verify a basic join operation rewrite
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
    let input_expression = input_metadata.expression.to_string();
    let exp :RecExpr<PlanLanguage> = input_expression.trim().parse().unwrap();
    let runner: EPlanRunner = make_runner(&exp);
    assert!(matches!(runner.stop_reason, Some(StopReason::Saturated)));

    let root = runner.roots[0];
    let cost_func = PlanCostFunction { egraph: &runner.egraph };
    let best = Extractor::new(&runner.egraph, cost_func).find_best(root).1;
   
    // print the input and output expression
    println!("input   [{}] {}", exp.as_ref().len(), exp);
    println!("normal  [{}] {}", best.as_ref().len(), best);
}

// fn get_table(input_metadata: &InputMetadata, table_name: String) -> Option<&Table> {
//     for t in input_metadata.tables.iter() {
//         if t.name == table_name {
//             return Some(t);
//         }
//     }
//     None
// }
// fn get_node_cost(egraph: &EPlanGraph, input_metadata: &InputMetadata, id: &Id) -> usize {
//     println!("nodecost = {}", *id);
//     let mut cost = 1;
//     for c in egraph.classes() {
//         if c.id == *id  {
//             for n in c.iter() {
//                 for t in input_metadata.tables.iter() {
//                     if t.name == n.to_string() {
//                         println!("TABLE = {}", n);
//                         cost = t.cardinality;
//                     }
//                 }                
//             }
//         }
//     }

//     let node = &egraph[*id];
//     //node.data.cardinality = global_get_table
//     println!("node-->{:#?}", node);
//     cost
// }
