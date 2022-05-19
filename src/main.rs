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
    index: String,
    cardinality: usize,
    rows: usize
}

// function that looks up the metadata for a table
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

// plan analysis classes
#[derive(Default, Clone)]
struct PlanAnalysis;

// auxillary data that is attached to egraph nodes
#[derive(Debug)]
struct Data {
    cardinality: usize,
    is_table: bool,
    index: Option<String>,
    penalty: usize
}

// get the cardinality of a join by summing its children's cardinalities
fn get_join_cardinality(egraph: &EPlanGraph, lhs_id: &Id, rhs_id: &Id) -> (usize, usize) {
    let t1_data = &egraph[*lhs_id].data;
    let t2_data = &egraph[*rhs_id].data;

    let mut scale: f32 = 1.0;
    if !t1_data.is_table || !t2_data.is_table {
        scale = 1.1;
    }

    let mut t1_cardinality: f32 = t1_data.cardinality as f32;
    let mut t2_cardinality: f32 = t2_data.cardinality as f32;
 
    if t1_data.is_table && t1_data.index.as_ref().unwrap() != "primary" {
        t1_cardinality *= scale;
    }

    if t2_data.is_table && t2_data.index.as_ref().unwrap() != "primary" {
        t2_cardinality *= scale;
    }

    println!("cardinality={}, t1={}, t2={}, scale={}", 
        (t1_cardinality + t2_cardinality) as usize, 
        t1_cardinality,  t2_cardinality, scale);

    ((t1_data.cardinality + t2_data.cardinality), (t1_cardinality + t2_cardinality) as usize)
}

impl Analysis<PlanLanguage> for PlanAnalysis {
    type Data = Data;
    fn merge(&mut self, _to: &mut Data, _from: Data) -> DidMerge {
        DidMerge(false, false)   
    }

    fn make(egraph: &EPlanGraph, enode: &PlanLanguage) -> Data {
        let mut cardinality: usize = 1;
        let mut penalty: usize = 1;
        let mut is_table: bool = false;
        let mut index: Option<String> = None;
        match enode {            
            PlanLanguage::Symbol(sym) => {                 
                let tbl = global_get_table(sym.to_string());
                if tbl.is_some() {
                    let table = tbl.unwrap(); 
                    is_table = true;
                    cardinality = table.cardinality;
                    index = Some(table.index);
                    // if index.as_ref().unwrap() == "primary" {
                    //     penalty = cardinality;
                    // } else {
                    //     penalty = cardinality * 2;
                    // }
                    
                }              
            },
            PlanLanguage::MergeJoin([table1_id, table2_id]) => {
                (cardinality, penalty) = get_join_cardinality(egraph, table1_id, table2_id);
            },
            PlanLanguage::HashJoin([table1_id, table2_id]) => {
                (cardinality, penalty) = get_join_cardinality(egraph, table1_id, table2_id);
            },            
            PlanLanguage::NestedLoopsJoin([table1_id, table2_id]) => {
                (cardinality, penalty) = get_join_cardinality(egraph, table1_id, table2_id);
            },
            _ => { }
        };
        Data { cardinality, is_table, index, penalty }
    }

    fn modify(_egraph: &mut EPlanGraph, _id: Id) {
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

// the following methods implement the plan expression cost function, this is 
// the core of determining which rewrite rules should be applied to an input expression
pub struct PlanCostFunction<'a> {
    egraph: &'a EPlanGraph
}

// get the cost for a generic symbol
fn get_symbol_cost(sym: &Symbol) -> usize {    
    // if the symbol is a table than lookup its cardinality, otherwise default to
    let table = global_get_table(sym.to_string());
    if table.is_some() {
        return table.as_ref().unwrap().cardinality;       
    }
    1
}

// get the costs for the join operations according to join preference rules
fn get_merge_join_cost(egraph: &EPlanGraph, table1_id: &Id, table2_id: &Id) -> usize {
    // println!("---merge");
    let mut rank: usize = 700000;
    let t1_data = &egraph[*table1_id].data;
    let t2_data = &egraph[*table2_id].data;
    let t1_cardinality = t1_data.cardinality;
    let t2_cardinality = t2_data.cardinality;
    if t1_cardinality > 50 && t2_cardinality > 50 && t1_cardinality + t2_cardinality > 1000 {
        rank = 1;
    }
    if t1_data.index.is_some() {
        if t1_data.index.as_ref().unwrap() != "primary" {
            rank += 20000;
        } else {
            rank += 10000;
        }
    } else {
        rank += 1;
    }
    rank + t1_data.penalty + t2_data.penalty    
}

fn get_hash_join_cost(egraph: &EPlanGraph, table1_id: &Id, table2_id: &Id) -> usize {
    // println!("---hash");
    let mut rank: usize = 800000;
    let t1_data = &egraph[*table1_id].data;
    let t2_data = &egraph[*table2_id].data;
    let t1_cardinality = t1_data.cardinality;
    let t2_cardinality = t2_data.cardinality;
    if (t1_cardinality <= 50 || t2_cardinality <= 50) && t1_cardinality + t2_cardinality > 1000 {
        rank = 2;
    }
    if t1_data.index.is_some() {
        if t1_data.index.as_ref().unwrap() != "primary" {
            rank += 20000;
        } else {
            rank += 10000;
        }
    } else {
        rank += 1;
    }
    rank + t1_data.penalty + t2_data.penalty
}

fn get_nested_loops_join_cost(egraph: &EPlanGraph, table1_id: &Id, table2_id: &Id) -> usize {
    // println!("---nested");
    let mut rank: usize = 900000;
    let t1_data = &egraph[*table1_id].data;
    let t2_data = &egraph[*table2_id].data;
    if t1_data.cardinality + t2_data.cardinality <= 1000 {
        rank = 3;
    }
    if t1_data.index.is_some() {
        if t1_data.index.as_ref().unwrap() != "primary" {
            rank += 20000;
        } else {
            rank += 10000;
        }
    } else {
        rank += 1;
    }
    rank + t1_data.penalty + t2_data.penalty
}

// core cost function that dispatches to language element cost functions
impl<'a> egg::CostFunction<PlanLanguage> for PlanCostFunction<'a> {
    type Cost = usize;
    fn cost<C>(&mut self, enode: &PlanLanguage, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let op_cost = match enode {            
            PlanLanguage::MergeJoin([table1_id, table2_id])
                => get_merge_join_cost(self.egraph, table1_id, table2_id),
            PlanLanguage::HashJoin([table1_id, table2_id])
                => get_hash_join_cost(self.egraph, table1_id, table2_id),           
            PlanLanguage::NestedLoopsJoin([table1_id, table2_id])
                => get_nested_loops_join_cost(self.egraph, table1_id, table2_id),
            PlanLanguage::Symbol(sym) => get_symbol_cost(&sym),
            _ => 1,
        };
       
        // sum the node cost with its child nodes
        let cost = enode.fold(op_cost, |sum, i| sum + costs(i));
        println!("Op_Cost = {c1:>width$}, Total_Cost = {c2:>width$}", c1=op_cost, c2=cost, width=8);
        cost
    }
}

// create a plan runner with the query plan rewrite rules
#[rustfmt::skip]
fn make_runner(exp: &RecExpr<PlanLanguage>) -> EPlanRunner {
    // rewrite rules vector
    let rules = vec![
        // join order rewrite rules        
        rewrite!("idx-left"; "(hashJoin ?a ?b)" => "(hashJoin ?b ?a)"),

        rewrite!("hjhj-order-right"; "(hashJoin (hashJoin ?a ?b) ?c)" => "(hashJoin (hashJoin ?a ?c) ?b)"),

        // join operations rewrite rules
        rewrite!("hash-join-merge-join"; "(hashJoin ?a ?b)"         => "(mergeJoin ?a ?b)"),
        rewrite!("hash-join-nested-loops-join"; "(hashJoin ?a ?b)"   => "(nestedLoopsJoin ?a ?b)"),
        rewrite!("merge-join-hash-join"; "(mergeJoin ?a ?b)"        => "(hashJoin ?a ?b)"),
        rewrite!("merge-join-nested-loops-join"; "(mergeJoin ?a ?b)" => "(nestedLoopsJoin ?a ?b)"),
        rewrite!("nested-loops-join-merge-join"; "(nestedLoopsJoin ?a ?b)" => "(mergeJoin ?a ?b)"),
        rewrite!("nested-loops-join-hash-join"; "(nestedLoopsJoin ?a ?b)" => "(hashJoin ?a ?b)"),
    ];

    // create the runner
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
                "cardinality": 3,
                "rows": 1000,
                "index": "primary"
            },
            {
                "name": "tbl2",
                "cardinality": 20,
                "rows": 1000,
                "index": "foreign"
            }
        ]
    }"#;

    let input_metadata = serde_json::from_str::<InputMetadata>(&input_metadata_json).unwrap();
    unsafe {
        // store the metadata input global variable to access it later
        GLOBAL_METADATA = Some(input_metadata.clone());
    }

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
