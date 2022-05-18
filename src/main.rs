// Egg-based Plan Optimizer for T-SQL Queries (eggplant)
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

static mut GLOBAL_METADATA: Option<InputMetadata> = None;

#[derive(Clone)]
struct PlanAnalysis {
    input_metadata: InputMetadata
}

#[derive(Debug)]
struct Data {
    // free: HashSet<Id>,
    // constant: Option<(Lambda, PatternAst<Lambda>)>,
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

        let table_name = match enode {            
            PlanLanguage::Symbol(sym) => {
                Some(sym.to_string())
            }
            _ => None,
        };

        let mut cardinality = 1;
        if table_name.is_some() {
            let table = global_get_table(table_name.unwrap());
            cardinality = table.unwrap().cardinality;            
        }
        Data { cardinality }
    }

    fn modify(egraph: &mut EPlanGraph, id: Id) {
        println!("analysis modify  --> {}", id);
    }
}

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
    egraph: &'a EPlanGraph,
    input_metadata: InputMetadata,
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

fn get_table(input_metadata: &InputMetadata, table_name: String) -> Option<&Table> {
    for t in input_metadata.tables.iter() {
        if t.name == table_name {
            return Some(t);
        }
    }
    None
}

fn get_symbol_cost(input_metadata: &InputMetadata, sym: &Symbol) -> usize {
    println!("symcost = {}", sym.to_string());    
    let table = get_table(input_metadata, sym.to_string());
    if table.is_some() {
        return table.unwrap().cardinality;
    }
    1
}

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

impl<'a> egg::CostFunction<PlanLanguage> for PlanCostFunction<'a> {
    type Cost = usize;
    fn cost<C>(&mut self, enode: &PlanLanguage, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let op_cost = match enode {            
            PlanLanguage::MergeJoin([table1_id, table2_id]) => {
                println!("---merge");
                let mut cost = 1000;
                let t1_cardinality = self.egraph[*table1_id].data.cardinality;
                let t2_cardinality = self.egraph[*table2_id].data.cardinality;
                if t1_cardinality > 50 && t2_cardinality > 50 && t1_cardinality + t2_cardinality > 1000 {
                    cost = 1
                }
                cost
            },
            PlanLanguage::HashJoin([table1_id, table2_id]) => {
                println!("---hash");
                let mut cost = 1100;
                let t1_cardinality = self.egraph[*table1_id].data.cardinality;
                let t2_cardinality = self.egraph[*table2_id].data.cardinality;
                if (t1_cardinality < 50 || t2_cardinality < 50) && t1_cardinality + t2_cardinality > 1000 {
                    cost = 2
                }
                cost
            },            
            PlanLanguage::NestedLoopsJoin([table1_id, table2_id]) => {
                println!("---nested");
                let mut cost = 1200;
                let t1_cardinality = self.egraph[*table1_id].data.cardinality;
                let t2_cardinality = self.egraph[*table2_id].data.cardinality;
                if t1_cardinality + t2_cardinality <= 1000 {
                    cost = 3
                }
                cost                
            },
            PlanLanguage::Symbol(sym) => {
                let cost = get_symbol_cost(&self.input_metadata, &sym);
                cost
            }
            _ => 1,
        };
       
        let cost = enode.fold(op_cost, |sum, i| sum + costs(i));
        println!("Op_Cost = {}, Cost = {}", op_cost, cost);
        cost
    }
}

#[rustfmt::skip]
fn make_runner(exp: &RecExpr<PlanLanguage>, input_metadata: &InputMetadata) -> EPlanRunner {
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

    let runner: EPlanRunner = Runner::new(PlanAnalysis {
            input_metadata: input_metadata.clone(),
        })
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

    unsafe {
        // store the metadata input global variable to access it later
        GLOBAL_METADATA = Some(input_metadata.clone());
    }

    // (select (hashJoin (scan tbl1 id)  (seek tbl2 fid)))
    // (select (hashJoin (nestedLoopsJoin (scan tbl1 id) (seek tbl3 fid)) (seek tbl2 fid)))
    let input_expression = input_metadata.expression.to_string(); 
    
    // parse the input expression and create a runner object using rewrite rules
    let exp :RecExpr<PlanLanguage> = input_expression.trim().parse().unwrap();
    let runner: EPlanRunner = make_runner(&exp, &input_metadata);

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
    let input_expression = input_metadata.expression.to_string();
    let exp :RecExpr<PlanLanguage> = input_expression.trim().parse().unwrap();
    let runner: EPlanRunner = make_runner(&exp, &input_metadata);
    assert!(matches!(runner.stop_reason, Some(StopReason::Saturated)));

    let root = runner.roots[0];
    let cost_func = PlanCostFunction { egraph: &runner.egraph, input_metadata: input_metadata };
    let best = Extractor::new(&runner.egraph, cost_func).find_best(root).1;
   
    // print the input and output expression
    println!("input   [{}] {}", exp.as_ref().len(), exp);
    println!("normal  [{}] {}", best.as_ref().len(), best);
}
