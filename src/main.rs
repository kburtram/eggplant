// Egg-based Plan Optimizer for T-SQL Queries (eggplant)
// Developed for CSEP 590D Spring 2022
// Karl Burtram, kburtram@uw.edu

use egg::{rewrite as rw, *};
use fxhash::FxHashSet as HashSet;
use ordered_float::NotNan;




define_language! {
    enum QueryLanguage {
        "select" = Select(Id),
        "hashJoin" = HashJoin(Id),
        "mergeJoin" = MergeJoin(Id),
        Constant(Constant),
        Symbol(Symbol),
    }
}



define_language! {
    enum Prop {
        Bool(bool),
        "&" = And([Id; 2]),
        "~" = Not(Id),
        "|" = Or([Id; 2]),
        "->" = Implies([Id; 2]),
        Symbol(Symbol),
    }
}

type EGraphM = egg::EGraph<Prop, ConstantFoldM>;
type RewriteM = egg::Rewrite<Prop, ConstantFoldM>;

#[derive(Default)]
struct ConstantFoldM;
impl Analysis<Prop> for ConstantFoldM {
    type Data = Option<(bool, PatternAst<Prop>)>;
    fn merge(&mut self, to: &mut Self::Data, from: Self::Data) -> DidMerge {
        merge_max(to, from)
    }

    fn make(egraph: &EGraphM, enode: &Prop) -> Self::Data {
        let x = |i: &Id| egraph[*i].data.as_ref().map(|c| c.0);
        let result = match enode {
            Prop::Bool(c) => Some((*c, c.to_string().parse().unwrap())),
            Prop::Symbol(_) => None,
            Prop::And([a, b]) => Some((
                x(a)? && x(b)?,
                format!("(& {} {})", x(a)?, x(b)?).parse().unwrap(),
            )),
            Prop::Not(a) => Some((!x(a)?, format!("(~ {})", x(a)?).parse().unwrap())),
            Prop::Or([a, b]) => Some((
                x(a)? || x(b)?,
                format!("(| {} {})", x(a)?, x(b)?).parse().unwrap(),
            )),
            Prop::Implies([a, b]) => Some((
                x(a)? || !x(b)?,
                format!("(-> {} {})", x(a)?, x(b)?).parse().unwrap(),
            )),
        };
        println!("Make: {:?} -> {:?}", enode, result);
        result
    }

    fn modify(egraph: &mut EGraphM, id: Id) {
        if let Some(c) = egraph[id].data.clone() {
            egraph.union_instantiations(
                &c.1,
                &c.0.to_string().parse().unwrap(),
                &Default::default(),
                "analysis".to_string(),
            );
        }
    }
}

macro_rules! rule {
    ($name:ident, $left:literal, $right:literal) => {
        #[allow(dead_code)]
        fn $name() -> RewriteM {
            rewrite!(stringify!($name); $left => $right)
        }
    };
    ($name:ident, $name2:ident, $left:literal, $right:literal) => {
        rule!($name, $left, $right);
        rule!($name2, $right, $left);
    };
}

rule! {def_imply, def_imply_flip,   "(-> ?a ?b)",       "(| (~ ?a) ?b)"          }
rule! {double_neg, double_neg_flip,  "(~ (~ ?a))",       "?a"                     }
rule! {assoc_or,    "(| ?a (| ?b ?c))", "(| (| ?a ?b) ?c)"       }
rule! {dist_and_or, "(& ?a (| ?b ?c))", "(| (& ?a ?b) (& ?a ?c))"}
rule! {dist_or_and, "(| ?a (& ?b ?c))", "(& (| ?a ?b) (| ?a ?c))"}
rule! {comm_or,     "(| ?a ?b)",        "(| ?b ?a)"              }
rule! {comm_and,    "(& ?a ?b)",        "(& ?b ?a)"              }
rule! {lem,         "(| ?a (~ ?a))",    "true"                      }
rule! {or_true,     "(| ?a true)",         "true"                      }
rule! {and_true,    "(& ?a true)",         "?a"                     }
rule! {contrapositive, "(-> ?a ?b)",    "(-> (~ ?b) (~ ?a))"     }
rule! {lem_imply, "(& (-> ?a ?b) (-> (~ ?a) ?c))", "(| ?b ?c)"}

fn prove_something(name: &str, start: &str, rewrites: &[RewriteM], goals: &[&str]) {
    let _ = env_logger::builder().is_test(true).try_init();
    println!("Proving {}", name);

    let start_expr: RecExpr<_> = start.parse().unwrap();
    let goal_exprs: Vec<RecExpr<_>> = goals.iter().map(|g| g.parse().unwrap()).collect();

    let egraph = Runner::default()
        .with_iter_limit(20)
        .with_node_limit(5_000)
        .with_expr(&start_expr)
        .run(rewrites)
        .egraph;

    for (i, (goal_expr, goal_str)) in goal_exprs.iter().zip(goals).enumerate() {
        println!("Trying to prove goal {}: {}", i, goal_str);
        let equivs = egraph.equivs(&start_expr, goal_expr);
        if equivs.is_empty() {
            panic!("Couldn't prove goal {}: {}", i, goal_str);
        }
    }
}



define_language! {
    enum Lambda {
        Bool(bool),
        Num(i32),

        "var" = Var(Id),

        "+" = Add([Id; 2]),
        "=" = Eq([Id; 2]),

        "app" = App([Id; 2]),
        "lam" = Lambda([Id; 2]),
        "let" = Let([Id; 3]),
        "fix" = Fix([Id; 2]),

        "if" = If([Id; 3]),

        Symbol(egg::Symbol),
    }
}

impl Lambda {
    fn num(&self) -> Option<i32> {
        match self {
            Lambda::Num(n) => Some(*n),
            _ => None,
        }
    }
}

type EGraphL = egg::EGraph<Lambda, LambdaAnalysis>;



#[derive(Default)]
struct LambdaAnalysis;

#[derive(Debug)]
struct Data {
    free: HashSet<Id>,
    constant: Option<(Lambda, PatternAst<Lambda>)>,
}

fn eval(egraph: &EGraphL, enode: &Lambda) -> Option<(Lambda, PatternAst<Lambda>)> {
    let x = |i: &Id| egraph[*i].data.constant.as_ref().map(|c| &c.0);
    match enode {
        Lambda::Num(n) => Some((enode.clone(), format!("{}", n).parse().unwrap())),
        Lambda::Bool(b) => Some((enode.clone(), format!("{}", b).parse().unwrap())),
        Lambda::Add([a, b]) => Some((
            Lambda::Num(x(a)?.num()? + x(b)?.num()?),
            format!("(+ {} {})", x(a)?, x(b)?).parse().unwrap(),
        )),
        Lambda::Eq([a, b]) => Some((
            Lambda::Bool(x(a)? == x(b)?),
            format!("(= {} {})", x(a)?, x(b)?).parse().unwrap(),
        )),
        _ => None,
    }
}

impl Analysis<Lambda> for LambdaAnalysis {
    type Data = Data;
    fn merge(&mut self, to: &mut Data, from: Data) -> DidMerge {
        let before_len = to.free.len();
        // to.free.extend(from.free);
        to.free.retain(|i| from.free.contains(i));
        if to.constant.is_none() && from.constant.is_some() {
            to.constant = from.constant;
            DidMerge(true, to.free.len() != from.free.len())
        } else {
            DidMerge(before_len != to.free.len(), true)
        }
    }

    fn make(egraph: &EGraphL, enode: &Lambda) -> Data {
        let f = |i: &Id| egraph[*i].data.free.iter().cloned();
        let mut free = HashSet::default();
        match enode {
            Lambda::Var(v) => {
                free.insert(*v);
            }
            Lambda::Let([v, a, b]) => {
                free.extend(f(b));
                free.remove(v);
                free.extend(f(a));
            }
            Lambda::Lambda([v, a]) | Lambda::Fix([v, a]) => {
                free.extend(f(a));
                free.remove(v);
            }
            _ => enode.for_each(|c| free.extend(&egraph[c].data.free)),
        }
        let constant = eval(egraph, enode);
        Data { constant, free }
    }

    fn modify(egraph: &mut EGraphL, id: Id) {
        if let Some(c) = egraph[id].data.constant.clone() {
            if egraph.are_explanations_enabled() {
                egraph.union_instantiations(
                    &c.0.to_string().parse().unwrap(),
                    &c.1,
                    &Default::default(),
                    "analysis".to_string(),
                );
            } else {
                let const_id = egraph.add(c.0);
                egraph.union(id, const_id);
            }
        }
    }
}

fn var(s: &str) -> Var {
    s.parse().unwrap()
}

fn is_not_same_var(v1: Var, v2: Var) -> impl Fn(&mut EGraphL, Id, &Subst) -> bool {
    move |egraph, _, subst| egraph.find(subst[v1]) != egraph.find(subst[v2])
}

fn is_constL(v: Var) -> impl Fn(&mut EGraphL, Id, &Subst) -> bool {
    move |egraph, _, subst| egraph[subst[v]].data.constant.is_some()
}

fn rules() -> Vec<egg::Rewrite<Lambda, LambdaAnalysis>> {
    vec![
        // open term rules
        rw!("if-true";  "(if  true ?then ?else)" => "?then"),
        rw!("if-false"; "(if false ?then ?else)" => "?else"),
        rw!("if-elim"; "(if (= (var ?x) ?e) ?then ?else)" => "?else"
            if ConditionEqual::parse("(let ?x ?e ?then)", "(let ?x ?e ?else)")),
        rw!("add-comm";  "(+ ?a ?b)"        => "(+ ?b ?a)"),
        rw!("add-assoc"; "(+ (+ ?a ?b) ?c)" => "(+ ?a (+ ?b ?c))"),
        rw!("eq-comm";   "(= ?a ?b)"        => "(= ?b ?a)"),
        // subst rules
        rw!("fix";      "(fix ?v ?e)"             => "(let ?v (fix ?v ?e) ?e)"),
        rw!("beta";     "(app (lam ?v ?body) ?e)" => "(let ?v ?e ?body)"),
        rw!("let-app";  "(let ?v ?e (app ?a ?b))" => "(app (let ?v ?e ?a) (let ?v ?e ?b))"),
        rw!("let-add";  "(let ?v ?e (+   ?a ?b))" => "(+   (let ?v ?e ?a) (let ?v ?e ?b))"),
        rw!("let-eq";   "(let ?v ?e (=   ?a ?b))" => "(=   (let ?v ?e ?a) (let ?v ?e ?b))"),
        rw!("let-const";
            "(let ?v ?e ?c)" => "?c" if is_constL(var("?c"))),
        rw!("let-if";
            "(let ?v ?e (if ?cond ?then ?else))" =>
            "(if (let ?v ?e ?cond) (let ?v ?e ?then) (let ?v ?e ?else))"
        ),
        rw!("let-var-same"; "(let ?v1 ?e (var ?v1))" => "?e"),
        rw!("let-var-diff"; "(let ?v1 ?e (var ?v2))" => "(var ?v2)"
            if is_not_same_var(var("?v1"), var("?v2"))),
        rw!("let-lam-same"; "(let ?v1 ?e (lam ?v1 ?body))" => "(lam ?v1 ?body)"),
        rw!("let-lam-diff";
            "(let ?v1 ?e (lam ?v2 ?body))" =>
            { CaptureAvoid {
                fresh: var("?fresh"), v2: var("?v2"), e: var("?e"),
                if_not_free: "(lam ?v2 (let ?v1 ?e ?body))".parse().unwrap(),
                if_free: "(lam ?fresh (let ?v1 ?e (let ?v2 (var ?fresh) ?body)))".parse().unwrap(),
            }}
            if is_not_same_var(var("?v1"), var("?v2"))),
    ]
}

struct CaptureAvoid {
    fresh: Var,
    v2: Var,
    e: Var,
    if_not_free: Pattern<Lambda>,
    if_free: Pattern<Lambda>,
}

impl Applier<Lambda, LambdaAnalysis> for CaptureAvoid {
    fn apply_one(
        &self,
        egraph: &mut EGraphL,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&PatternAst<Lambda>>,
        rule_name: Symbol,
    ) -> Vec<Id> {
        let e = subst[self.e];
        let v2 = subst[self.v2];
        let v2_free_in_e = egraph[e].data.free.contains(&v2);
        if v2_free_in_e {
            let mut subst = subst.clone();
            let sym = Lambda::Symbol(format!("_{}", eclass).into());
            subst.insert(self.fresh, egraph.add(sym));
            self.if_free
                .apply_one(egraph, eclass, &subst, searcher_ast, rule_name)
        } else {
            self.if_not_free
                .apply_one(egraph, eclass, subst, searcher_ast, rule_name)
        }
    }
}

// egg::test_fn! {
//     lambda_under, rules(),
//     "(lam x (+ 4
//                (app (lam y (var y))
//                     4)))"
//     =>
//     // "(lam x (+ 4 (let y 4 (var y))))",
//     // "(lam x (+ 4 4))",
//     "(lam x 8))",
// }

// egg::test_fn! {
//     lambda_if_elim, rules(),
//     "(if (= (var a) (var b))
//          (+ (var a) (var a))
//          (+ (var a) (var b)))"
//     =>
//     "(+ (var a) (var b))"
// }

// egg::test_fn! {
//     lambda_let_simple, rules(),
//     "(let x 0
//      (let y 1
//      (+ (var x) (var y))))"
//     =>
//     // "(let ?a 0
//     //  (+ (var ?a) 1))",
//     // "(+ 0 1)",
//     "1",
// }

// egg::test_fn! {
//     #[should_panic(expected = "Could not prove goal 0")]
//     lambda_capture, rules(),
//     "(let x 1 (lam x (var x)))" => "(lam x 1)"
// }

// egg::test_fn! {
//     #[should_panic(expected = "Could not prove goal 0")]
//     lambda_capture_free, rules(),
//     "(let y (+ (var x) (var x)) (lam x (var y)))" => "(lam x (+ (var x) (var x)))"
// }

// egg::test_fn! {
//     #[should_panic(expected = "Could not prove goal 0")]
//     lambda_closure_not_seven, rules(),
//     "(let five 5
//      (let add-five (lam x (+ (var x) (var five)))
//      (let five 6
//      (app (var add-five) 1))))"
//     =>
//     "7"
// }

// egg::test_fn! {
//     lambda_compose, rules(),
//     "(let compose (lam f (lam g (lam x (app (var f)
//                                        (app (var g) (var x))))))
//      (let add1 (lam y (+ (var y) 1))
//      (app (app (var compose) (var add1)) (var add1))))"
//     =>
//     "(lam ?x (+ 1
//                 (app (lam ?y (+ 1 (var ?y)))
//                      (var ?x))))",
//     "(lam ?x (+ (var ?x) 2))"
// }

// egg::test_fn! {
//     lambda_if_simple, rules(),
//     "(if (= 1 1) 7 9)" => "7"
// }

// egg::test_fn! {
//     lambda_compose_many, rules(),
//     "(let compose (lam f (lam g (lam x (app (var f)
//                                        (app (var g) (var x))))))
//      (let add1 (lam y (+ (var y) 1))
//      (app (app (var compose) (var add1))
//           (app (app (var compose) (var add1))
//                (app (app (var compose) (var add1))
//                     (app (app (var compose) (var add1))
//                          (app (app (var compose) (var add1))
//                               (app (app (var compose) (var add1))
//                                    (var add1)))))))))"
//     =>
//     "(lam ?x (+ (var ?x) 7))"
// }

// egg::test_fn! {
//     #[cfg(not(debug_assertions))]
//     #[cfg_attr(feature = "test-explanations", ignore)]
//     lambda_function_repeat, rules(),
//     runner = Runner::default()
//         .with_time_limit(std::time::Duration::from_secs(20))
//         .with_node_limit(150_000)
//         .with_iter_limit(60),
//     "(let compose (lam f (lam g (lam x (app (var f)
//                                        (app (var g) (var x))))))
//      (let repeat (fix repeat (lam fun (lam n
//         (if (= (var n) 0)
//             (lam i (var i))
//             (app (app (var compose) (var fun))
//                  (app (app (var repeat)
//                            (var fun))
//                       (+ (var n) -1)))))))
//      (let add1 (lam y (+ (var y) 1))
//      (app (app (var repeat)
//                (var add1))
//           2))))"
//     =>
//     "(lam ?x (+ (var ?x) 2))"
// }

// egg::test_fn! {
//     lambda_if, rules(),
//     "(let zeroone (lam x
//         (if (= (var x) 0)
//             0
//             1))
//         (+ (app (var zeroone) 0)
//         (app (var zeroone) 10)))"
//     =>
//     // "(+ (if false 0 1) (if true 0 1))",
//     // "(+ 1 0)",
//     "1",
// }

// egg::test_fn! {
//     #[cfg(not(debug_assertions))]
//     #[cfg_attr(feature = "test-explanations", ignore)]
//     lambda_fib, rules(),
//     runner = Runner::default()
//         .with_iter_limit(60)
//         .with_node_limit(50_000),
//     "(let fib (fix fib (lam n
//         (if (= (var n) 0)
//             0
//         (if (= (var n) 1)
//             1
//         (+ (app (var fib)
//                 (+ (var n) -1))
//             (app (var fib)
//                 (+ (var n) -2)))))))
//         (app (var fib) 4))"
//     => "3"
// }


pub type EGraph = egg::EGraph<Math, ConstantFold>;
pub type Rewrite = egg::Rewrite<Math, ConstantFold>;

pub type Constant = NotNan<f64>;

define_language! {
    pub enum Math {
        "d" = Diff([Id; 2]),
        "i" = Integral([Id; 2]),

        "+" = Add([Id; 2]),
        "-" = Sub([Id; 2]),
        "*" = Mul([Id; 2]),
        "/" = Div([Id; 2]),
        "pow" = Pow([Id; 2]),
        "ln" = Ln(Id),
        "sqrt" = Sqrt(Id),

        "sin" = Sin(Id),
        "cos" = Cos(Id),

        Constant(Constant),
        Symbol(Symbol),
    }
}

// You could use egg::AstSize, but this is useful for debugging, since
// it will really try to get rid of the Diff operator
pub struct MathCostFn;
impl egg::CostFunction<Math> for MathCostFn {
    type Cost = usize;
    fn cost<C>(&mut self, enode: &Math, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let op_cost = match enode {
            Math::Diff(..) => 100,
            Math::Integral(..) => 100,
            _ => 1,
        };
        enode.fold(op_cost, |sum, i| sum + costs(i))
    }
}

#[derive(Default)]
pub struct ConstantFold;
impl Analysis<Math> for ConstantFold {
    type Data = Option<(Constant, PatternAst<Math>)>;

    fn make(egraph: &EGraph, enode: &Math) -> Self::Data {
        let x = |i: &Id| egraph[*i].data.as_ref().map(|d| d.0);
        Some(match enode {
            Math::Constant(c) => (*c, format!("{}", c).parse().unwrap()),
            Math::Add([a, b]) => (
                x(a)? + x(b)?,
                format!("(+ {} {})", x(a)?, x(b)?).parse().unwrap(),
            ),
            Math::Sub([a, b]) => (
                x(a)? - x(b)?,
                format!("(- {} {})", x(a)?, x(b)?).parse().unwrap(),
            ),
            Math::Mul([a, b]) => (
                x(a)? * x(b)?,
                format!("(* {} {})", x(a)?, x(b)?).parse().unwrap(),
            ),
            Math::Div([a, b]) if x(b) != Some(NotNan::new(0.0).unwrap()) => (
                x(a)? / x(b)?,
                format!("(/ {} {})", x(a)?, x(b)?).parse().unwrap(),
            ),
            _ => return None,
        })
    }

    fn merge(&mut self, a: &mut Self::Data, b: Self::Data) -> DidMerge {
        match (a.as_mut(), &b) {
            (None, None) => DidMerge(false, false),
            (None, Some(_)) => {
                *a = b;
                DidMerge(true, false)
            }
            (Some(_), None) => DidMerge(false, true),
            (Some(_), Some(_)) => DidMerge(false, false),
        }
        // if a.is_none() && b.is_some() {
        //     *a = b
        // }
        // cmp
    }

    fn modify(egraph: &mut EGraph, id: Id) {
        let class = egraph[id].clone();
        if let Some((c, pat)) = class.data {
            if egraph.are_explanations_enabled() {
                egraph.union_instantiations(
                    &pat,
                    &format!("{}", c).parse().unwrap(),
                    &Default::default(),
                    "constant_fold".to_string(),
                );
            } else {
                let added = egraph.add(Math::Constant(c));
                egraph.union(id, added);
            }
            // to not prune, comment this out
            egraph[id].nodes.retain(|n| n.is_leaf());

            #[cfg(debug_assertions)]
            egraph[id].assert_unique_leaves();
        }
    }
}

fn is_const_or_distinct_var(v: &str, w: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let v = v.parse().unwrap();
    let w = w.parse().unwrap();
    move |egraph, _, subst| {
        egraph.find(subst[v]) != egraph.find(subst[w])
            && (egraph[subst[v]].data.is_some()
                || egraph[subst[v]]
                    .nodes
                    .iter()
                    .any(|n| matches!(n, Math::Symbol(..))))
    }
}

fn is_const(var: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let var = var.parse().unwrap();
    move |egraph, _, subst| egraph[subst[var]].data.is_some()
}

fn is_sym(var: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let var = var.parse().unwrap();
    move |egraph, _, subst| {
        egraph[subst[var]]
            .nodes
            .iter()
            .any(|n| matches!(n, Math::Symbol(..)))
    }
}

fn is_not_zero(var: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let var = var.parse().unwrap();
    move |egraph, _, subst| {
        if let Some(n) = &egraph[subst[var]].data {
            *(n.0) != 0.0
        } else {
            true
        }
    }
}

define_language! {
    enum Lang {
        "true" = True,
        Int(i32),
        Relation(Symbol, Box<[Id]>),
    }
}

trait DatalogExtTrait {
    fn assert(&mut self, s: &str);
    fn check(&mut self, s: &str);
    fn check_not(&mut self, s: &str);
}

impl DatalogExtTrait for egg::EGraph<Lang, ()> {
    fn assert(&mut self, s: &str) {
        let true_id = self.add(Lang::True);
        for e in s.split(',') {
            let exp = e.trim().parse().unwrap();
            let id = self.add_expr(&exp);
            self.union(true_id, id);
        }
    }

    fn check(&mut self, s: &str) {
        let true_id = self.add(Lang::True);
        for e in s.split(',') {
            let exp = e.trim().parse().unwrap();
            let id = self.add_expr(&exp);
            assert_eq!(true_id, id, "{} is not true", e);
        }
    }

    fn check_not(&mut self, s: &str) {
        let true_id = self.add(Lang::True);
        for e in s.split(',') {
            let exp = e.trim().parse().unwrap();
            let id = self.add_expr(&exp);
            assert!(true_id != id, "{} is true", e);
        }
    }
}

fn main() {
    println!("Hello, world!");
    
    let _input = "SELECT ( HASHJOIN (TABLE1, TABLE2) )";

    let mut egraph = egg::EGraph::<Lang, ()>::default();
    egraph.assert("(edge 1 2), (edge 2 3), (edge 3 4)");
    let rules = vec![
        multi_rewrite!("base-case"; "?x = true = (edge ?a ?b)" => "?x = (path ?a ?b)"),
        multi_rewrite!("transitive"; "?x = true = (path ?a ?b) = (edge ?b ?c)" => "?x = (path ?a ?c)"),
    ];

    let mut runner = Runner::default().with_egraph(egraph).run(&rules);
    runner.egraph.check("(path 1 4)");
    runner.egraph.check_not("(path 4 1)");

}




#[rustfmt::skip]
pub fn rulesM() -> Vec<Rewrite> { vec![
    rw!("comm-add";  "(+ ?a ?b)"        => "(+ ?b ?a)"),
    rw!("comm-mul";  "(* ?a ?b)"        => "(* ?b ?a)"),
    rw!("assoc-add"; "(+ ?a (+ ?b ?c))" => "(+ (+ ?a ?b) ?c)"),
    rw!("assoc-mul"; "(* ?a (* ?b ?c))" => "(* (* ?a ?b) ?c)"),

    rw!("sub-canon"; "(- ?a ?b)" => "(+ ?a (* -1 ?b))"),
    rw!("div-canon"; "(/ ?a ?b)" => "(* ?a (pow ?b -1))" if is_not_zero("?b")),
    // rw!("canon-sub"; "(+ ?a (* -1 ?b))"   => "(- ?a ?b)"),
    // rw!("canon-div"; "(* ?a (pow ?b -1))" => "(/ ?a ?b)" if is_not_zero("?b")),

    rw!("zero-add"; "(+ ?a 0)" => "?a"),
    rw!("zero-mul"; "(* ?a 0)" => "0"),
    rw!("one-mul";  "(* ?a 1)" => "?a"),

    rw!("add-zero"; "?a" => "(+ ?a 0)"),
    rw!("mul-one";  "?a" => "(* ?a 1)"),

    rw!("cancel-sub"; "(- ?a ?a)" => "0"),
    rw!("cancel-div"; "(/ ?a ?a)" => "1" if is_not_zero("?a")),

    rw!("distribute"; "(* ?a (+ ?b ?c))"        => "(+ (* ?a ?b) (* ?a ?c))"),
    rw!("factor"    ; "(+ (* ?a ?b) (* ?a ?c))" => "(* ?a (+ ?b ?c))"),

    rw!("pow-mul"; "(* (pow ?a ?b) (pow ?a ?c))" => "(pow ?a (+ ?b ?c))"),
    rw!("pow0"; "(pow ?x 0)" => "1"
        if is_not_zero("?x")),
    rw!("pow1"; "(pow ?x 1)" => "?x"),
    rw!("pow2"; "(pow ?x 2)" => "(* ?x ?x)"),
    rw!("pow-recip"; "(pow ?x -1)" => "(/ 1 ?x)"
        if is_not_zero("?x")),
    rw!("recip-mul-div"; "(* ?x (/ 1 ?x))" => "1" if is_not_zero("?x")),

    rw!("d-variable"; "(d ?x ?x)" => "1" if is_sym("?x")),
    rw!("d-constant"; "(d ?x ?c)" => "0" if is_sym("?x") if is_const_or_distinct_var("?c", "?x")),

    rw!("d-add"; "(d ?x (+ ?a ?b))" => "(+ (d ?x ?a) (d ?x ?b))"),
    rw!("d-mul"; "(d ?x (* ?a ?b))" => "(+ (* ?a (d ?x ?b)) (* ?b (d ?x ?a)))"),

    rw!("d-sin"; "(d ?x (sin ?x))" => "(cos ?x)"),
    rw!("d-cos"; "(d ?x (cos ?x))" => "(* -1 (sin ?x))"),

    rw!("d-ln"; "(d ?x (ln ?x))" => "(/ 1 ?x)" if is_not_zero("?x")),

    rw!("d-power";
        "(d ?x (pow ?f ?g))" =>
        "(* (pow ?f ?g)
            (+ (* (d ?x ?f)
                  (/ ?g ?f))
               (* (d ?x ?g)
                  (ln ?f))))"
        if is_not_zero("?f")
        if is_not_zero("?g")
    ),

    rw!("i-one"; "(i 1 ?x)" => "?x"),
    rw!("i-power-const"; "(i (pow ?x ?c) ?x)" =>
        "(/ (pow ?x (+ ?c 1)) (+ ?c 1))" if is_const("?c")),
    rw!("i-cos"; "(i (cos ?x) ?x)" => "(sin ?x)"),
    rw!("i-sin"; "(i (sin ?x) ?x)" => "(* -1 (cos ?x))"),
    rw!("i-sum"; "(i (+ ?f ?g) ?x)" => "(+ (i ?f ?x) (i ?g ?x))"),
    rw!("i-dif"; "(i (- ?f ?g) ?x)" => "(- (i ?f ?x) (i ?g ?x))"),
    rw!("i-parts"; "(i (* ?a ?b) ?x)" =>
        "(- (* ?a (i ?b ?x)) (i (* (d ?x ?a) (i ?b ?x)) ?x))"),
]}



#[test]
fn assoc_add_saturates() {
    let expr: RecExpr<Math> = "(+ x 1)".parse().unwrap();

    let runner: Runner<Math, ConstantFold> = Runner::default()
        .with_iter_limit(3)
        .with_expr(&expr)
        .run(&rulesM());

    assert!(matches!(runner.stop_reason, Some(StopReason::Saturated)));
}


//#[test]
fn assoc_mul_saturates() {
    let expr: RecExpr<Math> = "(* x 1)".parse().unwrap();

    let runner: Runner<Math, ConstantFold> = Runner::default()
        .with_iter_limit(3)
        .with_expr(&expr)
        .run(&rulesM());

    assert!(matches!(runner.stop_reason, Some(StopReason::Saturated)));
}

#[cfg(feature = "lp")]
//#[test]
fn math_lp_extract() {
    let expr: RecExpr<Math> = "(pow (+ x (+ x x)) (+ x x))".parse().unwrap();

    let runner: Runner<Math, ConstantFold> = Runner::default()
        .with_iter_limit(3)
        .with_expr(&expr)
        .run(&rules());
    let root = runner.roots[0];

    let best = Extractor::new(&runner.egraph, AstSize).find_best(root).1;
    let lp_best = LpExtractor::new(&runner.egraph, AstSize).solve(root);

    println!("input   [{}] {}", expr.as_ref().len(), expr);
    println!("normal  [{}] {}", best.as_ref().len(), best);
    println!("ilp cse [{}] {}", lp_best.as_ref().len(), lp_best);

    assert_ne!(best, lp_best);
    assert_eq!(lp_best.as_ref().len(), 4);
}
