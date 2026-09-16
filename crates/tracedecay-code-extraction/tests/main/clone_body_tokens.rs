#[cfg(feature = "lang-clojure")]
use tracedecay_code_extraction::ClojureExtractor;
#[cfg(feature = "lang-pascal")]
use tracedecay_code_extraction::PascalExtractor;
#[cfg(feature = "lang-perl")]
use tracedecay_code_extraction::PerlExtractor;
use tracedecay_code_extraction::{
    CloneBodyEligibilityV1, CloneBodyTokenizationIssueV1, CloneBodyTokenizationStatusV1,
    ConservativeCloneTokenV1, CppExtractor, ExtractedCloneBodyV1, ExtractionArtifactV1,
    GoExtractor, JavaExtractor, KotlinExtractor, LanguageExtractor, PythonExtractor, RustExtractor,
    TypeScriptExtractor,
};
use tracedecay_domain::NodeKind;

fn body_for_kind<'a>(
    artifact: &'a ExtractionArtifactV1,
    kind: &NodeKind,
) -> &'a ExtractedCloneBodyV1 {
    artifact
        .clone_bodies
        .iter()
        .find(|body| &body.symbol_kind == kind)
        .unwrap_or_else(|| {
            panic!(
                "missing clone body for {kind:?}; nodes: {:?}",
                artifact.result.nodes
            )
        })
}

fn assert_complete_body(body: &ExtractedCloneBodyV1, label: &str) {
    assert_eq!(
        body.tokenization_status,
        CloneBodyTokenizationStatusV1::Complete,
        "{label}: {:?}",
        body.tokenization_issues
    );
    assert!(
        body.tokenization_issues.is_empty(),
        "{label}: {:?}",
        body.tokenization_issues
    );
    assert!(!body.body_span.is_empty(), "{label}: empty body span");
}

fn tokens(
    extractor: &dyn LanguageExtractor,
    path: &str,
    source: &str,
) -> Vec<ConservativeCloneTokenV1> {
    let artifact = extractor.extract_artifact(path, source);
    assert!(
        artifact.result.errors.is_empty(),
        "{:?}",
        artifact.result.errors
    );
    assert_eq!(
        artifact.clone_bodies.len(),
        1,
        "{:?}",
        artifact.result.nodes
    );
    artifact.clone_bodies[0].conservative_tokens.clone()
}

#[test]
fn rust_conservative_tokens_ignore_formatting_and_comments_but_preserve_behavior() {
    let baseline = r#"
fn publish(input: &str) -> bool {
    let parsed = parse(input);
    validate(parsed, "read")
}
"#;
    let formatting_and_comments = r#"
fn publish(input: &str) -> bool
{
    // parser-owned comments are trivia
    let parsed=parse(input); /* so is this */
    validate(
        parsed,
        "read",
    )
}
"#;

    let expected = tokens(&RustExtractor, "src/lib.rs", baseline);
    assert_eq!(
        expected,
        tokens(&RustExtractor, "src/lib.rs", formatting_and_comments)
    );
    for changed in [
        baseline.replace("\"read\"", "\"write\""),
        baseline.replace("validate(parsed", "skip_validation(parsed"),
        baseline.replace("validate(parsed, \"read\")", "!validate(parsed, \"read\")"),
        baseline.replace(
            "let parsed = parse(input);\n    validate(parsed, \"read\")",
            "validate(input, \"read\");\n    parse(input)",
        ),
        baseline.replace(
            "validate(parsed, \"read\")",
            "if parsed.is_empty() { false } else { validate(parsed, \"read\") }",
        ),
    ] {
        assert_ne!(expected, tokens(&RustExtractor, "src/lib.rs", &changed));
    }
}

#[test]
fn syntax_tokens_keep_comment_markers_inside_literals_and_javascript_asi_boundaries() {
    assert_eq!(
        tokens(
            &TypeScriptExtractor,
            "src/a.ts",
            "function invokeOnce() { invoke(\"value\"); }",
        ),
        tokens(
            &TypeScriptExtractor,
            "src/a.ts",
            "function invokeOnce()\n{\n/* formatting */ invoke(\"value\")\n}",
        )
    );

    let literal = tokens(
        &TypeScriptExtractor,
        "src/a.ts",
        r#"function parseUrl() { return "https://example.test/*literal*/"; }"#,
    );
    let literal_text = literal
        .iter()
        .filter_map(|token| match token {
            ConservativeCloneTokenV1::Syntax { text, .. } => Some(text.as_str()),
            ConservativeCloneTokenV1::StructureStart { .. }
            | ConservativeCloneTokenV1::StructureEnd { .. } => None,
        })
        .collect::<String>();
    assert!(literal_text.contains("https://example.test/*literal*/"));
    let syntax_literals = tokens(
        &TypeScriptExtractor,
        "src/a.ts",
        r#"function display(δ: string) { const pattern = /a\/b/; return `${δ}\n`; }"#,
    );
    let changed_regex = tokens(
        &TypeScriptExtractor,
        "src/a.ts",
        r#"function display(δ: string) { const pattern = /a\/c/; return `${δ}\n`; }"#,
    );
    assert_ne!(syntax_literals, changed_regex);

    assert_ne!(
        tokens(
            &TypeScriptExtractor,
            "src/a.ts",
            "function value() { return object; }",
        ),
        tokens(
            &TypeScriptExtractor,
            "src/a.ts",
            "function value() { return\nobject; }",
        )
    );
}

#[test]
fn rust_macro_trailing_comma_remains_semantic_syntax() {
    assert_ne!(
        tokens(
            &RustExtractor,
            "src/lib.rs",
            "fn choose(value: i32) { choose!(value); }",
        ),
        tokens(
            &RustExtractor,
            "src/lib.rs",
            "fn choose(value: i32) { choose!(value,); }",
        )
    );
}

#[cfg(feature = "lang-perl")]
#[test]
fn perl_comments_are_parser_trivia() {
    assert_eq!(
        tokens(
            &PerlExtractor,
            "src/main.pl",
            "sub work { my $value = 1; # first comment\n return $value; }",
        ),
        tokens(
            &PerlExtractor,
            "src/main.pl",
            "sub work { my $value = 1; # changed comment\n return $value; }",
        )
    );
}

#[cfg(feature = "lang-clojure")]
#[test]
fn callable_without_a_body_field_is_typed_partial() {
    let artifact =
        ClojureExtractor.extract_artifact("src/core.clj", "(defn work [value] (+ value 1))");
    let body = artifact.clone_bodies.first().expect("partial clone body");
    assert_eq!(
        body.tokenization_status,
        CloneBodyTokenizationStatusV1::Partial
    );
    assert!(
        body.tokenization_issues
            .contains(&CloneBodyTokenizationIssueV1::BodyBoundaryUnavailable)
    );
    assert_eq!(
        body.eligibility,
        CloneBodyEligibilityV1::ExcludedIncompleteTokenization
    );
}

#[test]
fn python_significant_indentation_changes_structural_tokens() {
    let inside = r#"
def process(allowed):
    if allowed:
        commit()
        notify()
"#;
    let outside = r#"
def process(allowed):
    if allowed:
        commit()
    notify()
"#;
    assert_ne!(
        tokens(&PythonExtractor, "src/main.py", inside),
        tokens(&PythonExtractor, "src/main.py", outside)
    );
}

#[test]
fn automatic_discovery_minimum_is_thirty_non_trivia_tokens() {
    let artifact = |body: &str| {
        RustExtractor.extract_artifact("src/lib.rs", &format!("fn body() {{ {body} }}"))
    };
    let twenty_nine = artifact("foo(); foo(); foo(); foo(); foo(); foo(); foo()");
    let thirty = artifact("foo(); foo(); foo(); foo(); foo(); foo(); foo();");
    let thirty_one = artifact("foo(); foo(); foo(); foo(); foo(); foo(); !foo();");

    for (artifact, count, eligibility) in [
        (
            twenty_nine,
            29,
            CloneBodyEligibilityV1::ExcludedTooSmall { minimum_tokens: 30 },
        ),
        (thirty, 30, CloneBodyEligibilityV1::Eligible),
        (thirty_one, 31, CloneBodyEligibilityV1::Eligible),
    ] {
        let body = &artifact.clone_bodies[0];
        assert_eq!(body.non_trivia_token_count, count);
        assert_eq!(body.eligibility, eligibility);
    }
}

#[test]
fn clone_bodies_bind_to_method_and_stable_arrow_occurrences() {
    for (artifact, expected_kind, expected_language) in [
        (
            RustExtractor.extract_artifact(
                "src/store.rs",
                "struct Store; impl Store { fn read(&self) { load(); } }",
            ),
            NodeKind::Method,
            "rust",
        ),
        (
            TypeScriptExtractor.extract_artifact("src/store.ts", "const read = () => { load(); };"),
            NodeKind::ArrowFunction,
            "typescript",
        ),
    ] {
        let body = artifact.clone_bodies.first().expect("clone body");
        let callable = artifact
            .result
            .nodes
            .iter()
            .find(|node| node.kind == expected_kind)
            .expect("callable occurrence");
        assert_eq!(body.symbol_occurrence_id, callable.id);
        assert_eq!(body.symbol_kind, expected_kind);
        assert_eq!(body.language, expected_language);
        assert!(!body.body_span.is_empty());
    }
}

#[test]
fn clone_admission_covers_language_specific_callable_kinds() {
    for (extractor, path, source, kind) in [
        (
            &CppExtractor as &dyn LanguageExtractor,
            "src/box.cpp",
            r#"
class Box {
public:
    Box(int value) : value(value) { initialize(); }
private:
    int value;
    void initialize() { value += 1; }
};
"#,
            NodeKind::Constructor,
        ),
        (
            &GoExtractor,
            "src/box.go",
            r#"
package model

type Box struct { value int }

func (b *Box) Reset() {
    b.value = 0
    notify()
}
"#,
            NodeKind::StructMethod,
        ),
        (
            &JavaExtractor,
            "src/Box.java",
            r#"
class Box {
    private int value;

    Box(int value) {
        this.value = value;
        validate(value);
    }
}
"#,
            NodeKind::Constructor,
        ),
        (
            &KotlinExtractor,
            "src/Box.kt",
            r#"
class Box(val value: Int) {
    constructor(value: Int, extra: Int) : this(value) {
        val total = value + extra
        println(total)
    }
}
"#,
            NodeKind::Constructor,
        ),
        (
            &TypeScriptExtractor,
            "src/box.ts",
            r#"
class Box {
    constructor(value: number) {
        initialize(value);
        record(value);
    }
}
"#,
            NodeKind::Constructor,
        ),
    ] {
        let artifact = extractor.extract_artifact(path, source);
        assert!(
            artifact.result.errors.is_empty(),
            "{path}: {:?}",
            artifact.result.errors
        );
        let body = body_for_kind(&artifact, &kind);
        let node = artifact
            .result
            .nodes
            .iter()
            .find(|node| node.kind == kind)
            .expect("callable node");
        assert_eq!(body.symbol_occurrence_id, node.id, "{path}");
        assert_complete_body(body, path);
    }

    let artifact = KotlinExtractor.extract_artifact(
        "src/box.kt",
        r#"
class Box {
    fun reset(value: Int): Int {
        val next = value + 1
        println(next)
        return next
    }
}
"#,
    );
    assert!(
        artifact.result.errors.is_empty(),
        "{:?}",
        artifact.result.errors
    );
    assert_complete_body(body_for_kind(&artifact, &NodeKind::Method), "kotlin method");
}

#[test]
fn abstract_methods_are_retained_as_conservative_partial_evidence() {
    for (extractor, path, source) in [
        (
            &CppExtractor as &dyn LanguageExtractor,
            "src/shape.cpp",
            r#"
class Shape {
public:
    virtual double area() = 0;
};
"#,
        ),
        (
            &JavaExtractor,
            "src/Shape.java",
            r#"
public abstract class Shape {
    public abstract double area();
}
"#,
        ),
        (
            &KotlinExtractor,
            "src/Shape.kt",
            r#"
interface Shape {
    fun area(): Double
}
"#,
        ),
    ] {
        let artifact = extractor.extract_artifact(path, source);
        assert!(
            artifact.result.errors.is_empty(),
            "{path}: {:?}",
            artifact.result.errors
        );
        let body = body_for_kind(&artifact, &NodeKind::AbstractMethod);
        assert_eq!(
            body.tokenization_status,
            CloneBodyTokenizationStatusV1::Partial,
            "{path}"
        );
        assert!(
            body.tokenization_issues
                .contains(&CloneBodyTokenizationIssueV1::BodyBoundaryUnavailable),
            "{path}: {:?}",
            body.tokenization_issues
        );
        assert_eq!(
            body.eligibility,
            CloneBodyEligibilityV1::ExcludedIncompleteTokenization,
            "{path}"
        );
    }
}

#[cfg(feature = "lang-pascal")]
#[test]
fn pascal_procedures_receive_bounded_clone_bodies() {
    let source = r#"
program CloneBody;

procedure Emit(value: Integer);
begin
    WriteLn(value);
end;

begin
end.
"#;
    let artifact = PascalExtractor.extract_artifact("src/clone_body.pas", source);
    assert!(
        artifact.result.errors.is_empty(),
        "{:?}",
        artifact.result.errors
    );
    let body = body_for_kind(&artifact, &NodeKind::Procedure);
    let node = artifact
        .result
        .nodes
        .iter()
        .find(|node| node.kind == NodeKind::Procedure)
        .expect("procedure node");
    assert_eq!(body.symbol_occurrence_id, node.id);
    assert_complete_body(body, "pascal procedure");
}

#[test]
fn test_framework_closures_use_only_their_bounded_callback_body() {
    let source = r#"
describe("suite", () => {
    setup();
    verify();
});
"#;
    let artifact = TypeScriptExtractor.extract_artifact("src/suite.ts", source);
    assert!(
        artifact.result.errors.is_empty(),
        "{:?}",
        artifact.result.errors
    );
    let body = body_for_kind(&artifact, &NodeKind::Function);
    assert_complete_body(body, "typescript test callback");
    let start = usize::try_from(body.body_span.start_byte).expect("body start");
    let end = usize::try_from(body.body_span.end_byte).expect("body end");
    let body_source = &source[start..end];
    assert!(body_source.contains("setup"));
    assert!(body_source.contains("verify"));
    assert!(!body_source.contains("describe"));
}
