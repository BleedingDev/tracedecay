use super::*;

fn project(items: Vec<WorkItemV1>) -> Result<(Vec<String>, u32), WorkProductContractError> {
    let graph = graph(items);
    let bundle = WorkProductProjectionBundleV1::from_graph(
        &graph,
        &runtime(&graph, UtcMicros(100), Vec::new()),
        UtcMicros(100),
    )?;
    Ok((
        bundle
            .critical_path()
            .task_ids()
            .iter()
            .map(|task_id| task_id.as_str().to_owned())
            .collect(),
        bundle.critical_path().total_effort(),
    ))
}

fn brute_force_critical_path(tasks: &[(String, Vec<String>, u32)]) -> (Vec<String>, u32) {
    fn extend(
        tasks: &[(String, Vec<String>, u32)],
        path: &mut Vec<String>,
        effort: u32,
        best: &mut Option<(Vec<String>, u32)>,
    ) {
        let better = best.as_ref().is_none_or(|(best_path, best_effort)| {
            effort > *best_effort || (effort == *best_effort && *path < *best_path)
        });
        if better {
            *best = Some((path.clone(), effort));
        }
        let last = path.last().unwrap().clone();
        for (task, dependencies, task_effort) in tasks {
            if dependencies.contains(&last) {
                path.push(task.clone());
                extend(tasks, path, effort + task_effort, best);
                path.pop();
            }
        }
    }
    let mut best = None;
    for (task, dependencies, effort) in tasks {
        if dependencies.is_empty() {
            extend(tasks, &mut vec![task.clone()], *effort, &mut best);
        }
    }
    best.unwrap_or_default()
}

#[test]
fn critical_path_matches_the_heaviest_lexically_first_root_path_in_every_small_dag() {
    const LABELS: [&str; 4] = ["task.a", "task.b", "task.c", "task.d"];
    const EDGES: [(usize, usize); 6] = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)];
    let orders = (0..4usize.pow(4))
        .map(|code| [code % 4, code / 4 % 4, code / 16 % 4, code / 64])
        .filter(|order| order.iter().collect::<BTreeSet<_>>().len() == 4);
    let mut cases = 0;
    for order in orders {
        for edge_mask in 0..1u32 << EDGES.len() {
            for effort_mask in 0..1u32 << LABELS.len() {
                let tasks = (0..LABELS.len())
                    .map(|position| {
                        let dependencies = EDGES
                            .iter()
                            .enumerate()
                            .filter(|(bit, (_, to))| edge_mask >> bit & 1 == 1 && *to == position)
                            .map(|(_, (from, _))| LABELS[order[*from]].to_owned())
                            .collect::<Vec<_>>();
                        (
                            LABELS[order[position]].to_owned(),
                            dependencies,
                            1 + (effort_mask >> position & 1),
                        )
                    })
                    .collect::<Vec<_>>();
                let items = tasks
                    .iter()
                    .map(|(task, dependencies, effort)| {
                        let dependencies =
                            dependencies.iter().map(String::as_str).collect::<Vec<_>>();
                        item(task, &dependencies, *effort)
                    })
                    .collect();
                assert_eq!(
                    project(items).unwrap(),
                    brute_force_critical_path(&tasks),
                    "{tasks:?}"
                );
                cases += 1;
            }
        }
    }
    assert_eq!(cases, 24 * 64 * 16);
}

#[test]
fn critical_path_ties_break_on_the_whole_path_not_the_terminal_task() {
    let projected = project(vec![
        item("task.a", &[], 1),
        item("task.b", &[], 1),
        item("task.y", &["task.b"], 1),
        item("task.z", &["task.a"], 1),
        item("task.w", &[], 1),
    ])
    .unwrap();

    assert_eq!(
        projected,
        (vec!["task.a".to_owned(), "task.z".to_owned()], 2)
    );
}

#[test]
fn critical_path_effort_overflow_is_refused() {
    assert_eq!(
        project(vec![
            item("task.a", &[], u32::MAX),
            item("task.b", &["task.a"], 1),
        ])
        .unwrap_err(),
        WorkProductContractError::GraphTooLarge
    );
}

#[test]
fn a_dependency_chain_at_the_item_cap_projects_its_whole_path() {
    let task_ids = (0..MAX_WORK_PRODUCT_ITEMS)
        .map(|ordinal| format!("task.{ordinal:05}"))
        .collect::<Vec<_>>();
    let items = task_ids
        .iter()
        .enumerate()
        .map(|(ordinal, task_id)| {
            let dependencies = ordinal
                .checked_sub(1)
                .map(|previous| vec![task_ids[previous].as_str()])
                .unwrap_or_default();
            item(task_id, &dependencies, 1)
        })
        .collect();

    let (path, effort) = project(items).unwrap();

    assert_eq!(path, task_ids);
    assert_eq!(effort, u32::try_from(MAX_WORK_PRODUCT_ITEMS).unwrap());
}
