//! statsd_exporter v0.26.1's glob matcher (`pkg/mapper/fsm`), ported as it is, quirks included:
//! a rule whose last state already exists (a prefix of an earlier rule, or a duplicate) takes
//! that state's priority but not its result, and with ordering on, the matching rule written
//! first wins only among the rules that fit the name's length.

use super::MetricType;
use regex::Regex;
use std::collections::HashMap;

#[derive(Debug, Default)]
struct State {
    transitions: HashMap<String, usize>,
    min_remaining_length: usize,
    max_remaining_length: usize,
    /// The mapping that ends here, by index.
    result: Option<usize>,
    priority: usize,
}

/// A state saved to try the `*` transition later.
struct Cursor {
    field_index: usize,
    capture_index: usize,
    current_capture: String,
    state: usize,
}

#[derive(Debug)]
pub(super) struct Fsm {
    states: Vec<State>,
    states_count: usize,
    pub backtracking_needed: bool,
    ordering_disabled: bool,
}

const ROOTS: [MetricType; 3] = [MetricType::Counter, MetricType::Gauge, MetricType::Observer];

fn root(metric_type: MetricType) -> usize {
    1 + ROOTS.iter().position(|t| *t == metric_type).unwrap()
}

impl Fsm {
    pub fn new(ordering_disabled: bool) -> Self {
        let mut states = vec![State::default()];
        for (i, metric_type) in ROOTS.iter().enumerate() {
            states[0]
                .transitions
                .insert(metric_type.as_str().into(), i + 1);
            states.push(State::default());
        }
        Self {
            states,
            states_count: 0,
            backtracking_needed: false,
            ordering_disabled,
        }
    }

    /// `AddState`: the glob `pattern` (already validated) for `mapping`, under its metric type
    /// or all three. Returns the number of `*` in it.
    pub fn add_state(
        &mut self,
        pattern: &str,
        metric_type: Option<MetricType>,
        mapping: usize,
    ) -> usize {
        let fields: Vec<&str> = pattern.split('.').collect();
        let roots: Vec<usize> = match metric_type {
            None => ROOTS.iter().map(|t| root(*t)).collect(),
            Some(t) => vec![root(t)],
        };
        let mut capture_count = 0;
        let mut final_states = Vec::new();
        for mut state in roots {
            capture_count = 0;
            for (i, field) in fields.iter().enumerate() {
                let remaining = fields.len() - i - 1;
                let next = match self.states[state].transitions.get(*field) {
                    Some(&next) => {
                        let s = &mut self.states[next];
                        s.max_remaining_length = s.max_remaining_length.max(remaining);
                        s.min_remaining_length = s.min_remaining_length.min(remaining);
                        next
                    }
                    None => {
                        let next = self.states.len();
                        self.states.push(State {
                            min_remaining_length: remaining,
                            max_remaining_length: remaining,
                            // Only a state created for the last field gets the result.
                            result: (i == fields.len() - 1).then_some(mapping),
                            ..Default::default()
                        });
                        self.states[state]
                            .transitions
                            .insert(field.to_string(), next);
                        next
                    }
                };
                if *field == "*" {
                    capture_count += 1;
                }
                state = next;
            }
            final_states.push(state);
        }
        for state in final_states {
            self.states[state].priority = self.states_count;
        }
        self.states_count += 1;
        capture_count
    }

    /// `GetMapping`: the mapping matching `name`, and the captures of its `*`.
    pub fn get_mapping(&self, name: &str, metric_type: MetricType) -> (Option<usize>, Vec<String>) {
        let fields: Vec<&str> = name.split('.').collect();
        let count = fields.len();
        let mut current = root(metric_type);
        let mut stack: Vec<Cursor> = Vec::new();
        let mut resume_from_backtrack = false;
        let mut final_state: Option<usize> = None;
        let mut captures = vec![String::new(); count];
        let mut final_captures = vec![String::new(); count];
        let mut capture_index = 0;
        let mut i = 0;
        let mut state = 0;
        loop {
            loop {
                if !resume_from_backtrack {
                    let transitions = &self.states[current].transitions;
                    if transitions.is_empty() {
                        break;
                    }
                    let field = fields[i];
                    let fields_left = count - i - 1;
                    let fits = |s: &usize| {
                        let s = &self.states[*s];
                        fields_left <= s.max_remaining_length
                            && fields_left >= s.min_remaining_length
                    };
                    match transitions.get(field).filter(|s| fits(s)) {
                        None => match transitions.get("*").filter(|s| fits(s)) {
                            None => break,
                            Some(&next) => {
                                state = next;
                                captures[capture_index] = field.to_string();
                                capture_index += 1;
                            }
                        },
                        Some(&next) => {
                            state = next;
                            if self.backtracking_needed
                                && let Some(&alternative) = transitions.get("*").filter(|s| fits(s))
                            {
                                stack.push(Cursor {
                                    field_index: i,
                                    capture_index,
                                    current_capture: field.to_string(),
                                    state: alternative,
                                });
                            }
                        }
                    }
                }
                let reached = &self.states[state];
                if reached.result.is_some() && i == count - 1 {
                    if self.ordering_disabled {
                        return (reached.result, captures);
                    }
                    if final_state.is_none_or(|f| self.states[f].priority > reached.priority) {
                        final_state = Some(state);
                        final_captures.clone_from(&captures);
                    }
                    break;
                }
                i += 1;
                if i >= count {
                    break;
                }
                resume_from_backtrack = false;
                current = state;
            }
            let Some(cursor) = stack.pop() else {
                break;
            };
            state = cursor.state;
            current = cursor.state;
            i = cursor.field_index;
            capture_index = cursor.capture_index + 1;
            captures[capture_index - 1] = cursor.current_capture;
            resume_from_backtrack = true;
        }
        (
            final_state.and_then(|s| self.states[s].result),
            final_captures,
        )
    }
}

/// `TestIfNeedBacktracking`: always, unless ordering is disabled; then, when a glob shares a
/// prefix with another of the same length that it does not include and is not included in.
pub(super) fn needs_backtracking(globs: &[&str], ordering_disabled: bool) -> bool {
    if !ordering_disabled {
        return true;
    }
    let to_regex = |glob: &str| {
        Regex::new(&format!(
            "^{}$",
            glob.replace('.', "\\.").replace('*', "([^.]*)")
        ))
        .ok()
    };
    let mut by_length: HashMap<usize, Vec<(&str, Option<Regex>)>> = HashMap::new();
    for glob in globs {
        by_length
            .entry(glob.split('.').count())
            .or_default()
            .push((glob, to_regex(glob)));
    }
    let mut needed = false;
    for rules in by_length.values() {
        if rules.len() == 1 {
            continue;
        }
        for (i1, (r1, re1)) in rules.iter().enumerate() {
            let Some(re1) = re1 else { continue };
            if !r1.contains('*') {
                continue;
            }
            let mut current = false;
            for (index, _) in r1.match_indices('*') {
                let prefix = r1[..index].replace('.', "\\.").replace('*', "\\*");
                let re = Regex::new(&format!("^{prefix}")).unwrap();
                if rules
                    .iter()
                    .enumerate()
                    .any(|(i2, (r2, _))| i2 != i1 && re.is_match(r2))
                {
                    current = true;
                }
            }
            if rules
                .iter()
                .enumerate()
                .any(|(i2, (r2, _))| i2 != i1 && re1.is_match(r2))
            {
                current = false;
            }
            if rules
                .iter()
                .enumerate()
                .any(|(i2, (_, re2))| i2 != i1 && re2.as_ref().is_some_and(|re2| re2.is_match(r1)))
            {
                current = false;
            }
            needed |= current;
        }
    }
    needed
}
