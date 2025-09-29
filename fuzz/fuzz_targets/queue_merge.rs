#![no_main]

use arbitrary::Arbitrary;
use edgeforge::queue::{QueueLogEntry, Task, TaskKind};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
struct Op {
    selector: u8,
    id: u64,
    priority: u8,
    payload: Vec<u8>,
}

fuzz_target!(|ops: Vec<Op>| {
    let mut entries = Vec::with_capacity(ops.len());
    for op in ops {
        let kind = if op.id % 2 == 0 {
            TaskKind::Compute
        } else {
            TaskKind::Io
        };
        match op.selector % 3 {
            0 => entries.push(QueueLogEntry::Enqueue {
                task: Task::new(op.id, kind, op.priority, op.payload),
            }),
            1 => entries.push(QueueLogEntry::Dequeue {
                task_ids: vec![op.id],
            }),
            _ => entries.push(QueueLogEntry::Snapshot {
                payload: op.payload,
            }),
        }
    }

    edgeforge::queue::fuzz_apply(entries);
});
