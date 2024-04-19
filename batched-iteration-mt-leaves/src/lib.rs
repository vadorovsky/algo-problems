use std::{cmp, collections::BTreeMap};

use num_integer::div_ceil;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MyError {
    #[error("Number of leaves and Merkle trees should be equal, got {0} leaves and {1} trees")]
    LeavesTreesNotEqual(usize, usize),
}

/// Set of changelogs for different Merkle trees.
/// The number of changelogs it contains is batched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Changelogs {
    pub changelogs: Vec<ChangelogEvent>,
}

/// Changelog event for one Merkle tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangelogEvent {
    pub merkle_tree_pubkey: [u8; 32],
    pub leaves: Vec<[u8; 32]>,
}

pub fn build_merkle_tree_map(
    leaves: &Vec<[u8; 32]>,
    merkle_trees: &Vec<[u8; 32]>,
) -> Result<BTreeMap<[u8; 32], Vec<[u8; 32]>>, MyError> {
    if leaves.len() != merkle_trees.len() {
        return Err(MyError::LeavesTreesNotEqual(
            leaves.len(),
            merkle_trees.len(),
        ));
    }
    let mut merkle_tree_map = BTreeMap::new();

    for (i, merkle_tree) in merkle_trees.iter().enumerate() {
        merkle_tree_map
            .entry(merkle_tree.to_owned())
            .or_insert_with(|| Vec::new())
            .push(leaves[i]);
    }

    Ok(merkle_tree_map)
}

pub fn append_leaves(
    leaves: Vec<[u8; 32]>,
    merkle_trees: Vec<[u8; 32]>,
    batch_size: usize,
) -> Result<Vec<Changelogs>, MyError> {
    let merkle_tree_map = build_merkle_tree_map(&leaves, &merkle_trees)?;

    let num_batches = div_ceil(leaves.len(), batch_size);
    let mut leaves_in_batch = 0;
    let mut leaves_start = 0;
    let mut batches_of_changelogs = Vec::with_capacity(num_batches);
    let mut batch_of_changelogs = Changelogs {
        changelogs: Vec::with_capacity(batch_size),
    };

    let mut merkle_tree_map_iter = merkle_tree_map.iter();
    let mut merkle_tree_map_pair = merkle_tree_map_iter.next();

    while let Some((merkle_tree_pubkey, leaves)) = merkle_tree_map_pair {
        let leaves_to_process = cmp::min(leaves.len() - leaves_start, batch_size - leaves_in_batch);
        let mut changelog_event = ChangelogEvent {
            merkle_tree_pubkey: merkle_tree_pubkey.to_owned(),
            leaves: Vec::with_capacity(cmp::min(leaves.len(), batch_size)),
        };

        let leaves_end = leaves_start + leaves_to_process;

        changelog_event
            .leaves
            .extend_from_slice(&leaves[leaves_start..leaves_end]);

        batch_of_changelogs.changelogs.push(changelog_event);

        leaves_in_batch += leaves_to_process;
        leaves_start += leaves_to_process;

        if leaves_start == leaves.len() {
            leaves_start = 0;
            merkle_tree_map_pair = merkle_tree_map_iter.next();
        }

        if leaves_in_batch == batch_size {
            // BEWARE! We shouldn't do this clone in the actual program code.
            // It's here just to make test convenient.
            batches_of_changelogs.push(batch_of_changelogs.clone());

            leaves_in_batch = 0;
            batch_of_changelogs.changelogs.clear();
        }
    }

    if !batch_of_changelogs.changelogs.is_empty() {
        batches_of_changelogs.push(batch_of_changelogs)
    }

    Ok(batches_of_changelogs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_leaves() {
        let leaves = vec![
            // MT 0
            [0_u8; 32],
            [1_u8; 32],
            [2_u8; 32],
            [3_u8; 32],
            [4_u8; 32],
            [5_u8; 32],
            [6_u8; 32],
            [7_u8; 32],
            [8_u8; 32],
            [9_u8; 32],
            [10_u8; 32],
            [11_u8; 32],
            // MT 1
            [12_u8; 32],
            [13_u8; 32],
            [14_u8; 32],
            // MT 2
            [15_u8; 32],
            [16_u8; 32],
            [17_u8; 32],
            [18_u8; 32],
            // MT 3
            [19_u8; 32],
            [20_u8; 32],
            [21_u8; 32],
            [22_u8; 32],
            [23_u8; 32],
            [24_u8; 32],
        ];
        let merkle_trees = vec![
            // MT 0
            [0_u8; 32], [0_u8; 32], [0_u8; 32], [0_u8; 32], [0_u8; 32], [0_u8; 32], [0_u8; 32],
            [0_u8; 32], [0_u8; 32], [0_u8; 32], [0_u8; 32], [0_u8; 32], // MT 1
            [1_u8; 32], [1_u8; 32], [1_u8; 32], // MT 2
            [2_u8; 32], [2_u8; 32], [2_u8; 32], [2_u8; 32], // MT 3
            [3_u8; 32], [3_u8; 32], [3_u8; 32], [3_u8; 32], [3_u8; 32], [3_u8; 32],
        ];

        let merkle_tree_map = build_merkle_tree_map(&leaves, &merkle_trees).unwrap();
        assert_eq!(
            merkle_tree_map,
            BTreeMap::<[u8; 32], Vec<[u8; 32]>>::from([
                (
                    [0_u8; 32],
                    vec![
                        [0_u8; 32],
                        [1_u8; 32],
                        [2_u8; 32],
                        [3_u8; 32],
                        [4_u8; 32],
                        [5_u8; 32],
                        [6_u8; 32],
                        [7_u8; 32],
                        [8_u8; 32],
                        [9_u8; 32],
                        [10_u8; 32],
                        [11_u8; 32]
                    ]
                ),
                ([1_u8; 32], vec![[12_u8; 32], [13_u8; 32], [14_u8; 32]]),
                (
                    [2_u8; 32],
                    vec![[15_u8; 32], [16_u8; 32], [17_u8; 32], [18_u8; 32]]
                ),
                (
                    [3_u8; 32],
                    vec![
                        [19_u8; 32],
                        [20_u8; 32],
                        [21_u8; 32],
                        [22_u8; 32],
                        [23_u8; 32],
                        [24_u8; 32]
                    ]
                )
            ])
        );

        let changelogs = append_leaves(leaves, merkle_trees, 10).unwrap();
        for (i, changelog) in changelogs.iter().enumerate() {
            println!("EVENT {i}: {changelog:?}\n");
        }
        assert_eq!(
            changelogs,
            vec![
                // This set of changelogs contains 10 leaves from MT 0.
                Changelogs {
                    changelogs: vec![ChangelogEvent {
                        merkle_tree_pubkey: [0_u8; 32],
                        leaves: vec![
                            [0_u8; 32], [1_u8; 32], [2_u8; 32], [3_u8; 32], [4_u8; 32], [5_u8; 32],
                            [6_u8; 32], [7_u8; 32], [8_u8; 32], [9_u8; 32],
                        ]
                    }]
                },
                // This set of changelogs contains:
                //
                // * Remaining leaves from MT 0.
                // * All leaves from MT 1 and MT 2.
                // * One leaf from MT 3.
                //
                // The number of all leaves is 10.
                Changelogs {
                    changelogs: vec![
                        ChangelogEvent {
                            merkle_tree_pubkey: [0_u8; 32],
                            leaves: vec![[10_u8; 32], [11_u8; 32]]
                        },
                        ChangelogEvent {
                            merkle_tree_pubkey: [1_u8; 32],
                            leaves: vec![[12_u8; 32], [13_u8; 32], [14_u8; 32]]
                        },
                        ChangelogEvent {
                            merkle_tree_pubkey: [2_u8; 32],
                            leaves: vec![[15_u8; 32], [16_u8; 32], [17_u8; 32], [18_u8; 32]]
                        },
                        ChangelogEvent {
                            merkle_tree_pubkey: [3_u8; 32],
                            leaves: vec![[19_u8; 32]]
                        }
                    ]
                },
                // This set of changelogs contains remaining leaves from MT 3.
                Changelogs {
                    changelogs: vec![ChangelogEvent {
                        merkle_tree_pubkey: [3_u8; 32],
                        leaves: vec![
                            [20_u8; 32],
                            [21_u8; 32],
                            [22_u8; 32],
                            [23_u8; 32],
                            [24_u8; 32]
                        ]
                    }]
                }
            ]
        );
    }
}
