//! Capped index-entry counts for cost-aware index selection.
//!
//! Counting walks index keys only and stops at a cap, so estimating a scan
//! that matches every document in a collection costs at most `cap` steps.

use query::planner::index_selection::IndexScanType;
use storage::corekv::{MaybeSend, Reader, Result};
use storage::index::IndexIterator;

use crate::index::manager::IndexType;

/// Count the entries `scan` would visit on `index`, stopping at `cap`.
///
/// Scan types that cannot be counted report `cap`, so they never look more
/// selective than a counted scan.
pub(crate) async fn count_index_scan<R: Reader + MaybeSend>(
    index: &IndexType,
    reader: &R,
    scan: &IndexScanType,
    cap: usize,
) -> Result<usize> {
    match scan {
        IndexScanType::ExactMatch { values } => {
            index.get(reader, values).await?.count_up_to(cap).await
        }
        IndexScanType::InScan {
            values,
            suffix_values,
        } => {
            let fields = index.description().fields.len();
            let full_key = !suffix_values.is_empty() && suffix_values.len() == fields - 1;
            let mut total = 0;
            for value in values {
                let remaining = cap - total;
                total += if full_key {
                    let mut key = vec![value.clone()];
                    key.extend(suffix_values.iter().cloned());
                    index
                        .get(reader, &key)
                        .await?
                        .count_up_to(remaining)
                        .await?
                } else if fields > 1 {
                    let prefix = std::slice::from_ref(value);
                    index
                        .scan_prefix(reader, prefix, false)
                        .await?
                        .count_up_to(remaining)
                        .await?
                } else {
                    let key = std::slice::from_ref(value);
                    index.get(reader, key).await?.count_up_to(remaining).await?
                };
                if total >= cap {
                    break;
                }
            }
            Ok(total)
        }
        IndexScanType::PrefixScan { prefix_values, .. } => {
            index
                .scan_prefix(reader, prefix_values, false)
                .await?
                .count_up_to(cap)
                .await
        }
        IndexScanType::RangeScan {
            prefix_values,
            lower,
            upper,
            ..
        } => {
            index
                .scan_range(reader, prefix_values, lower.clone(), upper.clone(), false)
                .await?
                .count_up_to(cap)
                .await
        }
        IndexScanType::OrScan { branches } => {
            let mut total = 0;
            for branch in branches {
                total += Box::pin(count_index_scan(index, reader, branch, cap - total)).await?;
                if total >= cap {
                    break;
                }
            }
            Ok(total)
        }
        _ => Ok(cap),
    }
}
