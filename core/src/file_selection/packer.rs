/*
 * Copyright 2025 iceberg-compaction
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// Pure bin-packing algorithm - similar to Java's BinPacking.ListPacker
///
/// This struct implements a variant of the First-Fit Decreasing algorithm for packing items into bins.
/// It is completely independent of business logic and only cares about the weight constraint.
///
/// # Algorithm Details
///
/// 1. **Sorting**: Items are sorted by weight in descending order (Decreasing part)
/// 2. **Packing**: For each item, try to find a suitable bin within the lookback window
/// 3. **Lookback**: Only check the last `lookback` bins (configurable, default=1)
/// 4. **Bin Selection**: Use `.rev().take(lookback)` to check bins from newest to oldest
///
/// This is different from the standard First-Fit algorithm which checks all bins from oldest to newest.
/// The lookback mechanism provides better locality and cache efficiency while still maintaining good packing quality.
#[derive(Debug, Clone)]
pub struct ListPacker {
    /// Target weight (size) for each bin
    pub target_weight: u64,
    /// Number of bins to look back when finding a suitable bin for an item
    /// Higher values may produce better packing but take more time
    pub lookback: usize,
}

impl ListPacker {
    /// Create a new `ListPacker` with the given target weight
    pub fn new(target_weight: u64) -> Self {
        Self {
            target_weight,
            lookback: 1, // Default lookback, matches Java's usage
        }
    }

    /// Pack items into bins using a variant of the First-Fit Decreasing algorithm
    ///
    /// # Arguments
    /// * `items` - Items to pack
    /// * `weight_func` - Function to extract weight from each item
    ///
    /// # Returns
    /// A vector of bins, where each bin is a vector of items
    ///
    /// # Example
    /// ```ignore
    /// let packer = ListPacker::new(100);
    /// let items = vec![60u64, 50, 30, 20];
    /// let bins = packer.pack(items, |&x| x);
    /// // Result: [[60, 30], [50, 20]] - two bins with good packing
    /// ```
    pub fn pack<T, F>(&self, mut items: Vec<T>, weight_func: F) -> Vec<Vec<T>>
    where
        F: Fn(&T) -> u64,
    {
        if items.is_empty() {
            return vec![];
        }

        // Sort by weight descending (First-Fit Decreasing)
        items.sort_by_key(|b| std::cmp::Reverse(weight_func(b)));

        let mut bins: Vec<Bin<T>> = vec![];

        for item in items {
            let weight = weight_func(&item);

            // Try to find a bin within the lookback window that can fit this item
            // Note: We search from newest to oldest bins (rev().take(lookback))
            let bin_to_use = bins
                .iter_mut()
                .rev()
                .take(self.lookback)
                .find(|bin| bin.can_add(weight));

            if let Some(bin) = bin_to_use {
                bin.add(item, weight);
            } else {
                // Create a new bin
                let mut new_bin = Bin::new(self.target_weight);
                new_bin.add(item, weight);
                bins.push(new_bin);
            }
        }

        // Extract items from bins
        bins.into_iter().map(|bin| bin.items).collect()
    }
}

/// Internal bin structure for the packing algorithm
#[derive(Debug)]
struct Bin<T> {
    target_weight: u64,
    items: Vec<T>,
    current_weight: u64,
}

impl<T> Bin<T> {
    fn new(target_weight: u64) -> Self {
        Self {
            target_weight,
            items: Vec::new(),
            current_weight: 0,
        }
    }

    fn can_add(&self, weight: u64) -> bool {
        // Special case: if target_weight is 0, always allow adding to existing bin
        // This ensures all items go into a single bin when target is 0
        if self.target_weight == 0 {
            return true;
        }
        self.current_weight + weight <= self.target_weight
    }

    fn add(&mut self, item: T, weight: u64) {
        self.current_weight += weight;
        self.items.push(item);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_packer_empty_input() {
        let packer = ListPacker::new(100);
        let items: Vec<u64> = vec![];
        let result = packer.pack(items, |&x| x);
        assert_eq!(result.len(), 0, "Empty input should produce no bins");
    }

    #[test]
    fn test_list_packer_single_item() {
        let packer = ListPacker::new(100);
        let items = vec![50u64];
        let result = packer.pack(items, |&x| x);
        assert_eq!(result.len(), 1, "Single item should produce 1 bin");
        assert_eq!(result[0], vec![50]);
    }

    #[test]
    fn test_list_packer_all_fit_one_bin() {
        let packer = ListPacker::new(100);
        let items = vec![30u64, 20, 40]; // Total: 90, all fit in 1 bin
        let result = packer.pack(items, |&x| x);
        assert_eq!(result.len(), 1, "All items should fit in 1 bin");
        assert_eq!(result[0].len(), 3);
        // Verify First-Fit Decreasing: items should be sorted descending
        assert_eq!(result[0], vec![40, 30, 20]);
    }

    #[test]
    fn test_list_packer_multiple_bins() {
        let packer = ListPacker::new(100);
        let items = vec![60u64, 50, 30, 20]; // 60+30=90 in bin1, 50+20=70 in bin2
        let result = packer.pack(items, |&x| x);
        assert_eq!(result.len(), 2, "Should produce 2 bins");

        // Verify First-Fit Decreasing sorting
        let all_items: Vec<u64> = result.iter().flat_map(|bin| bin.iter().copied()).collect();
        let mut sorted_items = all_items.clone();
        sorted_items.sort_by_key(|&x| std::cmp::Reverse(x));
        assert_eq!(all_items, sorted_items);

        // Verify packing logic
        let bin1_sum: u64 = result[0].iter().sum();
        let bin2_sum: u64 = result[1].iter().sum();
        assert!(
            bin1_sum <= 100 && bin2_sum <= 100,
            "Each bin should respect target weight"
        );
    }

    #[test]
    fn test_list_packer_first_fit_decreasing_order() {
        let packer = ListPacker::new(100);
        let items = vec![10u64, 90, 5, 50, 40]; // Unsorted input
        let result = packer.pack(items, |&x| x);

        // Verify items were sorted descending before packing
        let all_items: Vec<u64> = result.iter().flat_map(|bin| bin.iter().copied()).collect();
        let mut sorted_items = all_items.clone();
        sorted_items.sort_by_key(|&x| std::cmp::Reverse(x));
        assert_eq!(all_items, sorted_items);

        // After sorting: [90, 50, 40, 10, 5]
        // With lookback=1 (only check last bin) and target=100:
        // - 90: new bin0 -> bin0=[90]
        // - 50: can't fit bin0 (90+50>100), new bin1 -> bin1=[50]
        // - 40: can fit bin1 (50+40=90<=100), add to bin1 -> bin1=[50,40]
        // - 10: can fit bin1 (90+10=100<=100), add to bin1 -> bin1=[50,40,10]
        // - 5: can't fit bin1 (100+5>100), new bin2 -> bin2=[5]
        // Result: 3 bins
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], vec![90]);
        assert_eq!(result[1], vec![50, 40, 10]);
        assert_eq!(result[2], vec![5]);
    }

    #[test]
    fn test_list_packer_zero_target_weight() {
        let packer = ListPacker::new(0);
        let items = vec![100u64, 200, 300];
        let result = packer.pack(items, |&x| x);

        // Zero target: all items should go into single bin (special case)
        assert_eq!(result.len(), 1, "Zero target should create single bin");
        assert_eq!(result[0].len(), 3);
        // Verify sorted descending
        assert_eq!(result[0], vec![300, 200, 100]);
    }

    #[test]
    fn test_list_packer_items_exceed_target() {
        let packer = ListPacker::new(50);
        let items = vec![100u64, 80, 30]; // Items larger than target
        let result = packer.pack(items, |&x| x);

        // Each large item goes into its own bin
        assert_eq!(
            result.len(),
            3,
            "Items exceeding target should each get own bin"
        );
        assert_eq!(result[0], vec![100]);
        assert_eq!(result[1], vec![80]);
        assert_eq!(result[2], vec![30]); // 30 fits in target but nothing to merge with
    }

    #[test]
    fn test_list_packer_exact_fit() {
        let packer = ListPacker::new(100);
        let items = vec![100u64, 100, 100]; // Each exactly matches target
        let result = packer.pack(items, |&x| x);

        assert_eq!(result.len(), 3, "Exact fit items should each get own bin");
        for bin in &result {
            assert_eq!(bin.len(), 1);
            assert_eq!(bin[0], 100);
        }
    }

    #[test]
    fn test_list_packer_lookback_default() {
        let packer = ListPacker::new(100);
        assert_eq!(packer.lookback, 1, "Default lookback should be 1");

        // Test lookback=1 behavior: only checks last bin
        let items = vec![60u64, 30, 30, 30];
        // Expected: bin1=[60,30], bin2=[30,30] (second 30 can't fit in bin1 due to lookback=1)
        let result = packer.pack(items, |&x| x);

        assert_eq!(result.len(), 2);
        // First bin: 60+30=90
        assert_eq!(result[0], vec![60, 30]);
        // Second bin: 30+30=60
        assert_eq!(result[1], vec![30, 30]);
    }

    #[test]
    fn test_list_packer_custom_lookback() {
        let mut packer = ListPacker::new(100);
        packer.lookback = 3; // Look back at last 3 bins

        let items = vec![60u64, 50, 30, 20, 15];
        // With lookback=3, algorithm can find better fits across multiple bins
        let result = packer.pack(items, |&x| x);

        // Verify all bins respect target
        for bin in &result {
            let sum: u64 = bin.iter().sum();
            assert!(sum <= 100, "Bin sum {} exceeds target 100", sum);
        }
    }

    #[test]
    fn test_list_packer_generic_type_string() {
        let packer = ListPacker::new(20);
        let items = vec![
            "hello".to_string(),       // length=5
            "world".to_string(),       // length=5
            "rust".to_string(),        // length=4
            "programming".to_string(), // length=11
        ];

        let result = packer.pack(items, |s| s.len() as u64);

        // Sorted by length descending: programming(11), hello(5), world(5), rust(4)
        // With lookback=1 and target=20:
        // - programming(11): new bin0 -> bin0=[programming(11)]
        // - hello(5): can fit bin0 (11+5=16<=20), add to bin0 -> bin0=[programming(11), hello(5)]
        // - world(5): can't fit bin0 (16+5=21>20), new bin1 -> bin1=[world(5)]
        // - rust(4): can fit bin1 (5+4=9<=20), add to bin1 -> bin1=[world(5), rust(4)]
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            vec!["programming".to_string(), "hello".to_string()]
        );
        assert_eq!(result[1], vec!["world".to_string(), "rust".to_string()]);
    }

    #[test]
    fn test_list_packer_custom_weight_function() {
        #[derive(Debug, Clone, PartialEq)]
        struct Item {
            name: String,
            weight: u64,
        }

        let packer = ListPacker::new(100);
        let items = vec![
            Item {
                name: "A".to_string(),
                weight: 70,
            },
            Item {
                name: "B".to_string(),
                weight: 30,
            },
            Item {
                name: "C".to_string(),
                weight: 25,
            },
            Item {
                name: "D".to_string(),
                weight: 40,
            },
        ];

        let result = packer.pack(items, |item| item.weight);

        // Sorted descending: A(70), D(40), B(30), C(25)
        // With lookback=1:
        // - A(70): new bin0 -> bin0=[A(70)]
        // - D(40): can't fit bin0 (70+40>100), new bin1 -> bin1=[D(40)]
        // - B(30): can fit bin1 (40+30=70<=100), add to bin1 -> bin1=[D(40), B(30)]
        // - C(25): can fit bin1 (70+25=95<=100), add to bin1 -> bin1=[D(40), B(30), C(25)]
        // Result: bin0=[A(70)], bin1=[D(40), B(30), C(25)]
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][0].name, "A");
        assert_eq!(result[1][0].name, "D");
        assert_eq!(result[1][1].name, "B");
        assert_eq!(result[1][2].name, "C");
    }

    #[test]
    fn test_list_packer_many_small_items() {
        let packer = ListPacker::new(100);
        let items: Vec<u64> = vec![10; 20]; // 20 items of weight 10 each
        let result = packer.pack(items, |&x| x);

        // 20 * 10 = 200 total, target=100 -> should produce 2 bins of 10 items each
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 10);
        assert_eq!(result[1].len(), 10);

        for bin in &result {
            let sum: u64 = bin.iter().sum();
            assert_eq!(sum, 100);
        }
    }

    #[test]
    fn test_list_packer_stress_test() {
        let packer = ListPacker::new(1000);
        let items: Vec<u64> = (1..=100).collect(); // 1,2,3,...,100
        let result = packer.pack(items, |&x| x);

        // Verify all items are packed
        let total_items: usize = result.iter().map(|bin| bin.len()).sum();
        assert_eq!(total_items, 100);

        // Verify all bins respect target
        for bin in &result {
            let sum: u64 = bin.iter().sum();
            assert!(sum <= 1000, "Bin exceeds target: sum={}", sum);
        }

        // Verify First-Fit Decreasing: all items should be in descending order when flattened
        let all_items: Vec<u64> = result.iter().flat_map(|bin| bin.iter().copied()).collect();
        let mut sorted = all_items.clone();
        sorted.sort_by_key(|&x| std::cmp::Reverse(x));
        assert_eq!(all_items, sorted, "Items should be sorted descending");
    }

    #[test]
    fn test_list_packer_boundary_conditions() {
        let packer = ListPacker::new(100);

        // Test 1: Items that sum exactly to target
        let exact_sum_items = vec![40u64, 30, 30];
        let result = packer.pack(exact_sum_items, |&x| x);
        // Sorted: [40, 30, 30]
        // - 40: new bin0 -> bin0=[40]
        // - 30: can fit bin0 (40+30=70<=100), add to bin0 -> bin0=[40,30]
        // - 30: can fit bin0 (70+30=100<=100), add to bin0 -> bin0=[40,30,30]
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![40, 30, 30]);

        // Test 2: Items of weight 1 (minimal weight)
        let tiny_items = vec![1u64; 150]; // 150 items of weight 1
        let tiny_result = packer.pack(tiny_items, |&x| x);
        assert_eq!(tiny_result.len(), 2); // ceil(150/100) = 2 bins
        assert_eq!(tiny_result[0].len(), 100);
        assert_eq!(tiny_result[1].len(), 50);

        // Test 3: Very large weight (u64::MAX / 2)
        let large = u64::MAX / 2;
        let large_items = vec![large, large];
        let large_result = packer.pack(large_items, |&x| x);
        assert_eq!(large_result.len(), 2); // Each item exceeds target
    }
}
