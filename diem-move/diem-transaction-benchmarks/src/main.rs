use diem_transaction_benchmarks::transactions::TransactionBencher;
use language_e2e_tests::account_universe::P2PTransferGen;
use num_cpus;
use proptest::prelude::*;

fn main() {
    let bencher = TransactionBencher::new(any_with::<P2PTransferGen>((1_000, 1_000_000)));

    let acts = [2, 10, 100, 1000, 10000];
    let txns = [1000, 10000];
    let num_warmups = 2;
    let num_runs = 10;

    let mut measurements = Vec::new();

    for block_size in txns {
        for num_accounts in acts {
            let mut times = bencher.manual_parallel(
                num_accounts,
                block_size,
                num_warmups,
                num_runs,
            );
            times.sort();
            measurements.push(times);
        }
    }

    println!("CPUS = {}", num_cpus::get());

    let mut i = 0;
    for block_size in txns {
        for num_accounts in acts {
            println!(
                "PARAMS: num_account = {}, block_size = {}",
                num_accounts, block_size
            );
            println!(
                "TPS: {:?}",
                measurements[i]
            );
            let mut sum = 0;
            for m in &measurements[i] {
                sum += m;
            }
            println!(
                "AVG TPS = {:?}",
                sum / measurements[i].len()
            );
            i = i + 1;
        }
        println!();
    }
}