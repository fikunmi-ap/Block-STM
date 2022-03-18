use diem_transaction_benchmarks::transactions::TransactionBencher;
use language_e2e_tests::account_universe::P2PTransferGen;
use num_cpus;
use proptest::prelude::*;

fn main() {
    let bencher = TransactionBencher::new(any_with::<P2PTransferGen>((1_000, 1_000_000)));

    let writes = [1.0];
    let reads = [1.0];
    let acts = [2, 10, 100, 1000, 10000];
    // let acts = [1000];
    let txns = [1000, 10000];
    let num_warmups = 2;
    let num_runs = 10;

    let mut measurements = Vec::new();

    for block_size in txns {
        for read_rate in reads {
            for num_accounts in acts {
                for write_rate in writes {
                    let tps = bencher.manual_parallel(
                        num_accounts,
                        block_size,
                        write_rate,
                        read_rate,
                        num_warmups,
                        num_runs,
                    );
                    measurements.push(tps);
                }
            }
        }
    }

    println!("CPUS = {}", num_cpus::get());

    let mut i = 0;
    for block_size in txns {
        for read_rate in reads {
            for num_accounts in acts {
                for write_rate in writes {
                    println!(
                        "PARAMS: (keep ratio) writes: {:?}, reads: {:?}, \
                         num accounts = {}, num txns in block = {}",
                        write_rate, read_rate, num_accounts, block_size
                    );
                    println!(
                        "TPS: (only parallel exec, with r/w inference) = {:?}",
                        measurements[i]
                    );
                    let mut sum1 = 0;
                    let mut sum2 = 0;
                    for m in &measurements[i] {
                        sum1 += m.0;
                    }
                    for m in &measurements[i] {
                        sum2 += m.1;
                    }
                    println!(
                        "AVG TPS: (only parallel, with inference) = {:?}, {:?}",
                        sum1 / measurements[i].len(),
                        sum2 / measurements[i].len(),
                    );
                    i = i + 1;
                }
                println!();
            }
        }
    }
}
