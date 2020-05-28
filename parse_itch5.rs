/// (C) Copyright 2020 Shawfeng Dong. All rights reserved.
/// Use of this source code is governed by an MIT-style
/// license that can be found in the LICENSE file.

/// NASDAQ ITCH 5.0 parser
/// Reference: Nasdaq TotalView-ITCH 5.0 Specification

use std::path::Path;
use std::fs;
use std::io::prelude::*;
use std::convert::TryInto;
use std::time::SystemTime;
// use std::collections::HashMap;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let argc = args.len();
    if argc<3 || argc>4 {
        eprintln!("Usage: {} input_file_path output_folder_path [msg_types]\n", args[0]);
        eprintln!("If msg_types is not provided, output will be generated for all types");
        std::process::exit(1);
    }
    
    // 22 message types in ITCH 5.0 specification
    let msg_type = ['S', 'R', 'H', 'Y', 'L', 'V', 'W', 'K', 'J',
        'h', 'A', 'F', 'E', 'C', 'X', 'D', 'U', 'P', 'Q', 'B', 'I', 'N'];

    // Set flags to process specific message types. If third (optional)
    // command line argument is not provided, assumes that all messages types
    // will be parsed
    let mut parse_flag: [bool; 128]  = [false; 128];
    if argc == 3 {
        for t in &msg_type {
            parse_flag[*t as usize] = true;
        }
    } else {
        for c in args[3].chars() {
            let mut found = false;
            for t in &msg_type {
                if c == *t {
                    parse_flag[c as usize] = true;
                    found = true;
                    break;
                }
            }
            if !found {
                eprintln!("{} is not a valid message type", c);
                eprintln!("Valid ITCH v5.0 message types are:");
                eprintln!("S R H Y L V W K J h A F E C X D U P Q B I N");
                std::process::exit(1);
            }
        }
    }

	// args[1]: input file path
    let mut in_file = fs::File::open(&args[1]).expect("Can't open the input file!");

	// args[2]: output folder path
    fs::create_dir_all(&args[2]).expect("Can't create the output folder!");

    // total number of all messages
    let mut total = 0u32;
    // total number of messages for each message type
    let mut total_type = [0u32; 22];

	println!("=========== Parsing ITCH v5.0 starts ===========");
	println!("Input file: {}", args[1]);
	println!("Output folder: {}", args[2]);

    let start = SystemTime::now();
    
    // open files only for specified message types
    let out_base = Path::new(&args[1]).file_stem().unwrap().to_str().unwrap();
    let mut csv_full_path: String;

    // This is ugly as hell!!! 
    // But I can't get arrays of FILEs, nor HashMap of FILEs, to work yet
    // let mut csv_file: [fs::File; 22];
    // let mut csv_file = HashMap::new();

    // rust scope!!!
    let mut t = msg_type[0];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_0 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");
    
    t = msg_type[1];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_1 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[2];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_2 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[3];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_3 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[4];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_4 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[5];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_5 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[6];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_6 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[7];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_7 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[8];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_8 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[9];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_9 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[10];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_10 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[11];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_11 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[12];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_12 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[13];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_13 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[14];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_14 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[15];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_15 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[16];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_16 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[17];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_17 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[18];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_18 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[19];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_19 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[20];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_20 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

    t = msg_type[21];
    if t == 'h' {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        csv_full_path = format!("{}/{}-halt.csv", args[2], out_base);
    } else {
        csv_full_path = format!("{}/{}-{}.csv", args[2], out_base, t);
    }
    println!("Output file: {}", csv_full_path);
    let mut csv_file_21 = fs::File::create(csv_full_path).expect("Can't create the csv output file!");

	// first two bytes before each message starts encodes the length of the message
	let mut msg_header = [0; 2];
    loop {
        match in_file.read(&mut msg_header) {
            Ok(n) => {
                if n < 2 {
                    println!("=========== Parsing ITCH v5.0 ends   ===========");
                    break;
                }
                let msg_length = u16::from_be_bytes(msg_header);
	            // println!("Message length: {}", msg_length);
                // read msg_length bytes
                let mut m = vec![0u8; msg_length.try_into().unwrap()];
                match in_file.read_exact(&mut m) {
                    Ok(_) => {
                        // message type
                        let t: char = m[0].into();
                        // println!("Message length: {}, type = {}", msg_length, t);
                        match t {
                            'S' => {
                                if parse_flag['S' as usize] {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6],m[7],
                                                                   m[8], m[9], m[10]]);
                                let event_code: char = m[11].into();
                                csv_file_0.write_fmt(format_args!("{},{},{},{}.{:09},{}\n", 
                                         t, stock_locate, tracking_number, 
                                         timestamp/1000000000, timestamp%1000000000, 
                                         event_code)).expect("Can't write to csv file!");
                                total_type[0] += 1;
                                total += 1;
                                }
                            },
                            'R' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let stock = String::from_utf8_lossy(&m[11..19]);
                                let market_category: char = m[19].into();
                                let financial_status_indicator: char= m[20].into();
                                let round_lot_size = u32::from_be_bytes([m[21], m[22],
                                                                       m[23], m[24]]);
                                let round_lots_only: char= m[25].into();
                                let issue_classification: char = m[26].into();
                                let issue_sub_type = String::from_utf8_lossy(&m[27..29]);
                                let authenticity: char = m[29].into();
                                let short_sale_threshold_indicator: char = m[30].into();
                                let ipo_flag: char = m[31].into();
                                let luld_reference_price_tier: char = m[32].into();
                                let etp_flag: char = m[33].into();
                                let etp_leverage_factor = u32::from_be_bytes([m[34], 
                                                            m[35], m[36], m[37]]);
                                let inverse_indicator: char = m[38].into();
                                csv_file_1.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    stock, market_category, financial_status_indicator,
                                    round_lot_size, round_lots_only, issue_classification,
                                    issue_sub_type, authenticity,
                                    short_sale_threshold_indicator, ipo_flag,
                                    luld_reference_price_tier, etp_flag, 
                                    etp_leverage_factor, inverse_indicator)).expect("Can't write to csv file!");
                                total_type[1] += 1;
                                total += 1;
                            },
                            'H' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let stock = String::from_utf8_lossy(&m[11..19]);
                                let trading_state: char = m[19].into();
                                let reserved: char= m[20].into();
                                let reason = String::from_utf8_lossy(&m[21..25]);
                                csv_file_2.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    stock, trading_state, reserved, reason)).expect("Can't write to csv file!");
                                total_type[2] += 1;
                                total += 1;
                            },
                            'Y' => {
                                let locate_code = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let stock = String::from_utf8_lossy(&m[11..19]);
                                let reg_sho_action: char = m[19].into();
                                csv_file_3.write_fmt(format_args!("{},{},{},{}.{:09},{},{}\n",
                                    t, locate_code, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    stock, reg_sho_action)).expect("Can't write to csv file!");
                                total_type[3] += 1;
                                total += 1;
                            },
                            'L' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let mpid = String::from_utf8_lossy(&m[11..15]);
                                let stock = String::from_utf8_lossy(&m[15..23]);
                                let primary_market_maker: char = m[23].into();
                                let market_maker_mode: char = m[24].into();
                                let market_participant_state: char= m[25].into();
                                csv_file_4.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{},{},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    mpid, stock, primary_market_maker, 
                                    market_maker_mode, 
                                    market_participant_state)).expect("Can't write to csv file!");
                                total_type[4] += 1;
                                total += 1;
                            },
                            'V' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let level1 = u64::from_be_bytes([m[11], m[12], m[13], 
                                                    m[14], m[15], m[16], m[17], m[18]]);
                                let level2 = u64::from_be_bytes([m[19], m[20], m[21], 
                                                    m[22], m[23], m[24], m[25], m[26]]);
                                let level3 = u64::from_be_bytes([m[27], m[28], m[29], 
                                                    m[30], m[31], m[32], m[33], m[34]]);
                                csv_file_5.write_fmt(format_args!("{},{},{},{}.{:09},{}.{:08},{}.{:08},{}.{:08}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    level1/100000000, level1%100000000,
                                    level2/100000000, level2%100000000,
                                    level3/100000000, level3%100000000)).expect("Can't write to csv file!");
                                total_type[5] += 1;
                                total += 1;
                            },
                            'W' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let breached_level : char = m[11].into();
                                csv_file_6.write_fmt(format_args!("{},{},{},{}.{:09},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    breached_level)).expect("Can't write to csv file!");
                                total_type[6] += 1;
                                total += 1;
                            },
                            'K' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let stock = String::from_utf8_lossy(&m[11..19]);
                                let ipo_quotation_release_time = u32::from_be_bytes([m[19], m[20],
                                                                       m[21], m[22]]);
                                let ipo_quotation_release_qualifier: char = m[23].into();
                                let ipo_price = u32::from_be_bytes([m[24], m[25],
                                                                    m[26], m[27]]);
                                csv_file_7.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{},{}.{:04}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    stock, ipo_quotation_release_time,
                                    ipo_quotation_release_qualifier, 
                                    ipo_price/10000, ipo_price%10000)).expect("Can't write to csv file!");
                                total_type[7] += 1;
                                total += 1;
                            },
                            'J' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let stock = String::from_utf8_lossy(&m[11..19]);
                                let acrp = u32::from_be_bytes([m[19], m[20],
                                                            m[21], m[22]]);
                                let uacp = u32::from_be_bytes([m[23], m[24],
                                                            m[25], m[26]]);
                                let lacp = u32::from_be_bytes([m[27], m[28],
                                                            m[29], m[30]]);
                                let auction_collar_extension = u32::from_be_bytes([m[31], m[32],
                                                            m[33], m[34]]);
                                csv_file_8.write_fmt(format_args!("{},{},{},{}.{:09},{},{}.{:04},{}.{:04},{}.{:04},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    stock, acrp/10000, acrp%10000, uacp/10000, uacp%10000,
                                    lacp/10000, lacp%10000, 
                                    auction_collar_extension)).expect("Can't write to csv file!");
                                total_type[8] += 1;
                                total += 1;
                            },
                            'h' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let stock = String::from_utf8_lossy(&m[11..19]);
                                let market_code: char = m[19].into();
                                let operational_halt_action: char= m[20].into();
                                csv_file_9.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    stock, market_code, 
                                    operational_halt_action)).expect("Can't write to csv file!");
                                total_type[9] += 1;
                                total += 1;
                            },
                            'A' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let order_reference_number = 
                                    u64::from_be_bytes([m[11], m[12], m[13], m[14], 
                                                        m[15], m[16], m[17], m[18]]);
                                let buy_sell_indicator: char = m[19].into();
                                let shares = u32::from_be_bytes([m[20], m[21],
                                                                 m[22], m[23]]);
                                let stock = String::from_utf8_lossy(&m[24..31]);
                                let price = u32::from_be_bytes([m[32], m[33],
                                                                m[34], m[35]]);
                                csv_file_10.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{},{},{}.{:04}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    order_reference_number, buy_sell_indicator,
                                    shares, stock,
                                    price/10000, price%10000)).expect("Can't write to csv file!");
                                total_type[10] += 1;
                                total += 1;
                            },
                            'F' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let order_reference_number =
                                    u64::from_be_bytes([m[11], m[12], m[13], m[14],
                                                        m[15], m[16], m[17], m[18]]);
                                let buy_sell_indicator: char = m[19].into();
                                let shares = u32::from_be_bytes([m[20], m[21],
                                                                 m[22], m[23]]);
                                let stock = String::from_utf8_lossy(&m[24..31]);
                                let price = u32::from_be_bytes([m[32], m[33],
                                                                m[34], m[35]]);
                                let attribution = String::from_utf8_lossy(&m[36..40]);
                                csv_file_11.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{},{},{}.{:04},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    order_reference_number, buy_sell_indicator,
                                    shares, stock,
                                    price/10000, price%10000, 
                                    attribution)).expect("Can't write to csv file!");
                                total_type[11] += 1;
                                total += 1;
                            },
                            'E' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let order_reference_number =
                                    u64::from_be_bytes([m[11], m[12], m[13], m[14],
                                                        m[15], m[16], m[17], m[18]]);
                                let executed_shares = 
                                    u32::from_be_bytes([m[19], m[20], m[21], m[22]]);
                                let match_number = 
                                    u64::from_be_bytes([m[23], m[24], m[25], m[26], 
                                                        m[27], m[28], m[29], m[30]]);
                                csv_file_12.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    order_reference_number, executed_shares,
                                    match_number)).expect("Can't write to csv file!");
                                total_type[12] += 1;
                                total += 1;
                            },
                            'C' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let order_reference_number =
                                    u64::from_be_bytes([m[11], m[12], m[13], m[14],
                                                        m[15], m[16], m[17], m[18]]);
                                let executed_shares = 
                                    u32::from_be_bytes([m[19], m[20], m[21], m[22]]);
                                let match_number = 
                                    u64::from_be_bytes([m[23], m[24], m[25], m[26], 
                                                        m[27], m[28], m[29], m[30]]);
                                let printable: char = m[31].into();
                                let execution_price = u32::from_be_bytes([m[32], m[33],
                                                                       m[34], m[35]]);
                                csv_file_13.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{},{},{}.{:04}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    order_reference_number, executed_shares,
                                    match_number, printable,
                                    execution_price/10000, 
                                    execution_price%10000)).expect("Can't write to csv file!");
                                total_type[13] += 1;
                                total += 1;
                            },
                            'X' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let order_reference_number =
                                    u64::from_be_bytes([m[11], m[12], m[13], m[14],
                                                        m[15], m[16], m[17], m[18]]);
                                let cancelled_shares = 
                                    u32::from_be_bytes([m[19], m[20], m[21], m[22]]);
                                csv_file_14.write_fmt(format_args!("{},{},{},{}.{:09},{},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    order_reference_number, 
                                    cancelled_shares)).expect("Can't write to csv file!");
                                total_type[14] += 1;
                                total += 1;
                            },
                            'D' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let order_reference_number =
                                    u64::from_be_bytes([m[11], m[12], m[13], m[14],
                                                        m[15], m[16], m[17], m[18]]);
                                csv_file_15.write_fmt(format_args!("{},{},{},{}.{:09},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    order_reference_number)).expect("Can't write to csv file!");
                                total_type[15] += 1;
                                total += 1;
                            },
                            'U' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let original_order_reference_number =
                                    u64::from_be_bytes([m[11], m[12], m[13], m[14],
                                                        m[15], m[16], m[17], m[18]]);
                                let new_order_reference_number =
                                    u64::from_be_bytes([m[19], m[20], m[21], m[22],
                                                        m[23], m[24], m[25], m[26]]);
                                let shares = 
                                    u32::from_be_bytes([m[27], m[28], m[29], m[30]]);
                                let price = u32::from_be_bytes([m[31], m[32],
                                                                m[33], m[34]]);
                                csv_file_16.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{},{}.{:04}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    original_order_reference_number,
                                    new_order_reference_number,
                                    shares, price/10000, price%10000)).expect("Can't write to csv file!");
                                total_type[16] += 1;
                                total += 1;
                            },
                            'P' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let order_reference_number =
                                    u64::from_be_bytes([m[11], m[12], m[13], m[14],
                                                        m[15], m[16], m[17], m[18]]);
                                let buy_sell_indicator: char = m[19].into();
                                let shares = u32::from_be_bytes([m[20], m[21],
                                                                 m[22], m[23]]);
                                let stock = String::from_utf8_lossy(&m[24..31]);
                                let price = u32::from_be_bytes([m[32], m[33],
                                                                m[34], m[35]]);
                                let match_number = 
                                    u64::from_be_bytes([m[36], m[37], m[38], m[39],
                                                        m[40], m[41], m[42], m[43]]);
                                csv_file_17.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{},{},{}.{:04},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    order_reference_number, buy_sell_indicator, 
                                    shares, stock,
                                    price/10000, price%10000, match_number)).expect("Can't write to csv file!");
                                total_type[17] += 1;
                                total += 1;
                            },
                            'Q' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let shares = 
                                    u64::from_be_bytes([m[11], m[12], m[13], m[14],
                                                        m[15], m[16], m[17], m[18]]);
                                let stock = String::from_utf8_lossy(&m[19..27]);
                                let cross_price = u32::from_be_bytes([m[27], m[28],
                                                                m[29], m[30]]);
                                let match_number = 
                                    u64::from_be_bytes([m[31], m[32], m[33], m[34],
                                                        m[35], m[36], m[37], m[38]]);
                                let cross_type: char = m[39].into();
                                csv_file_18.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{}.{:04},{},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    shares, stock,
                                    cross_price/10000, cross_price%10000,
                                    match_number, cross_type)).expect("Can't write to csv file!");
                                total_type[18] += 1;
                                total += 1;
                            },
                            'B' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let match_number = 
                                    u64::from_be_bytes([m[11], m[12], m[13], m[14],
                                                        m[15], m[16], m[17], m[18]]);
                                csv_file_19.write_fmt(format_args!("{},{},{},{}.{},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    match_number)).expect("Can't write to csv file!");
                                total_type[19] += 1;
                                total += 1;
                            },
                            'I' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let paired_shares = 
                                    u64::from_be_bytes([m[11], m[12], m[13], m[14],
                                                        m[15], m[16], m[17], m[18]]);
                                let imbalance_shares = 
                                    u64::from_be_bytes([m[19], m[20], m[21], m[22],
                                                        m[23], m[24], m[25], m[26]]);
                                let imbalance_direction: char = m[27].into();
                                let stock = String::from_utf8_lossy(&m[28..36]);
                                let far_price = u32::from_be_bytes([m[36], m[37],
                                                                       m[38], m[39]]);
                                let near_price = u32::from_be_bytes([m[40], m[41],
                                                                       m[42], m[43]]);
                                let current_ref_price = u32::from_be_bytes([m[44], m[45],
                                                                       m[46], m[47]]);
                                let cross_type: char= m[48].into();
                                let price_variation_indicator: char = m[49].into();
                                csv_file_20.write_fmt(format_args!("{},{},{},{}.{:09},{},{},{},{},{}.{:04},{}.{:04},{}.{:04},{},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    paired_shares, imbalance_shares, 
                                    imbalance_direction, stock,
                                    far_price/10000, far_price%10000,
                                    near_price/10000, near_price%10000,
                                    current_ref_price/10000, current_ref_price%10000,
                                    cross_type, price_variation_indicator)).expect("Can't write to csv file!");
                                total_type[20] += 1;
                                total += 1;
                            },
                            'N' => {
                                let stock_locate = u16::from_be_bytes([m[1], m[2]]);
                                let tracking_number = u16::from_be_bytes([m[3], m[4]]);
                                let timestamp = u64::from_be_bytes([0, 0, m[5], m[6], 
                                                            m[7], m[8], m[9], m[10]]);
                                let stock = String::from_utf8_lossy(&m[11..19]);
                                let interest_flag: char = m[19].into();
                                csv_file_21.write_fmt(format_args!("{},{},{},{}.{:09},{},{}\n",
                                    t, stock_locate, tracking_number,
                                    timestamp/1000000000, timestamp%1000000000,
                                    stock, interest_flag)).expect("Can't write to csv file!");
                                total_type[21] += 1;
                                total += 1;
                            },
                            _ => { 
                                eprintln!("How could it be? An unrecognized type: {}! I am freaking out...", t);
                                std::process::exit(1);
                            }
                        }
                    },
                    Err(_e) => {
                        eprintln!("Something wrong!");
                        std::process::exit(1);
                    },
                }
            },
            Err(_e) => {
                println!("EOF");
                // clean up
                break;
            },
        }
    }
    
    println!("Total number of all messages parsed: {}", total);
    for i in 0..22 {
        println!("Total number of {} messages parsed: {}", msg_type[i], total_type[i]);
    }

    match start.elapsed() {
       Ok(elapsed) => {
           println!("Time spent: {} seconds", elapsed.as_secs());
       }
       Err(e) => {
           // an error occurred!
           println!("Error: {:?}", e);
       }
   }
}
