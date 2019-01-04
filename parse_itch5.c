/*
(C) Copyright 2019 Shawfeng Dong. All rights reserved.
Use of this source code is governed by an MIT-style
license that can be found in the LICENSE file.
*/

// NASDAQ ITCH 5.0 parser
// Reference: Nasdaq TotalView-ITCH 5.0 Specification

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdint.h>
#include <stdbool.h>
#include <libgen.h>

#if defined(__linux__)
#include <byteswap.h>
#else /* macOS */
#define bswap_16(value) \
((((value) & 0xff) << 8) | ((value) >> 8))

#define bswap_32(value) \
(((uint32_t)bswap_16((uint16_t)((value) & 0xffff)) << 16) | \
(uint32_t)bswap_16((uint16_t)((value) >> 16)))

#define bswap_64(value) \
(((uint64_t)bswap_32((uint32_t)((value) & 0xffffffff)) << 32) | \
(uint64_t)bswap_32((uint32_t)((value) >> 32)))
#endif

#define parse_uint16(a) \
bswap_16(*((uint16_t *)(a)))

#define parse_uint32(a) \
bswap_32(*((uint32_t *)(a)))

#define parse_uint64(a) \
bswap_64(*((uint64_t *)(a)))

#define parse_uint16(a) \
bswap_16(*((uint16_t *)(a)))

#define parse_ts(a) \
(((uint64_t)bswap_16(*((uint16_t *)(a)))) << 32) | \
(uint64_t)bswap_32(*(uint32_t *)((a)+2))

#define parse_stock(n) \
for (i=0; i<8; i++) { \
  if (m[i+(n)] == ' ') { \
    stock[i] = 0; \
    break; \
  } else { \
    stock[i] = m[i+(n)]; \
  } \
}

#define parse_issue_subtype(n) \
for (i=0; i<2; i++) { \
  if (m[i+(n)] == ' ') { \
    issue_subtype[i] = 0; \
    break; \
  } else { \
    issue_subtype[i] = m[i+(n)]; \
  } \
}

#define parse_reason(n) \
for (i=0; i<4; i++) { \
  if (m[i+(n)] == ' ') { \
    reason[i] = 0; \
    break; \
  } else { \
    reason[i] = m[i+(n)]; \
  } \
}

#define parse_mpid(n) \
for (i=0; i<4; i++) { \
  if (m[i+(n)] == ' ') { \
    mpid[i] = 0; \
    break; \
  } else { \
    mpid[i] = m[i+(n)]; \
  } \
}

#define parse_attribution(n) \
for (i=0; i<4; i++) { \
  if (m[i+(n)] == ' ') { \
    attribution[i] = 0; \
    break; \
  } else { \
    attribution[i] = m[i+(n)]; \
  } \
}

int main(int argc, char *argv[])
{
  if(argc<3 || argc>4) {
    fprintf(stderr, "Usage: %s input_file_path output_folder_path [msg_types]\n\n", argv[0]);
    fprintf(stderr, "If msg_types is not provided, output will be generated for all types\n");
    exit(1);
  }

  unsigned char msg_type[22] = {'S', 'R', 'H', 'Y', 'L', 'V', 'W', 'K', 'J', 
    'h', 'A', 'F', 'E', 'C', 'X', 'D', 'U', 'P', 'Q', 'B', 'I', 'N'};
  // Set flags to process specific message types. If third (optional)
  // command line argument is not provided, assumes that all messages types
  // will be parsed
  bool parse_flag[128];
  int i, j;
  for (i=0; i<128; i++) {
    parse_flag[i] = false;
  }

  if (argc == 3) {
    for (i=0; i<22; i++) {
      parse_flag[msg_type[i]] = true;
    }
  }

  bool found;
  unsigned char c;
  if (argc == 4) {
    for (j=0; j<128; j++) {
      c = argv[3][j];
      if (c == '\0') break;
      found = false;
      for (i=0; i<22; i++) {
        if (c == msg_type[i]) {
          found = true;
          parse_flag[c] = true;
          break;
        }
      }
      if (!found) {
        fprintf(stderr, "%c is not a valid message type\n", c);
        fprintf(stderr, "Valid ITCH v5.0 message types are:\n");
        fprintf(stderr, "S R H Y L V W K J h A F E C X D U P Q B I N\n");
        exit(1);
      }
    }
  }

  // argv[1]: input file path
  FILE *f_input = fopen(argv[1], "r");
  if (f_input == NULL) {
    fprintf(stderr, "Error opening file %s: %s\n", argv[1], strerror(errno));
    exit(1);
  }

  char *argv1, *input_file_name, *output_base;
  argv1 = strdup(argv[1]);
  input_file_name = basename(argv1);
  output_base = strdup(input_file_name);

  for (unsigned long i=0; i<strlen(input_file_name); i++) {
    if (output_base[i] == '.') {
      output_base[i] = 0;
      break;
    }
  }

  // argv[2]: output folder path
  int rv = mkdir(argv[2], 0755);
  if (rv == -1) {
    if (errno == EEXIST) {
      fprintf(stderr, "Warning: output directory %s already exists!\n", argv[2]);
    }
    else {
      fprintf(stderr, "Error making directory %s: %s\n", argv[2], strerror(errno));
      exit(1);
    }
  }

  // total number of all messages
  unsigned long total = 0;
  // total number of messages for each message type
  unsigned long total_type[22];
  for (i=0; i<22; i++) {
    total_type[i] = 0;
  }

  time_t start =  time(NULL);

  printf("=========== Parsing ITCH v5.0 starts ===========\n");
  printf("Input file: %s\n", input_file_name);
  printf("Output folder: %s\n", argv[2]);

  FILE *f_output[22];
  char csv_filename[32];
  char csv_full_path[256];

  // open files only for specified message types
  for (i=0; i<22; i++) {
    unsigned char t = msg_type[i];
    f_output[i] = NULL;

    if (parse_flag[t]) {
      if (t == 'h') {
        // workaround of the limitation of case-insensitive filesystems
        // both 'H' and 'h' are valid mesaage types
        sprintf(csv_filename, "%s-halt.csv", output_base);
      } else {
        sprintf(csv_filename, "%s-%c.csv", output_base, t);
      }

      printf("Output file: %s\n", csv_filename);
      sprintf(csv_full_path, "%s/%s", argv[2], csv_filename);
      f_output[i] =  fopen(csv_full_path, "w");
      if (f_output[i] == NULL) {
        fprintf(stderr, "Error opening file %s: %s\n", csv_full_path, strerror(errno));
        exit(1);
      }
    }
  }
  free(argv1);
  free(output_base);

  uint16_t msg_header;
  uint16_t msg_length;
  // message buffer
  char m[64];

  while (true) {
    // first two bytes before each message starts encodes the length of the message
    if (fread((void*)&msg_header, sizeof(char), 2, f_input) < 2) {
      // EOF
      printf("=========== Parsing ITCH v5.0 ends   ===========\n");
      break;
    }
    msg_length = bswap_16(msg_header);
    if (fread((void*)m, sizeof(char), msg_length, f_input) < msg_length) {
        fprintf(stderr, "Error reading input file: %s\n", strerror(errno));
        goto panic;
    }

    // security symbol for the issue in the Nasdaq execution system
    char stock[9];
    stock[8] = 0;
    // security sub-type for the issue as assigned by Nasdaq
    char issue_subtype[3];
    issue_subtype[2] = 0;
    // trading action reason
    char reason[5];
    reason[4] = 0;
    // market participant identifier for which the position message is being generated
    char mpid[5];
    mpid[4] = 0;
    // market participant identifier associated with the entered order
    char attribution[5];
    attribution[4] = 0;

    char t = m[0];
    switch (t) {
      case 'S':
        if (parse_flag['S']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          fprintf(f_output[0], "%c,%u,%u,%llu.%09llu,%c\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            m[11]);
          total_type[0]++;
          total++;
        }
        break;
      case 'R':
        if (parse_flag['R']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          parse_stock(11)
          uint32_t round_lot_size = parse_uint32(m+21);
          parse_issue_subtype(27)
          uint32_t etp_leverage_factor = parse_uint32(m+34);
          fprintf(f_output[1],
            "%c,%u,%u,%llu.%09llu,%s,%c,%c,%u,%c,%c,%s,%c,%c,%c,%c,%c,%u,%c\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            stock, m[19], m[20], round_lot_size, m[25], m[26],
            issue_subtype, m[29], m[30], m[31], m[32], m[33],
            etp_leverage_factor, m[38]);
          total_type[1]++;
          total++;
        }
        break;
      case 'H':
        if (parse_flag['H']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          parse_stock(11)
          parse_reason(21)
          fprintf(f_output[2],
            "%c,%u,%u,%llu.%09llu,%s,%c,%c,%s\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            stock, m[19], m[20], reason);
          total_type[2]++;
          total++;
        }
        break;
      case 'Y':
        if (parse_flag['Y']) {
          uint16_t locate_code = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          parse_stock(11)
          fprintf(f_output[3],
            "%c,%u,%u,%llu.%09llu,%s,%c\n",
            t, locate_code, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            stock, m[19]);
          total_type[3]++;
          total++;
        }
        break;
      case 'L':
        if (parse_flag['L']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          parse_mpid(11)
          parse_stock(15)
          fprintf(f_output[4],
            "%c,%u,%u,%llu.%09llu,%s,%s,%c,%c,%c\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            mpid, stock, m[23], m[24], m[25]);
          total_type[4]++;
          total++;
        }
        break;
      case 'V':
        if (parse_flag['V']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t level1 = parse_uint64(m+11);
          uint64_t level2 = parse_uint64(m+19);
          uint64_t level3 = parse_uint64(m+27);
          fprintf(f_output[5],
            "%c,%u,%u,%llu.%09llu,%llu.%08llu,%llu.%08llu,%llu.%08llu\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            level1/100000000, level1%100000000,
            level2/100000000, level2%100000000,
            level3/100000000, level3%100000000);
          total_type[5]++;
          total++;
        }
        break;
      case 'W':
        if (parse_flag['W']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          fprintf(f_output[6],
            "%c,%u,%u,%llu.%09llu,%c\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            m[11]);
          total_type[6]++;
          total++;
        }
        break;
      case 'K':
        if (parse_flag['K']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          parse_stock(11)
          uint32_t ipo_quotation_release_time = parse_uint32(m+19);
          uint32_t ipo_price = parse_uint32(m+24);
          fprintf(f_output[7],
            "%c,%u,%u,%llu.%09llu,%s,%u,%c,%u.%04u\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            stock, ipo_quotation_release_time, m[23],
            ipo_price/10000, ipo_price%10000);
          total_type[7]++;
          total++;
        }
        break;
      case 'J':
        if (parse_flag['J']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          parse_stock(11)
          uint32_t acrp = parse_uint32(m+19);
          uint32_t uacp = parse_uint32(m+23);
          uint32_t lacp = parse_uint32(m+27);
          uint32_t auction_collar_extension = parse_uint32(m+31);
          fprintf(f_output[8],
            "%c,%u,%u,%llu.%09llu,%s,%u.%04u,%u.%04u,%u.%04u,%u\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000, stock,
            acrp/10000, acrp%10000, uacp/10000, uacp%10000,
            lacp/10000, lacp%10000, auction_collar_extension);
          total_type[8]++;
          total++;
        }
        break;
      case 'h':
        if (parse_flag['h']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          parse_stock(11)
          fprintf(f_output[9],
            "%c,%u,%u,%llu.%09llu,%s,%c,%c\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            stock, m[19], m[20]);
          total_type[9]++;
          total++;
        }
        break;
      case 'A':
        if (parse_flag['A']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t order_reference_number = parse_uint64(m+11);
          uint32_t shares = parse_uint32(m+20);
          parse_stock(24)
          uint32_t price = parse_uint32(m+32);
          fprintf(f_output[10],
            "%c,%u,%u,%llu.%09llu,%llu,%c,%u,%s,%u.%04u\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            order_reference_number, m[19], shares, stock,
            price/10000, price%10000);
          total_type[10]++;
          total++;
        }
        break;
      case 'F':
        if (parse_flag['F']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t order_reference_number = parse_uint64(m+11);
          uint32_t shares = parse_uint32(m+20);
          parse_stock(24)
          uint32_t price = parse_uint32(m+32);
          parse_attribution(36)
          fprintf(f_output[11],
            "%c,%u,%u,%llu.%09llu,%llu,%c,%u,%s,%u.%04u,%s\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            order_reference_number, m[19], shares, stock,
            price/10000, price%10000, attribution);
          total_type[11]++;
          total++;
        }
        break;
      case 'E':
        if (parse_flag['E']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t order_reference_number = parse_uint64(m+11);
          uint32_t executed_shares = parse_uint32(m+19);
          uint64_t match_number = parse_uint64(m+23);
          fprintf(f_output[12],
            "%c,%u,%u,%llu.%09llu,%llu,%u,%llu\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            order_reference_number, executed_shares, match_number);
          total_type[12]++;
          total++;
        }
        break;
      case 'C':
        if (parse_flag['C']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t order_reference_number = parse_uint64(m+11);
          uint32_t executed_shares = parse_uint32(m+19);
          uint64_t match_number = parse_uint64(m+23);
          uint32_t execution_price = parse_uint32(m+32);
          fprintf(f_output[13],
            "%c,%u,%u,%llu.%09llu,%llu,%u,%llu,%c,%u.%04u\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            order_reference_number, executed_shares,
            match_number, m[31],
            execution_price/10000, execution_price%10000);
          total_type[13]++;
          total++;
        }
        break;
      case 'X':
        if (parse_flag['X']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t order_reference_number = parse_uint64(m+11);
          uint32_t cancelled_shares = parse_uint32(m+19);
          fprintf(f_output[14],
            "%c,%u,%u,%llu.%09llu,%llu,%u\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            order_reference_number, cancelled_shares);
          total_type[14]++;
          total++;
        }
        break;
      case 'D':
        if (parse_flag['D']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t order_reference_number = parse_uint64(m+11);
          fprintf(f_output[15],
            "%c,%u,%u,%llu.%09llu,%llu\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            order_reference_number);
          total_type[15]++;
          total++;
        }
        break;
      case 'U':
        if (parse_flag['U']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t original_order_reference_number = parse_uint64(m+11);
          uint64_t new_order_reference_number = parse_uint64(m+19);
          uint32_t shares = parse_uint32(m+27);
          uint32_t price = parse_uint32(m+31);
          fprintf(f_output[16],
            "%c,%u,%u,%llu.%09llu,%llu,%llu,%u,%u.%04u\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            original_order_reference_number, new_order_reference_number,
            shares, price/10000, price%10000);
          total_type[16]++;
          total++;
        }
        break;
      case 'P':
        if (parse_flag['P']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t order_reference_number = parse_uint64(m+11);
          uint32_t shares = parse_uint32(m+20);
          parse_stock(24)
          uint32_t price = parse_uint32(m+32);
          uint64_t match_number = parse_uint64(m+36);
          fprintf(f_output[17],
            "%c,%u,%u,%llu.%09llu,%llu,%c,%u,%s,%u.%04u,%llu\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            order_reference_number, m[19], shares, stock,
            price/10000, price%10000, match_number);
          total_type[17]++;
          total++;
        }
        break;
      case 'Q':
        if (parse_flag['Q']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t shares = parse_uint64(m+11);
          parse_stock(19)
          uint32_t cross_price = parse_uint32(m+27);
          uint64_t match_number = parse_uint64(m+31);
          fprintf(f_output[18],
            "%c,%u,%u,%llu.%09llu,%llu,%s,%u.%04u,%llu,%c\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            shares, stock,
            cross_price/10000, cross_price%10000,
            match_number, m[39]);
          total_type[18]++;
          total++;
        }
        break;
      case 'B':
        if (parse_flag['B']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t match_number = parse_uint64(m+11);
          fprintf(f_output[19],
            "%c,%u,%u,%llu.%09llu,%llu\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            match_number);
          total_type[19]++;
          total++;
        }
        break;
      case 'I':
        if (parse_flag['I']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          uint64_t paired_shares = parse_uint64(m+11);
          uint64_t imbalance_shares = parse_uint64(m+19);
          parse_stock(28)
          uint32_t far_price = parse_uint32(m+36);
          uint32_t near_price = parse_uint32(m+40);
          uint32_t current_ref_price = parse_uint32(m+44);
          fprintf(f_output[20],
            "%c,%u,%u,%llu.%09llu,%llu,%llu,%c,%s,%u.%04u,%u.%04u,%u.%04u,%c,%c\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            paired_shares, imbalance_shares, m[27], stock,
            far_price/10000, far_price%10000,
            near_price/10000, near_price%10000,
            current_ref_price/10000, current_ref_price%10000,
            m[48], m[49]);
          total_type[20]++;
          total++;
        }
        break;
      case 'N':
        if (parse_flag['N']) {
          uint16_t stock_locate = parse_uint16(m+1);
          uint16_t tracking_number = parse_uint16(m+3);
          uint64_t timestamp = parse_ts(m+5);
          parse_stock(11)
          fprintf(f_output[21],
            "%c,%u,%u,%llu.%09llu,%s,%c\n",
            t, stock_locate, tracking_number,
            timestamp/1000000000, timestamp%1000000000,
            stock, m[19]);
          total_type[21]++;
          total++;
        }
        break;
      default:
        fprintf(stderr, "How could it be? An unrecognized type: %c! I am freaking out...\n", t);
        goto panic;
    }
  }

  printf("Total number of all messages parsed: %lu\n", total);
  for (i=0; i<22; i++) {
    printf("Total number of %c messages parsed: %lu\n", msg_type[i], total_type[i]);
  }

  time_t end = time(NULL);
  printf("Time spent: %ld seconds\n", (end - start));

  fclose(f_input);
  for (i=0; i<22; i++) {
    unsigned char t = msg_type[i];
    if (parse_flag[t]) fclose(f_output[i]);
  }
  return 0;

panic:
  fclose(f_input);
  for (i=0; i<22; i++) {
    unsigned char t = msg_type[i];
    if (parse_flag[t]) fclose(f_output[i]);
  }
  exit(1);
}
