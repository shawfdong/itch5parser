# NASDAQ TotalView-ITCH 5.0 parsers written in C, Go and Rust

Here are 3 parsers of NASDAQ TotalView-ITCH 5.0 outbound data feeds, written
in C, Go and Rust, respectively. The specification for NASDAQ TotalView-ITCH 5.0 can
be found at <https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHspecification.pdf>.

All integer fields in ITCH data feeds are big-endian (network byte order)
binary encoded numbers. Unless otherwise noted, they are unsigned.

## Notes on the C Implementation

In the C implementation, we use the `bswap` macros to do the conversion.
The [bswap](http://man7.org/linux/man-pages/man3/bswap.3.html)
macros are part of GNU extensions, so they are readily available on
Linux systems. On systems where the `bswap` macros are unavailable,
such as macOS, we define our own.

In C, we use the more portable integer types `uint16_t`, `uint32_t`,
& `uint64_t`. But alas, this introduces a minor compatibility issue!
On macOS, v10.13 High Sierra
at least, `uint64_t` is typedef'ed to `unsigned long long`; so we would use the
format specifier `%llu` for `uint64_t`, which works without a hitch on my 2017
MacBook Pro. However, when I compiled the same code on a host running 64-bit
Ubuntu 18.04, I got tons of warnings like the following:

```console
parse_itch5.c:638:33: warning: format ‘%llu’ expects argument of type ‘long long unsigned int’, but argument 7 has type ‘uint64_t {aka long unsigned int}’ [-Wformat=]
             "%c,%u,%u,%llu.%09llu,%s,%c\n",
                            ~~~~~^
                            %09lu
parse_itch5.c:640:35:
             timestamp/1000000000, timestamp%1000000000,
                                   ~~~~~~~~~~~~~~~~~~~~
```

Apparently, `uint64_t` is `unsigned long` on 64-bit Ubuntu 18.04 (likely on
other 64-bit Linux systems as well), and it expects the format specifier `%lu`!

But those warnings are harmless, we can safely suppress them with the compiler
flag `-Wno-format`.

To build the C parser on macOS:

```console
clang -O2 parse_itch5.c -o parse_itch5
```

To build the C parser on Linux:

```console
gcc -O2 -Wall -Wno-format parse_itch5.c -o parse_itch5
```

## Notes on the Go Implementation

In the Go implementation, we use the [encoding/binary](https://golang.org/pkg/encoding/binary/)
package of the [Go Standard Library](https://golang.org/pkg/#stdlib) to
convert big-endian integers to little-endian.

You can build the Go parser with:

```console
go build parseITCH5.go
```

## Notes on the Rust Implementation

I started to learn Rust recently. After some struggle, I implemented the
parser in Rust. The code is probably not idiomatic, certainly not optimized.
But the parsing results appear to be correct, consistent with those of the 
C and Go parsers.

## Usage

Running the executable without any argument will show you the usage:

```console
$ ./parse_itch5
Usage: ./parse_itch5 input_file_path output_folder_path [msg_types]

If msg_types is not provided, output will be generated for all types
```

For example, to parse all messages in the daily feed *S051018-v50.txt*, and
saved the parsed CSV files in folder *output*:

```console
./parse_itch5 /path/to/S051018-v50.txt output
```

If you only want to parse messages of type `R` and `A`:

```console
./parse_itch5 /path/to/S051018-v50.txt output RA
```

## Performance

On my 2017 MacBook Pro, which has a 2.9 GHz Intel Kaby Lake 4-core CPU
([Core i7-7820HQ][1]), it took about **120** seconds to process the 6GB
daily feed *S051018-v50.txt* using the C parser; while it took a whooping
**2200** seconds using the Go parser! The algorithms of the 2 parsers are 
largely identical. Needless to say, the performance of the Go parser is 
very disappointing.

I tested the Rust parser on my 16-inch 2019 MacBook Pro, which has a 
2.3 GHz Intel Coffee Lake 8-core CPU ([Core i9-9880H][2]). With Turbo 
Boost disabled, the Rust parser is even slower than the Go parser!

[1]: https://ark.intel.com/content/www/us/en/ark/products/97496/intel-core-i7-7820hq-processor-8m-cache-up-to-3-90-ghz.html
[2]: https://ark.intel.com/content/www/us/en/ark/products/192987/intel-core-i9-9880h-processor-16m-cache-up-to-4-80-ghz.html
