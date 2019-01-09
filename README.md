# NASDAQ TotalView-ITCH 5.0 parsers written in Go and C

Here are 2 parsers of NASDAQ TotalView-ITCH 5.0 outbound data feeds, written
C and Go, respectively. The specification for NASDAQ TotalView-ITCH 5.0 can
be found at <https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHspecification.pdf>.

All integer fields in ITCH data feeds are big-endian (network byte order)
binary encoded numbers. Unless otherwise noted, they are unsigned. In the
Go implementation, we use the [encoding/binary](https://golang.org/pkg/encoding/binary/)
package of the [Go Standard Library](https://golang.org/pkg/#stdlib) to
convert big-endian integers to little-endian.

## Notes on the C Implementation

In the C implementation, we use the `bswap` macros to do the conversion.
The [bswap](http://man7.org/linux/man-pages/man3/bswap.3.html)
macros are part of GPU extensions, so they are readily available on
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

## Compiling the codes

You can build the Go parser with:

```console
go build parseITCH5.go
```

To build the C parser on macOS:

```console
clang -O2 parse_itch5.c -o parse_itch5
```

To build the C parser on Linux:

```console
gcc -O2 -Wall -Wno-format parse_itch5.c -o parse_itch5
```

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
([Core-i7-7820HQ][1]), it took about **120** seconds to process the 6GB
daily feed *S051018-v50.txt* using the C parser; while it took a whooping
**2200** seconds using the Go parser!

The algorithms of the 2 parsers are largely identical. Needless to say,
the performance of the Go parser is very disappointing.

[1]: https://ark.intel.com/products/97496/Intel-Core-i7-7820HQ-Processor-8M-Cache-up-to-3-90-GHz-