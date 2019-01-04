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

In the C implementation, we the `bswap` macros to do the conversion.
The [bswap](http://man7.org/linux/man-pages/man3/bswap.3.html)
macros are part of GPU extensions, so they are readily available on Linux systems.
On systems where the `bswap` macros are unavailable, such as macOS, we define
our own.

In C, we use the more portable int types `uint16_t`, `uint32_t`, & `uint64_t`.
