// Copyright 2019 Shawfeng Dong. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// NASDAQ ITCH 5.0 parser
// Reference: Nasdaq TotalView-ITCH 5.0 Specification
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"strings"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	if len(os.Args) < 3 || len(os.Args) > 4 {
		fmt.Printf("Usage: %s input_file_path output_folder_path [msg_types]\n\n", os.Args[0])
		fmt.Printf("If msg_types is not provided, output will be generated for all types\n")
		os.Exit(1)
	}

	// 22 message types in ITCH 5.0 specification
	msgType := []byte{
		'S', 'R', 'H', 'Y', 'L', 'V',
		'W', 'K', 'J', 'h', 'A', 'F',
		'E', 'C', 'X', 'D', 'U', 'P',
		'Q', 'B', 'I', 'N'}

	// Set flags to process specific message types. If third (optional)
	// command line argument is not provided, assumes that all messages types
	// will be parsed
	parseFlag := make(map[byte]bool)
	if len(os.Args) == 3 {
		for _, t := range msgType {
			parseFlag[t] = true
		}
	} else {
		for _, a := range os.Args[3] {
			found := false
			for _, t := range msgType {
				if a == rune(t) {
					parseFlag[t] = true
					found = true
					break
				}
			}
			if !found {
				fmt.Printf("%c is not a valid message type\n", a)
				fmt.Printf("Valid ITCH v5.0 message types are:\n")
				fmt.Printf("S R H Y L V W K J h A F E C X D U P Q B I N\n")
				os.Exit(1)
			}
		}
	}
	// fmt.Printf("%v\n", parseFlag)

	// os.Args[1]: input file path
	fInput, err := os.Open(os.Args[1])
	check(err)
	defer fInput.Close()

	inputFileName := path.Base(os.Args[1])
	outputBase := strings.Split(inputFileName, ".")[0]

	// os.Args[2]: output folder path
	err = os.MkdirAll(os.Args[2], 0755)
	check(err)

	// total number of all messages
	var total uint64
	// total number of messages for each message type
	totalType := make(map[byte]uint64)
	for _, v := range msgType {
		totalType[v] = 0
	}

	start := time.Now()

	fmt.Printf("=========== Parsing ITCH v5.0 starts ===========\n")
	fmt.Printf("Input file: %s\n", inputFileName)
	fmt.Printf("Output folder: %s\n", os.Args[2])

	// open files only for specified message types
	fOutput := make(map[byte]*os.File)
	var outputFileName string
	for _, v := range msgType {
		if parseFlag[v] {
			if v == 'h' {
				// workaround of the limitation of case-insensitive filesystems
				// both 'H' and 'h' are valid mesaage types
				outputFileName = fmt.Sprintf("%s-halt.csv", outputBase)
			} else {
				outputFileName = fmt.Sprintf("%s-%c.csv", outputBase, v)
			}
			fmt.Printf("Output file: %s\n", outputFileName)
			fOutput[v], err = os.Create(path.Join(os.Args[2], outputFileName))
			check(err)
			defer fOutput[v].Close()
		}
	}

	// first two bytes before each message starts encodes the length of the message
	msgHeader := make([]byte, 2)
	var msgLength uint16
	var rMsgHeader *bytes.Reader
	for {
		count, _ := fInput.Read(msgHeader)
		if count < 2 {
			// EOF
			fmt.Printf("=========== Parsing ITCH v5.0 ends   ===========\n")
			break
		}

		rMsgHeader = bytes.NewReader(msgHeader)
		err := binary.Read(rMsgHeader, binary.BigEndian, &msgLength)
		check(err)

		// message buffer
		message := make([]byte, msgLength)
		count, _ = fInput.Read(message)
		if count < int(msgLength) {
			panic("Error reading input file")
		}

		t := message[0]
		switch t {
		case 'S':
			if parseFlag['S'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				eventCode := message[11]
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%c\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					eventCode)
				check(err)
			}
		case 'R':
			if parseFlag['R'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				stock := strings.TrimSpace(string(message[11:19]))
				marketCategory := message[19]
				financialStatusIndicator := message[20]
				roundLotSize := binary.BigEndian.Uint32(message[21:25])
				roundLotsOnly := message[25]
				issueClassification := message[26]
				issueSubType := string(message[27:29])
				authenticity := message[29]
				shortSaleThresholdIndicator := message[30]
				ipoFlag := message[31]
				luldReferencePriceTier := message[32]
				etpFlag := message[33]
				etpLeverageFactor := binary.BigEndian.Uint32(message[34:38])
				inverseIndicator := message[38]
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%s,%c,%c,%d,%c,%c,%s,%c,%c,%c,%c,%c,%d,%c\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					stock, marketCategory, financialStatusIndicator,
					roundLotSize, roundLotsOnly, issueClassification,
					issueSubType, authenticity,
					shortSaleThresholdIndicator, ipoFlag,
					luldReferencePriceTier, etpFlag, etpLeverageFactor,
					inverseIndicator)
				check(err)
			}
		case 'H':
			if parseFlag['H'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				stock := strings.TrimSpace(string(message[11:19]))
				tradingState := message[19]
				reserved := message[20]
				reason := string(message[21:25])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%s,%c,%c,%s\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					stock, tradingState, reserved, reason)
				check(err)
			}
		case 'Y':
			if parseFlag['Y'] {
				locateCode := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				stock := strings.TrimSpace(string(message[11:19]))
				regSHOAction := message[19]
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%s,%c\n",
					t, locateCode, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					stock, regSHOAction)
				check(err)
			}
		case 'L':
			if parseFlag['L'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				mpid := string(message[11:15])
				stock := strings.TrimSpace(string(message[15:23]))
				primaryMarketMaker := message[23]
				marketMakerMode := message[24]
				marketParticipantState := message[25]
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%s,%s,%c,%c,%c\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					mpid, stock, primaryMarketMaker,
					marketMakerMode, marketParticipantState)
				check(err)
			}
		case 'V':
			if parseFlag['V'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				level1 := binary.BigEndian.Uint64(message[11:19])
				level2 := binary.BigEndian.Uint64(message[19:27])
				level3 := binary.BigEndian.Uint64(message[27:35])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d.%08d,%d.%08d,%d.%08d\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					level1/100000000, level1%100000000,
					level2/100000000, level2%100000000,
					level3/100000000, level3%100000000)
				check(err)
			}
		case 'W':
			if parseFlag['W'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				breachedLevel := message[11]
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%c\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					breachedLevel)
				check(err)
			}
		case 'K':
			if parseFlag['K'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				stock := strings.TrimSpace(string(message[11:19]))
				ipoQuotationReleaseTime := binary.BigEndian.Uint32(message[19:23])
				ipoQuotationReleaseQualifier := message[23]
				ipoPrice := binary.BigEndian.Uint32(message[24:28])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%s,%d,%c,%d.%04d\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					stock, ipoQuotationReleaseTime,
					ipoQuotationReleaseQualifier,
					ipoPrice/10000, ipoPrice%10000)
				check(err)
			}
		case 'J':
			if parseFlag['J'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				stock := strings.TrimSpace(string(message[11:19]))
				acrp := binary.BigEndian.Uint32(message[19:23])
				uacp := binary.BigEndian.Uint32(message[23:27])
				lacp := binary.BigEndian.Uint32(message[27:31])
				auctionCollarExtension := binary.BigEndian.Uint32(message[31:35])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%s,%d.%04d,%d.%04d,%d.%04d,%d\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000, stock,
					acrp/10000, acrp%10000, uacp/10000, uacp%10000,
					lacp/10000, lacp%10000, auctionCollarExtension)
				check(err)
			}
		case 'h':
			if parseFlag['h'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				stock := strings.TrimSpace(string(message[11:19]))
				marketCode := message[19]
				operationalHaltAction := message[20]
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%s,%c,%c\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					stock, marketCode, operationalHaltAction)
				check(err)
			}
		case 'A':
			if parseFlag['A'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				orderReferenceNumber := binary.BigEndian.Uint64(message[11:19])
				buySellIndicator := message[19]
				shares := binary.BigEndian.Uint32(message[20:24])
				stock := strings.TrimSpace(string(message[24:32]))
				price := binary.BigEndian.Uint32(message[32:36])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d,%c,%d,%s,%d.%04d\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					orderReferenceNumber, buySellIndicator, shares, stock,
					price/10000, price%10000)
				check(err)
			}
		case 'F':
			if parseFlag['F'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				orderReferenceNumber := binary.BigEndian.Uint64(message[11:19])
				buySellIndicator := message[19]
				shares := binary.BigEndian.Uint32(message[20:24])
				stock := strings.TrimSpace(string(message[24:32]))
				price := binary.BigEndian.Uint32(message[32:36])
				attribution := string(message[36:40])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d,%c,%d,%s,%d.%04d,%s\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					orderReferenceNumber, buySellIndicator, shares, stock,
					price/10000, price%10000, attribution)
				check(err)
			}
		case 'E':
			if parseFlag['E'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				orderReferenceNumber := binary.BigEndian.Uint64(message[11:19])
				executedShares := binary.BigEndian.Uint32(message[19:23])
				matchNumber := binary.BigEndian.Uint64(message[23:31])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d,%d,%d\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					orderReferenceNumber, executedShares, matchNumber)
				check(err)
			}
		case 'C':
			if parseFlag['C'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				orderReferenceNumber := binary.BigEndian.Uint64(message[11:19])
				executedShares := binary.BigEndian.Uint32(message[19:23])
				matchNumber := binary.BigEndian.Uint64(message[23:31])
				printable := message[31]
				executionPrice := binary.BigEndian.Uint32(message[32:36])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d,%d,%d,%c,%d.%04d\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					orderReferenceNumber, executedShares,
					matchNumber, printable,
					executionPrice/10000, executionPrice%10000)
				check(err)
			}
		case 'X':
			if parseFlag['X'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				orderReferenceNumber := binary.BigEndian.Uint64(message[11:19])
				cancelledShares := binary.BigEndian.Uint32(message[19:23])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d,%d\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					orderReferenceNumber, cancelledShares)
				check(err)
			}
		case 'D':
			if parseFlag['D'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				orderReferenceNumber := binary.BigEndian.Uint64(message[11:19])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					orderReferenceNumber)
				check(err)
			}
		case 'U':
			if parseFlag['U'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				originalOrderReferenceNumber := binary.BigEndian.Uint64(message[11:19])
				newOrderReferenceNumber := binary.BigEndian.Uint64(message[19:27])
				shares := binary.BigEndian.Uint32(message[27:31])
				price := binary.BigEndian.Uint32(message[31:35])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d,%d,%d,%d.%04d\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					originalOrderReferenceNumber, newOrderReferenceNumber,
					shares, price/10000, price%10000)
				check(err)
			}
		case 'P':
			if parseFlag['P'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				orderReferenceNumber := binary.BigEndian.Uint64(message[11:19])
				buySellIndicator := message[19]
				shares := binary.BigEndian.Uint32(message[20:24])
				stock := strings.TrimSpace(string(message[24:32]))
				price := binary.BigEndian.Uint32(message[32:36])
				matchNumber := binary.BigEndian.Uint64(message[36:44])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d,%c,%d,%s,%d.%04d,%d\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					orderReferenceNumber, buySellIndicator, shares, stock,
					price/10000, price%10000, matchNumber)
				check(err)
			}
		case 'Q':
			if parseFlag['Q'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				shares := binary.BigEndian.Uint64(message[11:19])
				stock := strings.TrimSpace(string(message[19:27]))
				crossPrice := binary.BigEndian.Uint32(message[27:31])
				matchNumber := binary.BigEndian.Uint64(message[31:39])
				crossType := message[39]
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d,%s,%d.%04d,%d,%c\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					shares, stock,
					crossPrice/10000, crossPrice%10000,
					matchNumber, crossType)
				check(err)
			}
		case 'B':
			if parseFlag['B'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				matchNumber := binary.BigEndian.Uint64(message[11:19])
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					matchNumber)
				check(err)
			}
		case 'I':
			if parseFlag['I'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				pairedShares := binary.BigEndian.Uint64(message[11:19])
				imbalanceShares := binary.BigEndian.Uint64(message[19:27])
				imbalanceDirection := message[27]
				stock := strings.TrimSpace(string(message[28:36]))
				farPrice := binary.BigEndian.Uint32(message[36:40])
				nearPrice := binary.BigEndian.Uint32(message[40:44])
				currentRefPrice := binary.BigEndian.Uint32(message[44:48])
				crossType := message[48]
				priceVariationIndicator := message[49]
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%d,%d,%c,%s,%d.%04d,%d.%04d,%d.%04d,%c,%c\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					pairedShares, imbalanceShares, imbalanceDirection, stock,
					farPrice/10000, farPrice%10000,
					nearPrice/10000, nearPrice%10000,
					currentRefPrice/10000, currentRefPrice%10000,
					crossType, priceVariationIndicator)
				check(err)
			}
		case 'N':
			if parseFlag['N'] {
				stockLocate := binary.BigEndian.Uint16(message[1:3])
				trackingNumber := binary.BigEndian.Uint16(message[3:5])
				message[3] = 0
				message[4] = 0
				timestamp := binary.BigEndian.Uint64(message[3:11])
				stock := strings.TrimSpace(string(message[11:19]))
				interestFlag := message[19]
				_, err = fmt.Fprintf(fOutput[t],
					"%c,%d,%d,%d.%09d,%s,%c\n",
					t, stockLocate, trackingNumber,
					timestamp/1000000000, timestamp%1000000000,
					stock, interestFlag)
				check(err)
			}
		default:
			panic(fmt.Sprintf("How could it be? An unrecognized type: %c! I am freaking out...", t))
		}
		if parseFlag[t] {
			total++
			totalType[t]++
		}
	}

	fmt.Printf("Total number of all messages parsed: %d\n", total)
	for t, n := range totalType {
		fmt.Printf("Total number of %c messages parsed: %d\n", t, n)
	}
	fmt.Printf("Time spent: %d seconds\n", time.Since(start)/1000000000)
}
