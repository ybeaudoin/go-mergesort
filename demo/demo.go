package main

import (
    "fmt"
    //"github.com/ybeaudoin/go-mergesort"
    "math/rand"
    "mergesort"
    "os"
    "strings"
    "strconv"
)
func main() {
    const(
        inFile      = "demo.in"
        outFile     = "demo.out"
        numFields   = 5   //20
        numRecords  = 100 //1000000
        fieldWidth  = 5
        sep         = "\t"
        keysPerSort = 10  //1000
        sortAsc     = false
        verbose     = true
    )
    var(
        fieldFormat = "%" + strconv.Itoa(fieldWidth) + "v"
        usingFields = strconv.Itoa(numFields) + ",1"
    )
    //Create some random data with the last field containing numbers only
    fh, err := os.Create(inFile)
    if err != nil { panic(err) }
    for recNum := 1; recNum <= numRecords; recNum++ {
        var fields = [numFields]string{}
        for fieldNum := 0; fieldNum < numFields; fieldNum++ {
            var content string
            numChars := 1 + rand.Intn(fieldWidth)
            for charNo := 1; charNo <= numChars; charNo++ {
                if fieldNum == numFields - 1 {
                    content += fmt.Sprintf("%v", 1 + rand.Intn(9))
                } else {
                    content += fmt.Sprintf("%c", 65 + rand.Intn(58))
                }
            }
            fields[fieldNum] = fmt.Sprintf(fieldFormat, content)
        }
        fmt.Fprintln(fh, strings.Join(fields[:], sep))
    }
    if err := fh.Sync();  err != nil { panic(err) }
    if err := fh.Close(); err != nil { panic(err) }
    //Sort the data with the last field as the primary key and the first as a secondary key
    mergesort.Sort(inFile, outFile, sortAsc, usingFields, sep, keysPerSort, verbose)
}
