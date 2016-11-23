/*===== Copyright 2016, Webpraxis Consulting Ltd. - ALL RIGHTS RESERVED - Email: webpraxis@gmail.com ===========================
 * Package:
 *     mergesort
 * Overview:
 *     package for a stable, multi-index, partially concurrent hybrid merge sort of a text file.
 * Function:
 *     Sort(inFile, outFile string, sortAsc bool, usingFields, sep string, keysPerSort int, verbose bool)
 *         Does a stable, multi-index, partially concurrent hybrid merge sort of a text file.
 * History:
 *     v1.0.0 - November 19, 2016 - Original release.
 *============================================================================================================================*/
package mergesort

import(
    "bufio"
    "fmt"
    "io"
    "io/ioutil"
    "log"
    "math"
    "os"
    "path/filepath"
    "runtime"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)
//Exported ---------------------------------------------------------------------------------------------------------------------
func Sort(inFile, outFile string, sortAsc bool, usingFields, sep string, keysPerSort int, verbose bool) {
/*         Purpose : Does a stable, multi-index, partially concurrent hybrid merge sort of a text file.
 *       Arguments : inFile      = path of the file with the data to be sorted.
 *                   outFile     = path of the file for the sorted data.
 *                   sortAsc     = boolean flag for requesting an ascending alphanumeric sort. If false, sorting will be in
 *                                 descending order.
 *                   usingFields = CSV of field numbers to use as indexes, ordered as primary, secondary, etc., with the
 *                                 first field referenced as 1.
 *                   sep         = the field separator.
 *                   keysPerSort = the number of elements for in-place sorting of the initial composite-key files.
 *                   verbose     = boolean flag for verbose mode. If true, the main execution stages will be echoed to Stdout.
 *         Returns : None.
 * Externals -  In : _sync4Merge, keyParams
 * Externals - Out : None.
 *       Functions : createFile, createTempFile, halt, makeCompositeKeyFn, merge, openFile, readString, resetReader, seekFile,
                     updateProgressBar
 *         Remarks : The temporary files are prefixed as "keys_" and wiil be stored on the temporary directory reported by the
 *                   OS. They will be deleted as soon as they have been processed.
 *         History : v1.0.0 - November 19, 2016 - Original release.
 */
    if inFile      == "" { halt("the input file was not specified") }
    if outFile     == "" { halt("the output file was not specified") }
    if usingFields == "" { halt("the index fields columns were not specified") }
    if keysPerSort == 0  { halt("the number of keys for in-place sorting was not specified") }
    fi, err := os.Stat(inFile)
    if err != nil || fi.Size() == 0 { halt("the input file cannot be located or is empty") }

    var(
        start                 = time.Now()                        //record start of execution
        keys sort.StringSlice = []string{}                        //data keys
        recordStart           int64                               //data-record offset relative to the origin of the file
        tempDir               = filepath.ToSlash(os.TempDir())    //temporary directory for the merged files
        pattern4merged        = fmt.Sprintf("%s/keys_*", tempDir) //glob pattern for the temporary merged files
        todo                  = []string{}                        //key files to be processed

        chan4command          = make(chan string,    1)           //merge channel for signalling
        chan4tasks            = make(chan [2]string, 1)           //merge channel for key files to merge
    )

    if verbose { fmt.Println("func Sort - temporary directory =", tempDir) }
    //Launch coroutine for merging the composite-key files
    _sync4Merge.Add(1)
    go merge(sortAsc, chan4command, chan4tasks, verbose)
    //Get the number of fields from the first record
    fhIn, _   := openFile(inFile)
    defer fhIn.Close()
    readerIn  := bufio.NewReader(fhIn)
    record, _ := readString(readerIn)
    numFields := len(strings.Split(record, sep))
    if verbose { fmt.Println("func Sort - number of fields =", numFields) }
    //Get the field widths
    widths := make([]float64, numFields)
    errIn  := resetReader(fhIn, readerIn)
    for errIn != io.EOF {
        record, errIn = readString(readerIn)
        record        = strings.Trim(record, " \r\n")
        for k, v := range strings.Split(record, sep) {
            widths[k] = math.Max(widths[k], float64(len(v)))
        }
    }
    if verbose {
        fmt.Println("func Sort - field widths:")
        for k, v := range widths {
            fmt.Println("       column #", k + 1, ":", v)
        }
    }
    //Define the field formats for the composite keys
    keySpecs := []keyParams{}
    for _, v := range strings.Split(usingFields, ",") {
        colIdx, err := strconv.Atoi(v); colIdx--
        if err != nil { halt("the specification of the sort columns is syntactically incorrect") }
        keyFormat := fmt.Sprintf("%%%vs", widths[colIdx])
        keySpecs   = append(keySpecs, keyParams{COLIDX:colIdx, FORMAT:keyFormat})
    }
    //Create files of composite keys with seek pointers on the temp directory and enqueue merge tasks
    numKeys, numRecs := 0, 0
    compositeKeyFn   := makeCompositeKeyFn(sep, keySpecs, len(strconv.FormatInt(fi.Size(), 10)))
    errIn             = resetReader(fhIn, readerIn)
    for errIn != io.EOF {
        record, errIn  = readString(readerIn)
        recordLen     := len(record)
        numRecs++
        if record = strings.Trim(record, " \r\n"); len(record) > 0 {
            keys = append(keys, compositeKeyFn(record, recordStart))
            numKeys++
        }
        recordStart += int64(recordLen)
        if len(keys) > 0 && (len(keys) == keysPerSort || errIn == io.EOF) {
            fhKeys, tempFile := createTempFile()
            if sortAsc { keys.Sort() } else { sort.Sort(sort.Reverse(keys[:])) }
            for _, v := range keys {
                fmt.Fprintln(fhKeys, v)
            }
            if err := fhKeys.Sync();  err != nil { halt("fhKeys.Sync - " + err.Error()) }
            if err := fhKeys.Close(); err != nil { halt("fhKeys.Close - " + err.Error()) }
            if verbose { fmt.Println("func Sort - created", filepath.Base(tempFile)) }
            todo = append(todo, tempFile)
            if len(todo) == 2 {
                chan4tasks<- [2]string{todo[0], todo[1]}
                todo = nil
            }
            keys = nil
        }
    }
    if verbose { fmt.Println("func Sort - created", numKeys, "keys for", numRecs, "data records") }
    //Get list of merged files and enqueue further merge tasks until only one file remaining
    chan4command<- "e-o-t"
    if verbose { fmt.Println("func Sort - sent end-of-tasks signal") }
    _sync4Merge.Wait()
    todo, _ = filepath.Glob(pattern4merged)
    for len(todo) > 1 {
        if verbose { fmt.Printf("func Sort - %d files pending\n", len(todo)) }
        _sync4Merge.Add(1)
        for len(todo) > 1 {
            chan4tasks<- [2]string{todo[0], todo[1]}
            todo = todo[2:]
        }
        chan4command<- "e-o-t"
        if verbose { fmt.Println("func Sort - sent end-of-tasks signal") }
        _sync4Merge.Wait()
        todo, _ = filepath.Glob(pattern4merged)
    }
    chan4command<- "quit"
    if verbose { fmt.Println("func Sort - sent quit signal") }
    //Read sorted keys & output corresponding data records
    sortedKeysFile := todo[0]                  //open sorted keys file for read
    fhKeys, _      := openFile(sortedKeysFile)
    scannerKeys    := bufio.NewScanner(fhKeys)
    fhOut          := createFile(outFile)      //create destination file for sorted data
    numRecs         = 0
    for scannerKeys.Scan() {
        readerIn.Discard(readerIn.Buffered())
        seekFile(fhIn, (strings.Split(scannerKeys.Text(), _asciiGS))[1])
        record, _ = readString(readerIn)
        fmt.Fprint(fhOut, record)
        if verbose {
            numRecs++
            updateProgressBar("func Sort - creating outFile", numRecs, numKeys)
        }
    }
    if err := fhOut.Sync();  err != nil { halt("fhOut.Sync - " + err.Error()) }
    if err := fhOut.Close(); err != nil { halt("fhOut.Close - " + err.Error()) }
    fhIn.Close()
    fhKeys.Close()
    os.Remove(sortedKeysFile)
    if verbose { fmt.Println("func Sort - created", outFile, "in", time.Since(start)) }
    return
} //end func Sort
//Private ----------------------------------------------------------------------------------------------------------------------
type keyParams struct {
    COLIDX int
    FORMAT string
}
const _progressBarLen = 50
var(
    _asciiGS    = fmt.Sprintf("%c", 29) //ascii character for group separator
    _sync4Merge sync.WaitGroup
)
////Composite key
func makeCompositeKeyFn(fieldSep string, sortSpecs []keyParams, seekLen int) func(record string, recordStart int64) string {
    var(
        sep       = fieldSep
        keySpecs  = sortSpecs
        keyFormat = fmt.Sprintf("%%s%%s%%%dv", seekLen)
    )
    return func(record string, recordStart int64) string {
            var(
                key    string
                fields = strings.Split(record, sep)
            )
            for _,v := range keySpecs {
                key += fmt.Sprintf(v.FORMAT, fields[v.COLIDX])
            }
            return fmt.Sprintf(keyFormat, key, _asciiGS, recordStart)
           }
} //end func makeCompositeKeyFn
////Merge coroutine
func merge(sortAsc bool, chan4command <-chan string, chan4tasks <-chan [2]string, verbose bool) {
    var(
        key1, key2 = "", ""
        eot        bool
    )
    jobLoop: for {
        select {
            case command := <-chan4command:
                eot = (command == "e-o-t")
                if command == "quit" { break jobLoop }
            case tasks := <-chan4tasks:
                sourceKeys1, sourceKeys2 := tasks[0], tasks[1]
                fhKeys1, errKeys1        := openFile(sourceKeys1)           //open 1st keys file for read
                reader1                  := bufio.NewReader(fhKeys1)
                fhKeys2, errKeys2        := openFile(sourceKeys2)           //open 2nd keys file for read
                reader2                  := bufio.NewReader(fhKeys2)
                writer, tempFile         := createTempFile()                //create temp file for the merged keys
                //Process the two key files until one of them runs out of records
                for (key1 != "" || errKeys1 != io.EOF) && (key2 != "" || errKeys2 != io.EOF) {
                    if key1 == "" { key1, errKeys1 = readString(reader1) }  //get the next key in 1st file
                    if key2 == "" { key2, errKeys2 = readString(reader2) }  //get the next key in 2nd file
                    if sortAsc {                                            //sort ascending
                        if key1 < key2  {                                   // case of 1st key less than 2nd one
                            fmt.Fprint(writer, key1)                        //  add key from 1st file to new temp key file
                            key1 = ""                                       //  clear the current key from 1st file
                        } else {                                            // case of 2nd key less than or equal to 1st one
                            fmt.Fprint(writer, key2)                        //  add key from 2nd file to new temp key file
                            key2 = ""                                       //  clear the current key from 2nd file
                        }                                                   // end case of keys ordering
                    } else {                                                //else sort descending
                        if key1 > key2  {                                   // case of 1st key greater than 2nd one
                            fmt.Fprint(writer, key1)                        //  add key from 1st file to new temp key file
                            key1 = ""                                       //  clear the current key from 1st file
                        } else {                                            // case of 2nd key greater than or equal to 1st one
                            fmt.Fprint(writer, key2)                        //  add key from 2nd file to new temp key file
                            key2 = ""                                       //  clear the current key from 2nd file
                        }                                                   // end case of keys ordering
                    }                                                       //end if-else
                }
                //Save the remaining keys,if any, for the next pass
                if key1 != "" || errKeys1 != io.EOF {                       //if the 1st file has some unprocessed keys
                    if key1 != "" { fmt.Fprint(writer, key1) }              // add any unprocessed read key to new temp file
                    for errKeys1 != io.EOF {                                // add any unread keys to new temp file
                        key1, errKeys1 = readString(reader1)
                        fmt.Fprint(writer, key1)
                    }
                } else {                                                    //else the 2nd file has some unprocessed keys
                    if key2 != "" { fmt.Fprint(writer, key2) }              // add any unprocessed read key to new temp file
                    for errKeys2 != io.EOF {                                // add any unread keys to new temp file
                        key2, errKeys2 = readString(reader2)
                        fmt.Fprint(writer, key2)
                    }
                }
                fhKeys1.Close()
                fhKeys2.Close()
                os.Remove(sourceKeys1)
                os.Remove(sourceKeys2)
                if err := writer.Sync();  err != nil { halt("writer.Sync - " + err.Error()) }
                if err := writer.Close(); err != nil { halt("writer.Close - " + err.Error()) }
                if verbose { fmt.Println("\tfunc merge - merged", filepath.Base(sourceKeys1), "and", filepath.Base(sourceKeys2),
                                         "to", filepath.Base(tempFile)) }
            default:
                if eot && len(chan4tasks) == 0 {
                    if verbose { fmt.Println("\tfunc merge - all tasks done") }
                    _sync4Merge.Done()
                    eot = false
                }
        }
    }
    return
} // end func merge
////File ops
func createFile(file string) *os.File {
    fh, err := os.Create(file)
    if err != nil { halt("os.Create - " + err.Error()) }
    return fh
} //end func createFile
func createTempFile() (*os.File, string) {
    fh, err := ioutil.TempFile("", "keys_")
    if err != nil { halt("ioutil.TempFile - " + err.Error()) }
    return fh, fh.Name()
} //end func createTempFile
func openFile(file string) (fh *os.File, err error) {
    fh, err = os.Open(file)
    if err != nil { halt("os.Open - " + err.Error()) }
    return
} //end func openFile
func readString(reader *bufio.Reader) (record string, err error) {
    record, err = reader.ReadString('\n')
    if err != nil && err != io.EOF { halt("reader.ReadString - " + err.Error()) }
    return
} //end func readString
func resetReader(fh *os.File, reader *bufio.Reader) (err error) {
    reader.Discard(reader.Buffered())
    _, err = fh.Seek(0, 0)
    if err != nil { halt("fh.Seek - " + err.Error()) }
    return
} //end func resetReader
func seekFile(fh *os.File, offsetStr string) {
    offset, err := strconv.ParseInt(strings.TrimLeft(offsetStr, " "), 10, 64)
    if err != nil { halt("strconv.ParseInt - " + err.Error()) }
    _, err = fh.Seek(offset, 0)
    if err != nil { halt("fh.Seek - " + err.Error()) }
    return
} //end func seekFile
////Reporting
func halt(msg string) {
    pc, _, _, ok := runtime.Caller(1)
    details      := runtime.FuncForPC(pc)
    if ok && details != nil {
        log.Fatalln(fmt.Sprintf("\a%s: %s", details.Name(), msg))
    }
    log.Fatalln("\aoctree: FATAL ERROR!")
} //end func halt
func updateProgressBar(title string, current, total int) {
    //code derived from Graham King's post "Pretty command line / console output on Unix in Python and Go Lang"
    //(http://www.darkcoding.net/software/pretty-command-line-console-output-on-unix-in-python-and-go-lang/)
    prefix := fmt.Sprintf("%s: %d / %d ", title, current, total)
    amount := int(0.1 + float32(_progressBarLen) * float32(current) / float32(total))
    remain := _progressBarLen - amount
    bar    := strings.Repeat("\u2588", amount) + strings.Repeat("\u2591", remain)
    os.Stdout.WriteString(prefix + bar + "\r")
    if current == total { os.Stdout.WriteString(strings.Repeat(" ", len(prefix) + _progressBarLen) + "\r") }
    os.Stdout.Sync()
    return
} //end func updateProgressBar
//===== Copyright (c) 2016 Yves Beaudoin - All rights reserved - MIT LICENSE (MIT) - Email: webpraxis@gmail.com ================
//end of Package mergesort
