package app

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"

	slogmulti "github.com/samber/slog-multi"
)

const (
	outputFile = "s3hashes.csv"
	logFile    = "s3hash.log"
)

// Output is a struct for the output of the hash operation and the size of the
// object in bytes
type Output struct {
	key     string
	hash    []byte
	byteLen uint
}

// Stats is a struct for gathering total number of hashes written to the output
// file, and total byte count processed
type Stats struct {
	countHashesWritten uint
	totalBytesHashed   uint
}

// LogMsg is a struct for sending messages across a channel to a logger
// goroutine
type LogMsg struct {
	msg   string
	level slog.Level
}

func writeOutputFile(outputFilename string, chanOutput <-chan Output, chanStats chan<- Stats, logChan chan<- LogMsg) {
	// open file for writing
	outFh, err := os.OpenFile(outputFilename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		logChan <- LogMsg{
			msg:   fmt.Sprintf("unable to open output file: %v", err.Error()),
			level: slog.LevelError,
		}
	}
	defer outFh.Close()

	totalBytesHashed := uint(0)
	countHashesWritten := uint(0)
	for output := range chanOutput {
		// format output string
		outStr := fmt.Sprintf("%s,\\\\x%x\n", output.key, output.hash)
		_, err := outFh.WriteString(outStr)
		if err != nil {
			logChan <- LogMsg{
				msg:   fmt.Sprintf("error writing to output file: %v", err.Error()),
				level: slog.LevelError,
			}
		} else {
			countHashesWritten += 1
			totalBytesHashed += output.byteLen
		}
	}

	// chanOutput is closed, so we're done. Send final stats over channel
	chanStats <- Stats{countHashesWritten, totalBytesHashed}
}

func streamKeys(keysFile string, chanKeys chan<- string, logChan chan<- LogMsg) {
	defer close(chanKeys)
	// open keysFile for reading
	keysFh, err := os.Open(keysFile)
	if err != nil {
		logChan <- LogMsg{
			msg:   fmt.Sprintf("unable to open keys file: %v", err.Error()),
			level: slog.LevelError,
		}
	}
	defer keysFh.Close()

	// read lines from file
	scanner := bufio.NewScanner(keysFh)
	for scanner.Scan() {
		key := strings.TrimSpace(scanner.Text())
		chanKeys <- key
	}

	if err := scanner.Err(); err != nil {
		logChan <- LogMsg{
			msg:   fmt.Sprintf("error reading keys file: %v", err.Error()),
			level: slog.LevelError,
		}
	} else {
		logChan <- LogMsg{
			msg:   "done reading keys file",
			level: slog.LevelDebug,
		}
	}
}

func fetchAndHash(s3bucket string, chanKeys <-chan string, chanOutput chan<- Output, logChan chan<- LogMsg) {
	// create a new s3agent
	s3agent := NewS3Agent(s3bucket)

	for key := range chanKeys {
		bytes, err := s3agent.GetObjectBytes(key)
		if err != nil {
			logChan <- LogMsg{
				msg:   fmt.Sprintf("unable to fetch s3 object '%v', err: %v", key, err.Error()),
				level: slog.LevelError,
			}
		} else {
			hash := sha256Hash(bytes)
			output := Output{key: key, hash: hash, byteLen: uint(len(bytes))}
			chanOutput <- output
		}
	}
	// chanKeys is closed, so we're done
}

func Run(appName, bucket, keysFile string, numThreads uint16) {
	const logFile = "s3hash.log"
	logFh, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		_ = fmt.Errorf("unable to open log file: %v", err)
	}
	defer logFh.Close()

	logger := slog.New(
		slogmulti.Fanout(
			// Log to file, debug log level
			slog.NewTextHandler(logFh, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}),
			// Log to stdout, info log level
			slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		),
	)

	logger.Info(fmt.Sprintf("%v started, pid: %v, outfile: %v, num_cores: %v",
		appName,
		os.Getpid(),
		outputFile,
		runtime.NumCPU(),
	))

	logger.Debug(fmt.Sprintf("bucket: %v", bucket))
	logger.Debug(fmt.Sprintf("keys-file: %v", keysFile))
	logger.Debug(fmt.Sprintf("num-threads: %v", numThreads))

	var wgOutput sync.WaitGroup
	var wgWorkers sync.WaitGroup
	chanKeys := make(chan string, numThreads)
	chanOutput := make(chan Output)
	statsChan := make(chan Stats, 1)

	var wgLog sync.WaitGroup
	logChan := make(chan LogMsg)
	wgLog.Add(1)
	go func(logger *slog.Logger, logChan <-chan LogMsg) {
		defer wgLog.Done()
		for msg := range logChan {
			logger.Log(context.Background(), msg.level, msg.msg)
		}
	}(logger, logChan)

	// Dispatch output writer task
	wgOutput.Add(1)
	go func(outputFile string, chanOutput <-chan Output, statsChan chan<- Stats, logChan chan<- LogMsg) {
		defer wgOutput.Done()
		writeOutputFile(outputFile, chanOutput, statsChan, logChan)
	}(outputFile, chanOutput, statsChan, logChan)

	// input reader task
	go func(keysFile string, chanKeys chan<- string, logChan chan<- LogMsg) {
		streamKeys(keysFile, chanKeys, logChan)
	}(keysFile, chanKeys, logChan)

	// spawn worker tasks
	for i := 0; i < int(numThreads); i++ {
		wgWorkers.Add(1)
		go func(bucket string, chanKeys <-chan string, chanOutput chan<- Output, logChan chan<- LogMsg) {
			defer wgWorkers.Done()
			fetchAndHash(bucket, chanKeys, chanOutput, logChan)
		}(bucket, chanKeys, chanOutput, logChan)
	}

	// wait for output and workers to finish
	wgWorkers.Wait()
	close(chanOutput)
	wgOutput.Wait()

	// read stats from final stats channel
	stats := <-statsChan

	close(logChan)
	wgLog.Wait()

	// print file output msg w/stats
	logger.Info(
		fmt.Sprintf(
			"%v finished, hashed %v bytes and wrote %v hashes to %v",
			appName,
			stats.totalBytesHashed,
			stats.countHashesWritten,
			outputFile,
		),
	)
}

func sha256Hash(bytes []byte) []byte {
	hasher := sha256.New()
	hasher.Write(bytes)
	return hasher.Sum(nil)
}
