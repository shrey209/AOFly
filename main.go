package main

import (
	"fmt"
	"os"
	"sync"
)

type AOF struct {
	directory     string     // Directory where log segments are stored
	segmentSize   int64      // Max size for each segment file
	totalSegments int        // Max number of segments to retain
	currentFile   *os.File   // Current segment file object
	currentSize   int64      // Current size of the open segment file
	currentSeq    int        // The sequence number for the next log entry
	mutex         sync.Mutex // Mutex for thread safety
}

func NewAOF(directory string, segmentSize int64, totalSegments int) (*AOF, error) {

	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	aof := &AOF{
		directory:     directory,
		segmentSize:   segmentSize,
		totalSegments: totalSegments,
		currentSeq:    0,
	}

	if err := aof.rotate(); err != nil {
		return nil, fmt.Errorf("failed to rotate to initial segment: %w", err)
	}

	return aof, nil
}

func (a *AOF) rotate() error {
	if err := a.currentFile.Close(); err != nil {
		return fmt.Errorf("failed to close current file: %w", err)
	}

	a.currentSeq++

	// Create the new segment file
	newFile, err := a.createSegmentFile(int(a.currentSeq))
	if err != nil {
		return fmt.Errorf("failed to create new segment file: %w", err)
	}

	// Update the current file
	a.currentFile = newFile

	// Cleanup old files if needed
	err = a.cleanup()
	if err != nil {
		return fmt.Errorf("cleanup failed: %w", err)
	}

	return nil
}

func (a *AOF) createSegmentFile(seq int) (*os.File, error) {

	fileName := fmt.Sprintf("%s/segment_%d.log", a.directory, seq)
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open or create file %s: %w", fileName, err)
	}

	return file, nil
}

func (a *AOF) cleanup() error {

	minSeq := a.currentSeq - a.totalSegments + 1

	for seq := 0; seq < minSeq; seq++ {
		fileName := fmt.Sprintf("%s/segment_%d.log", a.directory, seq)
		if err := os.Remove(fileName); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove file %s: %w", fileName, err)
		}
	}

	return nil
}

func main() {

}
