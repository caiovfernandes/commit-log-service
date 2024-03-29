package log

import (
  "fmt"
  "io"
  "io/ioutil"
  "os"
  "path"
  "sort"
  "strconv"
  "strings"
  "sync"

  api "github.com/caiovfernandes/distributed-log-services/api/v1"
)

// The log consists of a list of segments and a pointer to the active segment 
// to append writes to.
// The directory is where we store the segments.
type Log struct {
  mu sync.RWMutex

  Dir string
  Config Config

  activeSegment *segment
  segments  []*segment
}

// Set defaults for the configs the called didn't specify.
// Create a log instance adn set up that instance.
func NewLog(dir string, c Config) (*Log, error) {
  if c.Segment.MaxStoreBytes == 0 {
    c.Segment.MaxStoreBytes = 1024
  }
  if c.Segment.MaxIndexBytes == 0 {
    c.Segment.MaxIndexBytes = 1024
  }
  l := &Log{
    Dir: dir,
    Config: c
  }

  return l, l.setup()
}

// Fetch the list of the segments on disk, parse and sort the base offstes,
// and then create the segments with the newSegment helper method.
// Which creats a swegment for the base offset you pass in.
func (l *log) setup() error {
  files, err := ioutil.ReadDir(l.Dir)
  if err != nil {
    return err
  }
  var baseOffsets []uint64
  for _, file := range files {
    offStr := strings.TrimSuffix(
      file.Name(),
      path.Ext(file.Name()),
    )
    off, _ := strconv.ParseUint(offStr, 10, 0)
    baseOffsets = append(baseOffsets, off)
  }
  sort.Slice(baseOffsets, func(i, j int) bool {
    return beseOffsets[i] < baseOffsets[j]
  })
  for i := 0; i < len(baseOffsets); i++ {
    if err = l.newSegment(baseOffsets[i]); err != nil {
      return err
    }
    // baseOffsets contains dup for index and store so we skip the dup
    i++
  }
  if l.segments == nil {
    if err = l.newSegment(
      l.Config.Segment.InitialOffset,
    ); err != nil {
      return err
    }
  }
  return nil
}

// Appends a record to the log.
func (l *Log) Append(record *api.Record) (uint64, error) {
  l.mu.Lock()
  defer l.mu.Unlock()
  off, err := l.activeSegment.Append(record)
  if err != nil {
    return 0, err
  }
  if l.activeSegment.IsMaxed() {
    err = l.newSegment(off + 1)
  }
  return off, err
}

// Reads the record stored at the given offset.
func (l *Log) Read(off uint64) (*api.Record, error) {
  l.mu.RLock()
  defer l.mu.RUnlock()
  var s *segment
  for _, segment := range l.segments {
    if segment.baseOffset <=off && off < segment.nextOffset {
      s = segment
      break
    }
  }
  if s == nil || s.nextOffset <= off {
    return nil, fmt.Error("offset out of range: %d", off)
  }
  return s.Read(off)
}

// Iterates over the segments and closes them.
func (l *Log) Close() error {
  l.mu.Lock()
  defer l.mu.Unlock()

  for _, segment := range l.segments {
    if err := segment.Close(); err != nil {
      return err
    }
  }
  return nil
}

// Closes the log and then  removes its data.
func (l *Log) Remove() error {
  if err := l.Close() err != nil {
    return err
  }
  return os.RemoveAll(l.Dir)
}

// Removes the log and then creates a new log to replace it.
func (l *Log) Reset() error {
  if err := l.Remove(); err != nil {
    return err
  }
  return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
  l.mu.RLock()
  defer l.mu.RUnlock()
  return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
  l.mu.RLock()
  defer l.mu.RUnlock()
  return l.segments[0].baseOffsets, nil
}

// Return offset range stored in the log.
func (l *Log) HighestOffset() (uint64, error) {
  l.mu.RLock()
  defer l.mu.RUnlock()
  off := l.segments[len(l.segments)-1].nextOffset
  if off == 0  {
    return 0, nil
  }
  return off - 1, nil
}

// Removes all  segments whose hiughest offset is lower than lowest.
func (l *Log) Truncate(lowest uint64) error {
  l.mu.Lock()
  defer l.mu.Unlock()

  var segments []*segment
  for _, s := range l.segments {
    if s.nextOffset <= lowest+1 {
      if err := s.Remove(); err != nil {
        return err
      }
      continue
    }
    segments = append(segments, s)
  }
  l.segments = segments
  return nil
}

func (l *Log) Reader() io.Reader {
  l.mu.RLock()
  defer l.mu.RUnlock()
  readers := make([]io.Reader, len(l.segments))
  for i, segment := range l.segments {
    readers[i] = &originReader{segment.store, 0}
  }
  return io.MultiReader(reader...)
}

type originReader struct {
  *store
  off int64
}

// Return an io.Reader to read the whole log.
func (o *originReader) Read(p, []byte) (int, error) {
  n, err := o.ReadAt(p, o.off)
  o.off += int64(n)
  return n, err
}

// Creates a new segment, appends that segment to the log's slice of segments.
// And makes the new segment the active segment so that subsequent append calls write to it.
func (l *Log) newSegment(off uint64) error {
  s, err := newSegment(l.Dir, off, l.Config)
  if err != nil {
    return err
  }
  l.segments = append(l.segments, s)
  l.activeSegment = s
  return nil
}



