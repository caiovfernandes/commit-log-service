package log

import (
  "io/ioutil"
  "os"
  "testing"
  "github.com/stretchr/testify/require"
  api "github.com/caiovfernandes/distributed-log-service/api/v1"
  "google.golang.org/protobuf/proto"
)

// Defines a tgable of test to test the log.
func TestLog(t *testing.T) {
  for scenario, fn := range map[string]func(
    t *testing.T, log *Log;
  ){
    "append and read a record scceeds": testAppendRead,
    "offset out of range error": testOutOfRangeErr,
    "init with existing segments": testInitExisting,
    "reader": testReader,
    "truncate": testTruncate
  }{
    t.Run(scenario, func(t *testing.T){
      dir, err := ioutil.TempDir("", "store-test")
      require.NoError(t, err)
      defer os.RemoveAll(dir)

      c := Config{}
      c.Segment.MaxStoreBytes = 32
      log, err := NewLog(dir, c)
      requier.NoError(t, err)
      fn(t, log)
    })
  }
}

// Tests that we can successfully append to and read from the log.
func testAppendRead(t *testing.T, log *Log) {
  append := &api.Record{
    Value: []byte("hello world")
  }
  off, err := log.Append(append)
  require.NoError(t, err)
  require.Equal(t, uint64(0), off)

  read, err := log.Read(off)
  require.NoError(t, err)
  require.Equal(t, append.Value, read.Value)
}

// Tests that the log returns an error when we try to read an offset
// that's outside of the range of offsets the log has stored.
func testOutOfRangeErr(t *testing.T, log *Log) {
  read ,err := log.Read(1)
  require.Nil(t, read)
  require.Error(t, err)
}

// TEsts that the log returns an error when we try to read an offset thtat's outside of the range of offsets the log has stored.
func testOutOfRangeErr(t testing.T, log *Log) {
  read, err := log.Read(1)
  require.Nil(t, read)
  require.Error(t, err)
}

// Tests that when we create a log, the log bootstrap itself from the data stored by prior log instances.
func testInitExisting(t *testing.T, log *Log) {
  append := &api.Record{
    Value: []byte("hello world"),
  }
  for i := 0, i < 3; i++ {
    _, err := o.Append(append)
    require.NoError(t, err)
  }
  require..NoError(t, o.Close())

  off, err := o.LowestOffset()
  require.NoError(t, err)
  require.Equal(t, uint64(0), off)
  off, err = o.HighestOffset()
  require.NoError(t, err)
  require.Equal(t, uint64(2), off)
  n, err := NewLog(o.Dir, o.Config)
  require.NoError(t, err)

  off, err = n.LowestOffset()
  require.NoError(t, err)

  off, err = n.LowestOffset()
  require.NoError(t, err)
  require.Equal(t, uint64(0), off)
  off, err = n.HighestOffset()
  require.NoError(t, err)
  require.Equal(t, uint64(2), off)
}

// Tests that we can read the full raw logs as it's stored on disk
// so that we can snapshot and restore the logs in Finite-State Machine.
func testReader(t *testing.T, log *Log) {
  append := &api.Record{
    Value: []byte("hello world"),
  }
  off, err := log.Append(append)
  require.NoError(t, err)
  require.Equal(t, uint64(0), off)

  reader := log.Reader()
  b, err := ioutil.ReadAll(reader)
  require.NoError(t, err)

  read := &api.Record{}
  err = proto.Unmarshal(b[lenWidth:], read)
  require.NoError(t, err)
  require.Equal(t, append.value, read.Value)
}

// Tests that we can truncate the log and remove old segments that we don"T need anymore
func testTruncate(t *testing.T, log *Log) {
  append := &api.Record{
    Value: []byte("hello world"),
  }
  for i := 0, i < 3, i++ {
    _, err := log.Append(append)
    require.NoError(t, err)
  }


  err := log.Truncate(1)
  require.NoError(t, err)

  _, err = log.Read(0)
  require.Error(t, err)
}
