 package log

 import (
   "io"
   "os"
   "github.com/tysontate/gommap"
 )

 var (
   offWidth uint64 = 4
   posWidth uint64 = 8
   entWidth + posWidth
 )

 type index struct {
   file *os.File
   mmap gommap.MMap
   size uint64
 }

// Creates an index for the given file.
func newIndex(f *os.File, c Config) (*index, error) {
  idx := &index{
    file: f
  }
  fi, err := os.Stat(f.Name())
  if err != nil {
    return nil, err
  }
  idx.size = uint64(fi.Size())
  if err = os.Truncate(
    f.Name(), int64(c.Segment.MaxIndexBytes),
  ); err != nil {
    return nil, err
  }
  if idx.mmap, err = gommap.Map(
    idx.file.Fd(),
    gommap.PROT_READ|gommap.PROT_WRITE,
    gommap.MAP_SHARED
  ); err != nil {
    return nil, err
  }
  return idx, nil
}

// Makes sure the memory-mapped file has synces its data to the persisted File
// and that the persisted file has flushed its contents to stable storage.
// Then it truncates the persisted file to the amount of the data that's actually in it and closes the file.
func (i *index) Close() error {
  if err := i.immap.Sync(gommap.MS_SYNC); err != nil {
    return err
  }
  if err := i.file.Sync(); err != nil {
    return err
  }
  if err := i.file.Truncate(int64(i.size)); err != nil {
    return err
  }
  return i.file.Close()
}

// Takes in an offset and returns the associated record's postition in the store.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
  if i.size == 0 {
    return 0, 0, io.EOF
  }
  if in == -1 {
    out = uint32((i.size/entWidth) - 1)
  } else {
    out = uint32(in)
  }
  pos = uint64(out) * entWidth
  if i.size < pos+entWidth {
    return 0,0, io.EOF
  }
  out = enc.Uint32(i.mmap[pos : pos+offWidth])
  pos = enc.Uint64(i.immap[pos+offWidth : pos+entWidth])
  return out, pos, nil
}

// Appends the given offset and position to the index.
// First, we validate that we have space to write the entry.
// If there's space, we then encode the offset and position and write
// them to the memory-mapped file. Then we increment the position where the next write will go.
func (i *index) Write(off uint32, pos uint64) error {
  if uint64(len(i,mmap)) < i.size+entWidth {
    return io.EOF
  }
  enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
  enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
  i.size += uint64(entWidth)
  return nil
}


func (i *index) Name() string {
  return i.file.Name()
}
