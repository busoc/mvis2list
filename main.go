package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/xml"
	"flag"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var FCC = []byte("MMA ")

const (
	MilFlag   = 0xFFFE
	FileFlag  = 0xFFFF
	LineSize  = 64
	blockSize = (LineSize - 2) * (32 << 10)
)

const (
	counterLimit = 32 << 10
	counterMask  = counterLimit - 1
)

const null byte = 0x00

const (
	Program   = "mvis2list"
	Version   = "0.1.0"
	BuildTime = "2018-12-03 07:50:00"
)

const helpText = `mvis2list transforms MVIS data from hadock archive to MVIS
listing files

Usage: mvis2list [-datadir] [-version] [-keep] [-meta] <list of dat files>

Options:

  -datadir DIR  base directory where listing files will be written
  -keep         keep content of bad files when creating listing
  -meta         create XML metadata file next to listing files
  -zero         use nul byte to fill buffer instead of 0x20
  -list         print the list of blocks
  -report       print a report on available blocks
  -version      print version and exit
  -help         print this text and exit

Examples:

# read dat files from given path and write listing files under /tmp directory
# without creating the metadata
$ mvis2list -datadir /tmp /var/hdk/51/2018/23/30/*dat

# create the list of files to process from a find and write listing files under
# /tmp directory with XML files next to those
$ find /var/hdk/51/2018/*dat -type f -name *dat | mvis2list -datadir /tmp -meta

# same as previous but instead of creating listing in file, write them to stdout
# (meaning of "-" for datadir)
$ find /var/hdk/51/2018/23/30/*dat -type f -name *dat | mvis2list -datadir -
`

func init() {
	log.SetFlags(0)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, helpText)
		os.Exit(2)
	}
}

type coze struct {
	Size    int
	Count   int
	Missing int
	Name    string
}

func main() {
	datadir := flag.String("datadir", "-", "")
	version := flag.Bool("version", false, "")
	keep := flag.Bool("keep", false, "")
	meta := flag.Bool("meta", false, "")
	zero := flag.Bool("zero", false, "")
	list := flag.Bool("list", false, "")
	report := flag.Bool("report", false, "")
	flag.Parse()
	if *version {
		fmt.Fprintf(os.Stderr, "%s-%s (%s)\n", Program, Version, BuildTime)
		os.Exit(2)
	}
	ps := flag.Args()
	if len(ps) == 0 {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			ps = append(ps, s.Text())
		}
		if err := s.Err(); err != nil {
			log.Fatalln(err)
		}
	}

	r, err := NewReader(ps, *keep)
	if err != nil {
		log.Fatalln(err)
	}
	if *list || *report {
		if err := listBlocks(r, *list && !*report); err != nil {
			log.Fatalln(err)
		}
		return
	}
	if err := dumpFiles(r, *datadir, *zero, *meta); err != nil {
		log.Fatalln(err)
	}
}

func listBlocks(r io.Reader, list bool) error {
	var (
		prev    uint16
		missing int
		count   int
		size    int
	)
	body := make([]byte, LineSize)
	for {
		if n, err := io.ReadFull(r, body); err != nil {
			if err == io.EOF {
				break
			}
			return err
		} else {
			size += n
			count++
		}
		s := binary.BigEndian.Uint16(body)
		if s == FileFlag {
			size := binary.BigEndian.Uint32(body[2:])
			name := string(bytes.Trim(body[6:], "\x00"))
			if list {
				fmt.Printf("%s (%d bytes)\n", name, size)
			}
			count--
			continue
		}
		if diff := (s - prev) & counterMask; diff != s && diff > 1 {
			log.Printf("missing blocks: %d (%d - %d)", diff-1, prev, s)
			missing += int(diff - 1)
		}
		prev = s
		if list {
			fmt.Printf("%5d (%04x): %x\n", s, body[:2], body[2:])
		}
	}
	fmt.Printf("%d blocks (%d missing), %dKB\n", count, missing, size>>10)
	return nil
}

func dumpFiles(r *fileReader, datadir string, zero, meta bool) error {
	var (
		curr *mvis
		err  error
	)
	for {
		body := make([]byte, LineSize)
		if _, err := io.ReadFull(r, body); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		sequence := binary.BigEndian.Uint16(body)
		if sequence == FileFlag {
			if curr != nil {
				curr.Close()
				if meta {
					if err := curr.WriteMetadata(); err != nil {
						return err
					}
				}
			}
			size := binary.BigEndian.Uint32(body[2:])
			name := string(bytes.Trim(body[6:], "\x00"))

			log.Printf("==> %s (%d bytes, %d blocks)", name, size, size/(LineSize-2))
			if curr, err = New(filepath.Join(datadir, name), int(size)); err != nil {
				return err
			}
			continue
		}
		if _, err := curr.Write(body); err != nil {
			return err
		}
	}
	if curr != nil {
		curr.Close()
		if meta {
			if err := curr.WriteMetadata(); err != nil {
				return err
			}
		}
	}
	return nil
}

type mvis struct {
	file    *os.File
	writer  io.Writer
	digest  hash.Hash

	Name    string
	Size    int
	Blocks  int
	Bytes   int

	last   uint16
	offset int
}

func New(n string, s int) (*mvis, error) {
	if err := os.MkdirAll(filepath.Dir(n), 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	w, err := os.Create(n)
	if err != nil {
		return nil, err
	}
	if err := w.Truncate(int64(s)); err != nil {
		return nil, err
	}
	digest := md5.New()
	m := mvis{
		Name: n,
		Size: s,
		file: w,
		digest: digest,
		writer: io.MultiWriter(w, digest),
	}
	return &m, nil
}

func (m *mvis) WriteMetadata() error {
	file := filepath.Join(m.Name+".xml")

	c := struct {
		XMLName xml.Name  `xml:"mvis"`
		When    time.Time `xml:"time"`
		Program string    `xml:"program,attr"`
		Version string    `xml:"version,attr"`
		Build   string    `xml:"build,attr"`
		File    string    `xml:"filename"`
		Sum     string    `xml:"md5"`
		Size    int       `xml:"size"`
		Blocks  int       `xml:"blocks"`
		Bytes   int       `xml:"bytes"`
	}{
		Program: Program,
		Version: Version,
		Build:   BuildTime,
		When:    time.Now(),
		File:    m.Name,
		Size:    m.Size,
		Sum:     fmt.Sprintf("%x", m.digest.Sum(nil)),
		Blocks:  m.Blocks,
		Bytes:   m.Bytes,
	}
	w, err := os.Create(file)
	if err != nil {
		return err
	}

	e := xml.NewEncoder(w)
	e.Indent("", "  ")
	if err := e.Encode(&c); err != nil {
		w.Close()
		os.Remove(file)
		return err
	}
	return w.Close()
}

func (m *mvis) Close() error {
	return m.file.Close()
}

func (m *mvis) Write(bs []byte) (int, error) {
	s := binary.BigEndian.Uint16(bs)
	if s >= counterLimit {
		return 0, fmt.Errorf("invalid sequence counter (%d)", s)
	}
	if s == m.last {
		return 0, nil
	}
	if diff := (s - m.last) & counterMask; s != diff && diff > 1 {
		offset := int(diff-1) * (LineSize - 2)
		if err := m.file.Truncate(int64(offset)); err != nil {
			return 0, nil
		}
		if _, err := m.file.Seek(int64(offset), io.SeekCurrent); err != nil {
			return 0, err
		}
	}
	m.last = s
	// n := copy(m.Payload[m.offset:], bs[2:])
	// n := copy(m.Payload[m.offset:], bytes.TrimRight(bs[2:], "\x00"))
	if n, err := m.writer.Write(bytes.TrimRight(bs[2:], "\x00")); err == nil {
		m.Blocks++
		m.Bytes += n
		return len(bs), err
	} else {
		return 0, err
	}
	// m.offset += n
	// m.offset += len(bs)-2
}

type fileReader struct {
	ps   []string
	file *os.File
}

func NewReader(ps []string, keep bool) (*fileReader, error) {
	sort.Strings(ps)
	var xs []string
	for i := 0; i < len(ps); i++ {
		p := ps[i]
		if !keep && strings.HasSuffix(p, ".bad") {
			continue
		}
		for j := i + 1; j < len(ps); j++ {
			f := ps[j]
			ix := strings.LastIndex(f, "_")
			if ix < 0 {
				return nil, fmt.Errorf("invalid filename")
			}
			if !strings.HasPrefix(p, f[:ix]) {
				xs, i = append(xs, ps[j-1]), j-1
				break
			}
		}
	}
	if len(xs) == 0 {
		return nil, fmt.Errorf("no valid files provided")
	}
	f, err := openFile(xs[0])
	if err != nil {
		return nil, err
	}
	if len(xs) > 1 {
		xs = xs[1:]
	} else {
		xs = xs[:0]
	}

	return &fileReader{file: f, ps: xs}, nil
}

func (f *fileReader) Filename() string {
	return f.file.Name()
}

func (f *fileReader) Read(bs []byte) (int, error) {
	if len(f.ps) == 0 && f.file == nil {
		return 0, io.EOF
	}

	n, err := f.file.Read(bs)
	if p := binary.BigEndian.Uint16(bs); err == nil && p == MilFlag {
		return 0, nil
	}
	if err == io.EOF {
		if len(f.ps) > 0 {
			f.file.Close()
			f.file, err = openFile(f.ps[0])
			if err != nil {
				return 0, err
			}
			if len(f.ps) == 1 {
				f.ps = f.ps[:0]
			} else {
				f.ps = f.ps[1:]
			}
			return 0, nil
		} else {
			f.file = nil
		}
	}
	return n, err
}

func openFile(f string) (*os.File, error) {
	r, err := os.Open(f)
	if err != nil {
		return nil, err
	}
	magic := make([]byte, 4)
	if _, err := r.Read(magic); err != nil {
		return nil, err
	}
	if !bytes.Equal(magic, FCC) {
		return nil, fmt.Errorf("expected magic %s (found: %s)", FCC, magic)
	}
	if _, err := r.Seek(12, io.SeekCurrent); err != nil {
		return nil, err
	}
	return r, err
}
