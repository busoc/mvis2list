package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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

func dumpFiles(r *fileReader, datadir string, zero, meta bool) error {
	var filler byte
	if !zero {
		filler = byte(0x20)
	}
	buffer, err := prepare(r, filler)
	if err != nil {
		return err
	}
	if datadir == "-" {
		for _, m := range buffer {
			if _, err := io.Copy(os.Stdout, bytes.NewReader(m.Payload)); err != nil {
				return err
			}
		}
		return nil
	}
	for _, m := range buffer {
		if err := m.WriteFile(datadir); err != nil {
			return err
		}
		if meta {
			if err := m.WriteMetadata(datadir); err != nil {
				return err
			}
		}
	}
	return nil
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

type mvis struct {
	Name    string
	Size    int
	Blocks  int
	Bytes   int
	Payload []byte

	last   uint16
	offset int
}

func (m *mvis) WriteFile(dir string) error {
	if len(m.Payload) == 0 {
		return nil
	}
	file := filepath.Join(dir, m.Name)
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil && !os.IsExist(err) {
		return nil
	}
	return ioutil.WriteFile(file, m.Payload, 0644)
}

func (m *mvis) WriteMetadata(dir string) error {
	if len(m.Payload) == 0 {
		return nil
	}
	file := filepath.Join(dir, m.Name+".xml")
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil && !os.IsExist(err) {
		return nil
	}
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
		Sum:     fmt.Sprintf("%x", md5.Sum(m.Payload)),
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

const blockRow = "%9d blocks (%d), offset: %9d (size: %9d, sequence: %5d), %9d written"

func (m *mvis) writeBlock(bs []byte) error {
	s := binary.BigEndian.Uint16(bs)
	if s >= counterLimit {
		return fmt.Errorf("invalid sequence counter (%d)", s)
	}
	if s == m.last {
		return nil
	}
	if diff := (s - m.last) & counterMask; s != diff && diff > 1 {
		m.offset += int(diff-1) * (LineSize - 2)
	}
	m.last = s
	n := copy(m.Payload[m.offset:], bs[2:])

	m.Blocks++
	m.Bytes += n
	m.offset += n

	return nil
}

func prepare(r io.Reader, filler byte) (map[string]*mvis, error) {
	buffer := make(map[string]*mvis)
	var name string
	for {
		body := make([]byte, LineSize)
		if _, err := io.ReadFull(r, body); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		sequence := binary.BigEndian.Uint16(body)
		if sequence == FileFlag {
			size := binary.BigEndian.Uint32(body[2:])
			name = string(bytes.Trim(body[6:], "\x00"))

			log.Printf("==> %s (%d, %d)", name, size, size/(LineSize-2))
			if _, ok := buffer[name]; !ok {
				m := mvis{
					Name:    name,
					Size:    int(size),
					Payload: make([]byte, int(size)),
				}
				if filler != null {
					for i := 0; i < int(size); i++ {
						m.Payload[i] = filler
					}
				}
				buffer[name] = &m
			}
		} else {
			m, ok := buffer[name]
			if !ok {
				continue
			}
			if err := m.writeBlock(body); err != nil {
				return nil, err
			}
		}
	}
	return buffer, nil
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
