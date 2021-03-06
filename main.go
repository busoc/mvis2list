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
  -list         print the list of blocks
  -batch        batch
  -text         stripped null bytes from blocks before writing
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

# run with a list of UPI in a flat file
$ mvis2list -datadir /tmp -meta -zero -batch /storage/archives/ ~/upi-285.txt
`

func init() {
	log.SetFlags(0)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, helpText)
		os.Exit(2)
	}
}

func main() {
	datadir := flag.String("datadir", "-", "")
	version := flag.Bool("version", false, "")
	keep := flag.Bool("keep", false, "")
	meta := flag.Bool("meta", false, "")
	list := flag.Bool("list", false, "")
	text := flag.Bool("text", false, "")
	batch := flag.Bool("batch", false, "")
	report := flag.Bool("report", false, "")
	flag.Parse()
	if *version {
		fmt.Fprintf(os.Stderr, "%s-%s (%s)\n", Program, Version, BuildTime)
		os.Exit(2)
	}
	var (
		r   *fileReader
		err error
	)
	if *batch {
		r, err = NewBatch(flag.Arg(0), flag.Arg(1), *keep)
	} else {
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
		r, err = NewReader(ps, *keep)
	}

	if err != nil {
		log.Fatalln(err)
	}
	if *list || *report {
		if err := listBlocks(r, *list && !*report); err != nil {
			log.Fatalln(err)
		}
		return
	}
	if err := dumpFiles(r, *datadir, *meta, *text); err != nil {
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
	var name string
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
			name = string(bytes.Trim(body[6:], "\x00"))
			if list {
				fmt.Printf("%s (%d bytes)\n", name, size)
			}
			count--
			continue
		}
		if diff := (s - prev) & counterMask; diff != s && diff > 1 {
			log.Printf("missing blocks (%s): %d (%d - %d)", name, diff-1, prev, s)
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

func dumpFiles(r *fileReader, datadir string, meta, text bool) error {
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

			kind := "binary"
			if text {
				kind = "text"
			}
			log.Printf("==> %s (%s file, %d bytes, %d blocks)", name, kind, size, size/(LineSize-2))
			if curr, err = New(filepath.Join(datadir, name), int(size), text); err != nil {
				return err
			}
			continue
		}
		if curr == nil {
			continue
		}
		if _, err := curr.Write(body); err != nil {
			log.Printf("error when writing %s: %s", curr.Name, err)
			curr.Close()
			curr = nil
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
	text    bool

	prev   uint16
	last   uint16
	offset int
}

func New(n string, s int, txt bool) (*mvis, error) {
	if err := os.MkdirAll(filepath.Dir(n), 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	w, err := os.Create(n)
	if err != nil {
		return nil, err
	}
	// if err := w.Truncate(int64(s)); err != nil {
	// 	return nil, err
	// }
	digest := md5.New()
	m := mvis{
		Name: n,
		Size: s,
		file: w,
		digest: digest,
		writer: io.MultiWriter(w, digest),
		text: txt,
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
	// if err := m.file.Truncate(int64(m.Bytes)); err != nil {
	// 	return err
	// }
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
		// if diff := (s - m.prev) & counterMask; s != diff && diff == 1 {
		// 	m.offset -= LineSize-2
		// 	m.Blocks--
		// 	m.Bytes -= LineSize-2
		// }
		// m.offset += int(diff-1) * (LineSize - 2)
	}
	m.last, m.prev = s, m.last
	// n := copy(m.Payload[m.offset:], bs[2:])
	if m.text {
		bs = bytes.TrimRight(bs[2:], "\x00")
	} else {
		bs = bs[2:]
	}
	if _, err := m.writer.Write(bs); err == nil {
		m.Blocks++
		m.Bytes += len(bs)-2
		return len(bs), err
	} else {
		return 0, err
	}
}

type fileReader struct {
	ps   []string
	file *os.File
}

func NewBatch(base, file string, keep bool) (*fileReader, error) {
	var fs []string
	switch r, err := os.Open(file); {
	case err == nil:
		defer r.Close()

		var set []string
		s := bufio.NewScanner(r)
		for s.Scan() {
			r := s.Text()
			if strings.HasPrefix(r, "#") || len(r) == 0 {
				continue
			}
			set = append(set, r)
		}
		if err := s.Err(); err != nil {
			return nil, err
		}
		if len(set) == 0 {
			return nil, fmt.Errorf("no upi provided")
		}
		fs = walkFiles(base, set)
	case err != nil && file == "":
	default:
		return nil, err
	}
	return NewReader(fs, keep)
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
				return nil, fmt.Errorf("invalid filename: %s", f)
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

func walkFiles(base string, set []string) []string {
	var fs []string

	queue := listFiles(base, set)
	var p string
	for {
		if p == "" {
			if f, ok := <-queue; ok {
				p = f
			} else {
				return fs
			}
		}
		for {
			f, ok := <-queue
			if !ok {
				return fs
			}
			if ix := strings.LastIndex(f, "_"); ix >= 0 {
				if strings.HasPrefix(p, f[:ix]) {
					p = f
					continue
				}
				fs = append(fs, p)
				p = f
				break
			} else {
				p = ""
			}
		}
	}
	return fs
}

func listFiles(base string, set []string) <-chan string {
	q := make(chan string)
	go func() {
		defer close(q)

		sort.Strings(set)

		var prefix string
		filepath.Walk(base, func(p string, i os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if i.IsDir() {
				return nil
			}
			if filepath.Ext(p) == ".bad" {
				return nil
			}
      if len(set) == 0 || (len(prefix) > 0 && strings.Contains(p, prefix)) {
        q <- p
        return nil
      }
			base := filepath.Base(p)
			for _, s := range set {
				if strings.HasPrefix(base[5:], s) {
					q <- p
					prefix = s
					break
				} else {
					prefix = ""
				}
			}
			return nil
		})
	}()
	return q
}
