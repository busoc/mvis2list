package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	// "path/filepath"
	"sort"
	"strings"
)

var FCC = []byte("MMA ")

const (
	MilFlag  = 0xFFFE
	FileFlag = 0xFFFF
	LineSize = 64
)

type mvis struct {
	File    string
	Count   int
	Missing int
}

func main() {
	flag.Parse()
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
	rs, err := NewReader(ps)
	if err != nil {
		log.Fatalln(err)
	}
	r := bufio.NewReader(rs)
	for {
		body := make([]byte, LineSize)
		for {
			if _, err := io.ReadFull(r, body); err != nil {
				log.Fatalln(err)
			}
			sequence := binary.BigEndian.Uint16(body)
			if sequence == MilFlag {
				continue
			} else if sequence == FileFlag {
				size := binary.BigEndian.Uint32(body[2:])
				name := bytes.Trim(body[6:], "\x00")
				log.Printf("%d: %s (%s)", size, name, rs.Filename())
				break
			} else {
				log.Fatalf("invalid sequence %d", sequence)
			}
		}
		for {
			if bs, err := r.Peek(2); err == nil {
				s := binary.BigEndian.Uint16(bs)
				if s == MilFlag || s == FileFlag {
					break
				}
			}
			if _, err := io.ReadFull(r, body); err != nil {
				log.Fatalln(err)
			}
			sequence := binary.BigEndian.Uint16(body)
			log.Printf("%4d (%04[1]x): %x", sequence, body[2:])
		}
	}
}

type fileReader struct {
	ps   []string
	file *os.File
}

func NewReader(ps []string) (*fileReader, error) {
	sort.Strings(ps)
	var xs []string
	for _, p := range ps {
		if strings.HasSuffix(p, ".bad") {
			continue
		}
		xs = append(xs, p)
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
	if err == io.EOF {
		if len(f.ps) > 0 {
			f.file.Close()
			f.file, err = openFile(f.ps[0])
			log.Println("openFile", f.ps[0])
			if len(f.ps) == 1 {
				f.ps = f.ps[:0]
			} else {
				f.ps = f.ps[1:]
			}
			return f.Read(bs)
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

// func process(ws io.Writer, p string) error {
// 	r, err := os.Open(p)
// 	if err != nil {
// 		return err
// 	}
// 	defer r.Close()
// 	magic := make([]byte, 4)
// 	if _, err := r.Read(magic); err != nil {
// 		return err
// 	}
// 	if !bytes.Equal(magic, FCC) {
// 		return fmt.Errorf("expected magic %s (found: %s)", FCC, magic)
// 	}
// 	if _, err := r.Seek(12, io.SeekCurrent); err != nil {
// 		return err
// 	}
// 	log.Println()
// 	var w bytes.Buffer
// 	for {
// 		bs := make([]byte, LineSize)
// 		if _, err := io.ReadFull(r, bs); err != nil {
// 			if err == io.EOF {
// 				break
// 			}
// 			return err
// 		}
// 		sequence := binary.BigEndian.Uint16(bs)
// 		switch sequence {
// 		case MilFlag:
// 			continue
// 		case FileFlag:
// 			size := binary.BigEndian.Uint32(bs[2:])
// 			name := bytes.Trim(bs[6:], "\x00")
// 			log.Printf("%d: %s", size, name)
// 		default:
// 		}
// 		log.Printf("%4d: %x", binary.BigEndian.Uint16(bs), bs)
//
// 		bs = bytes.Trim(bs[2:], "\x00")
// 		for i := 0; i < len(bs); i += 2 {
// 			w.WriteByte(bs[i])
// 			if j := i + 1; j < len(bs) {
// 				w.WriteByte(bs[j])
// 			}
// 		}
// 	}
// 	if w.Len() > 0 {
// 		io.WriteString(ws, w.String())
// 	}
// 	return nil
// }
