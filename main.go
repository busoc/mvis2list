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
	"path/filepath"
	"sort"
	"strings"
)

var FCC = []byte("MMA ")

func main() {
	file := flag.String("f", "", "file")
	erase := flag.Bool("x", false, "erase")
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
	if *erase && *file != "" {
		err := os.Remove(*file)
		if err != nil && !os.IsNotExist(err) {
			log.Fatalln(err)
		}
	}
	dir := filepath.Dir(*file)
	if err := os.MkdirAll(dir, 0755); dir != "" && err != nil && !os.IsExist(err) {
		log.Fatalln(err)
	}
	var w io.Writer
	switch f, err := os.OpenFile(*file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); {
	case *file == "":
		w = os.Stdout
	case *file != "" && err == nil:
		defer f.Close()
		w = f
	default:
		log.Fatalln(err)
	}
	sort.Strings(ps)
	for _, p := range ps {
		if strings.HasSuffix(p, ".bad") {
			continue
		}
		if err := process(w, p); err != nil {
			log.Printf("%s: %s", p, err)
		}
	}
}

func cleanPaths(ps []string) []string {
	sort.Strings(ps)
	for _, p := range ps {
		if strings.HasSuffix(p, ".bad") {
			continue
		}
	}
	return ps
}

type fileReader struct {
	ps []string
	file *os.File
}

func NewReader(ps []string) (io.Reader, error) {
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

func (f *fileReader) Read(bs []byte) (int, error) {
	if len(f.ps) == 0 && f.file == nil {
		return 0, io.EOF
	}
	n, err := f.file.Read(bs)
	if err == io.EOF {
		if len(f.ps) > 0 {
			f.file.Close()
			f.file, err = openFile(f.ps[0])
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

const (
	MilFlag  = 0xFFFE
	FileFlag = 0xFFFF
	LineSize = 64
)

func process(ws io.Writer, p string) error {
	r, err := os.Open(p)
	if err != nil {
		return err
	}
	defer r.Close()
	magic := make([]byte, 4)
	if _, err := r.Read(magic); err != nil {
		return err
	}
	if !bytes.Equal(magic, FCC) {
		return fmt.Errorf("expected magic %s (found: %s)", FCC, magic)
	}
	if _, err := r.Seek(12, io.SeekCurrent); err != nil {
		return err
	}
	log.Println()
	var w bytes.Buffer
	for {
		bs := make([]byte, LineSize)
		if _, err := io.ReadFull(r, bs); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		sequence := binary.BigEndian.Uint16(bs)
		switch sequence {
		case MilFlag:
			continue
		case FileFlag:
			size := binary.BigEndian.Uint32(bs[2:])
			name := bytes.Trim(bs[6:], "\x00")
			log.Printf("%d: %s", size, name)
		default:
		}
		log.Printf("%4d: %x", binary.BigEndian.Uint16(bs), bs)

		bs = bytes.Trim(bs[2:], "\x00")
		for i := 0; i < len(bs); i += 2 {
			w.WriteByte(bs[i])
			if j := i + 1; j < len(bs) {
				w.WriteByte(bs[j])
			}
		}
	}
	if w.Len() > 0 {
		io.WriteString(ws, w.String())
	}
	return nil
}
