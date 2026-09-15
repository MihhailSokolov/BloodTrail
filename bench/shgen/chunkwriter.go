// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// chunkWriter streams one object type into `{"data":[...],"meta":{...}}`
// files of at most chunk objects each, named
// `<prefix>_<type>_<NN>.json` -- data first and meta last, matching how
// SharpHound itself writes (its collector streams objects and only knows
// the count at the end, and BloodHound's ingest reads the file whole
// either way). Objects are encoded one at a time; nothing is accumulated.
type chunkWriter struct {
	prefix, typ, meta string
	chunk             int

	// methods/version fill the meta trailer. SharpHound files carry the
	// fixture's proven collection bitmask and format version; AzureHound
	// files carry their own (see newAzureChunkWriter).
	methods, version int

	file    *os.File
	buf     *bufio.Writer
	enc     *json.Encoder
	inFile  int // objects written to the current file
	fileIdx int
	onClose func(path string) // called with each finished file's path
}

func newChunkWriter(prefix, typ, meta string, chunk int) (*chunkWriter, error) {
	if chunk < 1 {
		chunk = 1
	}
	return &chunkWriter{prefix: prefix, typ: typ, meta: meta, chunk: chunk, methods: metaMethods, version: metaVersion}, nil
}

// newAzureChunkWriter is the AzureHound-format variant: meta.type "azure",
// AzureHound's own format version, no collection-methods bitmask (its files
// carry methods 0; BloodHound's azure decode path never reads it).
func newAzureChunkWriter(prefix string, chunk int) (*chunkWriter, error) {
	if chunk < 1 {
		chunk = 1
	}
	return &chunkWriter{prefix: prefix, typ: "azure", meta: "azure", chunk: chunk, methods: 0, version: azureMetaVersion}, nil
}

func (w *chunkWriter) path() string {
	return fmt.Sprintf("%s_%s_%02d.json", w.prefix, w.typ, w.fileIdx)
}

func (w *chunkWriter) write(obj any) error {
	if w.file == nil {
		f, err := os.Create(w.path())
		if err != nil {
			return err
		}
		w.file = f
		w.buf = bufio.NewWriterSize(f, 1<<20)
		if _, err := w.buf.WriteString(`{"data":[`); err != nil {
			return err
		}
		w.enc = json.NewEncoder(w.buf)
		w.inFile = 0
	}
	if w.inFile > 0 {
		if err := w.buf.WriteByte(','); err != nil {
			return err
		}
	}
	// json.Encoder appends a newline per Encode; harmless inside the array
	// and keeps the files diffable-ish without an extra marshal-to-buffer.
	if err := w.enc.Encode(obj); err != nil {
		return err
	}
	w.inFile++
	if w.inFile >= w.chunk {
		return w.finishFile()
	}
	return nil
}

func (w *chunkWriter) finishFile() error {
	trailer := Meta{Methods: w.methods, Type: w.meta, Count: w.inFile, Version: w.version}
	tb, err := json.Marshal(trailer)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w.buf, `],"meta":%s}`, tb); err != nil {
		return err
	}
	if err := w.buf.Flush(); err != nil {
		return err
	}
	path := w.file.Name()
	if err := w.file.Close(); err != nil {
		return err
	}
	if w.onClose != nil {
		w.onClose(path)
	}
	w.file, w.buf, w.enc = nil, nil, nil
	w.fileIdx++
	return nil
}

// close finishes the in-progress file, or -- when no object of this type was
// ever written -- emits one empty-but-valid file so every collection type is
// present in the output set.
func (w *chunkWriter) close() error {
	if w.file == nil {
		f, err := os.Create(w.path())
		if err != nil {
			return err
		}
		w.file = f
		w.buf = bufio.NewWriterSize(f, 4096)
		if _, err := w.buf.WriteString(`{"data":[`); err != nil {
			return err
		}
		w.inFile = 0
	}
	return w.finishFile()
}
