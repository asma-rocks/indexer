package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
)

// SapDocument is essential unit of indexed data
type SapDocument struct {
	Author string
	Name   string
	Date   string
	Stereo byte
}

func (sap *SapDocument) String() string {
	var b strings.Builder
	b.WriteString("[Author:")
	b.WriteString(sap.Author)
	b.WriteString(",Name:")
	b.WriteString(sap.Name)
	b.WriteString(",Date:")
	b.WriteString(sap.Date)
	b.WriteString(",Stereo:")
	b.WriteString(fmt.Sprint(sap.Stereo))
	b.WriteRune(']')
	return b.String()
}

// UseSapMapping describes and creates mapping
func UseSapMapping() mapping.IndexMapping {
	sapMapping := bleve.NewDocumentMapping()

	authorMapping := bleve.NewTextFieldMapping()
	nameMapping := bleve.NewTextFieldMapping()
	// dateMapping := bleve.NewDateTimeFieldMapping()
	dateMapping := bleve.NewNumericFieldMapping()
	stereoMapping := bleve.NewNumericFieldMapping()

	sapMapping.AddFieldMappingsAt("Author", authorMapping)
	sapMapping.AddFieldMappingsAt("Name", nameMapping)
	sapMapping.AddFieldMappingsAt("Date", dateMapping)
	sapMapping.AddFieldMappingsAt("Stereo", stereoMapping)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.AddDocumentMapping("Sap", sapMapping)
	return indexMapping
}

// ExtractSapInfo reads SAP information from the header
func ExtractSapInfo(file string) ([]byte, error) {
	buf, err := ioutil.ReadFile(file)
	bufs := bytes.Split(buf, []byte{0xff, 0xff})
	header := bufs[0]
	return header, err
}

func stripQuotes(s string) string {
	return strings.ReplaceAll(s, "\"", "")
}

func takeValue(piece []byte) string {
	pair := bytes.Split(piece, []byte{0x20, 0x22})
	return stripQuotes(string(pair[1]))
}

var yearRegexp = regexp.MustCompile("[1-9]{1}[0-9]{2}[0-9?]{1}")

func takeYearValue(piece []byte) string {
	pair := bytes.Split(piece, []byte{0x20, 0x22})
	return string(yearRegexp.Find(pair[1]))
}

// Stereo string reference in SAP file
var Stereo = []byte("STEREO")

// ExtractStructure exports structure
func ExtractStructure(info []byte) *SapDocument {
	pieces := bytes.Split(info, []byte{0xD, 0xA})
	// 0 SAP
	// 1 AUTHOR_"author"
	// 2 NAME_"name"
	// 3 DATE_"date"
	stereo := byte(0)
	if bytes.Contains(info, Stereo) {
		stereo = byte(1)
	}
	return &SapDocument{
		Author: takeValue(pieces[1]),
		Name:   takeValue(pieces[2]),
		Date:   takeYearValue(pieces[3]),
		Stereo: stereo,
	}
}

// NewIndexer returns new indexer instance
func NewIndexer(name string) (bleve.Index, error) {
	index, err := bleve.Open(name)
	if err != nil {
		mapping := UseSapMapping()
		index, err = bleve.New(name, mapping)
		if err != nil {
			log.Fatalln("Unable to create index file")
		}
	}
	return index, err
}

func main() {
	indexNamePtr := flag.String("i", "asma.bleve", "The name of the index directory")
	asmaDirPtr := flag.String("a", "", "Existing asma directory taken from the official repository")
	flag.Parse()

	index, err := NewIndexer(*indexNamePtr)
	if err != nil {
		log.Fatalln("Indexer init failed")
	}

	if *asmaDirPtr == "" {
		log.Fatalln("ASMA directory not specified")
	}

	batch := index.NewBatch()
	defer index.Batch(batch)

	filepath.Walk(*asmaDirPtr, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			index.Batch(batch)
			if batch.Size() > 50 {
				batch = index.NewBatch()
			}
			return nil
		}
		if filepath.Ext(path) == ".sap" {
			sapName := strings.TrimPrefix(path, *asmaDirPtr)
			sapInfo, sapErr := ExtractSapInfo(path)
			if sapErr != nil {
				return nil
			}
			sapDoc := ExtractStructure(sapInfo)
			batch.Index(sapName, sapDoc)
			fmt.Println(sapDoc)
		}
		return nil
	})
}
