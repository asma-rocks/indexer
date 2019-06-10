package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
)

// SapDocument is essential unit of indexed data
type SapDocument struct {
	Author string
	Name   string
	Date   string
}

func (sap *SapDocument) String() string {
	var b strings.Builder
	b.WriteRune('{')
	b.WriteString("Author:")
	b.WriteString(sap.Author)
	b.WriteRune(',')
	b.WriteString("Name:")
	b.WriteString(sap.Name)
	b.WriteRune(',')
	b.WriteString("Date:")
	b.WriteString(sap.Date)
	b.WriteRune('}')
	return b.String()
}

// UseSapMapping describes and creates mapping
func UseSapMapping() mapping.IndexMapping {
	sapMapping := bleve.NewDocumentMapping()

	authorMapping := bleve.NewTextFieldMapping()
	nameMapping := bleve.NewTextFieldMapping()
	dateMapping := bleve.NewDateTimeFieldMapping()

	sapMapping.AddFieldMappingsAt("Author", authorMapping)
	sapMapping.AddFieldMappingsAt("Name", nameMapping)
	sapMapping.AddFieldMappingsAt("Date", dateMapping)

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

// ExtractStructure exports structure
func ExtractStructure(info []byte) *SapDocument {
	pieces := bytes.Split(info, []byte{0xD, 0xA})
	// 0 SAP
	// 1 AUTHOR_"author"
	// 2 NAME_"name"
	// 3 DATE_"date"
	return &SapDocument{
		Author: takeValue(pieces[1]),
		Name:   takeValue(pieces[2]),
		Date:   takeValue(pieces[3]),
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

	// asmaDir := "/Users/alesnajmann/Work/asma.rocks/asma-svn/asma"

	if *asmaDirPtr == "" {
		log.Fatalln("ASMA directory not specified")
	}

	filepath.Walk(*asmaDirPtr, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".sap" {
			sapName := strings.TrimPrefix(path, *asmaDirPtr)
			sapInfo, sapErr := ExtractSapInfo(path)
			if sapErr != nil {
				return nil
			}
			sapDoc := ExtractStructure(sapInfo)
			fmt.Println(sapDoc)
			index.Index(sapName, sapDoc)
		}
		return nil
	})
}
